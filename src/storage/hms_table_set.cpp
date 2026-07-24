#include "hms_api.hpp"
#include "hms_utils.hpp"
#include "hms_format_detector.hpp"

#include "storage/hms_catalog.hpp"
#include "storage/hms_table_set.hpp"
#include "storage/hms_transaction.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "storage/hms_schema_entry.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

HMSTableSet::HMSTableSet(HMSSchemaEntry &schema) : HMSInSchemaSet(schema) {
	// Tables participate in bounded LRU eviction (schemas do not).
	apply_capacity = true;
}

static ColumnDefinition CreateColumnDefinition(ClientContext &context, HMSAPIColumnDefinition &coldef) {
	// HMS types (e.g. "int", "string") are compatible with HMSUtils::TypeToLogicalType parser
	return {coldef.name, HMSUtils::TypeToLogicalType(context, coldef.type)};
}

// Resolve a table's DuckDB column list from HMS metadata, appending them to
// `columns`. For non-partitioned Parquet/ORC/text tables the metastore is the
// source of truth — matching Spark, which reads the catalog schema rather than
// opening the data files — so the schema comes from metadata alone and this
// does no remote I/O. Doing per-table file reads here is what made
// listing/DESCRIBE pay remote-storage latency for every table in the schema.
//
// Delta/Iceberg still require remote discovery because their real schema lives
// in the table's own metadata (the transaction log / manifest), not HMS.
// Partitioned Parquet uses HMS metadata like any other Parquet table: the
// partition-aware scan (HMSMultiFileReader) emits partition columns in HMS's
// declared order, so the catalog schema built here — data columns followed by
// partition keys in declared order — matches the scan output positionally.
static void ResolveTableColumns(ClientContext &context, Catalog &catalog, SchemaCatalogEntry &schema,
                                HMSAPITable &table_data, ColumnList &columns) {
	auto format = hms::FormatDetector::Detect(table_data);
	if (format.IsDelta() || format.IsIceberg()) {
		vector<ColumnDefinition> discovered_columns;
		if (HMSTableEntry::DiscoverDynamicSchema(context, catalog, schema, table_data, discovered_columns)) {
			for (auto &col : discovered_columns) {
				columns.AddColumn(std::move(col));
			}
			return;
		}
		// Discovery failed (e.g. path unreadable): fall back to HMS metadata below.
	}

	// Prefer the Spark schema in table properties: it carries full type fidelity
	// and already includes partition columns. Cheap parse of metadata we already
	// fetched.
	vector<HMSAPIColumnDefinition> spark_columns;
	if (HMSUtils::ParseSparkSchema(table_data.parameters, spark_columns)) {
		for (auto &col : spark_columns) {
			// col.type is already a DuckDB LogicalType string (e.g. "INTEGER", "STRUCT(...)")
			auto logical_type = TransformStringToLogicalType(col.type, context);
			columns.AddColumn(ColumnDefinition(col.name, logical_type));
		}
		return;
	}

	// Fall back to the Hive columns. sd.cols excludes partition keys, so append
	// them explicitly — Hive/Spark expose partition columns as part of the table
	// schema, listed after the data columns.
	for (auto &col : table_data.columns) {
		auto logical_type = HMSUtils::TypeToLogicalType(context, col.type);
		columns.AddColumn(ColumnDefinition(col.name, logical_type));
	}
	for (auto &pk : table_data.partition_keys) {
		auto logical_type = HMSUtils::TypeToLogicalType(context, pk.type);
		columns.AddColumn(ColumnDefinition(pk.name, logical_type));
	}
}

// Build a catalog entry from one HMS table's metadata. Shared by the whole-schema
// load and the streaming Scan so a lazily-built entry is identical to a bulk one.
static unique_ptr<HMSTableEntry> BuildTableEntry(ClientContext &context, Catalog &catalog, HMSSchemaEntry &schema,
                                                 HMSAPITable &table) {
	CreateTableInfo info;
	info.table = table.name;

	ResolveTableColumns(context, catalog, schema, table, info.columns);

	// Add HMS metadata as tags for inter-extension communication (e.g., with OpenLineage)
	// This allows other extensions to access HMS metadata without code dependencies
	info.tags["hms_storage_location"] = table.storage_location;
	info.tags["hms_table_type"] = table.table_type;
	info.tags["hms_input_format"] = table.input_format;
	info.tags["hms_output_format"] = table.output_format;
	info.tags["hms_serialization_lib"] = table.serialization_lib;

	auto table_entry = make_uniq<HMSTableEntry>(catalog, schema, info);
	table_entry->table_data = make_uniq<HMSAPITable>(table);
	return table_entry;
}

void HMSTableSet::LoadEntries(ClientContext &context) {
	auto &hms_catalog = catalog.Cast<HMSCatalog>();

	// TODO: handle out-of-order columns using position property

	auto tables = hms_catalog.GetConnection().Execute(
	    [&](HMSClient &client) { return HMSAPI::GetTablesInSchema(client, schema.name); });

	for (auto &table : tables) {
		// Validate that the table's database matches our schema name
		// This should always hold if the HMS API is working correctly
		if (schema.name != table.db_name) {
			// Log but continue - this is a data inconsistency issue, not a crash-worthy error
			// The HMS API returned a table for the wrong database
			continue;
		}
		CreateEntry(BuildTableEntry(context, catalog, schema, table));
	}
}

void HMSTableSet::RefreshCapacity(ClientContext &context) {
	idx_t cap = HMS_DEFAULT_TABLE_CACHE_SIZE;
	Value val;
	if (context.TryGetCurrentSetting("hms_table_cache_size", val)) {
		cap = val.GetValue<idx_t>();
	}
	catalog.Cast<HMSCatalog>().GetEntryCache().SetCapacity(cap);
}

void HMSTableSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	RefreshCapacity(context);
	auto &hms_catalog = catalog.Cast<HMSCatalog>();

	// Authoritative, always-fresh table list. Cheap (names only). We do not rely on
	// the resident map being complete because LRU eviction may have removed entries.
	auto names = hms_catalog.GetConnection().Execute(
	    [&](HMSClient &client) { return client.GetAllTables(schema.name); });

	// Process in chunks so peak memory (fetched table objects + freshly-built
	// entries) is bounded even for a very large schema; each chunk's misses are
	// fetched in a single batch RPC rather than one get_table per table.
	constexpr idx_t CHUNK = 1000;
	for (idx_t start = 0; start < names.size(); start += CHUNK) {
		idx_t end = start + CHUNK < names.size() ? start + CHUNK : names.size();

		// Phase 1: deliver entries already resident, and collect the misses. These
		// are delivered before any building below, so a later build's eviction can
		// never drop one of them undelivered.
		vector<string> missing;
		for (idx_t i = start; i < end; i++) {
			if (auto cached = GetCachedEntry(names[i])) {
				callback(*cached);
			} else {
				missing.push_back(names[i]);
			}
		}
		if (missing.empty()) {
			continue;
		}

		// Phase 2: one batch fetch for this chunk's misses, then build + deliver each
		// immediately. Building inserts into the cache and may evict — but only
		// already-delivered entries (phase-1 hits or earlier phase-2 builds), never
		// one still pending, so the listing stays complete.
		auto fetched = hms_catalog.GetConnection().Execute(
		    [&](HMSClient &client) { return HMSAPI::GetTableObjects(client, schema.name, missing); });
		case_insensitive_map_t<HMSAPITable> by_name;
		for (auto &t : fetched) {
			if (schema.name != t.db_name) {
				continue;
			}
			by_name[t.name] = std::move(t);
		}
		for (auto &name : missing) {
			auto it = by_name.find(name);
			if (it == by_name.end()) {
				continue; // dropped between get_all_tables and the batch fetch
			}
			auto entry = CreateEntry(BuildTableEntry(context, catalog, schema, it->second));
			if (entry) {
				callback(*entry);
			}
		}
	}
}

optional_ptr<CatalogEntry> HMSTableSet::GetEntry(ClientContext &context, const string &name) {
	// Lazy single-table load. A DESCRIBE/SELECT of one table must not drag in the
	// whole schema — which, for every Delta/Iceberg sibling, would also open that
	// table's remote metadata (transaction log / manifest). Serve from cache if
	// present; otherwise fetch just this one table with a single get_table.
	RefreshCapacity(context);
	if (auto cached = GetCachedEntry(name)) {
		return cached;
	}

	unique_ptr<HMSTableInfo> table_info;
	try {
		table_info = GetTableInfo(context, schema, name);
	} catch (const HMSTableNotFoundError &) {
		// Unknown table: return a null lookup so DuckDB emits its standard
		// "table does not exist" (with name suggestions) instead of an IOException.
		return nullptr;
	}
	if (!table_info) {
		return nullptr;
	}

	auto table_entry = make_uniq<HMSTableEntry>(catalog, schema, *table_info);
	// table_data is required by GetScanFunction (format detection, partition-aware
	// scans). The HMSTableInfo constructor does not copy it, so attach it here —
	// mirroring CreateTable — or a later SELECT would hit a null table_data.
	if (table_info->table_data) {
		table_entry->table_data = make_uniq<HMSAPITable>(*table_info->table_data);
	}
	return CreateEntry(std::move(table_entry));
}

optional_ptr<CatalogEntry> HMSTableSet::RefreshTable(ClientContext &context, const string &table_name) {
	auto table_info = GetTableInfo(context, schema, table_name);
	if (!table_info) {
		throw IOException("Failed to fetch table info for '%s.%s': table info is null", schema.name.c_str(),
		                  table_name.c_str());
	}
	auto table_entry = make_uniq<HMSTableEntry>(catalog, schema, *table_info);
	auto table_ptr = table_entry.get();
	CreateEntry(std::move(table_entry));
	return table_ptr;
}

unique_ptr<HMSTableInfo> HMSTableSet::GetTableInfo(ClientContext &context, HMSSchemaEntry &schema,
                                                   const string &table_name) {
	auto &hms_catalog = catalog.Cast<HMSCatalog>();

	Apache::Hadoop::Hive::Table ht;
	try {
		ht = hms_catalog.GetConnection().Execute(
		    [&](HMSClient &client) { return client.GetTable(schema.name, table_name); });
	} catch (const HMSTableNotFoundError &) {
		// Preserve "not found" unchanged so callers (lazy GetEntry) can translate
		// it into a null lookup rather than a hard error.
		throw;
	} catch (const std::exception &ex) {
		throw IOException("Failed to fetch table info for '%s.%s': %s", schema.name.c_str(), table_name.c_str(),
		                  ex.what());
	}

	auto result = make_uniq<HMSTableInfo>(schema, table_name);
	HMSAPITable t;
	t.name = ht.tableName;
	t.db_name = ht.dbName;
	t.table_type = ht.tableType;
	t.storage_location = ht.sd.location;
	t.input_format = ht.sd.inputFormat;
	t.output_format = ht.sd.outputFormat;
	t.serialization_lib = ht.sd.serdeInfo.serializationLib;
	t.serde_parameters = ht.sd.serdeInfo.parameters;
	t.parameters = ht.parameters;

	for (const auto &col : ht.sd.cols) {
		HMSAPIColumnDefinition c;
		c.name = col.name;
		c.type = col.type;
		c.comment = col.comment;
		t.columns.push_back(c);
	}

	for (const auto &pk : ht.partitionKeys) {
		HMSAPIColumnDefinition c;
		c.name = pk.name;
		c.type = pk.type;
		c.comment = pk.comment;
		t.partition_keys.push_back(c);
	}

	result->create_info->table = table_name;
	result->create_info->columns = CreateTableInfo().columns; // initialize empty then fill

	result->create_info->sql = "";

	// Replace tags by constructing a fresh map from parameters
	result->create_info->tags = InsertionOrderPreservingMap<string>();
	for (auto &kv : t.parameters) {
		result->create_info->tags[kv.first] = kv.second;
	}

	result->create_info->comment = Value();

	// attach table_data BEFORE schema resolution (needed for format detection)
	result->table_data = make_uniq<HMSAPITable>(std::move(t));

	// Resolve schema using the same logic as LoadEntries.
	result->create_info->columns = CreateTableInfo().columns; // start empty
	ResolveTableColumns(context, catalog, schema, *result->table_data, result->create_info->columns);

	// Add HMS metadata as tags for inter-extension communication (e.g., with OpenLineage)
	// This allows other extensions to access HMS metadata without code dependencies
	result->create_info->tags["hms_storage_location"] = result->table_data->storage_location;
	result->create_info->tags["hms_table_type"] = result->table_data->table_type;
	result->create_info->tags["hms_input_format"] = result->table_data->input_format;
	result->create_info->tags["hms_output_format"] = result->table_data->output_format;
	result->create_info->tags["hms_serialization_lib"] = result->table_data->serialization_lib;

	// ensure create_info has columns
	result->create_info->table = table_name;

	// Return the HMSTableInfo containing create_info and table_data

	return result;
}

optional_ptr<CatalogEntry> HMSTableSet::CreateTable(ClientContext &context, BoundCreateTableInfo &info) {
	auto &base = info.Base();

	// Basic checks: reject unsupported features
	if (!info.query && base.columns.empty()) {
		throw BinderException("CREATE TABLE must specify columns or be CREATE TABLE AS");
	}

	if (base.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT ||
	    base.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		throw NotImplementedException("ON CONFLICT clauses are not supported for HMS CREATE TABLE");
	}

	// Extract format: prefer tag 'format', fallback to provider tag or empty (default PARQUET)
	string format;
	auto fmt_it = base.tags.find("format");
	if (fmt_it != base.tags.end()) {
		format = fmt_it->second;
	} else {
		auto prov_it = base.tags.find("provider");
		if (prov_it != base.tags.end()) {
			format = prov_it->second;
		}
	}

	// Require either an explicit 'location' tag or a warehouse_location configured on the HMS catalog
	auto &hms_catalog = catalog.Cast<HMSCatalog>();
	auto loc_tag_it = base.tags.find("location");
	if (loc_tag_it == base.tags.end() || loc_tag_it->second.empty()) {
		if (hms_catalog.warehouse_location.empty()) {
			throw BinderException("CREATE TABLE requires a LOCATION to be provided or the HMS catalog to be attached "
			                      "with a WAREHOUSE_LOCATION");
		}
	}

	// Build Thrift Table
	auto thrift_table = HMSUtils::BuildThriftTable(context, schema, info, format, hms_catalog.warehouse_location);

	// Call HMS API to create over the catalog's shared connection. ExecuteWrite:
	// a create must not be blindly re-sent after a mid-operation disconnect.
	hms_catalog.GetConnection().ExecuteWrite([&](HMSClient &client) {
		HMSAPI::CreateTable(client, thrift_table);
		return true;
	});

	// Fetch table info first to ensure we have complete data before creating entry
	// This avoids creating an incomplete entry if GetTableInfo fails
	auto table_info = GetTableInfo(context, schema, base.table);
	if (!table_info) {
		throw IOException("Failed to fetch table info after creating table '%s'", base.table);
	}

	// Register the new table entry in the catalog. Reuse the columns assembled by GetTableInfo
	// rather than the user-declared columns so subsequent SELECTs see exactly what a re-attach
	// would resolve from the HMS-stored schema.
	auto table_entry = make_uniq<HMSTableEntry>(catalog, schema, *table_info);
	if (table_info->table_data) {
		table_entry->table_data = make_uniq<HMSAPITable>(*table_info->table_data);
	}

	auto ptr = table_entry.get();
	CreateEntry(std::move(table_entry));
	return ptr;
}

void HMSTableSet::DropEntry(ClientContext &context, DropInfo &info) {
	auto &hms_catalog = catalog.Cast<HMSCatalog>();
	// ExecuteWrite: a drop must not be blindly re-sent after a mid-operation
	// disconnect (the first send may have been applied).
	bool dropped = hms_catalog.GetConnection().ExecuteWrite(
	    [&](HMSClient &client) { return HMSAPI::DropTable(client, schema.name, info.name); });
	if (!dropped) {
		// Table did not exist in HMS. Without IF EXISTS, surface the error. With IF EXISTS,
		// fall through so any stale local cache entry (e.g. dropped by another process after
		// LoadEntries cached it) is also pruned.
		if (info.if_not_found != OnEntryNotFound::RETURN_NULL) {
			throw CatalogException("Table '%s.%s' does not exist", schema.name, info.name);
		}
	}
	EraseEntryInternal(info.name);
}

void HMSTableSet::AlterTable(ClientContext &context, RenameTableInfo &info) {
	throw NotImplementedException("HMSTableSet::AlterTable");
}

void HMSTableSet::AlterTable(ClientContext &context, RenameColumnInfo &info) {
	throw NotImplementedException("HMSTableSet::AlterTable");
}

void HMSTableSet::AlterTable(ClientContext &context, AddColumnInfo &info) {
	throw NotImplementedException("HMSTableSet::AlterTable");
}

void HMSTableSet::AlterTable(ClientContext &context, RemoveColumnInfo &info) {
	throw NotImplementedException("HMSTableSet::AlterTable");
}

void HMSTableSet::AlterTable(ClientContext &context, AlterTableInfo &alter) {
	throw NotImplementedException("HMSTableSet::AlterTable");
}

} // namespace duckdb
