#include "storage/hms_catalog.hpp"
#include "storage/hms_schema_entry.hpp"
#include "storage/hms_table_entry.hpp"
#include "storage/hms_multi_file_reader.hpp"
#include "hms_api.hpp"
#include "hms_constants.hpp"
#include "hms_format_detector.hpp"
#include "hms_path_utils.hpp"
#include "storage/hms_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/common/string_util.hpp"
#include "hms_utils.hpp"

namespace duckdb {

// Helper function to ensure the required extension is loaded for Delta/Iceberg tables
static void AutoLoadExtensionIfNeeded(ClientContext &context, const hms::FormatDetectionResult &format) {
	if (!hms::FormatDetector::RequiresExtension(format.format)) {
		return;
	}

	const char *extension_name = hms::FormatDetector::GetExtensionName(format.format);
	if (extension_name) {
		ExtensionHelper::TryAutoLoadExtension(context, extension_name);
		// Extension loading failed, but we don't want to prevent the table from being discovered
		// The actual data access will fail later with a more informative error
	}
}

HMSTableEntry::HMSTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
	this->internal = false;
	// Copy tags from CreateTableInfo to CatalogEntry
	// This ensures inter-extension communication (e.g., with OpenLineage) works
	for (auto &tag : info.tags) {
		this->tags[tag.first] = tag.second;
	}
}

HMSTableEntry::HMSTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, HMSTableInfo &info)
    : TableCatalogEntry(catalog, schema, *info.create_info) {
	this->internal = false;
	// Copy tags from CreateTableInfo to CatalogEntry
	// This ensures inter-extension communication (e.g., with OpenLineage) works
	for (auto &tag : info.create_info->tags) {
		this->tags[tag.first] = tag.second;
	}
}

unique_ptr<BaseStatistics> HMSTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

optional_ptr<Catalog> HMSTableEntry::GetInternalCatalog() {
	if (!internal_attached_database) {
		return nullptr;
	}
	return internal_attached_database->GetCatalog();
}

void HMSTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &, LogicalProjection &, LogicalUpdate &,
                                          ClientContext &) {
	throw NotImplementedException("BindUpdateConstraints");
}

bool HMSTableEntry::DiscoverDynamicSchema(ClientContext &context, Catalog &catalog, SchemaCatalogEntry &schema,
                                          HMSAPITable &table_data, vector<ColumnDefinition> &columns) {
	// Detect format
	auto format_result = hms::FormatDetector::Detect(table_data);

	if (!format_result.IsDelta() && !format_result.IsIceberg() && !format_result.IsParquet()) {
		return false; // Not a dynamic schema table
	}

	// Autoload the required extension
	AutoLoadExtensionIfNeeded(context, format_result);

	// Normalize the scan path
	auto path_result = hms::PathUtils::NormalizeScanPath(table_data.storage_location, table_data, format_result);

	// Get the scan function for this table type
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &system_schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);

	TableFunction scan_function;
	const char *scan_func_name = hms::FormatDetector::GetScanFunctionName(format_result.format);
	if (!scan_func_name) {
		return false;
	}

	auto catalog_entry = system_schema.GetEntry(data, CatalogType::TABLE_FUNCTION_ENTRY, scan_func_name);
	if (!catalog_entry) {
		return false; // Extension not loaded
	}
	auto &function_set = catalog_entry->Cast<TableFunctionCatalogEntry>();
	scan_function = function_set.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});

	// Build glob pattern for directory-based scans (similar to GetScanFunction)
	string scan_path = path_result.scan_path;
	scan_path = hms::PathUtils::BuildGlobPattern(scan_path, format_result, format_result.is_partitioned);

	// Bind the function to discover the schema
	try {
		vector<Value> inputs = {Value(scan_path)};
		named_parameter_map_t param_map;
		vector<LogicalType> return_types;
		vector<string> names;
		TableFunctionRef empty_ref;

		TableFunctionBindInput bind_input(inputs, param_map, return_types, names, nullptr, nullptr, scan_function,
		                                  empty_ref);

		auto bind_result = scan_function.bind(context, bind_input, return_types, names);

		// Convert the discovered types and names to column definitions
		if (return_types.size() != names.size() || return_types.empty()) {
			return false;
		}

		// Build a map of partition column types from HMS schema
		// For Parquet tables, partition columns must use HMS types (not discovered from files)
		case_insensitive_map_t<LogicalType> partition_key_types;
		if (format_result.IsParquet() && !table_data.partition_keys.empty()) {
			for (const auto &pk : table_data.partition_keys) {
				partition_key_types[pk.name] = HMSUtils::TypeToLogicalType(context, pk.type);
			}
		}

		columns.clear();
		for (idx_t i = 0; i < names.size(); i++) {
			// For partition columns in Parquet tables, use HMS type instead of discovered type
			auto partition_type_it = partition_key_types.find(names[i]);
			if (partition_type_it != partition_key_types.end()) {
				columns.push_back(ColumnDefinition(names[i], partition_type_it->second));
			} else {
				columns.push_back(ColumnDefinition(names[i], return_types[i]));
			}
		}

		return true;
	} catch (const std::exception &ex) {
		// Binding failed - this is expected when the extension is not loaded
		// or the table path is invalid. Return false to fall back to HMS schema.
		// Note: We catch by const reference to avoid slicing and to potentially
		// log the error message in debug builds.
		return false;
	}
}

TableFunction HMSTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto &db = DatabaseInstance::GetDatabase(context);

	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &system_schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);

	if (!table_data) {
		throw InternalException("HMSTableEntry::GetScanFunction called with null table_data for table '%s'", name);
	}

	// Detect table format
	auto format_result = hms::FormatDetector::Detect(*table_data);

	// Autoload the required extension
	AutoLoadExtensionIfNeeded(context, format_result);

	// Get the scan function name
	const char *scan_func_name = hms::FormatDetector::GetScanFunctionName(format_result.format);
	if (!scan_func_name) {
		throw NotImplementedException("Table '%s' has unsupported format: %s / %s", table_data->name,
		                              table_data->input_format, table_data->serialization_lib);
	}

	// Get the scan function
	auto catalog_entry = system_schema.GetEntry(data, CatalogType::TABLE_FUNCTION_ENTRY, scan_func_name);
	if (!catalog_entry) {
		string extension_name = scan_func_name;
		// Remove "_scan" suffix for error message
		auto scan_pos = extension_name.find("_scan");
		if (scan_pos != string::npos) {
			extension_name = extension_name.substr(0, scan_pos);
		}
		throw InvalidInputException("Function '%s' not found. The '%s' extension failed to autoload.", scan_func_name,
		                            extension_name.c_str());
	}
	auto &function_set = catalog_entry->Cast<TableFunctionCatalogEntry>();
	TableFunction scan_function = function_set.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});

	// Normalize the scan path
	auto path_result = hms::PathUtils::NormalizeScanPath(table_data->storage_location, *table_data, format_result);
	string scan_path = path_result.scan_path;

	// Configure S3 if needed (for MinIO compatibility)
	if (path_result.needs_s3_config) {
		Value endpoint_val = Value(path_result.s3_endpoint);
		Value use_ssl_val = Value(false);
		Value url_style_val = Value("path");
		context.db->config.SetOption("s3_endpoint", endpoint_val);
		context.db->config.SetOption("s3_use_ssl", use_ssl_val);
		context.db->config.SetOption("s3_url_style", url_style_val);
	}

	bool partitioned_parquet = format_result.IsParquet() && !table_data->partition_keys.empty();

	vector<Value> inputs;
	named_parameter_map_t param_map;

	if (partitioned_parquet) {
		// HMS is the source of truth for partitions. Enumerate them and scan each
		// partition's own recorded location (which may live outside the table
		// root, e.g. a relocated partition), then let HMSMultiFileReader supply
		// the partition-column values from HMS instead of parsing key=value path
		// segments. This also fixes the alphabetical partition-column ordering
		// DuckDB's built-in hive_partitioning would otherwise impose.
		auto &hms_catalog = catalog.Cast<HMSCatalog>();
		auto partitions = hms_catalog.GetConnection().Execute(
		    [&](HMSClient &client) { return HMSAPI::GetPartitions(client, schema.name, name); });

		auto scan_info = make_shared_ptr<HMSPartitionScanInfo>();
		for (auto &pk : table_data->partition_keys) {
			scan_info->partition_key_names.push_back(pk.name);
			scan_info->partition_key_types.push_back(HMSUtils::TypeToLogicalType(context, pk.type));
		}
		scan_info->partitions = std::move(partitions);

		vector<Value> partition_globs;
		partition_globs.reserve(scan_info->partitions.size());
		for (auto &partition : scan_info->partitions) {
			auto part_path = hms::PathUtils::NormalizeScanPath(partition.location, *table_data, format_result);
			string part_dir = part_path.scan_path;
			// Keep a trailing slash: it marks the location as a directory (so a
			// dotted partition value like amount=10.99 is not mistaken for a file),
			// and it makes the reader's prefix match exact — region=North/ won't
			// spuriously match a sibling region=Northland/.
			if (!StringUtil::EndsWith(part_dir, "/")) {
				part_dir += "/";
			}
			// Store the normalized location back so HMSMultiFileReader can match
			// scanned file paths (also normalized, e.g. s3a:// -> s3://) to their
			// partition by prefix.
			partition.location = part_dir;
			// Glob the partition's own directory non-recursively (is_partitioned =
			// false): a partition location is a leaf that holds its data files
			// directly, so we must not descend into subdirectories — that would
			// read staging dirs (e.g. Spark's _temporary) or, if partition
			// locations nest, another partition's files. Format-aware via
			// BuildGlobPattern, though this branch is Parquet-only.
			partition_globs.push_back(Value(hms::PathUtils::BuildGlobPattern(part_dir, format_result, false)));
		}
		inputs.push_back(Value::LIST(LogicalType::VARCHAR, std::move(partition_globs)));

		scan_function.get_multi_file_reader = HMSMultiFileReader::CreateInstance;
		scan_function.function_info = make_shared_ptr<HMSMultiFileReaderFunctionInfo>(std::move(scan_info));
	} else {
		// Build glob pattern for directory-based scans
		scan_path = hms::PathUtils::BuildGlobPattern(scan_path, format_result, format_result.is_partitioned);
		inputs.push_back(Value(scan_path));

		// For Iceberg tables, add allow_moved_paths for better path handling
		if (format_result.IsIceberg()) {
			param_map["allow_moved_paths"] = Value::BOOLEAN(true);
		}
	}

	// For CSV/Text tables, we must provide the schema to avoid type mismatch crashes
	// and to ensure correct parsing (Hive tables usually have no header).
	if (!format_result.IsDelta() && !format_result.IsIceberg() && format_result.IsCSV()) {
		child_list_t<Value> struct_children;

		// Try to parse Spark schema first for CSV tables, as HMS type definitions for CSV
		// (via USING CSV) might be incorrect (e.g. all array<string> or similar).
		vector<HMSAPIColumnDefinition> columns;
		bool has_spark_schema = HMSUtils::ParseSparkSchema(table_data->parameters, columns);

		if (has_spark_schema) {
			// Successfully parsed Spark schema, use it.
			// The types in 'columns' are already DuckDB LogicalType strings from ParseSparkSchema
			for (const auto &col : columns) {
				struct_children.push_back(make_pair(col.name, Value(col.type)));
			}
		} else {
			// Fallback to standard HMS columns
			for (const auto &col : table_data->columns) {
				// Convert HMS type to DuckDB LogicalType string
				auto duckdb_type = HMSUtils::TypeToLogicalType(context, col.type);
				struct_children.push_back(make_pair(col.name, Value(duckdb_type.ToString())));
			}
		}
		param_map["columns"] = Value::STRUCT(std::move(struct_children));

		// Handle delimiter and other CSV options
		// Default Hive delimiter is \001 (Ctrl-A)
		string delim = string(1, hms::constants::DEFAULT_HIVE_DELIMITER);
		auto it = table_data->serde_parameters.find(hms::serde_param::FIELD_DELIM);
		if (it != table_data->serde_parameters.end()) {
			delim = it->second;
		}

		bool is_default_hive_delim = (delim == string(1, hms::constants::DEFAULT_HIVE_DELIMITER));

		bool is_spark_csv = false;
		auto csv_provider_it = table_data->parameters.find(hms::spark_param::PROVIDER);
		if (csv_provider_it != table_data->parameters.end() &&
		    StringUtil::CIEquals(csv_provider_it->second, hms::format::CSV)) {
			is_spark_csv = true;
		}

		// Logic to determine CSV parsing mode:
		// 1. If it's a Spark CSV table (provider=csv), we enable auto_detect.
		//    If the delimiter is default (\x01), we ignore it to let the sniffer find the real one (likely comma).
		// 2. If it has a Spark schema AND a non-default delimiter (e.g. comma), it's likely a compatible CSV table.
		//    We enable auto_detect.
		// 3. Otherwise (Standard Hive table, usually LazySimpleSerDe), we disable auto_detect and enforce strict
		// parsing.

		if (is_spark_csv || (has_spark_schema && !is_default_hive_delim)) {
			// Spark CSV or compatible (e.g. comma separated)
			// Enable auto_detect to allow sniffing of quotes, headers, etc.
			param_map["auto_detect"] = Value::BOOLEAN(true);

			if (it != table_data->serde_parameters.end()) {
				// Set sep if explicitly defined, UNLESS it's the default hive delimiter for a Spark CSV
				// (because Spark CSVs often leave SerDe delim as default \x01 while actual file is comma)
				if (!is_spark_csv || !is_default_hive_delim) {
					param_map["sep"] = Value(delim);
				}
			}
		} else {
			// Strict Hive behavior (LazySimpleSerDe) or Default Hive
			param_map["header"] = Value::BOOLEAN(false); // Hive tables usually have no header
			param_map["sep"] = Value(delim);
			param_map["quote"] = Value("");  // Disable quoting
			param_map["escape"] = Value(""); // Disable escaping

			// Explicitly disable auto detection for strict Hive tables
			param_map["auto_detect"] = Value::BOOLEAN(false);
		}

		// If strict mode is failing, we might want to relax it, but for now let's try with correct delimiters
		param_map["null_padding"] = Value::BOOLEAN(true);  // Hive treats missing columns as null
		param_map["ignore_errors"] = Value::BOOLEAN(true); // Best effort
	}

	vector<LogicalType> return_types;
	vector<string> names;
	TableFunctionRef empty_ref;

	TableFunctionBindInput bind_input(inputs, param_map, return_types, names, nullptr, nullptr, scan_function,
	                                  empty_ref);

	auto result = scan_function.bind(context, bind_input, return_types, names);
	bind_data = std::move(result);

	// Create a wrapper function with a custom name to preserve HMS table identity
	// This allows the OpenLineage optimizer to identify this as an HMS table
	// The metadata is now read from table entry tags instead of being encoded here
	TableFunction hms_function = scan_function;
	string fully_qualified_name = catalog.GetName() + "." + schema.name + "." + name;
	// URL encode to safely handle special characters like '##'
	string encoded_name = StringUtil::URLEncode(fully_qualified_name);
	hms_function.name = "hms_scan##" + encoded_name;
	hms_function.verify_serialization = false; // Custom dynamic function cannot be serialized

	return hms_function;
}

virtual_column_map_t HMSTableEntry::GetVirtualColumns() const {
	//! FIXME: requires changes in core to be able to delegate this
	return TableCatalogEntry::GetVirtualColumns();
}

vector<column_t> HMSTableEntry::GetRowIdColumns() const {
	//! FIXME: requires changes in core to be able to delegate this
	return TableCatalogEntry::GetRowIdColumns();
}

TableStorageInfo HMSTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;
	// TODO fill info
	return result;
}

HMSLineageInfo HMSTableEntry::GetLineageInfo() const {
	HMSLineageInfo info;

	// Set basic table information
	info.catalog_name = catalog.GetName();
	info.schema_name = schema.name;
	info.table_name = name;
	info.fully_qualified_name = info.catalog_name + "." + info.schema_name + "." + info.table_name;

	// Extract HMS-specific metadata if available
	if (table_data) {
		info.storage_location = table_data->storage_location;
		info.table_type = table_data->table_type;
	}

	return info;
}

} // namespace duckdb
