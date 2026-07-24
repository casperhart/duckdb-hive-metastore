#include "storage/hms_catalog_set.hpp"
#include "storage/hms_transaction.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "storage/hms_schema_entry.hpp"

namespace duckdb {

HMSCatalogSet::HMSCatalogSet(Catalog &catalog) : catalog(catalog), is_loaded(false), last_load_time() {
}

void HMSCatalogSet::EnsureLoaded(ClientContext &context) {
	// Check if we need to load entries (either first time or cache expired)
	bool need_to_load = false;
	{
		lock_guard<mutex> l(entry_lock);
		if (!is_loaded) {
			need_to_load = true;
		} else {
			// Check if cache is stale (older than TTL)
			auto now = std::chrono::steady_clock::now();
			auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_load_time).count();
			if (elapsed >= CACHE_TTL_SECONDS) {
				need_to_load = true;
			}
		}
	}

	if (!need_to_load) {
		return;
	}

	// LoadEntries reaches out to the metastore and may throw (connection,
	// SASL/Kerberos, MetaException). We deliberately let that propagate: a
	// failed connection must surface as an error, not a silently empty catalog.
	// is_loaded stays false on failure, so the next access retries once the
	// underlying problem is fixed. GetSchemas/GetTablesInSchema connect before
	// creating any entry, so a throw leaves the cache untouched (retry-safe).
	LoadEntries(context);
	lock_guard<mutex> l(entry_lock);
	is_loaded = true;
	last_load_time = std::chrono::steady_clock::now();
}

optional_ptr<CatalogEntry> HMSCatalogSet::GetEntry(ClientContext &context, const string &name) {
	EnsureLoaded(context);
	return GetCachedEntry(name);
}

optional_ptr<CatalogEntry> HMSCatalogSet::GetCachedEntry(const string &name) {
	lock_guard<mutex> l(entry_lock);
	auto entry = entries.find(name);
	if (entry == entries.end()) {
		return nullptr;
	}
	return entry->second.get();
}

void HMSCatalogSet::DropEntry(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("HMSCatalogSet::DropEntry");
}

void HMSCatalogSet::EraseEntryInternal(const string &name) {
	lock_guard<mutex> l(entry_lock);
	entries.erase(name);
}

void HMSCatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	EnsureLoaded(context);

	// Now scan the entries
	lock_guard<mutex> l(entry_lock);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> HMSCatalogSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
	if (!entry) {
		throw InternalException("HMSCatalogSet::CreateEntry called with null entry");
	}
	lock_guard<mutex> l(entry_lock);
	if (entry->name.empty()) {
		throw InternalException("HMSCatalogSet::CreateEntry called with empty name");
	}
	// Copy the key before moving `entry` — argument evaluation order within a
	// single make_pair(...) call is unspecified, so we must not read entry->name
	// in the same expression that moves entry.
	string entry_name = entry->name;
	// insert() does not overwrite an existing key: on a conflict the freshly-built
	// `entry` is dropped here. Return the pointer that is actually stored (the
	// pre-existing entry on conflict, the new one otherwise) — never a pointer
	// captured before the move, which would dangle after the drop. Keeping the
	// existing entry (rather than replacing it) also avoids freeing an entry that
	// an in-flight query may still hold a raw pointer to.
	auto inserted = entries.insert(make_pair(std::move(entry_name), std::move(entry)));
	return inserted.first->second.get();
}

void HMSCatalogSet::ClearEntries() {
	lock_guard<mutex> l(entry_lock);
	entries.clear();
	is_loaded = false;
}

HMSInSchemaSet::HMSInSchemaSet(HMSSchemaEntry &schema) : HMSCatalogSet(schema.ParentCatalog()), schema(schema) {
}

optional_ptr<CatalogEntry> HMSInSchemaSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
	if (!entry) {
		throw InternalException("HMSInSchemaSet::CreateEntry called with null entry");
	}
	if (!entry->internal) {
		entry->internal = schema.internal;
	}
	return HMSCatalogSet::CreateEntry(std::move(entry));
}

} // namespace duckdb
