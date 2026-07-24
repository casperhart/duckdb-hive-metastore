#include "storage/hms_catalog_set.hpp"
#include "storage/hms_transaction.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "storage/hms_schema_entry.hpp"
#include "storage/hms_catalog.hpp"

namespace duckdb {

// When a long-running transaction pins the retirement bin (it can't be drained
// until that transaction ends), stop evicting once the bin reaches this multiple
// of the cap rather than growing it without bound. The live map is then allowed
// to exceed the cap: memory stays finite and correctness is preserved; the cap
// is merely soft under this rare condition.
static constexpr idx_t RETIRE_BACKSTOP_FACTOR = 4;

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
	LRUTouch(name);
	return entry->second.get();
}

void HMSCatalogSet::LRUInsertFront(const string &name) {
	lru_list.push_front(name);
	lru_pos[name] = lru_list.begin();
}

void HMSCatalogSet::LRUTouch(const string &name) {
	auto it = lru_pos.find(name);
	if (it == lru_pos.end()) {
		return;
	}
	lru_list.erase(it->second);
	lru_list.push_front(name);
	it->second = lru_list.begin();
}

void HMSCatalogSet::LRUErase(const string &name) {
	auto it = lru_pos.find(name);
	if (it == lru_pos.end()) {
		return;
	}
	lru_list.erase(it->second);
	lru_pos.erase(it);
}

void HMSCatalogSet::EvictToCapacity() {
	if (!apply_capacity) {
		return;
	}
	auto &cache = catalog.Cast<HMSCatalog>().GetEntryCache();
	idx_t cap = cache.GetCapacity();
	if (cap == 0) {
		return; // unbounded
	}
	while (entries.size() > cap && !lru_list.empty()) {
		// Backstop against a long-running transaction pinning the retirement bin.
		if (cache.RetiredCount() >= cap * RETIRE_BACKSTOP_FACTOR) {
			break;
		}
		// Evict the least-recently-used entry. Hand its shared_ptr to the catalog's
		// retirement bin instead of freeing it: a query bound earlier in the current
		// (or a concurrent) transaction may still hold a raw pointer to it. The bin
		// is freed only once no transaction is in flight (see HMSEntryCache).
		string victim = lru_list.back();
		auto e = entries.find(victim);
		if (e != entries.end()) {
			cache.Retire(std::move(e->second));
			entries.erase(e);
		}
		lru_pos.erase(victim);
		lru_list.pop_back();
	}
}

void HMSCatalogSet::DropEntry(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("HMSCatalogSet::DropEntry");
}

void HMSCatalogSet::EraseEntryInternal(const string &name) {
	lock_guard<mutex> l(entry_lock);
	entries.erase(name);
	LRUErase(name);
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
	// Copy the key before moving `entry`, and take shared ownership so the object
	// survives eviction from the map (evicted entries are parked in the catalog's
	// retirement bin until no query can hold a pointer to them).
	string entry_name = entry->name;
	shared_ptr<CatalogEntry> shared = std::move(entry);
	// insert() does not overwrite an existing key: on a conflict the freshly-built
	// entry is dropped and the pre-existing one is kept (never freed — an in-flight
	// query may hold a raw pointer to it). Return whichever pointer is actually
	// stored, never a pointer captured before a move that could dangle.
	auto inserted = entries.insert(make_pair(entry_name, shared));
	if (!inserted.second) {
		LRUTouch(entry_name);
		return inserted.first->second.get();
	}
	LRUInsertFront(entry_name);
	// Enforce the cap. The just-inserted entry is most-recently-used, so eviction
	// (from the LRU tail) never targets it.
	EvictToCapacity();
	return shared.get();
}

void HMSCatalogSet::ClearEntries() {
	lock_guard<mutex> l(entry_lock);
	entries.clear();
	lru_list.clear();
	lru_pos.clear();
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
