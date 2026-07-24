//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/hms_catalog_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include <atomic>
#include <chrono>
#include <list>

namespace duckdb {
struct DropInfo;
class HMSSchemaEntry;
class HMSTransaction;

// Default upper bound on resident table entries per schema before least-recently
// -used eviction kicks in. Tunable via the `hms_table_cache_size` setting;
// 0 means unbounded (grow-only).
constexpr idx_t HMS_DEFAULT_TABLE_CACHE_SIZE = 10000;

// Per-catalog coordinator for bounded table-entry caching. Shared by every
// HMSCatalogSet of one attached catalog and by its transaction manager.
//
// Why it exists: DuckDB hands catalog lookups out as raw, non-owning
// TableCatalogEntry pointers that a query holds through binding and execution.
// So an entry evicted from a set's map must NOT be freed while any query might
// still hold that pointer. A table-entry borrow never outlives the transaction
// it was made in (multi-file scans always re-bind, so nothing caches an entry
// pointer across statements), which gives a safe reclamation boundary: an
// evicted entry is moved (not freed) into `retired`, and `retired` is freed only
// when no transaction is in flight against this catalog.
class HMSEntryCache {
public:
	idx_t GetCapacity() const {
		return capacity.load();
	}
	void SetCapacity(idx_t new_capacity) {
		capacity.store(new_capacity);
	}

	void TransactionStarted() {
		lock_guard<mutex> l(lock);
		active_transactions++;
	}
	// Called on commit/rollback. When the last in-flight transaction ends, no
	// query can still hold a pointer to a retired (already-unmapped) entry, so it
	// is finally safe to free them.
	void TransactionEnded() {
		lock_guard<mutex> l(lock);
		if (active_transactions > 0 && --active_transactions == 0) {
			retired.clear();
		}
	}

	// Hand off ownership of an evicted entry. Kept alive until the reclamation
	// boundary above.
	void Retire(shared_ptr<CatalogEntry> entry) {
		lock_guard<mutex> l(lock);
		retired.push_back(std::move(entry));
	}
	idx_t RetiredCount() {
		lock_guard<mutex> l(lock);
		return retired.size();
	}

private:
	std::atomic<idx_t> capacity {0};
	mutex lock;
	int64_t active_transactions {0};
	vector<shared_ptr<CatalogEntry>> retired;
};

class HMSCatalogSet {
public:
	explicit HMSCatalogSet(Catalog &catalog);
	virtual ~HMSCatalogSet() = default;

	virtual optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const string &name);
	virtual void DropEntry(ClientContext &context, DropInfo &info);
	virtual void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
	virtual optional_ptr<CatalogEntry> CreateEntry(unique_ptr<CatalogEntry> entry);
	void ClearEntries();

protected:
	virtual void LoadEntries(ClientContext &context) = 0;

	void EraseEntryInternal(const string &name);

	// Look up an already-cached entry without triggering a load. Returns nullptr
	// if the entry is not resident. Marks the entry most-recently-used. Used by
	// lazy subclasses that populate the cache one table at a time instead of
	// loading the whole schema.
	optional_ptr<CatalogEntry> GetCachedEntry(const string &name);

private:
	// Populate the entry cache from HMS if it has never loaded or the TTL has
	// expired. Errors from the metastore (connection refused, SASL/Kerberos
	// failure, MetaException, ...) propagate to the caller so a broken catalog
	// surfaces as a clear error rather than silently appearing empty.
	void EnsureLoaded(ClientContext &context);

	// LRU recency bookkeeping. All callers hold entry_lock.
	void LRUInsertFront(const string &name);
	void LRUTouch(const string &name);
	void LRUErase(const string &name);
	// Evict least-recently-used entries into the catalog's retirement bin until
	// the resident count is within capacity. No-op unless apply_capacity is set
	// and the coordinator capacity is non-zero. Caller holds entry_lock.
	void EvictToCapacity();

protected:
	Catalog &catalog;
	// Whether this set participates in bounded LRU eviction. Table sets set this;
	// the schema set does not (few schemas, and schema entries are long-lived).
	bool apply_capacity = false;

private:
	mutex entry_lock;
	case_insensitive_map_t<shared_ptr<CatalogEntry>> entries;
	// Most-recently-used at the front, least-recently-used at the back.
	std::list<string> lru_list;
	case_insensitive_map_t<std::list<string>::iterator> lru_pos;
	bool is_loaded;
	std::chrono::steady_clock::time_point last_load_time;
	// Cache TTL in seconds - after this time, cache is considered stale
	static constexpr int CACHE_TTL_SECONDS = 5;
};

class HMSInSchemaSet : public HMSCatalogSet {
public:
	explicit HMSInSchemaSet(HMSSchemaEntry &schema);
	~HMSInSchemaSet() override = default;

	optional_ptr<CatalogEntry> CreateEntry(unique_ptr<CatalogEntry> entry) override;

protected:
	HMSSchemaEntry &schema;
};

} // namespace duckdb
