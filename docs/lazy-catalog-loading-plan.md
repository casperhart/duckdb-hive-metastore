# Plan: Lazy per-table catalog loading + bounded LRU cache

## Goal

Two problems, one design:

1. **Single-table ops must not load the whole schema.** On a 10k-table
   metastore, `DESCRIBE one_table` (or binding one table reference) should fetch
   that one table, not all 10k.
2. **Memory must be bounded for long-running processes.** A grow-only cache
   against a metastore with hundreds of thousands of tables leaks slowly: a
   long-lived process that eventually touches many tables (or runs one
   `SHOW TABLES` over a huge schema) accumulates every entry forever. The cache
   must have a ceiling.

Non-goals / preserved behavior:

- Whole-schema ops (`SHOW TABLES`, `duckdb_tables()`, `information_schema.tables`)
  keep working. These are inherently O(n) in DuckDB — `duckdb_tables()` reads
  `column_count` per table — so they still visit every table; we just make sure
  they are the *only* thing that does, and that they do it in **one batch RPC**,
  not N.
- Per-table metadata staleness within a session (external `ALTER` not seen until
  the entry is re-fetched) is unchanged from today.

## Current state (verified in code)

- `HMSCatalogSet` (base) owns `case_insensitive_map_t<unique_ptr<CatalogEntry>>
  entries` + `is_loaded` + `last_load_time` + `CACHE_TTL_SECONDS = 5`. **The map
  owns entries; `GetEntry`/`Scan` return raw `optional_ptr<CatalogEntry>` /
  `CatalogEntry&`.** The consumer holds a raw pointer with no refcount — this is
  the fact that governs eviction safety (below).
- Both `GetEntry` and `Scan` call `EnsureLoaded` → `LoadEntries` (pure virtual),
  which for tables loads the **whole schema**: `GetTablesInSchema`
  (`get_all_tables` + batch `get_table_objects_by_name_req`) + build every entry.
- `CreateEntry` uses `entries.insert(...)`, which does **not** overwrite an
  existing key. Net effect today: the 5s TTL reload only *adds* newly-created
  tables; it never refreshes or removes existing entries. So the cache is already
  effectively grow-only — the only thing that frees entries is
  `ClearEntries()` (`hms_clear_cache`, user-invoked between statements).
- Per-table building blocks already exist on `HMSTableSet`: `GetTableInfo`
  (single `GetTable`) and `RefreshTable` (build + insert one entry). Both the
  batch path (`LoadEntries`) and the single path funnel schema resolution through
  `ResolveTableColumns`, so a lazily-built entry is byte-for-byte what the batch
  path builds.
- All catalog lookups funnel through `HMSCatalogSet::GetEntry` / `Scan`, so
  overriding those two on `HMSTableSet` is sufficient.

## Why single-level (no separate name-list cache)

The cache exists to serve **repeated `GetEntry` hits within one statement** —
DuckDB resolves a table reference multiple times during binding/planning, and
each would otherwise be a `GetTable` RPC. That is a *per-table* benefit.

The **listing** (`Scan`) is called **once per statement**, so it gains nothing
from being cached for its own sake. The only thing a name-list cache buys is
skipping a cheap `get_all_tables` RPC on a *later* `SHOW TABLES` — marginal, and
paid for in listing staleness. So we drop the name-list level entirely:

- `Scan` fetches `get_all_tables` **fresh** every time (one cheap RPC → listings
  always reflect reality; new/dropped external tables appear immediately, no TTL
  wait), then batch-loads the per-table objects it needs.
- The per-table **entry** cache is the only cache. It is what `GetEntry` dedups
  against, and what a subsequent single-table query reuses after a `Scan`
  populated it.

This is simpler than the earlier two-level TTL design (no name-list state, no
name-list DDL upkeep) and strictly fresher for listings.

## Behavior per operation

- `GetEntry(name)`:
  1. Under the lock, if cached → mark most-recently-used, return it.
  2. Else `GetTable(name)` (catch `NoSuchObject` → `nullptr`), build the entry
     via the same path as `RefreshTable`, insert (evicting LRU victim if over
     cap), return. **One `GetTable` RPC on a miss; zero RPCs on a hit.**
- `Scan(callback)` (`SHOW TABLES` / `duckdb_tables()` / `information_schema`):
  1. `get_all_tables` fresh → authoritative name set for this statement.
  2. **Stream** the names in chunks: for each chunk, batch-fetch the table
     objects not already cached in **one** `get_table_objects_by_name_req`
     (never a per-table loop — that regresses listing from 1 RPC to N), build
     entries, invoke `callback` for each listed name, insert into the LRU.
  3. Because a `Scan` entry is only read *inside* its callback (DuckDB copies out
     the row and does not retain the pointer), `Scan` may evict freely as it
     streams — peak residency is bounded by the cap + one chunk, even for a
     100k-table `SHOW TABLES`.
- Our own DDL (`CreateTable` / `DropEntry`): update the entries map directly so
  the change is visible immediately (unchanged from today). `DropEntry` erases;
  `CreateTable` inserts.

## Freshness

- **Listings**: always current (fresh `get_all_tables` each `Scan`). Better than
  today (external drops previously lingered until `hms_clear_cache`).
- **Per-table metadata**: an entry, once cached, is not auto-refreshed on access
  (same as today). External `ALTER` to an existing, still-cached table is picked
  up after the entry is evicted-and-refetched, or via `hms_clear_cache`.
  Auto-refresh-on-access is deferred — it needs retire-on-replace handling, same
  family as eviction below.

## Eviction & lifetime safety (the hard part)

The map hands out raw pointers, so **freeing an entry that a live query still
holds is a use-after-free.** Grow-only avoided this by never freeing (except
`hms_clear_cache`, which is deliberate and between statements). LRU must free
routinely, so we need a reclamation scheme that never frees an in-use entry.

### What the DuckDB source establishes (v1.5.4, verified)

The reclamation design hinges on *who holds an entry pointer and for how long*.
Traced through the binder:

- **The pointer lives through execution, not just binding.**
  `bind_basetableref.cpp` calls `GetEntry` → casts to `TableCatalogEntry &` →
  `table.GetScanFunction(...)` produces `bind_data` → `LogicalGet` owns
  `bind_data`, and `LogicalGet::GetTable()` re-derives the entry from
  `function.get_bind_info(bind_data).table`. So the raw `TableCatalogEntry*` is
  embedded in the scan's bind data and stays reachable for the life of the plan —
  through optimization and execution, until the statement's plan is destroyed.
- **Prepared statements do NOT pin a stale entry across executions.**
  `bind_basetableref.cpp`: `if (bind_data && !bind_data->SupportStatementCache())
  SetAlwaysRequireRebind();`. `MultiFileBindData::SupportStatementCache()` returns
  **false**, and every HMS scan is multi-file (Parquet/CSV/ORC/Avro). So binding
  one sets `always_require_rebind = true`; `PreparedStatementData::RequireRebind`
  then returns true on every `EXECUTE`, forcing a fresh bind (fresh `GetEntry`).
  A prepared statement's entry pointer therefore lives only for a *single
  execution*, exactly like an ad-hoc query.

**Conclusion:** an entry pointer is only ever held by an **in-flight query**.
Nothing retains it across query boundaries. That makes safe reclamation exactly:
*free a retired entry once no in-flight query could still hold it* — which DuckDB
lets us observe via `ClientContextState::QueryBegin` / `QueryEnd` hooks.

> Caveat to keep honest: this rests on HMS scans always using multi-file bind data
> (`SupportStatementCache() == false`). If any code path (a future non-multifile
> reader, or a Delta/Iceberg scan whose bind data returns `true`) is added, that
> path *could* cache an entry pointer into a reusable plan across executions —
> re-check `SupportStatementCache()` for any new scan type before trusting this.

### Design (query-boundary reclamation)

1. **Own entries by `shared_ptr`.** Change the map to
   `case_insensitive_map_t<shared_ptr<CatalogEntry>>` and track LRU recency
   (`std::list<key>` + iterator map). `GetEntry` / `Scan` still return raw
   pointers (DuckDB's contract), but lifetime is now controlled by a `shared_ptr`
   we can hold past removal from the map.
2. **A per-catalog query epoch, driven by a registered `ClientContextState`.**
   Register a state (once per attached catalog / connection) whose `QueryBegin`
   records the current epoch as "active" and whose `QueryEnd` clears it. Track the
   multiset of epochs with an in-flight query (supports concurrent connections);
   `oldest_active` = min of that set (or "none").
3. **Evict = unlink from map, retire (don't free).** When an insert exceeds the
   cap, pick the LRU victim **that is not marked used by any currently-active
   query**, unlink it from the map, and move its `shared_ptr` into a retirement
   list tagged with the current epoch. If every over-cap candidate is in use by an
   active query, the cap is *softly* exceeded until those queries finish — correct
   beats tight.
4. **Reclaim only what no in-flight query can hold.** Free retired entries whose
   retire-epoch is strictly older than `oldest_active` (or free all of them when
   no query is active). Drain at `QueryEnd` and opportunistically on insert. This
   is provably safe given the "held only by in-flight queries" result above.

Asymmetry we still exploit: **`Scan` entries are read only inside their callback**
(DuckDB copies the row out and does not retain the pointer), so a huge
`SHOW TABLES` can stream-and-evict with the same machinery and never needs to
retain more than the cap + one chunk.

**Cross-connection safety is now covered**, not hand-waved: the epoch multiset
spans all connections sharing the catalog, so an entry retired while *any*
connection has an in-flight query that predates the retirement is not freed until
that query ends. (Today's `hms_clear_cache` remains the one explicitly-accepted
unsafe-under-concurrency path — it force-clears regardless — and is unchanged.)

**Residual risk — a genuinely long-running query pins the retirement list.** A
multi-hour query keeps `oldest_active` low, so nothing retired during it can be
freed, and the retirement list grows. Bound it with a hard backstop
(e.g. `retirement > 4 × cap`): past the backstop, stop *evicting* (let the live
map exceed cap) rather than free-and-risk-UAF. Memory is then bounded by
`cap + backstop + whatever one enormous query legitimately references` — finite,
and only reached under a pathological long-query workload.

## Cache size (configurable)

- Add a setting, e.g. `hms_table_cache_size` (attach option and/or global),
  default modeled on Spark's `spark.sql.filesourceTableRelationCacheSize` (1000).
  Our entries are lighter, so a higher default (e.g. 10000) is reasonable; make
  it tunable. `0` (or unset) → unbounded (grow-only, current behavior) for users
  who prefer it.
- The bound applies to **tables** only. `HMSSchemaSet` (few databases) stays
  unbounded / whole-load.

## Code changes

1. Epoch / query-boundary plumbing (shared by all `HMSTableSet`s of a catalog):
   - Register a `ClientContextState` (via `context.registered_state`) whose
     `QueryBegin`/`QueryEnd` maintain a per-catalog **active-epoch multiset** and
     a monotonic epoch counter. Home this state on the `HMSCatalog` /
     `HMSTransactionManager` so all its catalog sets share one view of in-flight
     queries. Expose `oldest_active_epoch()` and `current_epoch()`.
2. `HMSCatalogSet`:
   - Change `entries` to `shared_ptr`-owned; add LRU recency (`std::list<key>` +
     iterator map), a `cap` (0 = unbounded), a retirement list (tagged by epoch),
     and each entry's `last_used_epoch`.
   - Add `Touch(name)` (mark used by current epoch + move to MRU),
     `InsertWithEviction(...)` (evict LRU victim not in use by an active query;
     else soft-overflow), and `ReclaimRetired()` (free retired entries with
     `retire_epoch < oldest_active`, or all when idle; respect the backstop). All
     under `entry_lock`.
   - Make `GetEntry` and `Scan` `virtual`.
   - Fix `CreateEntry`: today's `insert` (no-overwrite) frees the *new* entry and
     returns a dangling pointer on an existing key. Make it replace-or-return
     explicitly, routed through the eviction-aware insert.
   - Keep the base whole-load (`EnsureLoaded`/`LoadEntries`) as the default for
     `HMSSchemaSet`; a `cap` of 0 leaves its behavior unchanged.
3. `HMSTableSet`:
   - Override `GetEntry` — `Touch` on hit; on miss, lazy single `GetTable`
     (`nullptr` on `NoSuchObject`) + `InsertWithEviction`.
   - Override `Scan` — fresh `get_all_tables`, chunked/streamed batch-fetch of
     missing entries via `get_table_objects_by_name_req`, callback per listed
     name, evict as it streams.
   - Reuse `GetTableInfo` / `RefreshTable` for the single-table build.
   - `is_loaded`/`last_load_time`/`CACHE_TTL_SECONDS` lose their meaning for the
     table set (no whole-schema "loaded" state, no name-list TTL) — remove or
     bypass for tables; ensure nothing else depends on them.
4. `HMSSchemaSet`: unchanged (few schemas; whole-load acceptable).

## Testing

- **Parity:** an entry from lazy `GetEntry` is identical (columns, types, tags,
  partition handling) to the batch-built entry.
- **Single-table locality:** `DESCRIBE one_table` issues `get_all_tables`-free
  work — just one `GetTable` — not a whole-schema `get_table_objects`. Assert via
  an RPC counter / log hook.
- **Listing stays one batch per chunk:** `SHOW TABLES` issues `get_all_tables` +
  batched `get_table_objects_by_name_req`, never N `GetTable` calls; lists the
  same set across `SHOW TABLES` / `duckdb_tables()` / `information_schema.tables`.
- **Bound holds:** after listing a schema far larger than the cap, resident entry
  count ≤ cap (+ retirement buffer); a huge `SHOW TABLES` does not grow unbounded.
- **Freshness:** new external table appears in listings and `DESCRIBE` within one
  statement; external drop disappears; nonexistent table → error from the fresh
  name set with a `nullptr` `GetEntry` (one `GetTable` that 404s, or served from
  the fresh list on the `Scan` path).
- **Eviction safety (stress):** interleave many distinct-table `GetEntry`s
  (forcing eviction) with a long-running query holding an entry pointer; assert no
  crash / no use-after-free (ASan). Include a concurrent-connection variant.
- **Existing suites pass:** `show_tables_from_database`,
  `attach_reattach_restart`, case-sensitivity, and the partition-aware scan
  (lazy `GetEntry` must still drive `GetScanFunction` for partitioned tables).

## Risks / open questions

- **The "held only by in-flight queries" invariant** (established above) is what
  makes reclamation safe. It rests on every HMS scan using multi-file bind data
  (`SupportStatementCache() == false` → always-rebind). **Re-check
  `SupportStatementCache()` for any new/added scan type** (a non-multifile reader,
  or a Delta/Iceberg path whose bind data returns `true`); such a path could
  cache an entry pointer into a reused plan across executions and would need
  separate handling (pin, or exclude from the cap).
- **Scan must batch/stream**, not per-table loop, or `SHOW TABLES` regresses to N
  RPCs. Ties into the chunked-fetch follow-up already noted for partitions.
- **`CreateEntry` dangling-on-existing-key bug** — fix as part of this,
  independently correct.
- **Long-running query vs retirement growth** — bounded by the backstop
  (stop evicting rather than free-and-risk-UAF); documented under Design.
- **`ClientContextState` lifetime / registration** — ensure the epoch state is
  registered once per catalog and cleaned up on detach; confirm `QueryEnd` fires
  on error/rollback paths (the hook has `QueryEnd(context, error)` overloads, so
  it does) so `oldest_active` can't get stuck.
- **First `SHOW TABLES` is still O(n)** and still fetches all table objects (in
  batches) — unchanged from today and inherent to `duckdb_tables()`.

## Suggested implementation order

1. Fix `CreateEntry` existing-key handling (foundational, low risk, independently
   correct).
2. Add the epoch `ClientContextState` (QueryBegin/QueryEnd → active-epoch
   multiset) on the catalog; no cache behavior change yet, just observable
   `oldest_active`/`current` epochs. Unit-test the multiset under nested/concurrent
   queries.
3. Convert entry ownership to `shared_ptr` with LRU recency + cap + retirement
   list + query-boundary reclamation, cap defaulting to unbounded (no behavior
   change yet). Land + test the mechanism in isolation.
4. Make `GetEntry`/`Scan` virtual; implement lazy `GetEntry` on `HMSTableSet`
   (`Touch` on hit, single `GetTable` + `InsertWithEviction` on miss).
5. Rework `HMSTableSet::Scan` to fresh-list + chunked batch-load + stream-evict.
6. Wire the `hms_table_cache_size` setting; set a sane non-zero default.
7. Tests: RPC-shape assertions (single vs batch), bound-holds, and the ASan
   eviction-safety stress — many-distinct-table `GetEntry`s + a wide multi-table
   join + a long-running query, single- and cross-connection.
