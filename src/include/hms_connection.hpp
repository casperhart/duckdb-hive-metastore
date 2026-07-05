//===----------------------------------------------------------------------===//
//                         DuckDB
//
// hms_connection.hpp
//
// Catalog-scoped Hive Metastore connection manager. Owns one thrift client for
// the lifetime of an attached catalog (rather than per transaction), resolving
// the endpoint list + auth once from the ATTACH path / hive-site.xml. It:
//   - fails over across every hive.metastore.uris entry (random start, then
//     round-robin), matching Hive's client HA behaviour;
//   - serializes access with a mutex (the thrift client is not thread-safe);
//   - transparently reconnects and retries once when the connection drops,
//     re-authenticating from the ambient Kerberos ticket on the new socket.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/mutex.hpp"

#include "hms_client.hpp"

#include <utility>

namespace duckdb {

class HMSConnection {
public:
	// `attach_path` is the ATTACH target: empty (discover from hive-site.xml), a
	// single "thrift://host:port", or a comma-separated list of them.
	explicit HMSConnection(string attach_path);

	// Run `fn` against a live client. On a transport failure the connection is
	// re-established (failing over across URIs) and `fn` retried exactly once;
	// logical errors (NoSuchObject, MetaException, ...) propagate unchanged.
	// `fn` must return a value (wrap void operations to return e.g. true).
	template <class FN>
	auto Execute(FN &&fn) -> decltype(fn(std::declval<HMSClient &>())) {
		lock_guard<mutex> lock(conn_lock);
		try {
			return fn(GetOrConnect());
		} catch (HMSTransportError &first) {
			// Connection dropped mid-use: rebuild it and try once more.
			Reset();
			try {
				return fn(GetOrConnect());
			} catch (HMSTransportError &second) {
				throw IOException("Hive Metastore connection failed after reconnect: %s", second.what());
			}
		}
	}

private:
	// Ensure `client` is open, connecting with URI failover if needed.
	HMSClient &GetOrConnect();
	// Drop the current client so the next call reconnects.
	void Reset();
	// Populate `endpoints` + `auth` from the ATTACH path / ambient config (once).
	void ResolveTargets();

	mutex conn_lock;
	string attach_path;
	bool resolved = false;
	vector<string> endpoints;
	HMSClientAuth auth;
	unique_ptr<HMSClient> client;
	// Index of the endpoint to try first; advanced on each (re)connect so load
	// and failovers spread across the metastore URIs.
	idx_t next_endpoint = 0;
};

} // namespace duckdb
