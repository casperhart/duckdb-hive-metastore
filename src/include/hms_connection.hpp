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

// Whether to authenticate with Kerberos, from the ATTACH `kerberos` option.
// AUTO (the default, and the behaviour when the option is absent) follows the
// ambient hive-site.xml / core-site.xml discovery.
enum class HMSKerberosMode : uint8_t { AUTO, ON, OFF };

class HMSConnection {
public:
	// `attach_path` is the ATTACH target: empty (discover from hive-site.xml), a
	// single "thrift://host:port", or a comma-separated list of them.
	explicit HMSConnection(string attach_path, HMSKerberosMode kerberos_mode = HMSKerberosMode::AUTO);

	// Run an idempotent (read) `fn` against a live client. On a transport
	// failure the connection is re-established (failing over across URIs) and
	// `fn` retried exactly once; logical errors (NoSuchObject, MetaException,
	// ...) propagate unchanged. `fn` must return a value (wrap void operations
	// to return e.g. true).
	template <class FN>
	auto Execute(FN &&fn) -> decltype(fn(std::declval<HMSClient &>())) {
		lock_guard<mutex> lock(conn_lock);
		HMSThriftLogSuppressor silence_thrift;
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

	// Run a non-idempotent (write) `fn` — CREATE/DROP — against a live client.
	// Never retried: if the connection drops after the request was sent, the
	// metastore may or may not have applied it, and blindly re-sending would
	// turn a succeeded CREATE TABLE into "already exists" (or a succeeded DROP
	// into "does not exist"). Instead the uncertainty is surfaced to the user.
	// Connecting itself (with URI failover) happens before `fn` runs and is safe.
	template <class FN>
	auto ExecuteWrite(FN &&fn) -> decltype(fn(std::declval<HMSClient &>())) {
		lock_guard<mutex> lock(conn_lock);
		HMSThriftLogSuppressor silence_thrift;
		try {
			return fn(GetOrConnect());
		} catch (HMSTransportError &e) {
			throw IOException("The Hive Metastore connection was lost while executing a catalog-modifying operation; "
			                  "it may or may not have been applied on the server. Verify the catalog state before "
			                  "retrying. (%s)",
			                  e.what());
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
	HMSKerberosMode kerberos_mode;
	bool resolved = false;
	vector<string> endpoints;
	HMSClientAuth auth;
	unique_ptr<HMSClient> client;
	// Index of the endpoint to try first; advanced on each (re)connect so load
	// and failovers spread across the metastore URIs.
	idx_t next_endpoint = 0;
};

} // namespace duckdb
