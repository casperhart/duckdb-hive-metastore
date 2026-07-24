#pragma once

#include <string>
#include <vector>
#include <memory>
#include "duckdb/common/common.hpp"

// Thrift headers
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

// Generated Thrift headers
#include "ThriftHiveMetastore.h"

#include <stdexcept>

namespace duckdb {

// Raised by HMSClient when the underlying Thrift transport fails (dead socket,
// EOF, connect reset) — as distinct from a logical metastore error such as
// NoSuchObject or MetaException. HMSConnection catches this to transparently
// reconnect (failing over across URIs) and retry the operation once.
class HMSTransportError : public std::runtime_error {
public:
	explicit HMSTransportError(const string &msg) : std::runtime_error(msg) {
	}
};

// Raised by HMSClient::GetTable when the metastore reports the table does not
// exist (NoSuchObject). Distinct from HMSTransportError (a transport failure)
// and from a generic IOException (a real metastore/logical error) so the catalog
// layer can translate "not found" into a null lookup — letting DuckDB emit its
// standard "table does not exist" (with name suggestions) — while genuine
// failures still propagate as errors.
class HMSTableNotFoundError : public std::runtime_error {
public:
	explicit HMSTableNotFoundError(const string &msg) : std::runtime_error(msg) {
	}
};

// Scoped, thread-local suppression of Thrift's GlobalOutput logging. Thrift
// logs transport errors (e.g. connection-refused during URI failover) to stderr
// even though our exceptions already carry that detail. GlobalOutput is
// process-global, so instead of silencing it for every Thrift user in the
// process, the extension installs a pass-through handler that only drops
// messages while one of these guards is alive on the current thread — i.e.
// during our own metastore traffic.
class HMSThriftLogSuppressor {
public:
	HMSThriftLogSuppressor();
	~HMSThriftLogSuppressor();
	// True when a suppressor is active on this thread.
	static bool Active();
};

// How to authenticate the metastore Thrift connection. Defaults to the historic
// behaviour: plaintext, no SASL. Kerberos is opt-in and only enabled when the
// ambient config requires SASL or the ATTACH option forces it.
struct HMSClientAuth {
	bool kerberos = false;
	// Kerberos service primary (the part before '/' in the metastore principal).
	string service = "hive";
	// Concrete SPN instance from the metastore principal. Empty => the
	// principal had a "_HOST" (or no) instance: use each endpoint's own host.
	// Either way the SPN is built literally (lowercased, no DNS
	// canonicalization), matching Hive's Java clients.
	string fqdn;
};

class HMSClient {
public:
	explicit HMSClient(const string &host, int port, const HMSClientAuth &auth = HMSClientAuth());
	~HMSClient();

	void Open();
	void Close();
	bool IsConnected() const;

	vector<string> GetAllDatabases();
	vector<string> GetAllTables(const string &db_name);

	Apache::Hadoop::Hive::Database GetDatabase(const string &db_name);
	Apache::Hadoop::Hive::Table GetTable(const string &db_name, const string &table_name);
	vector<Apache::Hadoop::Hive::Table> GetTableObjects(const string &db_name, const vector<string> &table_names);
	// List all partitions of a table. Each Partition carries its own values
	// (positional to the table's partition keys, in declared order) and its own
	// storage descriptor (location, format) — the metastore, not the on-disk
	// path layout, is the source of truth for where partition data lives.
	vector<Apache::Hadoop::Hive::Partition> GetPartitions(const string &db_name, const string &table_name);
	// Create a table in the metastore using a Thrift Table object
	void CreateTable(const Apache::Hadoop::Hive::Table &table);
	// Drop a table from the metastore. delete_data=false preserves the underlying
	// storage files (we treat HMS as a pure catalog and do not own the bucket layout).
	// Returns true if the table was dropped, false if it did not exist. Other Thrift
	// errors (transport, auth, MetaException) are re-thrown so callers can distinguish
	// "missing" from "broken" — needed for IF EXISTS to behave correctly.
	bool DropTable(const string &db_name, const string &table_name, bool delete_data);

private:
	string host;
	int port;
	bool connected;

	std::shared_ptr<apache::thrift::transport::TSocket> socket;
	std::shared_ptr<apache::thrift::transport::TTransport> transport;
	std::shared_ptr<apache::thrift::protocol::TProtocol> protocol;
	std::unique_ptr<Apache::Hadoop::Hive::ThriftHiveMetastoreClient> client;
};

} // namespace duckdb
