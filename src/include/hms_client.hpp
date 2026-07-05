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

// How to authenticate the metastore Thrift connection. Defaults to the historic
// behaviour: plaintext, no SASL. Kerberos is opt-in and only enabled when the
// ambient hive-site.xml declares hive.metastore.sasl.enabled=true.
struct HMSClientAuth {
	bool kerberos = false;
	// Kerberos service primary (the part before '/' in the metastore principal).
	string service = "hive";
	// Server FQDN used to build the "service/fqdn@REALM" SPN. Empty => use host.
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
