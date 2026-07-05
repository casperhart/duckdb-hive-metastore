#pragma once

#include "duckdb.hpp"
#include "hms_client.hpp"

namespace duckdb {

struct HMSAPISchema {
	string schema_name;
	string description;
};

struct HMSAPIColumnDefinition {
	string name;
	string type;
	string comment;
};

struct HMSAPITable {
	string name;
	string db_name;
	string table_type;
	string storage_location;
	string input_format;
	string output_format;
	string serialization_lib;
	map<string, string> serde_parameters;
	map<string, string> parameters;
	vector<HMSAPIColumnDefinition> columns;
	vector<HMSAPIColumnDefinition> partition_keys;
};

class HMSAPI {
public:
	// The operations below run against a caller-owned, already-open client,
	// typically obtained via HMSConnection::Execute (see HMSCatalog::GetConnection)
	// so one connection is reused with failover/reconnect instead of dialing the
	// metastore per call.
	static vector<HMSAPISchema> GetSchemas(HMSClient &client);
	static vector<HMSAPITable> GetTablesInSchema(HMSClient &client, const string &schema);

	// Create a table in HMS
	static void CreateTable(HMSClient &client, const Apache::Hadoop::Hive::Table &table);

	// Drop a table from HMS (metadata only; storage files are not removed).
	// Returns true if dropped, false if the table did not exist. Other failures throw.
	static bool DropTable(HMSClient &client, const string &db_name, const string &table_name);

	// Open a metastore client for a single "thrift://host:port" endpoint with the
	// given auth. Low-level: HMSConnection owns endpoint/URI selection, failover,
	// and reconnect — callers should go through HMSCatalog::GetConnection().
	static unique_ptr<HMSClient> CreateClient(const string &endpoint, const HMSClientAuth &auth);
};

} // namespace duckdb
