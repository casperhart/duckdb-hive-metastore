#include "hms_client.hpp"
#include "hms_kerberos.hpp"
#include "duckdb/common/exception.hpp"
#include <thrift/transport/TTransportException.h>
#include <cstdio>

namespace duckdb {

// TCP connect timeout for the metastore socket. Applies to both the plaintext
// and SASL paths; keeps a dead endpoint from stalling the first catalog access.
static constexpr int CONNECT_TIMEOUT_MS = 10000;
// Recv/send timeouts so a server that accepts the connection but stops
// responding (half-dead network, packet-dropping firewall, peer wedged
// mid-SASL-handshake) cannot block a query forever. Matches the default of
// Hive's own client (hive.metastore.client.socket.timeout = 600s).
static constexpr int SOCKET_TIMEOUT_MS = 600000;

static thread_local int thrift_log_suppress_depth = 0;

HMSThriftLogSuppressor::HMSThriftLogSuppressor() {
	thrift_log_suppress_depth++;
}

HMSThriftLogSuppressor::~HMSThriftLogSuppressor() {
	thrift_log_suppress_depth--;
}

bool HMSThriftLogSuppressor::Active() {
	return thrift_log_suppress_depth > 0;
}

HMSClient::HMSClient(const string &host, int port, const HMSClientAuth &auth)
    : host(host), port(port), connected(false) {
	socket = std::make_shared<apache::thrift::transport::TSocket>(host, port);
	// Bound the TCP connect so an unreachable/wrong metastore fails fast with a
	// clear error instead of hanging the query that first touches the catalog.
	socket->setConnTimeout(CONNECT_TIMEOUT_MS);
	socket->setRecvTimeout(SOCKET_TIMEOUT_MS);
	socket->setSendTimeout(SOCKET_TIMEOUT_MS);
	if (auth.kerberos) {
		// SASL/GSSAPI: the SASL transport does its own length-framing, so the
		// binary protocol sits directly on top of it (no TBufferedTransport).
		// The SPN instance is the principal's concrete instance, or this
		// endpoint's host for _HOST — used literally either way (Hive/Spark
		// semantics; see HMSMakeKerberosTransport).
		transport = HMSMakeKerberosTransport(socket, auth.service, auth.fqdn.empty() ? host : auth.fqdn);
	} else {
		// Historic plaintext path — unchanged.
		transport = std::make_shared<apache::thrift::transport::TBufferedTransport>(socket);
	}
	protocol = std::make_shared<apache::thrift::protocol::TBinaryProtocol>(transport);
	client = unique_ptr<Apache::Hadoop::Hive::ThriftHiveMetastoreClient>(
	    new Apache::Hadoop::Hive::ThriftHiveMetastoreClient(protocol));
}

HMSClient::~HMSClient() {
	Close();
}

void HMSClient::Open() {
	if (!connected) {
		try {
			transport->open();
			connected = true;
		} catch (apache::thrift::TException &tx) {
			throw IOException("Failed to connect to Hive Metastore at %s:%d - %s", host, port, tx.what());
		}
	}
}

void HMSClient::Close() {
	if (connected) {
		try {
			transport->close();
			connected = false;
		} catch (apache::thrift::TException &) {
			// Silently ignore errors during close - we're likely in a destructor
			// and there's no good way to handle this without risking double-throw
			connected = false;
		}
	}
}

bool HMSClient::IsConnected() const {
	return connected;
}

vector<string> HMSClient::GetAllDatabases() {
	if (!connected)
		Open();
	vector<string> dbs;
	try {
		client->get_all_databases(dbs);
	} catch (apache::thrift::transport::TTransportException &tx) {
		connected = false;
		throw HMSTransportError(tx.what());
	} catch (apache::thrift::TException &tx) {
		throw IOException("Failed to get all databases: %s", tx.what());
	}
	return dbs;
}

vector<string> HMSClient::GetAllTables(const string &db_name) {
	if (!connected)
		Open();
	vector<string> tables;
	try {
		client->get_all_tables(tables, db_name);
	} catch (apache::thrift::transport::TTransportException &tx) {
		connected = false;
		throw HMSTransportError(tx.what());
	} catch (apache::thrift::TException &tx) {
		throw IOException("Failed to get all tables for database '%s': %s", db_name, tx.what());
	}
	return tables;
}

Apache::Hadoop::Hive::Database HMSClient::GetDatabase(const string &db_name) {
	if (!connected)
		Open();
	Apache::Hadoop::Hive::Database db;
	try {
		client->get_database(db, db_name);
	} catch (Apache::Hadoop::Hive::NoSuchObjectException &e) {
		throw IOException("Database '%s' not found: %s", db_name, e.message);
	} catch (apache::thrift::transport::TTransportException &tx) {
		connected = false;
		throw HMSTransportError(tx.what());
	} catch (apache::thrift::TException &tx) {
		throw IOException("Failed to get database '%s': %s", db_name, tx.what());
	}
	return db;
}

Apache::Hadoop::Hive::Table HMSClient::GetTable(const string &db_name, const string &table_name) {
	if (!connected)
		Open();
	Apache::Hadoop::Hive::Table table;
	try {
		client->get_table(table, db_name, table_name);
	} catch (Apache::Hadoop::Hive::NoSuchObjectException &e) {
		throw IOException("Table '%s.%s' not found: %s", db_name, table_name, e.message);
	} catch (apache::thrift::transport::TTransportException &tx) {
		connected = false;
		throw HMSTransportError(tx.what());
	} catch (apache::thrift::TException &tx) {
		throw IOException("Failed to get table '%s.%s': %s", db_name, table_name, tx.what());
	}
	return table;
}

vector<Apache::Hadoop::Hive::Table> HMSClient::GetTableObjects(const string &db_name,
                                                               const vector<string> &table_names) {
	if (!connected)
		Open();
	vector<Apache::Hadoop::Hive::Table> tables;
	// Nothing to fetch, and some servers reject an empty name list outright.
	if (table_names.empty()) {
		return tables;
	}

	// Use the request-based call rather than the deprecated
	// get_table_objects_by_name(dbname, tbl_names). The deprecated method
	// declares no `throws` in the Thrift IDL, so a server-side MetaException
	// (e.g. a storage-based-authorization "Permission denied ... access=EXECUTE"
	// on the table's HDFS path) is flattened by Thrift into a generic
	// TApplicationException whose message is only the fixed string "Internal
	// error processing get_table_objects_by_name" — the real cause is lost on
	// the wire. The _req variant declares its exceptions, so the actual Hive
	// message reaches the user, and it also lets us advertise client
	// capabilities so insert-only/ACID tables are returned rather than rejected.
	Apache::Hadoop::Hive::GetTablesRequest req;
	req.__set_dbName(db_name);
	req.__set_tblNames(table_names);
	Apache::Hadoop::Hive::ClientCapabilities capabilities;
	capabilities.__set_values({Apache::Hadoop::Hive::ClientCapability::INSERT_ONLY_TABLES});
	req.__set_capabilities(capabilities);

	Apache::Hadoop::Hive::GetTablesResult result;
	try {
		client->get_table_objects_by_name_req(result, req);
	} catch (apache::thrift::transport::TTransportException &tx) {
		connected = false;
		throw HMSTransportError(tx.what());
	} catch (Apache::Hadoop::Hive::MetaException &e) {
		throw IOException("Failed to get table objects for database '%s': %s", db_name, e.message);
	} catch (Apache::Hadoop::Hive::UnknownDBException &e) {
		throw IOException("Failed to get table objects for database '%s': unknown database: %s", db_name, e.message);
	} catch (Apache::Hadoop::Hive::InvalidOperationException &e) {
		throw IOException("Failed to get table objects for database '%s': %s", db_name, e.message);
	} catch (apache::thrift::TException &tx) {
		throw IOException("Failed to get table objects for database '%s': %s", db_name, tx.what());
	}
	return std::move(result.tables);
}

void HMSClient::CreateTable(const Apache::Hadoop::Hive::Table &table) {
	if (!connected)
		Open();
	try {
#ifdef THRIFT_HAS_CREATE_TABLE_WITH_ENV
		Apache::Hadoop::Hive::EnvironmentContext env;
		client->create_table_with_environment_context(table, env);
#else
		client->create_table(table);
#endif
	} catch (Apache::Hadoop::Hive::AlreadyExistsException &e) {
		throw IOException("Table '%s.%s' already exists: %s", table.dbName, table.tableName, e.message);
	} catch (apache::thrift::transport::TTransportException &tx) {
		connected = false;
		throw HMSTransportError(tx.what());
	} catch (apache::thrift::TException &tx) {
		throw IOException("Failed to create table '%s.%s': %s", table.dbName, table.tableName, tx.what());
	}
}

bool HMSClient::DropTable(const string &db_name, const string &table_name, bool delete_data) {
	if (!connected)
		Open();
	try {
		client->drop_table(db_name, table_name, delete_data);
		return true;
	} catch (Apache::Hadoop::Hive::NoSuchObjectException &) {
		// Distinct return value lets the caller honor IF EXISTS without swallowing
		// transport or auth errors that look the same from a generic catch.
		return false;
	} catch (apache::thrift::transport::TTransportException &tx) {
		connected = false;
		throw HMSTransportError(tx.what());
	} catch (apache::thrift::TException &tx) {
		throw IOException("Failed to drop table '%s.%s': %s", db_name, table_name, tx.what());
	}
}

} // namespace duckdb
