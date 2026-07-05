//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/hms_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"

namespace duckdb {
class HMSCatalog;
class HMSClient;
class HMSSchemaEntry;
class HMSTableEntry;

enum class HMSTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class HMSTransaction : public Transaction {
public:
	HMSTransaction(HMSCatalog &hms_catalog, TransactionManager &manager, ClientContext &context);
	~HMSTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	// Lazily open — and thereafter reuse — a single metastore connection for the
	// lifetime of this transaction. All catalog operations in the transaction
	// share it, so a query that touches many schemas/tables no longer opens (and,
	// once Kerberos is enabled, re-authenticates) a fresh connection per call.
	HMSClient &GetConnection();

	static HMSTransaction &Get(ClientContext &context, Catalog &catalog);
	AccessMode GetAccessMode() const {
		return access_mode;
	}

private:
	HMSCatalog &hms_catalog;
	unique_ptr<HMSClient> connection;
	HMSTransactionState transaction_state;
	AccessMode access_mode;
};

} // namespace duckdb
