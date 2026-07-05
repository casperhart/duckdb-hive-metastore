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

	static HMSTransaction &Get(ClientContext &context, Catalog &catalog);
	AccessMode GetAccessMode() const {
		return access_mode;
	}

private:
	HMSTransactionState transaction_state;
	AccessMode access_mode;
};

} // namespace duckdb
