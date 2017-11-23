//------------------------------------------------------------------------------
/*
This file is part of rippled: https://github.com/ripple/rippled
Copyright (c) 2015 Ripple Labs Inc.

Permission to use, copy, modify, and/or distribute this software for any
purpose  with  or without fee is hereby granted, provided that the above
copyright notice and this permission notice appear in all copies.

THE  SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
WITH  REGARD  TO  THIS  SOFTWARE  INCLUDING  ALL  IMPLIED  WARRANTIES  OF
MERCHANTABILITY  AND  FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
ANY  SPECIAL ,  DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
WHATSOEVER  RESULTING  FROM  LOSS  OF USE, DATA OR PROFITS, WHETHER IN AN
ACTION  OF  CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/
//=============================================================================

#include <vector>
#include <string>

#include <ripple/app/misc/TxStore.h>
#include <ripple/json/Output.h>
#include <ripple/app/misc/STTx2SQL.h>
#include <ripple/protocol/JsonFields.h>
#include <ripple/protocol/ErrorCodes.h>
#include <ripple/net/RPCErr.h>
#include <ripple/rpc/impl/RPCHelpers.h>
#include <boost/algorithm/string.hpp>

namespace ripple {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// class TxStoreDBConn
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TxStoreDBConn::TxStoreDBConn(const Config& cfg)
: databasecon_(nullptr) {
	DatabaseCon::Setup setup = ripple::setup_SyncDatabaseCon(cfg);
	std::pair<std::string, bool> result_type = setup.sync_db.find("type");
	std::string database_name;
	if (result_type.second == false || result_type.first.empty() || result_type.first.compare("sqlite") == 0) {
		std::pair<std::string, bool> database = setup.sync_db.find("db");
		database_name = "ripple";
		if (database.second)
			database_name = database.first;
	}
    int count = 0;

    while (count < 3)
    {
        try
        {
            count++;
            databasecon_ = std::make_shared<DatabaseCon>(setup, database_name, nullptr, 0);
        }
        catch (...)
        {
            databasecon_ = NULL;
        }
        if (databasecon_)
            break;
    }
}

TxStoreDBConn::~TxStoreDBConn() {
	if (databasecon_)
		databasecon_.reset();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// class TxStoreTransaction
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TxStoreTransaction::TxStoreTransaction(TxStoreDBConn* storeDBConn)
    : tr_(std::make_shared<soci::transaction>(storeDBConn->GetDBConn()->getSession()))
{

}

TxStoreTransaction::~TxStoreTransaction() {
	if (tr_)
		tr_.reset();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// class TxStore
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//TxStore::TxStore(const Config& cfg)
//: cfg_(cfg)
//, databasecon_(nullptr) {
//	DatabaseCon::Setup setup = ripple::setup_SyncDatabaseCon(cfg_);
//	std::pair<std::string, bool> result_type = setup.sync_db.find("type");
//	std::string database_name;
//	if (result_type.second == false || result_type.first.empty() || result_type.first.compare("sqlite") == 0) {
//		std::pair<std::string, bool> database = setup.sync_db.find("db");
//		database_name = "ripple";
//		if (database.second)
//			database_name = database.first;
//	}
//	databasecon_ = std::make_shared<DatabaseCon>(setup, database_name, nullptr, 0);
//}

TxStore::TxStore(DatabaseCon* dbconn, const Config& cfg, const beast::Journal& journal)
: cfg_(cfg)
, db_type_()
, databasecon_(dbconn)
, journal_(journal) {
	const ripple::Section& sync_db = cfg_.section("sync_db");
	std::pair<std::string, bool> result = sync_db.find("type");
	if (result.second)
		db_type_ = result.first;
}

TxStore::~TxStore() {
	
}

std::pair<bool, std::string> TxStore::Dispose(const STTx& tx) {
	std::pair<bool, std::string> ret = {false, "inner error"};
	do {
		if (databasecon_ == nullptr) {
			ret = { false, "database occupy error" };
			break;
		}

		// filter type
		if (tx.getTxnType() != ttTABLELISTSET
			&& tx.getTxnType() != ttSQLSTATEMENT) {
			ret = { false, "tx's type is unexcepted" };
			break;
		}

		//if (isPermitted(tx) == false)
		//	break;
		
		STTx2SQL tx2sql(db_type_, databasecon_);
		std::pair<int, std::string> result = tx2sql.ExecuteSQL(tx);
		if (result.first != 0) {
			std::string errmsg = std::string("Execute failure." + result.second);
			ret = { false,  errmsg};
			JLOG(journal_.error()) << errmsg;
			break;
		} else {
			JLOG(journal_.info()) << "Execute sucess." + result.second;
		}

		ret = { true, "success" };
	} while (0);
	return ret;
}

std::pair<bool, std::string> TxStore::DropTable(const std::string& tablename) {
	std::pair<bool, std::string> result = { false, "inner error" };
	if (databasecon_ == nullptr) {
		result = {false, "Can't connect db."};
		return result;
	}
	std::string sql_str = std::string("drop table if exists t_") + tablename;
	soci::session& sql = *databasecon_->checkoutDb();
	sql << sql_str;
	return {true, "success"};
}

bool TxStore::isPermitted(const STTx& tx) {
	bool permitted = false;

	const ripple::Section& sync_tables = cfg_.section("sync_db");
	const std::vector<std::string>& values = sync_tables.values();
	//bool validate = false;
	for (size_t i = 0; i < values.size(); i++) {
		const std::string& v = values[i];
		std::vector<std::string> conditions;
		boost::split(conditions, v, boost::is_any_of(" "), boost::token_compress_on);
		if (conditions.size() < 1) // config error
			break;

		const std::string& id = conditions[0];
		const std::string& tn = conditions[1];
		ripple::AccountID expected_id;
        RPC::accountFromString(expected_id, id, false);
		
		ripple::Blob expected_tn(tn.begin(), tn.end());
		ripple::AccountID creator;
		ripple::Blob tablename;

		if (tx.getTxnType() == ttTABLELISTSET) {
			creator = tx.getAccountID(sfAccount);
			tablename = tx.getFieldVL(sfTableName);
		} else if (tx.getTxnType() == ttSQLSTATEMENT) {
			creator = tx.getAccountID(sfOwner);
			if (tx.isFieldPresent(sfTables)) {
				const ripple::STArray& tables = tx.getFieldArray(sfTables);
				// tables has only one element in current
				const ripple::STObject& e = tables[0];
				tablename = e.getFieldVL(sfTableName);
			}
			else {
				tablename = tx.getFieldVL(sfTableName);
			}
		}

		if (expected_id == creator
			&& expected_tn == tablename) {
			permitted = true;
			break;
		}
	}
	
	return permitted;
}

}	// namespace ripple