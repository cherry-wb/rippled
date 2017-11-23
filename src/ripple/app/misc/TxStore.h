//------------------------------------------------------------------------------
/*
This file is part of rippled: https://github.com/ripple/rippled
Copyright (c) 2012, 2013 Ripple Labs Inc.

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

#ifndef RIPPLE_APP_MISC_TXSTORE_H_INCLUDED
#define RIPPLE_APP_MISC_TXSTORE_H_INCLUDED

#include <memory>
#include <string>
#include <utility>

#include <ripple/app/tx/impl/ApplyContext.h>
#include <ripple/core/DatabaseCon.h>
#include <ripple/rpc/Context.h>
#include <ripple/json/json_value.h>
#include <ripple/basics/Log.h>

namespace ripple {

class TxStoreDBConn {
public:
	TxStoreDBConn(const Config& cfg);
	~TxStoreDBConn();

	DatabaseCon* GetDBConn() {
		if (databasecon_)
			return databasecon_.get();
		return nullptr;
	}

private:
	std::shared_ptr<DatabaseCon> databasecon_;
};

class TxStoreTransaction {
public:
	TxStoreTransaction(TxStoreDBConn* storeDBConn);
	~TxStoreTransaction();

	soci::transaction* GetTransaction() {
		if (tr_)
			return tr_.get();
		return nullptr;
	}

	void commit() {
		tr_->commit();
	}

    void rollback() {
        tr_->rollback();
    }

private:
	std::shared_ptr<soci::transaction> tr_;    
};

class TxStore {
public:
	//TxStore(const Config& cfg);
	TxStore(DatabaseCon* dbconn, const Config& cfg, const beast::Journal& journal);
	~TxStore();

	// dispose one transaction
	std::pair<bool,std::string> Dispose(const STTx& tx);
	std::pair<bool, std::string> DropTable(const std::string& tablename);

	Json::Value txHistory(RPC::Context& context);

private:
	bool isPermitted(const STTx& tx);
	const Config& cfg_;
	std::string db_type_;
	DatabaseCon* databasecon_;
	beast::Journal journal_;
};	// class TxStore

}	// namespace ripple
#endif // RIPPLE_APP_MISC_TXSTORE_H_INCLUDED