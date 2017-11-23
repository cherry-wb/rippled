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
//==============================================================================

#ifndef RIPPLE_APP_MISC_JSON2SQL_H_INCLUDED
#define RIPPLE_APP_MISC_JSON2SQL_H_INCLUDED

#include <memory>
#include <string>
#include <utility>

#include <ripple/json/json_value.h>
#include <ripple/json/Object.h>
#include <ripple/json/json_writer.h>
#include <ripple/protocol/STTx.h>

namespace ripple {

class BuildSQL;
class DatabaseCon;

class STTx2SQL {
public:
	STTx2SQL(const std::string& db_type);
	STTx2SQL(const std::string& db_type, DatabaseCon* dbconn);
	~STTx2SQL();
	
	// convert json into sql
	std::pair<int /*retcode*/, std::string /*sql*/> ExecuteSQL(const ripple::STTx& tx);
private:
	STTx2SQL() {};
	int GenerateCreateTableSql(const Json::Value& raw, BuildSQL *buildsql);
	int GenerateRenameTableSql(const Json::Value& tx_json, std::string& sql);
	int GenerateInsertSql(const Json::Value& raw, BuildSQL *buildsql);
	int GenerateUpdateSql(const Json::Value& raw, BuildSQL *buildsql);
	int GenerateDeleteSql(const Json::Value& raw, BuildSQL *buildsql);
	int GenerateSelectSql(const Json::Value& raw, BuildSQL *buildsql);

	std::string db_type_;
	DatabaseCon* db_conn_;
}; // STTx2SQL

}
#endif // RIPPLE_APP_MISC_JSON2SQL_H_INCLUDED