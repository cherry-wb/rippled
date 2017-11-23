//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2012-2014 Ripple Labs Inc.

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

#include <BeastConfig.h>
#include <ripple/app/misc/NetworkOPs.h>
#include <ripple/json/json_value.h>
#include <ripple/net/RPCErr.h>
#include <ripple/protocol/JsonFields.h>
#include <ripple/rpc/Context.h>
#include <ripple/rpc/impl/TransactionSign.h>
#include <ripple/rpc/Role.h>
#include <ripple/rpc/handlers/Handlers.h>
#include <ripple/app/ledger/LedgerMaster.h>
#include <ripple/app/misc/TxStore.h>
#include <ripple/app/storage/TableStorage.h>
#include <iostream> 
#include <fstream> 
#include <ripple/json/json_reader.h>

namespace ripple {

Json::Value doRpcSubmit(RPC::Context& context)
{
    Json::Value& tx_json(context.params["tx_json"]);

    if (tx_json["Raw"].asString() != "")
        tx_json["Raw"] = strHex(tx_json["Raw"].asString());
    else
        tx_json.removeMember("Raw");

    if(tx_json["AutoFillField"].asString()!="")
        tx_json["AutoFillField"] = strHex(tx_json["AutoFillField"].asString());
    else
        tx_json.removeMember("AutoFillField");
    Json::Value& tables_json(tx_json["Tables"]);
    for (auto &json : tables_json)
    {
        json["Table"]["TableName"] = strHex(json["Table"]["TableName"].asString());
    }
    Json::Value ret = doSubmit(context);
    return ret;
}

Json::Value doCreateFromRaw(RPC::Context& context)
{ 
    using namespace std;
    Json::Value& tx_json(context.params);
    Json::Value& jsons(tx_json["params"]);
    Json::Value create_json;
    std::string secret;
    std::string tablename;
    std::string filename;
    for (auto json : jsons)
    {
        Json::Reader reader;
        Json::Value params_json;
        if (reader.parse(json.asString(), params_json))
        {
            create_json["Account"] = params_json["Account"];
            secret = params_json["Secret"].asString();
            tablename = params_json["TableName"].asString();
            filename = params_json["RawPath"].asString();
        }
    }

    Json::Value jvResult;
    ifstream myfile;
    myfile.open(filename, ios::in);
    if (!myfile)
    {    
        jvResult[jss::error_message] = "can not open file,please checkout path!";
        jvResult[jss::error] = "error";
        return jvResult;
    }

    char ch;
    string content;
    while (myfile.get(ch))
        content += ch;
    myfile.close();

    create_json["TransactionType"] = "TableListSet";
    create_json["Raw"] = content;
    create_json["OpType"] = T_CREATE;
    Json::Value tables_json;
    Json::Value table_json;
    Json::Value table;
    table["TableName"] = tablename;

    AccountID accountID(*ripple::parseBase58<AccountID>(create_json["Account"].asString()));
    create_json["TableName"] = tablename;
    context.params["tx_json"] = create_json;
    auto ret = doGetDBName(context);

    if (ret["nameInDB"].asString().size()== 0)
    {
        jvResult[jss::error_message] = "can not getDBName,please checkout program is had sync!";
        jvResult[jss::error] = "error";
        return jvResult;
    }

    table["NameInDB"] = ret["nameInDB"];

    create_json.removeMember("TableName");
    table_json["Table"] = table;
    tables_json.append(table_json);
    create_json["Tables"] = tables_json;

    context.params["command"] = "t_create";
    context.params["secret"] = secret;
    context.params["tx_json"] = create_json;
    return doRpcSubmit(context);
}


Json::Value doGetRecord(RPC::Context&  context)
{
    Json::Value ret(Json::objectValue);
	Json::Value& tx_json(context.params["tx_json"]);

    if (tx_json["Owner"].asString().empty())
    {
        ret[jss::error] = "field Owner is empty!";
        return ret;
    }

	AccountID ownerID(*ripple::parseBase58<AccountID>(tx_json["Owner"].asString()));

	Json::Value &tables_json = tx_json["Tables"];
    if (!tables_json.isArray())
    {
        ret[jss::error] = "field Tables is not array!";
        return ret;
    }

	auto j = context.app.journal("RPCHandler");
	JLOG(j.debug())
		<< "get record from tables: " << tx_json.toStyledString();

    uint160 nameInDB;

	for (Json::UInt idx = 0; idx < tables_json.size(); idx++) {
		Json::Value& e = tables_json[idx];
        if (!e.isObject()) 
        {
            ret[jss::error] = "field Tables is not object!";
            return ret;
        };

		Json::Value& v = e["Table"];
        if (!v.isObject())
        {
            ret[jss::error] = "field Table is not object!";
            return ret;
        }

		Json::Value tn = v["TableName"];
        if (!tn.isString())
        {
            ret[jss::error] = "field TableName is not string!";
            return ret;
        }
        
        auto retPair = context.ledgerMaster.getNameInDB(context.ledgerMaster.getValidLedgerIndex(), ownerID, v["TableName"].asString());
		
        if (!retPair.first)
        {
            ret[jss::error] = "can't get TableName in DB ,please check field tablename!";
            return ret;
        }
        nameInDB = retPair.first;
		v["TableName"] = to_string(retPair.first);
	}

    if (tables_json.size() == 1)
        return context.app.getTableStorage().GetTxStore(nameInDB).txHistory(context);
    else
        return context.app.getTxStore().txHistory(context);
}
} // ripple
