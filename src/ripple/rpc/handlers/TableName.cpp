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
#include <ripple/rpc/impl/RPCHelpers.h>
#include <ripple/protocol/digest.h>

namespace ripple {
std::string hash(std::string &pk)
{
   ripesha_hasher rsh;
   rsh(pk.c_str(), pk.size());
   auto const d = static_cast<
   ripesha_hasher::result_type>(rsh);
   std::string str;
   str = strHex(d.data(), d.size());
   return str;
}


Json::Value doGetDBName(RPC::Context&  context)
{
    Json::Value ret(Json::objectValue);
    Json::Value& tx_json(context.params["tx_json"]);

    if (tx_json["Account"].asString().empty() || tx_json["TableName"].asString().empty())
    {
        ret[jss::status] = "error";
        ret[jss::error_message] = "Account or TableName is null";
        return ret;
    }

    auto accountIdStr = tx_json["Account"].asString();
    auto tableNameStr = tx_json["TableName"].asString();
    
    ripple::AccountID accountID;
    auto jvAccepted = RPC::accountFromString(accountID, accountIdStr, false);

    if (jvAccepted)
        return jvAccepted;

    //first,we query from ledgerMaster
    auto retPair = context.ledgerMaster.getNameInDB(context.ledgerMaster.getValidLedgerIndex(), accountID, tableNameStr);
    auto nameInDB = retPair.first;
    if (!nameInDB) //not exist,then generate nameInDB
    {
        uint32_t ledgerSequence = 0;
        auto ledger = context.ledgerMaster.getValidatedLedger();
        if (ledger)
            ledgerSequence = ledger->info().seq;
        else
        {
            ret[jss::status] = "error";
            ret[jss::error_message] = "can not find ledger!";
            return ret;
        }
        std::string ledgerSequenceStr = to_string(ledgerSequence);

        try
        {
            std::string tmp = ledgerSequenceStr + accountIdStr + tableNameStr;
            std::string str = hash(tmp);
            nameInDB = from_hex_text<uint160>(str);
        }
        catch (std::exception const& e)
        {
            ret[jss::status] = "error";
            ret[jss::error_message] = e.what();
            return ret;
        }
        ret[jss::status] = "success";
    }
    ret["nameInDB"] = to_string(nameInDB);
    return ret;
}

} // ripple
