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

#include <BeastConfig.h>
#include <ripple/app/tx/impl/TableListSet.h>
#include <ripple/ledger/View.h>
#include <ripple/app/paths/RippleCalc.h>
#include <ripple/basics/Log.h>
#include <ripple/core/Config.h>
#include <ripple/protocol/st.h>
#include <ripple/protocol/TxFlags.h>
#include <ripple/protocol/JsonFields.h>
#include <ripple/protocol/STEntry.h>
#include <ripple/protocol/TableDefines.h>
#include <ripple/basics/StringUtilities.h>
#include <ripple/app/ledger/LedgerMaster.h>
#include <ripple/app/storage/TableStorage.h>


namespace ripple {

XRPAmount
TableListSet::calculateMaxSpend(STTx const& tx)
{
    if (tx.isFieldPresent(sfSendMax))
    {
        auto const& sendMax = tx[sfSendMax];
        return sendMax.native() ? sendMax.xrp() : beast::zero;
    }
    /* If there's no sfSendMax in XRP, and the sfAmount isn't
    in XRP, then the transaction can not send XRP. */
    auto const& saDstAmount = tx.getFieldAmount(sfAmount);
    return saDstAmount.native() ? saDstAmount.xrp() : beast::zero;
}

TER
clearTxInTableNode(ApplyContext & ctx)
{
	AccountID uTableOwnerID;
	//get the right tableNodeID
	auto eCruTxType = ctx.tx.getTxnType();
	if (eCruTxType == ttTABLELISTSET)
	{
		uTableOwnerID = ctx.tx.getAccountID(sfAccount);
	}
	else if (eCruTxType == ttSQLSTATEMENT)
	{
		uTableOwnerID = ctx.tx.getAccountID(sfOwner);
	}
	else
	{
		return  temBAD_OPTYPE;
	}
	auto id = keylet::table(uTableOwnerID);
	auto tablesle = ctx.view().peek(id);
	auto &aTableEntries = tablesle->peekFieldArray(sfTableEntries);
	//check whether clear tx array
	auto it = ctx.view().txs.begin();
	for (; it != ctx.view().txs.end(); it++)
	{
		auto eTxType = it->first->getTxnType();
		if (eTxType == ttTABLELISTSET)
		{
			if (it->first && it->first->isFieldPresent(sfAccount))
			{
				if (it->first->getAccountID(sfAccount) == uTableOwnerID)
					break;
			}
		}
		else if (eTxType == ttSQLSTATEMENT)
		{
			if (it->first && it->first->isFieldPresent(sfOwner))
			{
				if (it->first->getAccountID(sfOwner) == uTableOwnerID)
					break;
			}
		}
		else
			continue;
	}
	//clear all TX in this table node.
	if (it == ctx.view().txs.end())
	{
		for (auto & table : aTableEntries)
		{
			std::vector <uint256> aTx;
			if (table.isFieldPresent(sfTxs))
			{
				aTx = static_cast<decltype(aTx)>(table.getFieldV256(sfTxs));
				aTx.clear();
				table.setFieldV256(sfTxs, STVector256(aTx));
			}
		}
	}
	return tesSUCCESS;
}

TER
TableListSet::preflight (PreflightContext const& ctx)
{
    auto const ret = preflight1 (ctx);
    if (!isTesSuccess (ret))
        return ret;

    auto& tx = ctx.tx;
    auto& j = ctx.j;

    if (tx.isFieldPresent(sfRaw))  //check sfTables
    {
        auto raw = tx.getFieldString(sfRaw);
        if (raw.size() == 0)
        {
            JLOG(j.trace()) <<
                "sfRaw is not invalid";
            return temINVALID;
        }
    }
    auto optype = ctx.tx.getFieldU16(sfOpType);
    if (!tx.isFieldPresent(sfTables))  //check sfTables
    {
        JLOG(j.trace()) <<
            "sfTables is not invalid";
        return temINVALID;
    }
    else
    {
        auto tables = tx.getFieldArray(sfTables);
        if (tables.size() == 0)
        {
            JLOG(j.trace()) <<
                "sfTables is not filled";
            return temINVALID;
        }
        else
        {
            for (auto table : tables)
            {
                if (!table.isFieldPresent(sfTableName))
                {
                    JLOG(j.trace()) <<
                        "sfTableName is not present";
                    return temINVALID;
                }
                auto tablename = table.getFieldString(sfTableName);
                if (tablename.size() == 0)
                {
                    JLOG(j.trace()) <<
                        "sfTableName is not filled";
                    return temINVALID;
                }
                if (!table.isFieldPresent(sfNameInDB))
                {
                    JLOG(j.trace()) <<
                        "sfNameInDB is not present";
                    return temINVALID;
                }
                auto nameindb = table.getFieldH160(sfNameInDB);
                if (nameindb.isZero() == 1)
                {
                    JLOG(j.trace()) <<
                        "sfNameInDB is not filled";
                    return temINVALID;
                }
            }
        }
    }
    if (optype == T_RENAME)  //check sfTableNewName
    {
        auto tables = ctx.tx.getFieldArray(sfTables);
        if (!tables[0].isFieldPresent(sfTableNewName))
        {
            JLOG(j.trace()) <<
                "rename opreator but sfTableNewName is not filled";
            return temINVALID;
        }
    }
    if (tx.isFieldPresent(sfFlags))  //check sfFlags
    {
        auto flags = tx.getFieldU32(sfFlags);

        if (tx.isFieldPresent(sfUser))
        {
            if (!((flags & lsfSelect) || (flags & lsfInsert) || (flags & lsfUpdate) || (flags & lsfDelete) || (flags &  lsfExecute)))
            {
                JLOG(j.trace()) <<
                    "bad auth";
                return tefNO_AUTH_REQUIRED;
            }
        }
    }

    return preflight2 (ctx);
}


STObject
TableListSet::generateTableEntry(ApplyContext const & ctx)  //preflight assure sfTables must exist
{
    STEntry obj_tableEntry;

    ripple::Blob tableName;      //store Name
    auto tables = ctx.tx.getFieldArray(sfTables);
    tableName = tables[0].getFieldVL(sfTableName);

    uint160 nameInDB = tables[0].getFieldH160(sfNameInDB);
    uint8 isTableDeleted = 0;  //is not deleted

    uint32 ledgerSequence = ctx.view().info().seq;
    ripple::uint256 txnLedgerHash = ctx.view().info().hash;
    ripple::uint256 prevTxnLedgerHash; //default null

    std::vector<uint256> hashes; //store Txs
    hashes.push_back(ctx.tx.getTransactionID());

    STArray users;//store Users
    STObject obj_user(sfUser);

	if (ctx.tx.isFieldPresent(sfUser))//preflight assure sfUser and sfFlags exist together or not exist at all
	{
		obj_user.setAccountID(sfUser, ctx.tx.getAccountID(sfUser));
		obj_user.setFieldU32(sfFlags, ctx.tx.getFieldU32(sfFlags));
	}
	else
	{
		obj_user.setAccountID(sfUser, ctx.tx.getAccountID(sfAccount));
		obj_user.setFieldU32(sfFlags, lsfAll);
	}
	
	users.push_back(obj_user);

    obj_tableEntry.init(tableName, nameInDB,isTableDeleted, ledgerSequence ,txnLedgerHash,0,prevTxnLedgerHash, hashes, users);
    return std::move(obj_tableEntry);
}
STEntry * TableListSet::getTableEntry(const STArray & aTables, std::string sCheckName)
{
	auto iter(aTables.end());
	iter = std::find_if(aTables.begin(), aTables.end(),
		[sCheckName](STObject const &item) {
		if (!item.isFieldPresent(sfTableName))  return false;
		if (!item.isFieldPresent(sfDeleted))    return false;
		return item.getFieldString(sfTableName) == sCheckName && item.getFieldU8(sfDeleted) != 1;
		});

	if (iter == aTables.end())  return NULL;

	return (STEntry*)(&(*iter));
}

TER
TableListSet::preclaim(PreclaimContext const& ctx)  //just do some pre job
{ 
    AccountID sourceID(ctx.tx.getAccountID(sfAccount));
    TER ret = tesSUCCESS;
    
    auto optype = ctx.tx.getFieldU16(sfOpType);
    if ((optype < T_CREATE) || (optype > R_DELETE)) //check op range
        return temBAD_OPTYPE;
    auto key = keylet::table(sourceID);
    auto const tablesle = ctx.view.read(key);

    auto tables = ctx.tx.getFieldArray(sfTables);
    std::string tableNameStr = tables[0].getFieldString(sfTableName);

    if (tablesle)  //table exist
    {
        STArray tablentries = tablesle->getFieldArray(sfTableEntries);
 
        switch (optype)
        {
        case T_CREATE:
        {
            STEntry const *pEntry = getTableEntry(tablentries, tableNameStr);
            if (pEntry != NULL)                ret = terTABLE_EXISTANDNOTDEL;
            else                               ret = tesSUCCESS;

            break;
        }
        case T_DROP:
        {   
            STEntry const *pEntry = getTableEntry(tablentries, tableNameStr);
			if (pEntry != NULL)                ret = tesSUCCESS;
			else                               ret = terTABLE_NOTEXIST;

            break;
        }
        case T_RENAME:
        {
            if (tableNameStr == tables[0].getFieldString(sfTableNewName))
            {
                ret = terTABLE_SAMENAME;
                break;
            }
            STEntry const *pEntry = getTableEntry(tablentries, tableNameStr);
            STEntry const *pEntryNew = getTableEntry(tablentries, tables[0].getFieldString(sfTableNewName));
             
            if (pEntry != NULL)
            {
                if (pEntryNew != NULL)                    ret = terTABLE_EXISTANDNOTDEL;
				else   					                  ret = tesSUCCESS;
            }
			else
			{
				ret = terTABLE_NOTEXIST;
			}
            break;   
        }
        case T_ASSIGN:
        case T_CANCELASSIGN:
        {
            STEntry const *pEntry = getTableEntry(tablentries, tableNameStr);

            if (pEntry != NULL)
            {
                if (pEntry->isFieldPresent(sfUsers))
                {
                    auto& users = pEntry->getFieldArray(sfUsers);


                    if (ctx.tx.isFieldPresent(sfUser))
                    {
                        auto  addUserID = ctx.tx.getAccountID(sfUser);
                    
                        //check user is effective?
                        auto key = keylet::account(addUserID);
                        if (!ctx.view.exists(key))
                        {
                            return tefBAD_AUTH;
                        };
                        bool isSameUser = false;
                        for (auto & user : users)  //check if there same user
                        {
                            auto userID = user.getAccountID(sfUser);
                            if (userID == addUserID)
                            {
                                isSameUser = true;
                                auto userFlags = user.getFieldU32(sfFlags);
                                if (optype == T_CANCELASSIGN)
                                {
                                    if (!(userFlags & ctx.tx.getFieldU32(sfFlags))) //optype == T_CANCELASSIGN but cancel auth not exist
                                    {
                                        ret = tefBAD_AUTH_NO;
                                    }
                                    else
                                        ret = tesSUCCESS;
                                }
                                else
                                {
                                    if (~userFlags & ctx.tx.getFieldU32(sfFlags))  //optype == T_ASSIGN and same TableFlags
                                    {
                                        ret = tesSUCCESS;
                                    }
                                    else
                                        ret = tefBAD_AUTH_EXIST;
                                }
							}
                        }
                        if (!isSameUser)  //mean that there no same user
                        {
                            if (optype == T_CANCELASSIGN)
                                ret = tefBAD_AUTH_NO;
                            else
                                ret = tesSUCCESS;
                        }
                    }
                    else
                        ret = terBAD_USER;
                }
                else
                {
                    if (optype == T_ASSIGN)               ret = tesSUCCESS;
                    else                                  ret = terBAD_USER;
                }
            }
            else
                ret = terTABLE_NOTEXIST;
            break;
        }
        default:
        {
            ret = temBAD_OPTYPE;
            break;
        }
        }
    }
    else         //table not exist
    {
        if (optype >= T_DROP)
            ret = temBAD_OPTYPE; 
    }
    
    return ret;
}

TER
TableListSet::doApply ()
{
	auto optype = ctx_.tx.getFieldU16(sfOpType);

	if (ctx_.view().flags() & tapFromClient)
	{
		if (TableStorage::STORAGE_DBERROR == ctx_.app.getTableStorage().InitItem(ctx_.tx))
		{
			return terTABLE_STORAGEERROR;
		}
	}

    TER terResult = tesSUCCESS;
    // Open a ledger for editing.
    auto id = keylet::table(account_);
    auto const tablesle = view().peek(id);

    if (!tablesle)
    {
        auto const tablesle = std::make_shared<SLE>(
            ltTABLELIST, id.key);

        SLE::pointer sleTicket = std::make_shared<SLE>(ltTICKET,
            getTicketIndex(account_, ctx_.tx.getSequence()));

        auto viewJ = ctx_.app.journal("View");
        std::uint64_t hint;
        auto result = dirAdd(view(), hint, keylet::ownerDir(account_),
            sleTicket->getIndex(), describeOwnerDir(account_), viewJ);

        JLOG(j_.trace()) <<
            "Creating ticket " << to_string(sleTicket->getIndex()) <<
            ": " << transHuman(result.first);

        if (result.first == tesSUCCESS)
        {
            tablesle->setFieldU64(sfOwnerNode, hint);
        }

        STArray tablentries; 
        STObject obj = generateTableEntry(ctx_);
        tablentries.push_back(obj);
        //test only
        //auto json = tablentries.getJson(0);
        //auto str = tablentries.getFullText();
        tablesle->setFieldArray(sfTableEntries, tablentries);

        view().insert(tablesle);
    }
    else
    {
		clearTxInTableNode(ctx_);

        auto &aTableEntries = tablesle->peekFieldArray(sfTableEntries);

        std::string sTxTableName;

        auto const & sTxTables = ctx_.tx.getFieldArray(sfTables);
        sTxTableName = sTxTables[0].getFieldString(sfTableName);

        //add the new tx to the node
        for (auto & table : aTableEntries)
        {
            if (table.getFieldString(sfTableName) == sTxTableName && table.getFieldU8(sfDeleted) != 1)
            {
                std::vector <uint256> aTx;
                if (table.isFieldPresent(sfTxs))
                {
                    table.peekFieldV256(sfTxs).push_back(ctx_.tx.getTransactionID());
                    if (table.getFieldU32(sfTxnLgrSeq) != ctx_.view().info().seq || table.getFieldH256(sfTxnLedgerHash) != ctx_.view().info().hash)
                    {
                        table.setFieldU32(sfPreviousTxnLgrSeq, table.getFieldU32(sfTxnLgrSeq));
                        table.setFieldH256(sfPrevTxnLedgerHash, table.getFieldH256(sfTxnLedgerHash));
                        table.setFieldU32(sfTxnLgrSeq, ctx_.view().info().seq);
                        table.setFieldH256(sfTxnLedgerHash, ctx_.view().info().hash);
                    }
                }
            }
        }
        auto tables = ctx_.tx.getFieldArray(sfTables);
        std::string tableNameStr = tables.begin()->getFieldString(sfTableName);

        switch (optype)
        {
        case T_CREATE:
            aTableEntries.push_back(generateTableEntry(ctx_));
            break;
        case T_DROP:
        {
          STEntry  *pEntry = getTableEntry(aTableEntries, tableNameStr);
          if (pEntry)
          {
              assert(pEntry->getFieldU8(sfDeleted) == 0);
              pEntry->setFieldU8(sfDeleted, 1);

              getTableEntry(aTableEntries, tableNameStr);
              //assert(pEntrytmp->getFieldU8(sfDeleted) == 1);
          }      
          break;
        }
        case  T_RENAME:
        {
            STEntry  *pEntry = getTableEntry(aTableEntries, tableNameStr);
            if (pEntry)
            {
                ripple::Blob tableNewName;
                if (sTxTables[0].isFieldPresent(sfTableNewName))
                {
                    tableNewName = sTxTables[0].getFieldVL(sfTableNewName);
                }
                pEntry->setFieldVL(sfTableName, tableNewName);
            }
        break;
        }
        case T_ASSIGN:
        case T_CANCELASSIGN:
        {
            STEntry  *pEntry = getTableEntry(aTableEntries, tableNameStr);
            {
                if (pEntry->isFieldPresent(sfUsers))
                {
                    auto& users = pEntry->peekFieldArray(sfUsers);

                    if (ctx_.tx.isFieldPresent(sfUser))
                    {
                        auto  addUserID = ctx_.tx.getAccountID(sfUser);

                        bool isSameUser = false;
                        for (auto & user : users)  //check if there same user
                        {
                            auto userID = user.getAccountID(sfUser);
                            if (userID == addUserID)
                            {
                                isSameUser = true;
                                if (optype == T_ASSIGN)
                                {
                                    auto userFlags = user.getFieldU32(sfFlags);
                                    if (ctx_.tx.isFieldPresent(sfFlags))
                                    {
                                        auto newFlags = userFlags | ctx_.tx.getFieldU32(sfFlags); //add auth of this user
                                        user.setFieldU32(sfFlags, newFlags);
                                    }
                                }
                                else  // optype == CancelAuth)
                                {
                                    auto userFlags = user.getFieldU32(sfFlags);
                                    if (userFlags & ctx_.tx.getFieldU32(sfFlags))
                                    {
                                        if (ctx_.tx.isFieldPresent(sfFlags))
                                        {
                                            auto newFlags = userFlags & (~ctx_.tx.getFieldU32(sfFlags));   //cancel auth of this user
                                            user.setFieldU32(sfFlags, newFlags);
                                        }
                                    }                         
                                }
                            }
                        }
                        if (!isSameUser)  //mean that there no same user
                        {
                            // optype must be Auth(preclaim assure that),just add a new user
                            STObject obj_user(sfUser);
                            if (ctx_.tx.isFieldPresent(sfUser))
                                obj_user.setAccountID(sfUser, ctx_.tx.getAccountID(sfUser));
                            if (ctx_.tx.isFieldPresent(sfFlags))
                                obj_user.setFieldU32(sfFlags, ctx_.tx.getFieldU32(sfFlags));
                            users.push_back(obj_user);
                        }
                    }
                }
            }
            break;
        }
        default:
            break;
        }

        view().update(tablesle);
    }

    return terResult;
}

}  // ripple
