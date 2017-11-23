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
#include <ripple/app/tx/impl/SqlStatement.h>
#include <ripple/ledger/View.h>
#include <ripple/app/paths/RippleCalc.h>
#include <ripple/basics/Log.h>
#include <ripple/core/Config.h>
#include <ripple/protocol/st.h>
#include <ripple/protocol/TxFlags.h>
#include <ripple/protocol/JsonFields.h>
#include <ripple/protocol/TableDefines.h>
#include <ripple/app/storage/TableStorage.h>

namespace ripple {

extern TER clearTxInTableNode(ApplyContext & ctx);

XRPAmount
SqlStatement::calculateMaxSpend(STTx const& tx)
{
	if (tx.isFieldPresent(sfSendMax))
	{
		auto const& sendMax = tx[sfSendMax];
		return sendMax.native() ? sendMax.xrp() : beast::zero;
	}
	/* If there's no sfSendMax in XRP, and the sfAmount isn't
	-    in XRP, then the transaction can not send XRP. */
	auto const& saDstAmount = tx.getFieldAmount(sfAmount);
	return saDstAmount.native() ? saDstAmount.xrp() : beast::zero;
}

inline TableRoleFlags getFlagFromOptype(TableOpType eOpType)
{
	switch (eOpType)
	{
	case R_INSERT:
		return lsfInsert;
	case R_UPDATE:
		return lsfUpdate;
	case R_DELETE:
		return lsfDelete;
	case R_GET:
		return lsfSelect;
	default:
		return lsfNone;
	}
}

TER
SqlStatement::preflight (PreflightContext const& ctx)
{
    auto const ret = preflight1 (ctx);
    if (!isTesSuccess (ret))
        return ret;

    auto& tx = ctx.tx;
    auto& j = ctx.j;

    //check myown special fields
	if (!tx.isFieldPresent(sfOwner))
	{
		JLOG(j.trace()) << "Malformed transaction: " <<
			"Invalid owner";
		return temBAD_OWNER;
	}	
	else
	{
		auto const &owner = tx.getAccountID(sfOwner);
		if (owner.isZero())
		{
			JLOG(j.trace()) << "Malformed transaction: " <<
				"Invalid owner";
			return temBAD_OWNER;
		}
	}

	if (!tx.isFieldPresent(sfOpType))
	{
		JLOG(j.trace()) << "Malformed transaction: " <<
			"Invalid opType";
		return temBAD_OPTYPE;
	}

	if (!tx.isFieldPresent(sfTables))
	{
		JLOG(j.trace()) << "Malformed transaction: " <<
			"Invalid table tables";
		return temBAD_TABLES;
	}
    else
    {
        if (tx.getFieldArray(sfTables).size() != 1)
        {
            JLOG(j.trace()) << "Malformed transaction: " <<
                "Invalid table size";
            return temBAD_TABLES;
        }
    }
	
	if (!tx.isFieldPresent(sfRaw))
	{
		JLOG(j.trace()) << "Malformed transaction: " <<
			"Invalid table flags";
		return temBAD_RAW;
	}
	else
	{
		Blob const raw = tx.getFieldVL(sfRaw);
		if (raw.empty())
		{
			JLOG(j.trace()) << "Malformed transaction: " <<
				"Invalid table flags";
			return temBAD_RAW;
		}
	}

	return preflight2(ctx);
}

TER
SqlStatement::preclaim(PreclaimContext const& ctx)
{
	AccountID const uOwnerID(ctx.tx[sfOwner]);

	//whether the owner node is existed
	auto const kOwner = keylet::account(uOwnerID);
	auto const sleOwner = ctx.view.read(kOwner);
	if (!sleOwner)
	{
		JLOG(ctx.j.trace()) <<
			"Delay transaction: Destination account does not exist.";
		return tecNO_DST;
	}

	//whether the table node is existed
	auto const kTable = keylet::table(uOwnerID);
	auto const sleTable = ctx.view.read(kTable);
	if (!sleTable)
	{
		JLOG(ctx.j.trace()) <<
			"Delay transaction: Destination table node not exist.";
		return tecNO_TARGET;
	}

	//check whether the owner has the table, the authority, and the table flag
	STArray const * pTablesTx = &ctx.tx.getFieldArray(sfTables);
	STArray aTableOneItem;

	//STArray const & aTablesTx(ctx.tx.getFieldArray(sfTables));
	STArray const & aTablesNode(sleTable->getFieldArray(sfTableEntries));
	auto tableFalgs = getFlagFromOptype((TableOpType)ctx.tx.getFieldU16(sfOpType));

	AccountID sourceID(ctx.tx.getAccountID(sfAccount));
	for (auto const& table1 : *pTablesTx)
	{
        std::string sTxTableName = table1.getFieldString(sfTableName);
        uint160 uTxDBName = table1.getFieldH160(sfNameInDB);

		bool bFindTable = false;
		for (auto const& table2 : aTablesNode)
		{			
			std::string sSleTableName = table2.getFieldString(sfTableName);            
            uint160 uSleDBName = table2.getFieldH160(sfNameInDB);

			if (sTxTableName == sSleTableName && uTxDBName == uSleDBName && table2.getFieldU8(sfDeleted) != 1)
			{
				auto const & aUsers(table2.getFieldArray(sfUsers));
				bool bFindUser = false;
				for (auto const& user : aUsers)
				{
					auto const userID =  user.getAccountID(sfUser);
					if (sourceID == userID)
					{
						if ((user.getFieldU32(sfFlags) & tableFalgs) == 0)
						{
							JLOG(ctx.j.trace()) <<
								"Delay transaction: Destination table does not exist.";
							return temBAD_TABLEFLAGS;
						}
						bFindUser = true;
						break;
					}
				}
				if (!bFindUser)
				{
					JLOG(ctx.j.trace()) <<
						"Invalid table flags: Destination table does not authorith this account.";
					return tecNO_TARGET;
				}

				bFindTable = true;
				break;
			}
		}
		if (!bFindTable)
		{
			JLOG(ctx.j.trace()) <<
				"Invalid table flags: the dst node does not support the flag";
			return tecNO_TARGET;
		}
	}

    return tesSUCCESS;
}

TER
SqlStatement::doApply ()
{
    if (ctx_.view().flags() & tapFromClient)
    {
        if (TableStorage::STORAGE_DBERROR == ctx_.app.getTableStorage().InitItem(ctx_.tx))
        {
            return terTABLE_STORAGEERROR;
        }
    }

    ripple::uint160  nameInDB;
	//1. clear the TX in this table node
	clearTxInTableNode(ctx_);

	//2. add the new tx to the node
	auto const curTxOwnID = ctx_.tx.getAccountID(sfOwner);
	auto const k = keylet::table(curTxOwnID);
	SLE::pointer pTableSle = view().peek(k);

    auto &aTableEntries = pTableSle->peekFieldArray(sfTableEntries);

    auto const & sTxTables = ctx_.tx.getFieldArray(sfTables);
    std::string sTxTableName = sTxTables[0].getFieldString(sfTableName);
    uint160 uTxDBName = sTxTables[0].getFieldH160(sfNameInDB);

    for (auto & table : aTableEntries)
	{
		if (table.getFieldString(sfTableName) == sTxTableName 
            && table.getFieldH160(sfNameInDB) == uTxDBName 
            && table.getFieldU8(sfDeleted) != 1)
		{
            table.peekFieldV256(sfTxs).push_back(ctx_.tx.getTransactionID());

            if (table.getFieldU32(sfTxnLgrSeq) != ctx_.view().info().seq || table.getFieldH256(sfTxnLedgerHash) != ctx_.view().info().hash)
            {
                table.setFieldU32(sfPreviousTxnLgrSeq, table.getFieldU32(sfTxnLgrSeq));
                table.setFieldH256(sfPrevTxnLedgerHash, table.getFieldH256(sfTxnLedgerHash));

                table.setFieldU32(sfTxnLgrSeq, ctx_.view().info().seq);
                table.setFieldH256(sfTxnLedgerHash, ctx_.view().info().hash);
            }
			break;
		}
	}
    view().update(pTableSle);
    return tesSUCCESS;
}

}  // ripple
