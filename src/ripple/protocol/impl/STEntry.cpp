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
#include <ripple/protocol/STEntry.h>
#include <ripple/protocol/HashPrefix.h>
#include <ripple/basics/contract.h>
#include <ripple/basics/Log.h>
#include <ripple/json/to_string.h>
#include <ripple/basics/StringUtilities.h>

namespace ripple {

    STEntry::STEntry()
        : STObject(getFormat(), sfEntry)
    {

    }

    void STEntry::init(ripple::Blob tableName, uint160 nameInDB,uint8 deleted,uint32 txnLedgerSequence, uint256 txnLedgerhash,uint32 prevTxnLedgerSequence,uint256 prevTxnLedgerhash,std::vector<uint256> hashes, STArray users)
    {
        setFieldVL(sfTableName, tableName); //if (tableName == NUll ) then set to null ,no exception
        setFieldH160(sfNameInDB, nameInDB);
        setFieldU8(sfDeleted, deleted);
        setFieldU32(sfTxnLgrSeq, txnLedgerSequence);
        setFieldH256(sfTxnLedgerHash, txnLedgerhash);
        setFieldU32(sfPreviousTxnLgrSeq, prevTxnLedgerSequence);
        setFieldH256(sfPrevTxnLedgerHash, prevTxnLedgerhash);
        setFieldV256(sfTxs, STVector256(hashes));
        setFieldArray(sfUsers, users);
    }

    SOTemplate const& STEntry::getFormat()
    {
        struct FormatHolder
        {
            SOTemplate format;

            FormatHolder()
            {
                format.push_back(SOElement(sfTableName, SOE_REQUIRED));
                format.push_back(SOElement(sfNameInDB, SOE_REQUIRED));
                format.push_back(SOElement(sfDeleted, SOE_REQUIRED));
                format.push_back(SOElement(sfTxnLgrSeq, SOE_REQUIRED));
                format.push_back(SOElement(sfTxnLedgerHash, SOE_REQUIRED));
                format.push_back(SOElement(sfPreviousTxnLgrSeq, SOE_REQUIRED));
                format.push_back(SOElement(sfPrevTxnLedgerHash, SOE_REQUIRED));
                format.push_back(SOElement(sfTxs, SOE_REQUIRED));
                format.push_back(SOElement(sfUsers, SOE_REQUIRED));
            }
        };

        static FormatHolder holder;

        return holder.format;
    }

} // ripple
