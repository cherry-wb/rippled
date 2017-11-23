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

#ifndef RIPPLE_PROTOCOL_STENTRY_H_INCLUDED
#define RIPPLE_PROTOCOL_STENTRY_H_INCLUDED

#include <ripple/protocol/PublicKey.h>
#include <ripple/protocol/SecretKey.h>
#include <ripple/protocol/STObject.h>
#include <ripple/app/tx/impl/ApplyContext.h>
#include <cstdint>
#include <memory>

namespace ripple { 

    class STEntry final
        : public STObject
        , public CountedObject <STEntry>
    {
    public:
        static char const* getCountedObjectName() { return "STEntry"; }

        using pointer = std::shared_ptr<STEntry>;
        using ref = const std::shared_ptr<STEntry>&;

        enum
        {
            kFullFlag = 0x1
        };

        STEntry();

        void init(ripple::Blob tableName, uint160 nameInDB,uint8 deleted,uint32 TxnLedgerSequence, uint256 txnLedgerhash, uint32 prevTxnLedgerSequence, uint256 prevTxnLedgerhash,std::vector<uint256> hashes, STArray users);

        STBase*
            copy(std::size_t n, void* buf) const override
        {
            return emplace(n, buf, *this);
        }

        STBase*
            move(std::size_t n, void* buf) override
        {
            return emplace(n, buf, std::move(*this));
        }
    private:
        static SOTemplate const& getFormat();
    };

} // ripple

#endif
