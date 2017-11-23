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

#ifndef RIPPLE_PROTOCOL_TABLEDEFINES_H_INCLUDED
#define RIPPLE_PROTOCOL_TABLEDEFINES_H_INCLUDED


enum TableOpType {
    T_CREATE = 1,
    T_DROP = 2,
    T_RENAME = 3,
    T_ASSIGN = 4,
    T_CANCELASSIGN = 5,
    R_INSERT = 6,
    R_GET = 7,
    R_UPDATE = 8,
    R_DELETE = 9
};

enum TableRoleFlags
{
	lsfNone = 0,
	lsfSelect = 0x00010000,
	lsfInsert = 0x00020000,
	lsfUpdate = 0x00040000,
	lsfDelete = 0x00080000,
	lsfExecute = 0x00100000,

	lsfAll = lsfSelect | lsfInsert | lsfUpdate | lsfDelete | lsfExecute,
};
#endif
