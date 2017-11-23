#ifndef RIPPLE_APP_TABLE_TABLESTATUSDBMYSQL_H_INCLUDED
#define RIPPLE_APP_TABLE_TABLESTATUSDBMYSQL_H_INCLUDED

#include <ripple/app/table/TableStatusDB.h>

namespace ripple {

class TableStatusDBMySQL : public TableStatusDB
{
public:
    TableStatusDBMySQL(DatabaseCon* dbconn, Application * app, beast::Journal& journal);
    ~TableStatusDBMySQL();

    bool InitDB(DatabaseCon::Setup setup);

    bool ReadSyncDB(std::string nameInDB, LedgerIndex &txnseq,
        uint256 &txnhash, LedgerIndex &seq, uint256 &hash, uint256 &txnupdatehash, bool &bDeleted);

    bool GetMaxTxnInfo(std::string TableName, std::string Owner, LedgerIndex &TxnLedgerSeq, uint256 &TxnLedgerHash);

    bool InsertSnycDB(std::string TableName, std::string TableNameInDB, std::string Owner, LedgerIndex LedgerSeq, uint256 LedgerHash, bool IsAutoSync);

    bool CreateSnycDB(DatabaseCon::Setup setup);

    bool isNameInDBExist(std::string TableName, std::string Owner, bool delCheck, std::string &TableNameInDB) ;

    bool RenameRecord(AccountID accountID, std::string TableNameInDB, std::string TableName);

    bool UpdateSyncDB(AccountID accountID, std::string TableName, std::string TableNameInDB);

    bool DeleteRecord(AccountID accountID, std::string TableName);

    bool IsExist(AccountID accountID, std::string TableNameInDB);

    bool UpdateSyncDB(const std::string &Owner, const std::string &TableNameInDB,
        const std::string &TxnLedgerHash, const std::string &TxnLedgerSeq, const std::string &LedgerHash,
        const std::string &LedgerSeq, const std::string &TxnUpdateHash, const std::string &PreviousCommit);

    bool UpdateSyncDB(const std::string &Owner, const std::string &TableNameInDB,
        const std::string &LedgerHash, const std::string &LedgerSeq, const std::string &PreviousCommit);

    bool UpdateSyncDB(const std::string &Owner, const std::string &TableNameInDB,
        const std::string &TxnUpdateHash, const std::string &PreviousCommit);

    bool UpdateSyncDB(const std::string &Owner, const std::string &TableNameInDB,
        bool bDel, const std::string &PreviousCommit);

    bool GetAutoListFromDB(bool bAutoSunc, std::list<std::tuple<std::string, std::string, bool> > &tuple);

    bool UpdateStateDB(const std::string & owner, const std::string & tablename, const bool &isAutoSync);
};

}
#endif