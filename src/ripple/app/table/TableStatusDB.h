#ifndef RIPPLE_APP_TABLE_TABLESTATUSDB_H_INCLUDED
#define RIPPLE_APP_TABLE_TABLESTATUSDB_H_INCLUDED

#include <ripple/app/main/Application.h>
#include <ripple/core/DatabaseCon.h>
#include <ripple/protocol/AccountID.h>
#include <ripple/protocol/Protocol.h>

namespace ripple {

class TableStatusDB {
public:
    TableStatusDB(DatabaseCon* dbconn, Application*  app,beast::Journal& journal);
    virtual ~TableStatusDB();

    virtual bool InitDB(DatabaseCon::Setup setup) = 0;

    virtual bool GetAutoListFromDB(bool bAutoSunc, std::list<std::tuple<std::string, std::string, bool> > &list) = 0;

    virtual bool ReadSyncDB(std::string nameInDB, LedgerIndex &txnseq,
        uint256 &txnhash, LedgerIndex &seq, uint256 &hash, uint256 &TxnUpdateHash, bool &bDeleted) = 0;

    virtual bool GetMaxTxnInfo(std::string TableName, std::string Owner, LedgerIndex &TxnLedgerSeq, uint256 &TxnLedgerHash) = 0;

    virtual bool InsertSnycDB(std::string TableName, std::string TableNameInDB, std::string Owner,LedgerIndex LedgerSeq, uint256 LedgerHash, bool IsAutoSync) = 0;

    virtual bool CreateSnycDB(DatabaseCon::Setup setup) = 0;

    virtual bool isNameInDBExist(std::string TableName, std::string Owner, bool delCheck, std::string &TableNameInDB) = 0;

    virtual bool RenameRecord(AccountID accountID, std::string TableNameInDB, std::string TableName) = 0;

    virtual bool UpdateSyncDB(AccountID accountID, std::string TableName, std::string TableNameInDB) = 0;

    virtual bool UpdateStateDB(const std::string & owner, const std::string & tablename, const bool &isAutoSync) = 0;
    
    virtual bool DeleteRecord(AccountID accountID, std::string TableName) =0;

    virtual bool IsExist(AccountID accountID, std::string TableNameInDB) = 0;

    virtual bool UpdateSyncDB(const std::string &Owner, const std::string &TableNameInDB,
        const std::string &TxnLedgerHash, const std::string &TxnLedgerSeq, const std::string &LedgerHash,
        const std::string &LedgerSeq, const std::string &TxHash, const std::string &PreviousCommit) = 0;

    virtual bool UpdateSyncDB(const std::string &Owner, const std::string &TableNameInDB,
            const std::string &LedgerHash, const std::string &LedgerSeq, const std::string &PreviousCommit) = 0;

    virtual bool UpdateSyncDB(const std::string &Owner, const std::string &TableNameInDB,
        const std::string &TxnUpdateHash, const std::string &PreviousCommit) = 0;

    virtual bool UpdateSyncDB(const std::string &Owner, const std::string &TableNameInDB,
        bool bDel, const std::string &PreviousCommit) = 0;

protected:
    DatabaseCon*                                                 databasecon_;
    Application*                                                 app_;
    beast::Journal&                                              journal_;
}; // class TxStoreStatus

}

#endif