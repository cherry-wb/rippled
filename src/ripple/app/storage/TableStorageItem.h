#ifndef RIPPLE_APP_TABLE_TABLESTORAGE_ITEM_H_INCLUDED
#define RIPPLE_APP_TABLE_TABLESTORAGE_ITEM_H_INCLUDED

#include <ripple/app/misc/TxStore.h>
namespace ripple {

class TableStorageItem
{
    enum TableStorageFlag
    {
        STORAGE_NONE,
        STORAGE_ROLLBACK,
        STORAGE_COMMIT
    };

    typedef struct txInfo_
    {
        AccountID                                                    accountID;
        uint256                                                      uTxHash;
        LedgerIndex                                                  uTxLedgerVersion;        
        bool                                                         bCommit;
    }txInfo;

public:   
    TableStorageItem(Application& app, Config& cfg, beast::Journal journal);
    void InitItem(AccountID account ,std::string nameInDB, std::string tableName);
    void SetItemParam(LedgerIndex txnLedgerSeq, uint256 txnHash, LedgerIndex LedgerSeq, uint256 ledgerHash);
    virtual ~TableStorageItem();
    
    bool PutElem(STTx const& tx);
    bool doJob(LedgerIndex CurLedgerVersion);
 
    TxStore& getTxStore();
private: 
    bool rollBack();
    bool commit();
    void Put(STTx const& tx);
    bool CheckExistInLedger(LedgerIndex CurLedgerVersion);

    TableStorageFlag CheckSuccessive(LedgerIndex validatedIndex);
   
    TxStoreDBConn& getTxStoreDBConn();
    TxStoreTransaction& getTxStoreTrans();
   
    TableStatusDB& getTableStatusDB();

    STEntry *GetTableEntry(const STArray& aTables, LedgerIndex iLastSeq, AccountID accountID, std::string TableNameInDB, bool bStrictEqual);
private:
    std::list<txInfo>                                                           txList_;
    std::shared_ptr <TxStoreDBConn>                                             dbconn_;
    std::unique_ptr <TxStoreTransaction>                                        uTxStoreTrans_;
    std::string                                                                 sTableNameInDB_;
    std::string                                                                 sTableName_;
    AccountID                                                                   accountID_;

    std::unique_ptr <TxStoreDBConn>                                             conn_;
    std::unique_ptr <TxStore>                                                   pObjTxStore_;
    std::unique_ptr <TableStatusDB>                                             pObjTableStatusDB_;

    uint256                                                                    txnHash_;
    LedgerIndex                                                                txnLedgerSeq_;
    uint256                                                                    ledgerHash_;
    LedgerIndex                                                                LedgerSeq_;
    uint256                                                                    txUpdateHash_;

    Application&                                                                app_;
    beast::Journal                                                              journal_;
    Config&                                                                     cfg_;
};
}
#endif

