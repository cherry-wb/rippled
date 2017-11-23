#ifndef RIPPLE_APP_TABLE_TABLESYNC_ITEM_H_INCLUDED
#define RIPPLE_APP_TABLE_TABLESYNC_ITEM_H_INCLUDED

#include <ripple/beast/utility/PropertyStream.h>
#include <ripple/app/main/Application.h>
#include <ripple/beast/clock/abstract_clock.h>
#include <ripple/beast/core/WaitableEvent.h>
#include <ripple/protocol/AccountID.h>
#include <ripple/protocol/Protocol.h>
#include <ripple/overlay/Peer.h>

namespace ripple {

//class Peer;
class TableSyncItem
{
public:
    using clock_type = beast::abstract_clock <std::chrono::steady_clock>;
    using sqldata_type = std::pair<LedgerIndex, protocol::TMTableData>;

    enum TableSyncState
    {        
        SYNC_INIT,
        SYNC_REINIT,
        SYNC_WAIT_DATA,        
        SYNC_BLOCK_STOP,

        SYNC_WAIT_LOCAL_ACQUIRE,
        SYNC_LOCAL_ACQUIRING,

        SYNC_DELETED,
        SYNC_STOP
    };

    enum LedgerSyncState
    {
        SYNC_NO_LEDGER,
        SYNC_WAIT_LEDGER,
        SYNC_GOT_LEDGER
    };

    struct BaseInfo
    {
        AccountID                                                    accountID;
        std::string                                                  sTableName;
        std::string                                                  sTableNameInDB;

        LedgerIndex                                                  u32SeqLedger; 
        uint256                                                      uHash;

        LedgerIndex                                                  uTxSeq ;
        uint256                                                      uTxHash;

        LedgerIndex                                                  uStopSeq;

        TableSyncState                                               eState;
        LedgerSyncState                                              lState;
        
        bool                                                         isAutoSync;
    };
public:
        
    TableSyncItem(Application& app, beast::Journal journal,Config& cfg);
    virtual ~TableSyncItem();

    TxStoreDBConn& getTxStoreDBConn();
    TxStore& getTxStore();

    void ReInit();
    void Init(AccountID id, std::string sName,bool isAutoSync);
    void SetPara(std::string sNameInDB, LedgerIndex iSeq, uint256 hash, LedgerIndex TxnSeq, uint256 Txnhash, uint256 TxnUpdateHash);
    void SetPeer(std::shared_ptr<Peer> peer);

    bool IsGetLedgerExpire();
    bool IsGetDataExpire();
    void UpdateLedgerTm();
    void UpdateDataTm();

    void StartLocalLedgerRead();
    void StopLocalLedgerRead();
    bool StopSync();

    AccountID                GetAccount();
    std::string              GetTableName();    
    TableSyncState           GetSyncState();

    bool getAutoSync();
   
    std::shared_ptr<Peer> GetRightPeerTarget(LedgerIndex iSeq);
            
    void GetBaseInfo(BaseInfo &stInfo);
    void GetSyncLedger(LedgerIndex &iSeq, uint256 &uHash);
    void GetSyncTxLedger(LedgerIndex &iSeq, uint256 &uHash);
    LedgerSyncState GetCheckLedgerState();
     
    void SendTableMessage(Message::pointer const& m);

    void SetTableName(std::string sName);
    void SetSyncLedger(LedgerIndex iSeq, uint256 uHash);
    void SetSyncTxLedger(LedgerIndex iSeq, uint256 uHash);

    bool IsExist(AccountID accountID, std::string TableNameInDB);

    bool DeleteRecord(AccountID accountID, std::string TableName);
    bool DeleteTable(std::string nameInDB);

    bool GetMaxTxnInfo(std::string TableName, std::string Owner, LedgerIndex &TxnLedgerSeq, uint256 &TxnLedgerHash);

    bool RenameRecord(AccountID accountID, std::string TableNameInDB,std::string TableName);

    void SetSyncState(TableSyncState eState);
    void SetLedgerState(LedgerSyncState lState);

    bool IsInFailList(beast::IP::Endpoint& peerAddr);
    
    void TryOperateSQL();
    void OperateSQLThread();

    void PushDataToWaitCheckQueue(sqldata_type &sqlData);
    void DealWithWaitCheckQueue(std::function<bool(sqldata_type const&)>);

    bool GetRightRequestRange(TableSyncItem::BaseInfo &stRange);
    void PushDataToBlockDataQueue(sqldata_type &sqlData);
    bool TransBlock2Whole(LedgerIndex iSeq, uint256 uHash);

    void PushDataToWholeDataQueue(sqldata_type &sqlData);
    void PushDataToWholeDataQueue(std::list <sqldata_type>  &aSqlData);

    bool IsNameInDBExist(std::string TableName, std::string Owner, bool delCheck, std::string &TableNameInDB);
    bool DoUpdateSyncDB(AccountID accountID, std::string tableName, std::string nameInDB);

    bool UpdateSyncDB(AccountID accountID, std::string tableName, std::string nameInDB);

    bool UpdateStateDB(const std::string & owner, const std::string & tablename, const bool &isAutoSync);

    bool DoUpdateSyncDB(const std::string &Owner, const std::string &TableNameInDB, const std::string &LedgerHash, const std::string &LedgerSeq, const std::string &PreviousCommit);

    bool DoUpdateSyncDB(const std::string &Owner, const std::string &TableName, const std::string &TxnLedgerHash, const std::string &TxnLedgerSeq, const std::string &LedgerHash, const std::string &LedgerSeq, const std::string &TxHash, const std::string &PreviousCommit);

    bool DoUpdateSyncDB(const std::string &Owner, const std::string &TableNameInDB, const std::string &TxHash, const std::string &PreviousCommit);

    bool DoUpdateSyncDB(const std::string &Owner, const std::string &TableNameInDB, bool bDel,
        const std::string &PreviousCommit);
    
    std::string TableNameInDB();
    void SetTableNameInDB(uint160 NameInDB);
    void SetTableNameInDB(std::string sNameInDB);

    void SetIsDataFromLocal(bool bLocal);
    void ReSetContexAfterDrop();

    void ClearFailList();

    std::mutex &WriteDataMutex();
private:
    bool GetIsChange();
    void PushDataByOrder(std::list <sqldata_type> &aData, sqldata_type &sqlData);
    void ReSetContex();
    
    TableStatusDB& getTableStatusDB();

private:
    AccountID                                                    accountID_;
    std::string                                                  sTableName_;
    std::string                                                  sTableNameInDB_;
    LedgerIndex                                                  u32SeqLedger_;  //seq of ledger, last syned ledger seq
    uint256                                                      uHash_;
    uint256                                                      uTxDBUpdateHash_;

    LedgerIndex                                                  uTxSeq_;
    uint256                                                      uTxHash_;

    TableSyncState                                               eState_;
    LedgerSyncState                                              lState_;

    std::mutex                                                   mutexInfo_;
    std::mutex                                                   mutexWriteData_;
private:
    std::chrono::steady_clock::time_point                        clock_data_;
    std::chrono::steady_clock::time_point                        clock_ledger_;        

    std::list <sqldata_type>                                     aBlockData_;
    std::mutex                                                   mutexBlockData_;

    std::list <sqldata_type>                                     aWholeData_;
    std::mutex                                                   mutexWholeData_;   

    std::list <sqldata_type>                                     aWaitCheckData_;
    std::mutex                                                   mutexWaitCheckQueue_;
    
    bool                                                         bOperateSQL_;

    bool                                                         bGetLocalData_;

    beast::IP::Endpoint                                          uPeerAddr_;
    std::list <beast::IP::Endpoint>                              lfailList_;
    bool                                                         bIsChange_;

    bool                                                         bIsAutoSync_;

    std::unique_ptr <TxStoreDBConn>                              conn_;
    std::unique_ptr <TxStore>                                    pObjTxStore_;
    std::unique_ptr <TableStatusDB>                              pObjTableStatusDB_;
  
    Application &                                                app_;
    beast::Journal                                               journal_;
    Config&                                                      cfg_;
    
    beast::WaitableEvent                                         operateSqlEvent;
    beast::WaitableEvent                                         readDataEvent;
};

}
#endif

