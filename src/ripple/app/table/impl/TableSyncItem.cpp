#include <ripple/app/table/TableSyncItem.h>
#include <ripple/app/ledger/LedgerMaster.h>
#include <ripple/app/misc/TxStore.h>
#include <ripple/protocol/STEntry.h>
#include <ripple/core/JobQueue.h>
#include <boost/optional.hpp>
#include <ripple/overlay/Peer.h>
#include <ripple/overlay/Overlay.h>
#include <ripple/app/main/Application.h>
#include <ripple/app/table/TableStatusDB.h>
#include <ripple/protocol/TableDefines.h>
#include <ripple/app/storage/TableStorage.h>
#include <ripple/app/table/TableStatusDBMySQL.h>
#include <ripple/app/table/TableStatusDBSQLite.h>

using namespace std::chrono;
auto constexpr TABLE_DATA_OVERTM = 30s;
auto constexpr LEDGER_DATA_OVERTM = 30s;

#define random(x) (rand()%x)
namespace ripple {

TableSyncItem::~TableSyncItem()
{
}

TableSyncItem::TableSyncItem(Application& app, beast::Journal journal, Config& cfg)
    :app_(app)
    ,journal_(journal)
    ,cfg_(cfg)
    ,operateSqlEvent(true,true)
    ,readDataEvent(true, true)
{   
    eState_               = SYNC_INIT;
    bOperateSQL_          = false;
    bIsChange_            = true;
    bGetLocalData_        = false;
    conn_                 = NULL;
    pObjTxStore_          = NULL;
    sTableNameInDB_       = "";
}

void TableSyncItem::Init(AccountID id, std::string sName,bool isAutoSync)
{
    accountID_ = id;
    sTableName_ = sName;
    bIsAutoSync_ = isAutoSync;
}

void TableSyncItem::ReInit()
{    
    ReSetContex();
    {
        std::lock_guard<std::mutex> lock(mutexInfo_);
        eState_ = SYNC_REINIT;
    }
}

void TableSyncItem::SetPara(std::string sNameInDB,LedgerIndex iSeq, uint256 hash, LedgerIndex TxnSeq, uint256 Txnhash, uint256 TxnUpdateHash)
{    
    sTableNameInDB_ = sNameInDB;
    u32SeqLedger_ = iSeq;
    uHash_ = hash;
    uTxSeq_ = TxnSeq;
    uTxHash_ = Txnhash;
    uTxDBUpdateHash_ = TxnUpdateHash;
}

TxStoreDBConn& TableSyncItem::getTxStoreDBConn()
{
    if (conn_ == NULL)
    {
        conn_ = std::make_unique<TxStoreDBConn>(cfg_);
    }
    return *conn_;
}

TxStore& TableSyncItem::getTxStore()
{
    if (pObjTxStore_ == NULL)
    {
        auto &conn = getTxStoreDBConn();
        pObjTxStore_ = std::make_unique<TxStore>(conn.GetDBConn(),cfg_,journal_);
    }
    return *pObjTxStore_;
}

TableStatusDB& TableSyncItem::getTableStatusDB()
{
    if (pObjTableStatusDB_ == NULL)
    {
        DatabaseCon::Setup setup = ripple::setup_SyncDatabaseCon(cfg_);
        std::pair<std::string, bool> result = setup.sync_db.find("type");

        if (result.first.compare("mysql") == 0)
            pObjTableStatusDB_ = std::make_unique<TableStatusDBMySQL>(getTxStoreDBConn().GetDBConn(), &app_, journal_);
        else
            pObjTableStatusDB_ = std::make_unique<TableStatusDBSQLite>(getTxStoreDBConn().GetDBConn(), &app_, journal_);
    }

    return *pObjTableStatusDB_;
}

bool TableSyncItem::getAutoSync()
{
    return bIsAutoSync_;
}

void TableSyncItem::ReSetContex()
{
    {
        std::lock_guard<std::mutex> lock(mutexInfo_);
        u32SeqLedger_ = uTxSeq_ = 0;
        uHash_.zero();
        uTxHash_.zero();
        uTxDBUpdateHash_.zero();
    }

    {
        std::lock_guard<std::mutex> lock(mutexBlockData_);
        aBlockData_.clear();
    }
    {
        std::lock_guard<std::mutex> lock(mutexWholeData_);
        aWholeData_.clear();
    }
    {
        std::lock_guard<std::mutex> lock(mutexWaitCheckQueue_);
        aWaitCheckData_.clear();
    }
}

void TableSyncItem::ReSetContexAfterDrop()
{   
    {
        std::lock_guard<std::mutex> lock(mutexInfo_);
        sTableNameInDB_.clear();
        eState_ = SYNC_DELETED;
    }

    ReSetContex();
    
}

void TableSyncItem::SetIsDataFromLocal(bool bLocal)
{
    bGetLocalData_ = bLocal;
}
void TableSyncItem::PushDataByOrder(std::list <sqldata_type> &aData, sqldata_type &sqlData)
{
    if (aData.size() == 0)
    {
        aData.push_back(sqlData);
        return;
    }
    else if (sqlData.first > aData.rbegin()->first)
    {
        aData.push_back(sqlData);
        return;
    }
    else
    {
        if (aData.size() == 1)
        {
            aData.push_front(sqlData);
            return;
        }
        else
        {
            for (auto it = aData.begin(); it != aData.end(); it++)
            {
                if (sqlData.first == it->first)  return;
                else if (sqlData.first > it->first)
                {
                    aData.insert(it++, sqlData);
                    return;
                }
            }
        }
    }
}

void TableSyncItem::DealWithWaitCheckQueue(std::function<bool (sqldata_type const&)> f)
{
    std::lock_guard<std::mutex> lock(mutexWaitCheckQueue_);
    for (auto it = aWaitCheckData_.begin(); it != aWaitCheckData_.end(); it++)
    {
        f(*it);
    }
    aWaitCheckData_.clear();
}

void TableSyncItem::PushDataToWaitCheckQueue(sqldata_type &sqlData)
{
    std::lock_guard<std::mutex> lock(mutexWaitCheckQueue_);
    PushDataByOrder(aWaitCheckData_, sqlData);
}
void TableSyncItem::PushDataToBlockDataQueue(sqldata_type &sqlData)
{
    std::lock_guard<std::mutex> lock(mutexBlockData_);
    
    PushDataByOrder(aBlockData_, sqlData);
}

bool TableSyncItem::GetRightRequestRange(TableSyncItem::BaseInfo &stRange)
{
    std::lock_guard<std::mutex> lock(mutexBlockData_);

    if (aBlockData_.size() > 0)
    {
        LedgerIndex iBegin = u32SeqLedger_; 
        LedgerIndex iCheckSeq = uTxSeq_;
        uint256 uHash = uHash_;
        uint256 uCheckHash = uTxHash_;

        for (auto it = aBlockData_.begin(); it != aBlockData_.end(); it++)
        {
            if (it->second.seekstop())
            {
                if (iBegin == it->second.lastledgerseq())
                {
                    stRange.u32SeqLedger = 0;
                    stRange.uHash = uint256();
                    stRange.uStopSeq = 0;
                    stRange.uTxSeq = 0;
                    stRange.uTxHash = uint256();;
                    return true;
                }
                else
                {
                    stRange.u32SeqLedger = iBegin;
                    stRange.uHash = uHash;
                    stRange.uStopSeq = it->second.ledgerseq() - 1;
                    stRange.uTxSeq = iCheckSeq;
                    stRange.uTxHash = uCheckHash;
                    return true;
                }                
            }
            
            if (iBegin == it->second.lastledgerseq())
            {
                iBegin = it->second.ledgerseq();
                iCheckSeq = it->second.ledgerseq();
                uHash = from_hex_text<uint256>(it->second.ledgerhash()); 
                uCheckHash = from_hex_text<uint256>(it->second.ledgercheckhash());
            }
            else
            {
                stRange.u32SeqLedger = iBegin;
                stRange.uHash = uHash;
                stRange.uStopSeq = it->second.ledgerseq() - 1;
                stRange.uTxSeq = iCheckSeq;
                stRange.uTxHash = uCheckHash;
                return true;
            }
        }

        stRange.u32SeqLedger = iBegin;
        stRange.uHash = uHash;
        stRange.uStopSeq = (u32SeqLedger_ + 255) & (~255);
        stRange.uTxSeq = iCheckSeq;
        stRange.uTxHash = uCheckHash;
        return true;
    }

    stRange.u32SeqLedger = u32SeqLedger_;
    stRange.uHash = uHash_;
    stRange.uStopSeq = (u32SeqLedger_ + 1 + 255) & (~255);
    stRange.uTxSeq = uTxSeq_;
    stRange.uTxHash = uTxHash_;

    return true;
}

bool TableSyncItem::TransBlock2Whole(LedgerIndex iSeq, uint256)
{
    std::lock_guard<std::mutex> lock1(mutexBlockData_);
    std::lock_guard<std::mutex> lock2(mutexWholeData_);
    auto iBegin = iSeq;    

    bool bHasStop = false;
    while(aBlockData_.size() > 0)
    {
        auto it = aBlockData_.begin();
        if (iBegin == it->second.lastledgerseq())
        {
            uint256 uCurhash = from_hex_text<uint256>(it->second.ledgerhash());  
            uint256 uCheckhash = from_hex_text<uint256>(it->second.ledgercheckhash());
            iBegin = it->second.ledgerseq();            
            bHasStop = it->second.seekstop();

            SetSyncLedger(iBegin, uCurhash);
            if (it->second.txnodes().size() > 0)
            {
                SetSyncTxLedger(iBegin, uCheckhash);
            }            
            aWholeData_.push_back(std::move(*it));
            aBlockData_.erase(it);
        }        
        else
        {
            break;
        }
    }
    bool bStop = aBlockData_.size() == 0 && bHasStop;
    if (bStop && !bGetLocalData_)
    {        
        SetSyncState(SYNC_BLOCK_STOP);
    }
    return bStop;
}

bool TableSyncItem::IsGetLedgerExpire()
{
    if (duration_cast<seconds>(steady_clock::now() - clock_ledger_) > LEDGER_DATA_OVERTM)
    {
        auto iter = std::find_if(lfailList_.begin(), lfailList_.end(),
            [this](beast::IP::Endpoint &item) {
            return item == uPeerAddr_;
        });
        if (iter == lfailList_.end())
        {
            lfailList_.push_back(uPeerAddr_);
        }

        bIsChange_ = true;
        return true;
    }
    return false;
}

bool TableSyncItem::IsGetDataExpire()
{    
    if (duration_cast<seconds>(steady_clock::now() - clock_data_) > TABLE_DATA_OVERTM)
    {
        auto iter = std::find_if(lfailList_.begin(), lfailList_.end(),
            [this](beast::IP::Endpoint &item) {
            return item == uPeerAddr_;
        });
        if (iter == lfailList_.end())
        {
            lfailList_.push_back(uPeerAddr_);
        }

        bIsChange_ = true;
        return true;
    }
    return false;
}

void TableSyncItem::UpdateLedgerTm()
{
    clock_ledger_ = steady_clock::now();
}
void TableSyncItem::UpdateDataTm()
{
    clock_data_ = steady_clock::now();
}

AccountID TableSyncItem::GetAccount()
{
    return accountID_;
}

std::string TableSyncItem::GetTableName()
{
    return sTableName_;
}

std::mutex &TableSyncItem::WriteDataMutex()
{
    return this->mutexWriteData_;
}

void TableSyncItem::GetSyncLedger(LedgerIndex &iSeq, uint256 &uHash)
{
    std::lock_guard<std::mutex> lock(mutexInfo_);
    iSeq = u32SeqLedger_;
    uHash = uHash_;
}

void TableSyncItem::GetSyncTxLedger(LedgerIndex &iSeq, uint256 &uHash)
{
    std::lock_guard<std::mutex> lock(mutexInfo_);
    iSeq = uTxSeq_;
    uHash = uTxHash_;
}

TableSyncItem::TableSyncState TableSyncItem::GetSyncState()
{
    std::lock_guard<std::mutex> lock(mutexInfo_);
    return eState_;
}

void TableSyncItem::GetBaseInfo(BaseInfo &stInfo)
{
    std::lock_guard<std::mutex> lock(mutexInfo_);
    stInfo.accountID                = accountID_;

    stInfo.sTableNameInDB           = sTableNameInDB_;
    stInfo.sTableName               = sTableName_;
    stInfo.u32SeqLedger             = u32SeqLedger_;
    stInfo.uHash                    = uHash_;
    stInfo.uTxSeq                   = uTxSeq_;
    stInfo.uTxHash                  = uTxHash_;
    stInfo.eState                   = eState_;
    stInfo.lState                   = lState_;
    stInfo.isAutoSync               = bIsAutoSync_;
}

void TableSyncItem::SetSyncLedger(LedgerIndex iSeq, uint256 uHash)
{
    std::lock_guard<std::mutex> lock(mutexInfo_);
    u32SeqLedger_ = iSeq;
    uHash_ = uHash;
}

void TableSyncItem::SetSyncTxLedger(LedgerIndex iSeq, uint256 uHash)
{
    std::lock_guard<std::mutex> lock(mutexInfo_);
    uTxSeq_ = iSeq;
    uTxHash_ = uHash;
}

void TableSyncItem::SetSyncState(TableSyncState eState)
{
    std::lock_guard<std::mutex> lock(mutexInfo_);
    if(eState_ != SYNC_STOP)    eState_ = eState;
}

void TableSyncItem::SetLedgerState(LedgerSyncState lState)
{
    std::lock_guard<std::mutex> lock(mutexInfo_);
    lState_ = lState;
}

bool TableSyncItem::IsInFailList(beast::IP::Endpoint& peerAddr)
{
    bool ret = false;

    auto iter = std::find_if(lfailList_.begin(), lfailList_.end(),
        [peerAddr](beast::IP::Endpoint &item) {
        return item == peerAddr;
    });

    if (iter != lfailList_.end())
        ret = true;
    return ret;
}


std::shared_ptr<Peer> TableSyncItem::GetRightPeerTarget(LedgerIndex iSeq)
{  
    auto peerList = app_.overlay().getActivePeers();

    bool isChange = GetIsChange();
    if (!isChange)
    {
        for (auto const& peer : peerList)
        {
            if (uPeerAddr_ == peer->getRemoteAddress())
                return peer;
        }
    }  

    std::shared_ptr<Peer> target = NULL;

    if (peerList.size() > 0)
    {
        int iRandom = random(peerList.size());

        for (int i = 0; i < peerList.size(); i++)
        {
            int iRelIndex = (iRandom + i) % peerList.size();
            auto peer = peerList.at(iRelIndex);
            if (peer == NULL) continue;

            auto addrTmp = peer->getRemoteAddress();
            if (IsInFailList(addrTmp))
                continue;
            target = peer;
            SetPeer(peer);
            break;
        }
    }

    if (!target)
    {
        lfailList_.clear();
        if (peerList.size() > 0)
        { 
            target = peerList.at(0);
            SetPeer(target);
        }            
    }

    return target;
}


void TableSyncItem::ClearFailList()
{
    lfailList_.clear();
}
void TableSyncItem::SendTableMessage(Message::pointer const& m)
{
    auto peer = GetRightPeerTarget(u32SeqLedger_);
    if(peer != NULL)
        peer->send(m);
}

TableSyncItem::LedgerSyncState TableSyncItem::GetCheckLedgerState()
{
    std::lock_guard<std::mutex> lock(mutexInfo_);
    return lState_;
}
void TableSyncItem::SetTableName(std::string sName)
{
    std::lock_guard<std::mutex> lock(mutexInfo_);
    sTableName_ = sName;
}
std::string TableSyncItem::TableNameInDB()
{
    return sTableNameInDB_;
}

void  TableSyncItem::SetTableNameInDB(uint160 NameInDB)
{
    sTableNameInDB_ = to_string(NameInDB);
}

void  TableSyncItem::SetTableNameInDB(std::string sNameInDB)
{
    sTableNameInDB_ = sNameInDB;
}

void TableSyncItem::TryOperateSQL()
{
    if (bOperateSQL_)    return;

    bOperateSQL_ = true;

    operateSqlEvent.reset();
    app_.getJobQueue().addJob(jtOPERATESQL, "operateSQL", [this](Job&) { OperateSQLThread(); });
}

bool TableSyncItem::IsExist(AccountID accountID,  std::string TableNameInDB)
{    
    return app_.getTableStatusDB().IsExist(accountID, TableNameInDB);
}

bool TableSyncItem::IsNameInDBExist(std::string TableName, std::string Owner, bool delCheck, std::string &TableNameInDB)
{
    return app_.getTableStatusDB().isNameInDBExist(TableName, Owner, delCheck, TableNameInDB);
}

bool TableSyncItem::DeleteRecord(AccountID accountID, std::string TableName)
{
    return app_.getTableStatusDB().DeleteRecord(accountID, TableName);
}

bool TableSyncItem::GetMaxTxnInfo(std::string TableName, std::string Owner, LedgerIndex &TxnLedgerSeq, uint256 &TxnLedgerHash)
{
    return app_.getTableStatusDB().GetMaxTxnInfo(TableName, Owner, TxnLedgerSeq, TxnLedgerHash);
}

bool TableSyncItem::DeleteTable(std::string nameInDB)
{
    auto ret = app_.getTxStore().DropTable(nameInDB);
    return ret.first;
}

bool TableSyncItem::RenameRecord(AccountID accountID, std::string TableNameInDB, std::string TableName)
{
    return app_.getTableStatusDB().RenameRecord(accountID, TableNameInDB, TableName);
}

bool TableSyncItem::UpdateSyncDB(AccountID accountID, std::string TableName, std::string TableNameInDB)
{    
    return app_.getTableStatusDB().UpdateSyncDB(accountID, TableName, TableNameInDB);
}

bool TableSyncItem::UpdateStateDB(const std::string & owner, const std::string & tablename, const bool &isAutoSync)
{
    return app_.getTableStatusDB().UpdateStateDB(owner, tablename, isAutoSync);
}

bool TableSyncItem::DoUpdateSyncDB(AccountID accountID, std::string TableName, std::string TableNameInDB)
{
    return false;
    /*
    bool ret = false;
    std::string Owner = to_string(accountID);
    try
    {
        if (IsExist(accountID, TableName,TableNameInDB))
        {
            //delete this item,item's TableNameInDB == TableNameInDB
            DeleteRecord(accountID, TableName);
            //rename remain item's tablename = TableName
            RenameRecord(accountID, TableNameInDB, TableName);
        }
        else
        {
            ret = UpdateSyncDB(accountID,TableName,TableNameInDB);
        }
    }
    catch (std::exception const& e)
    {
        JLOG(journal_.error()) <<
            "DoUpdateSyncDB exception" << e.what();
    }
    return ret;
    */
}

bool TableSyncItem::DoUpdateSyncDB(const std::string &Owner, const std::string &TableNameInDB, const std::string &LedgerHash,
    const std::string &LedgerSeq, const std::string &PreviousCommit)
{
    return app_.getTableStatusDB().UpdateSyncDB(Owner, TableNameInDB, LedgerHash, LedgerSeq, PreviousCommit);;
}

bool TableSyncItem::DoUpdateSyncDB(const std::string &Owner, const std::string &TableNameInDB, const std::string &TxnFailHash, 
    const std::string &PreviousCommit)
{
    return app_.getTableStatusDB().UpdateSyncDB(Owner, TableNameInDB, TxnFailHash, PreviousCommit);
}

bool TableSyncItem::DoUpdateSyncDB(const std::string &Owner, const std::string &TableNameInDB, bool bDel,
    const std::string &PreviousCommit)
{
    return app_.getTableStatusDB().UpdateSyncDB(Owner, TableNameInDB, bDel, PreviousCommit);
}

bool TableSyncItem::DoUpdateSyncDB(const std::string &Owner, const std::string &TableNameInDB, const std::string &TxnLedgerHash,
    const std::string &TxnLedgerSeq, const std::string &LedgerHash, const std::string &LedgerSeq, const std::string &TxHash, 
    const std::string &PreviousCommit)
{
    return app_.getTableStatusDB().UpdateSyncDB(Owner, TableNameInDB, TxnLedgerHash, TxnLedgerSeq, LedgerHash, 
        LedgerSeq, TxHash, PreviousCommit);
}

void TableSyncItem::OperateSQLThread()
{    
    std::vector<protocol::TMTableData> vec_tmdata;
    std::list <sqldata_type> list_tmdata;
    {
        std::lock_guard<std::mutex> lock(mutexWholeData_);
        for (std::list<sqldata_type>::iterator iter = aWholeData_.begin(); iter != aWholeData_.end(); ++iter)
        {
            vec_tmdata.push_back((*iter).second);
        }
        aWholeData_.clear();
    }
    for (std::vector<protocol::TMTableData>::iterator iter = vec_tmdata.begin();iter!= vec_tmdata.end(); ++iter)
    { 
        std::string Owner = iter->account();
        std::string TableName  = iter->tablename();
        std::string LedgerHash = iter->ledgerhash();
        std::string LedgerCheckHash = iter->ledgercheckhash();
        std::string LedgerSeq = to_string(iter->ledgerseq());
        std::string PreviousCommit;

        if (iter->txnodes().size() > 0)
        {  
            int count = 0; //count success times
            bool bFindLastSuccessTx = uTxDBUpdateHash_.isZero() ? true : false;

            for (int i = 0; i < iter->txnodes().size(); i++)
            {
                const protocol::TMLedgerNode &node = iter->txnodes().Get(i);
                auto str = node.nodedata();
                Blob blob;
                blob.assign(str.begin(), str.end());
                STTx const& tx = (STTx)(SerialIter{ blob.data(), blob.size() });

                JLOG(journal_.warn()) <<
                    "got sync tx" << tx.getFullText();

                if (!bFindLastSuccessTx)
                {
                    if (tx.getTransactionID() == uTxDBUpdateHash_)
                    {
                        bFindLastSuccessTx = true;
                    }
                    count++;
                    continue;
                }

                try {

                    LockedSociSession sql_session = getTxStoreDBConn().GetDBConn()->checkoutDb();
                    TxStoreTransaction stTran(&getTxStoreDBConn());
                    auto ret = this->getTxStore().Dispose(tx);
                    if (ret.first)
                    {
                        JLOG(journal_.trace()) << "Dispose success";
                    }
                    else
                        JLOG(journal_.trace()) << "Dispose error";

                    uTxDBUpdateHash_ = tx.getTransactionID();
                    getTableStatusDB().UpdateSyncDB(Owner, TableName, to_string(uTxDBUpdateHash_), PreviousCommit);
                    stTran.commit();
                }
                catch (std::exception const& e)
                {
                    JLOG(journal_.error()) <<
                        "Dispose exception" << e.what();
                }
                auto op_type = tx.getFieldU16(sfOpType);
                if (T_DROP == op_type)
                {                    
                    DoUpdateSyncDB(Owner, TableName, true, PreviousCommit);                    
                    this ->ReSetContexAfterDrop();
                }
                else if (T_RENAME == op_type)
                {
                    auto tables = tx.getFieldArray(sfTables);
                    if (tables.size() > 0)
                    {
                        auto tableBlob = tables[0].getFieldVL(sfTableNewName);
                        std::string newTableName;
                        newTableName.assign(tableBlob.begin(), tableBlob.end());
                        AccountID ownerID(*ripple::parseBase58<AccountID>(Owner));
                        app_.getTableStatusDB().RenameRecord(ownerID, TableName, newTableName);
                    }                    
                }
                count++;
            }

            if (count == iter->txnodes().size())
            {
                uTxDBUpdateHash_.zero();
                DoUpdateSyncDB(Owner, TableName, LedgerCheckHash, LedgerSeq, LedgerHash, LedgerSeq, to_string(uTxDBUpdateHash_), PreviousCommit);
            }

            if(uTxDBUpdateHash_.isNonZero())
                SetSyncState(SYNC_STOP);
        }
        else
        {
            DoUpdateSyncDB(Owner, TableName, LedgerHash, LedgerSeq,PreviousCommit);
            JLOG(journal_.trace()) <<
                "DoUpdateSyncDB LedgerSeq " << LedgerSeq;
        }
    }
    bOperateSQL_ = false;
    operateSqlEvent.signal();
}

void TableSyncItem::PushDataToWholeDataQueue(sqldata_type &sqlData)
{
    std::lock_guard<std::mutex> lock(mutexWholeData_);
    aWholeData_.push_back(sqlData);        
    
    if (sqlData.second.txnodes().size() > 0)
    {
        SetSyncLedger(sqlData.first, from_hex_text<uint256>(sqlData.second.ledgerhash()));
        SetSyncTxLedger(sqlData.first, from_hex_text<uint256>(sqlData.second.ledgercheckhash()));
    }
    else
    {
        SetSyncLedger(sqlData.first, from_hex_text<uint256>(sqlData.second.ledgerhash()));
    }
    
    if (sqlData.second.seekstop() && !bGetLocalData_)
    {
        SetSyncState(TableSyncItem::SYNC_BLOCK_STOP);
    }
}

bool TableSyncItem::GetIsChange()
{
    return  bIsChange_;
}

void TableSyncItem::PushDataToWholeDataQueue(std::list <sqldata_type>  &aSqlData)
{
    if (aSqlData.size() <= 0)  return;
    std::lock_guard<std::mutex> lock(mutexWholeData_);
    for (std::list<sqldata_type>::iterator it = aSqlData.begin(); it != aSqlData.end(); it++)
    {
        aWholeData_.push_back(*it);
    }    

    auto &lastItem = aSqlData.back();
    SetSyncLedger(lastItem.first, from_hex_text<uint256>(lastItem.second.ledgerhash()));

    if (lastItem.second.seekstop() && !bGetLocalData_)
    {
        SetSyncState(TableSyncItem::SYNC_BLOCK_STOP);
    }
}

void TableSyncItem::SetPeer(std::shared_ptr<Peer> peer)
{ 
    std::lock_guard<std::mutex> lock(mutexInfo_);
    bIsChange_ = false;
    uPeerAddr_ = peer->getRemoteAddress();
}

void TableSyncItem::StartLocalLedgerRead()
{
    readDataEvent.reset();
}
void TableSyncItem::StopLocalLedgerRead()
{
    readDataEvent.signal();
}

bool TableSyncItem::StopSync()
{
    SetSyncState(SYNC_STOP);

    bool bRet = false;
    bRet = readDataEvent.wait(1000);
    if (!bRet)
    {
        SetSyncState(SYNC_BLOCK_STOP);
        return false;        
    }

    bRet = operateSqlEvent.wait(2000);
    if (!bRet)
    {
        SetSyncState(SYNC_BLOCK_STOP);
        return false;
    }

    int iSize = 0;
    {
        std::lock_guard<std::mutex> lock(mutexWholeData_);
        aWholeData_.size();
    }
    iSize = aWholeData_.size();
    if (iSize > 0)
    {
        TryOperateSQL();
        bRet = operateSqlEvent.wait(2000);
        if (!bRet)
        {
            SetSyncState(SYNC_BLOCK_STOP);
            return false;
        }
    }

    return true;
}

}