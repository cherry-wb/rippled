#include <ripple/app/storage/TableStorage.h>
#include <ripple/core/JobQueue.h>
#include <ripple/app/ledger/LedgerMaster.h>
#include <ripple/basics/base_uint.h>
#include <ripple/app/ledger/TransactionMaster.h>
#include <ripple/protocol/TableDefines.h>
#include <ripple/app/table/TableStatusDBMySQL.h>
#include <ripple/app/table/TableStatusDBSQLite.h>

#define MAX_GAP_NOW2VALID  5
namespace ripple {
    TableStorage::TableStorage(Application& app, Config& cfg, beast::Journal journal)
        : app_(app)
        , journal_(journal)
        , cfg_(cfg)
    {
        DatabaseCon::Setup setup = ripple::setup_SyncDatabaseCon(cfg_);
        if (setup.sync_db.size() == 0)
            m_IsHaveStorage = false;
        else
            m_IsHaveStorage = true;

        std::pair<std::string, bool> result = setup.sync_db.find("firstStorage");

        if (!result.second)
            m_IsStorageOn = true;
        else
        {
            if (result.first.compare("1") == 0)
                m_IsStorageOn = true;
            else
                m_IsStorageOn = false;
        }

        
        bTableStorageThread_ = false;
    }

    TableStorage::~TableStorage()
    {
    }

    void TableStorage::SetHaveSyncFlag(bool flag)
    {
        m_IsHaveStorage = flag;
    }

    TxStore& TableStorage::GetTxStore(uint160 nameInDB)
    {
        std::lock_guard<std::mutex> lock(mutexMap_);
        auto it = m_map.find(nameInDB);
        if (it == m_map.end())  return app_.getTxStore();
        else                    return it->second->getTxStore();
    }

    void TableStorage::TryTableStorage()
    {
        if (!m_IsHaveStorage) return;

        if (!bTableStorageThread_)
        {
            bTableStorageThread_ = true;
            app_.getJobQueue().addJob(jtTABLESTORAGE, "tableStorage", [this](Job&) { TableStorageThread(); });
        }
    }

    TableStorage::TableStorageFlag TableStorage::InitItem(STTx const&tx)
    {      
        if (!m_IsHaveStorage) return STORAGE_NORMALERROR;

        if (!m_IsStorageOn) return STORAGE_SUCCESS;

        uint16_t transType = tx.getFieldU16(sfTransactionType);

        if (transType != ttTABLELISTSET && transType != ttSQLSTATEMENT)
            return STORAGE_NORMALERROR;

        auto const & sTxTables = tx.getFieldArray(sfTables);
        uint160 uTxDBName = sTxTables[0].getFieldH160(sfNameInDB);
        ripple::Blob nameBlob1 = sTxTables[0].getFieldVL(sfTableName);
        std::string sTableName1;
        sTableName1.assign(nameBlob1.begin(), nameBlob1.end());
        std::lock_guard<std::mutex> lock(mutexMap_);

        auto it = m_map.find(uTxDBName);
        if (it == m_map.end())
        {            
            AccountID accountID = tx.getAccountID(sfAccount);            
            if (tx.getTxnType() == ttSQLSTATEMENT)
                accountID = tx.getAccountID(sfOwner);
             
            ripple::Blob nameBlob = sTxTables[0].getFieldVL(sfTableName);
            std::string sTableName;
            sTableName.assign(nameBlob.begin(), nameBlob.end());

            auto validIndex = app_.getLedgerMaster().getValidLedgerIndex();
            auto validLedger = app_.getLedgerMaster().getValidatedLedger();
            uint256 txnHash, ledgerHash, utxUpdatehash;
            LedgerIndex txnLedgerSeq, LedgerSeq;
            bool bDeleted;

            bool bRet = app_.getTableStatusDB().ReadSyncDB(to_string(uTxDBName), txnLedgerSeq, txnHash, LedgerSeq, ledgerHash, utxUpdatehash, bDeleted);
            if (bRet)
            {
                if (validIndex - LedgerSeq < MAX_GAP_NOW2VALID)  //catch up valid ledger
                {
                    auto pItem = std::make_shared<TableStorageItem>(app_, cfg_, journal_);
                    auto itRet = m_map.insert(make_pair(uTxDBName, pItem));
                    if (itRet.second)
                    {
                        pItem->InitItem(accountID, to_string(uTxDBName), sTableName);
                        if (utxUpdatehash.isNonZero())
                        {
                            LedgerSeq--;
                            ledgerHash--;
                        }
                        pItem->SetItemParam(txnLedgerSeq, txnHash, LedgerSeq, ledgerHash);
                        if (pItem->PutElem(tx))
                            return STORAGE_SUCCESS;
                        else
                            return STORAGE_DBERROR;
                    }
                    else
                    {
                        return STORAGE_NORMALERROR;
                    }
                }
                else
                {
                    return STORAGE_NORMALERROR;
                }
            }
            else
            {
                AccountID uOwnerID = tx.getAccountID(sfAccount);
                if (tx.getTxnType() == ttSQLSTATEMENT)
                    uOwnerID = tx.getAccountID(sfOwner);

                auto const kOwner = keylet::account(uOwnerID);
                auto const sleOwner = validLedger->read(kOwner);
                if (!sleOwner)  return  STORAGE_NORMALERROR;

                auto const kTable = keylet::table(uOwnerID);
                auto const sleTable = validLedger->read(kTable);

                if (!sleTable) return STORAGE_NORMALERROR;
                STArray tablentries = sleTable->getFieldArray(sfTableEntries);

                auto iter(tablentries.end());
                iter = std::find_if(tablentries.begin(), tablentries.end(),
                    [uTxDBName](STObject const &item) {
                    return item.getFieldH160(sfNameInDB) == uTxDBName && item.getFieldU8(sfDeleted) != 1;
                });

                if (iter != tablentries.end())
                {
                    return STORAGE_NORMALERROR;
                }
                else
                {
                    auto pItem = std::make_shared<TableStorageItem>(app_, cfg_, journal_);
                    auto itRet = m_map.insert(make_pair(uTxDBName, pItem));
                    if (itRet.second)
                    {
                        pItem->InitItem(accountID, to_string(uTxDBName), sTableName);
                        pItem->SetItemParam(0, txnHash, validLedger->info().seq, validLedger->info().hash);//txnHash ==NULL
                        if (pItem->PutElem(tx))
                            return STORAGE_SUCCESS;
                        else
                            return STORAGE_DBERROR;

                    }
                    else
                    {
                        return STORAGE_NORMALERROR;
                    }
                }
            }
        }
        else
        {
            if (it->second->PutElem(tx))
                return STORAGE_SUCCESS;
            else
                return STORAGE_DBERROR;
        }

        return STORAGE_SUCCESS;
    }

    void TableStorage::TableStorageThread()
    {
        auto validIndex = app_.getLedgerMaster().getValidLedgerIndex();
        auto mapTmp = m_map;

        for(auto item : mapTmp)
        {
            uint160 uTxDBName; //how to get value ?
            {
                std::lock_guard<std::mutex> lock(mutexMap_);
                bool bRet = item.second->doJob(validIndex);
                if (bRet)
                {
                    m_map.erase(item.first);
                }
            }
        }
        bTableStorageThread_ = false;
    }
}
  