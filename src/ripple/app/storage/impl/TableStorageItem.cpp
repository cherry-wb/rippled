#include <ripple/app/storage/TableStorageItem.h>
#include <ripple/app/main/Application.h>
#include <ripple/app/ledger/LedgerMaster.h>
#include <ripple/app/ledger/TransactionMaster.h>
#include <ripple/app/table/TableStatusDBMySQL.h>
#include <ripple/app/table/TableStatusDBSQLite.h>
#include <ripple/protocol/STEntry.h>
#include <ripple/app/table/TableSync.h>
#include <ripple/protocol/TableDefines.h>
namespace ripple {    
    TableStorageItem::TableStorageItem(Application& app, Config& cfg, beast::Journal journal)
        : app_(app)
        , journal_(journal)
        , cfg_(cfg)
    {          
    }

    TableStorageItem::~TableStorageItem()
    {
    }

    void TableStorageItem::InitItem(AccountID account, std::string nameInDB, std::string tableName)
    {
        accountID_ = account;
        sTableNameInDB_ = nameInDB;
        sTableName_ = tableName;

        getTxStoreTrans();
    }

    void TableStorageItem::SetItemParam(LedgerIndex txnLedgerSeq, uint256 txnHash, LedgerIndex LedgerSeq, uint256 ledgerHash)
    {
        txnHash_ = txnHash;
        txnLedgerSeq_ = txnLedgerSeq;
        ledgerHash_ = ledgerHash;
        LedgerSeq_ = LedgerSeq;
    }

    void TableStorageItem::Put(STTx const& tx)
    {
        txInfo txInfo_;
        txInfo_.accountID = accountID_;
        txInfo_.uTxHash = tx.getTransactionID();
        txInfo_.bCommit = false;
        if (tx.isFieldPresent(sfLastLedgerSequence))
            txInfo_.uTxLedgerVersion = tx.getFieldU32(sfLastLedgerSequence);  //uTxLedgerVersion

        auto const & sTxTables = tx.getFieldArray(sfTables);
        auto tableNameBlob = sTxTables[0].getFieldVL(sfTableName);
        sTableName_.assign(tableNameBlob.begin(), tableNameBlob.end());
        JLOG(journal_.warn()) << " TableStorageItem::put "<< sTableName_;
        txList_.push_back(txInfo_);
    }

    bool TableStorageItem::PutElem(STTx const& tx)
    {
        bool result = false;        

        if (txList_.size() <= 0)
        {
            app_.getTableSync().StopOneTable(accountID_, sTableNameInDB_, tx.getFieldU16(sfOpType) == T_CREATE);
        }
        
		if (tx.getFieldU16(sfOpType) == T_RENAME)
		{
			auto const & sTxTables = tx.getFieldArray(sfTables);
			auto tableNameBlob = sTxTables[0].getFieldVL(sfTableName);
			sTableName_.assign(tableNameBlob.begin(), tableNameBlob.end());
			Put(tx);
			result = true;
		}
		else if (tx.getFieldU16(sfOpType) == T_ASSIGN || tx.getFieldU16(sfOpType) == T_CANCELASSIGN)
		{
			Put(tx);
			result = true;
		}
		else
		{
			auto ret = this->getTxStore().Dispose(tx);
			if (ret.first)
			{
				JLOG(journal_.trace()) << "Dispose success";
				Put(tx);
				result = true;
			}
			else
				JLOG(journal_.trace()) << "Dispose error";
		}        

        return result;
    }
    bool TableStorageItem::CheckExistInLedger(LedgerIndex CurLedgerVersion)
    {
        auto iter = txList_.begin();
        for (; iter != txList_.end(); iter++)
        {
            if (iter->bCommit)   continue;
            
            if (iter->uTxLedgerVersion <= CurLedgerVersion)
            {
               if (app_.getMasterTransaction().fetch(iter->uTxHash, true) == NULL)
               {
                    break;
                }
            }
        }
        return iter == txList_.end();
    }

    STEntry *TableStorageItem::GetTableEntry(const STArray& aTables, LedgerIndex iLastSeq, AccountID accountID,std::string TableNameInDB, bool bStrictEqual)
    {
        auto iter(aTables.end());
        iter = std::find_if(aTables.begin(), aTables.end(),
            [iLastSeq, accountID, TableNameInDB, bStrictEqual](STObject const &item) {
            uint160 uTxDBName = item.getFieldH160(sfNameInDB);
            std::string str = to_string(uTxDBName);
            return to_string(uTxDBName) == TableNameInDB /*&& item.getFieldU8(sfDeleted) != 1*/
                && (bStrictEqual ? item.getFieldU32(sfPreviousTxnLgrSeq) == iLastSeq : item.getFieldU32(sfPreviousTxnLgrSeq) >= iLastSeq);
        });

        if (iter == aTables.end())     return NULL;

        return (STEntry*)(&(*iter));
    }

    TableStorageItem::TableStorageFlag TableStorageItem::CheckSuccessive(LedgerIndex validatedIndex)
    {     
        for (int index = LedgerSeq_ + 1; index <= validatedIndex; index++)
        {              
            auto ledger = app_.getLedgerMaster().getLedgerBySeq(index);
            if (!ledger) continue;

            LedgerSeq_ = index;
            ledgerHash_ = app_.getLedgerMaster().getHashBySeq(index);

            auto const sleAccepted = ledger->read(keylet::table(accountID_));
            if (sleAccepted == NULL) continue;            

            const STEntry * pEntry = NULL;
            auto aTableEntries = sleAccepted->getFieldArray(sfTableEntries);
            pEntry = this->GetTableEntry(aTableEntries, txnLedgerSeq_, accountID_, sTableNameInDB_,true); 
            if (pEntry == NULL)  continue;            

            auto aTx = pEntry->getFieldV256(sfTxs);
            int iCount = 0;
            for (auto tx : aTx)
            {
                iCount++;
                auto iter = std::find_if(txList_.begin(), txList_.end(),
                    [tx](txInfo &item) {
                    return item.uTxHash == tx;
                });
                
                if (iter == txList_.end())
                {
                    return STORAGE_ROLLBACK;
                }
                else
                {   
                    iter->bCommit = true;
                    auto initIter = std::find_if(txList_.begin(), txList_.end(),
                        [tx](txInfo &item) {
                        return !item.bCommit;
                    });

                    if (initIter == txList_.end()) //mean that each tx had set flag to commit
                    {
                        if (iCount < aTx.size())
                        {
                            LedgerSeq_ = index -1;
                            ledgerHash_ = app_.getLedgerMaster().getHashBySeq(LedgerSeq_);

                            txUpdateHash_ = tx;
                        }
                        else
                        {
                            txnHash_ = pEntry->getFieldH256(sfTxnLedgerHash);
                            txnLedgerSeq_ = pEntry->getFieldU32(sfTxnLgrSeq);
                        }
                        
                        return STORAGE_COMMIT;
                    }
                }
            }
            txnHash_ = pEntry->getFieldH256(sfTxnLedgerHash);
            txnLedgerSeq_ = pEntry->getFieldU32(sfTxnLgrSeq);            
        }
        return STORAGE_NONE;
    }

    bool TableStorageItem::rollBack()
    {
        {
            LockedSociSession sql_session = getTxStoreDBConn().GetDBConn()->checkoutDb();
            TxStoreTransaction &stTran = getTxStoreTrans();
            stTran.rollback();
            JLOG(journal_.warn()) << " TableStorageItem::rollBack " << sTableName_;
        }

        app_.getTableSync().ReStartOneTable(accountID_, sTableNameInDB_, sTableName_, false);
        return true;
    }


    bool TableStorageItem::commit()
    {
        {
            LockedSociSession sql_session = getTxStoreDBConn().GetDBConn()->checkoutDb();
            TxStoreTransaction &stTran = getTxStoreTrans();

            auto &db = getTableStatusDB();

            if (!db.IsExist(accountID_, sTableNameInDB_))
            {
                db.InsertSnycDB(sTableName_, sTableNameInDB_, to_string(accountID_), LedgerSeq_, ledgerHash_, true);
            }

            db.UpdateSyncDB(to_string(accountID_), sTableNameInDB_, to_string(txnHash_), to_string(txnLedgerSeq_), to_string(ledgerHash_), to_string(LedgerSeq_), txUpdateHash_.isNonZero()?to_string(txUpdateHash_) : "", "");

            stTran.commit();
        }

        app_.getTableSync().ReStartOneTable(accountID_, sTableNameInDB_, sTableName_, true);  
        
        return true;
    }


    TxStoreDBConn& TableStorageItem::getTxStoreDBConn()
    {
        if (conn_ == NULL)
        {
            conn_ = std::make_unique<TxStoreDBConn>(cfg_);
        }
        return *conn_;
    }

    TxStoreTransaction& TableStorageItem::getTxStoreTrans()
    {
        if (uTxStoreTrans_ == NULL)
        {
            uTxStoreTrans_ = std::make_unique<TxStoreTransaction>(&getTxStoreDBConn());
        }
        return *uTxStoreTrans_;
    }

    TxStore& TableStorageItem::getTxStore()
    {
        if (pObjTxStore_ == NULL)
        {
            auto& conn = getTxStoreDBConn();
            pObjTxStore_ = std::make_unique<TxStore>(conn.GetDBConn(), cfg_, journal_);
        }
        return *pObjTxStore_;
    }

    TableStatusDB& TableStorageItem::getTableStatusDB()
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

    bool TableStorageItem::doJob(LedgerIndex CurLedgerVersion)
    {
        bool bRet = false;
        bRet = CheckExistInLedger(CurLedgerVersion);
        if (!bRet)
        {
            rollBack();
            return true;
        }
        auto eType = CheckSuccessive(CurLedgerVersion);
        if (eType == STORAGE_ROLLBACK)
        {
            rollBack();
            return true;
        }
        else if (eType == STORAGE_COMMIT)
        {
            commit();
            return true;
        }
        else
        {
            return false;
        }
        
        return false;
    }
}