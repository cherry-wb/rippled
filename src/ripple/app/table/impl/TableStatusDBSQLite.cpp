#include <ripple/app/table/TableStatusDBSQLite.h>


namespace ripple {

    TableStatusDBSQLite::TableStatusDBSQLite(DatabaseCon* dbconn, Application * app, beast::Journal& journal) : TableStatusDB(dbconn,app,journal)
    {
    }

    TableStatusDBSQLite::~TableStatusDBSQLite() {

    }

    bool TableStatusDBSQLite::CreateSnycDB(DatabaseCon::Setup setup)
    {
        mSyncTableStateDB = std::make_unique <DatabaseCon>(setup, "SyncTableState.db",
            SyncTableStateDBInit, SyncTableStateDBCount);

        return mSyncTableStateDB!=nullptr;
    }

    bool TableStatusDBSQLite::isNameInDBExist(std::string TableName, std::string Owner, bool delCheck,std::string &TableNameInDB)
    {
        bool ret = false;
        try
        {
            auto db(mSyncTableStateDB->checkoutDb());
            soci::transaction tr(*db);

            static std::string const prefix(
                R"(SELECT TableNameInDB from SyncTableState
            WHERE )");

            std::string sql = boost::str(boost::format(
                    prefix +
                    (R"(TableName = '%s' AND Owner = '%s')"))
                    % TableName
                    % Owner);
            
            if (delCheck)
                sql = sql + " AND deleted = 0;";
            else
                sql = sql + ";";

            boost::optional<std::string> tableNameInDB_;

            soci::statement st = (db->prepare << sql
                , soci::into(tableNameInDB_));

            if (tableNameInDB_ && !tableNameInDB_.value().empty())
            {
                ret = true;
                TableNameInDB = tableNameInDB_.value();
            }
        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error()) <<
                "isNameInDBExist exception" << e.what();
        }

        return ret;
    }

    bool TableStatusDBSQLite::InitDB(DatabaseCon::Setup setup)
    {
        return CreateSnycDB(setup);
    }

    bool TableStatusDBSQLite::ReadSyncDB(std::string nameInDB, LedgerIndex &txnseq,
        uint256 &txnhash, LedgerIndex &seq, uint256 &hash, uint256 &txnupdatehash, bool &bDeleted)
    {
        bool ret = false;

        try {
            auto db(mSyncTableStateDB->checkoutDb());
            soci::transaction tr(*db);

            static std::string const prefix(
                R"(select TxnLedgerHash,TxnLedgerSeq,LedgerHash,LedgerSeq,TxnUpdateHash,deleted from SyncTableState
            WHERE )");


            std::string sql = boost::str(boost::format(
                prefix +
                (R"(TableNameInDB = '%s';)"))
                % nameInDB);

            boost::optional<std::string> TxnLedgerSeq;
            boost::optional<std::string> TxnLedgerHash;
            boost::optional<std::string> LedgerSeq;
            boost::optional<std::string> LedgerHash;
            boost::optional<std::string> TxnUpdateHash;
            boost::optional<std::string> deleted;
            boost::optional<std::string> status;

            soci::statement st = (db->prepare << sql
                , soci::into(TxnLedgerHash)
                , soci::into(TxnLedgerSeq)
                , soci::into(LedgerHash)
                , soci::into(LedgerSeq)
                , soci::into(TxnUpdateHash)
                , soci::into(deleted));

            bool dbret = st.execute(true);

            if (dbret)
            {
                if (TxnLedgerSeq != boost::none && !TxnLedgerSeq.value().empty())
                    txnseq = std::stoi(TxnLedgerSeq.value());
                if (TxnLedgerHash && !TxnLedgerHash.value().empty())
                    txnhash = from_hex_text<uint256>(TxnLedgerHash.value());
                if (LedgerSeq && !LedgerSeq.value().empty())
                    seq = std::stoi(LedgerSeq.value());
                if (LedgerHash && !LedgerHash.value().empty())
                    hash = from_hex_text<uint256>(LedgerHash.value());
                if (TxnUpdateHash && !TxnUpdateHash.value().empty())
                    txnupdatehash = from_hex_text<uint256>(TxnUpdateHash.value());
                if (deleted && !deleted.value().empty())
                    bDeleted = std::stoi(deleted.value());
                ret = true;
            }
        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error()) <<
                "InitFromDB exception" << e.what();
        }

        return ret;
    }

    bool TableStatusDBSQLite::GetMaxTxnInfo(std::string TableName, std::string Owner, LedgerIndex &TxnLedgerSeq, uint256 &TxnLedgerHash)
    {
        bool ret = false;
        try
        {
            auto db(mSyncTableStateDB->checkoutDb());
            soci::transaction tr(*db);

            static std::string const prefix(
                R"(select TxnLedgerHash,TxnLedgerSeq from SyncTableState
            WHERE )");

            std::string sql = boost::str(boost::format(
                prefix +
                (R"(TableName = '%s' AND Owner = '%s' ORDER BY TxnLedgerSeq DESC;)"))
                % TableName
                % Owner);            

            boost::optional<std::string> txnLedgerHash_;
            boost::optional<std::string> txnLedgerSeq_;
            soci::statement st =
                (db->prepare << sql,
                    soci::into(txnLedgerHash_),
                    soci::into(txnLedgerSeq_));

            st.execute();

            if (st.fetch())
            {
                if (txnLedgerSeq_ != boost::none && !txnLedgerSeq_.value().empty())
                {
                    TxnLedgerSeq = std::stoi(txnLedgerSeq_.value());
                    TxnLedgerHash = from_hex_text<uint256>(txnLedgerHash_.value());
                }  
                ret = true;
            }
            else
                ret = false;
        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error()) <<
                "GetMaxTxnInfo exception" << e.what();
        }
        return ret;
    }

    bool TableStatusDBSQLite::InsertSnycDB(std::string TableName, std::string TableNameInDB, std::string Owner, LedgerIndex LedgerSeq,uint256 LedgerHash, bool IsAutoSync)
    {
        bool ret = false;
        try
        {
            auto db(mSyncTableStateDB->checkoutDb());
            soci::transaction tr(*db);

            std::string sql(
                "INSERT OR REPLACE INTO SyncTableState "
                "(Owner, TableName,TableNameInDB,LedgerSeq,LedgerHash,deleted,AutoSync) VALUES('");

            sql += Owner;
            sql += "','";
            sql += TableName;
            sql += "','";
            sql += TableNameInDB;
            sql += "','";
            sql += to_string(LedgerSeq);
            sql += "','";
            sql += to_string(LedgerHash);
            sql += "','";
            sql += to_string(0);
            sql += "','";
            sql += to_string(IsAutoSync);
            sql += "');";
            *db << sql;
            tr.commit();
            ret = true;
        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error()) <<
                "InsertSnycDB exception" << e.what();
        }

        return ret;
    }

    bool TableStatusDBSQLite::GetAutoListFromDB(bool /*bAutoSunc*/, std::list<std::tuple<std::string, std::string, bool> > &list)
    {
        bool ret = false;
        try
        {
            auto db(mSyncTableStateDB->checkoutDb());
            soci::transaction tr(*db);

            static std::string const sql(
                R"(select Owner,TableName from SyncTableState
            WHERE AutoSync = '1' ORDER BY TxnLedgerSeq DESC;)");

            boost::optional<std::string> Owner_;
            boost::optional<std::string> TableName_;

            soci::statement st =
                (db->prepare << sql,
                    soci::into(Owner_),
                    soci::into(TableName_));

            st.execute();

            bool isAutoSync = true;
            while (st.fetch())
            {
                std::string owner;
                std::string tablename;


                if (Owner_ != boost::none && !Owner_.value().empty())
                {
                    owner = Owner_.value();
                    tablename = TableName_.value();

                    std::tuple<std::string, std::string, bool>tp = make_tuple(owner, tablename, isAutoSync);
                    list.push_back(tp);
                }
                ret = true;
            }

        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error()) <<
                "GetMaxTxnInfo exception" << e.what();
        }
        return ret;
    }

    bool TableStatusDBSQLite::UpdateStateDB(const std::string & owner, const std::string & tablename, const bool &isAutoSync)
    {
        return true;
    }
    bool TableStatusDBSQLite::RenameRecord(AccountID accountID, std::string TableNameInDB, std::string TableName)
    {
        bool ret = false;
      
        std::string Owner = to_string(accountID);
        try
        {
            auto db(mSyncTableStateDB->checkoutDb());
            soci::transaction tr(*db);

            static std::string updateVal(
                R"sql(UPDATE SyncTableState SET TableName = :TableName
                WHERE Owner = :Owner AND TableNameInDB = :TableNameInDB;)sql");

            *db << updateVal,
                soci::use(TableName),
                soci::use(Owner),
                soci::use(TableNameInDB);

            JLOG(journal_.trace()) << "RenameRecord dataSync: " << updateVal
                << " TableNameInDB: " << TableNameInDB
                << " Owner: " << Owner
                << " TableName: " << TableName;

            tr.commit();
            ret = true;
        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error()) <<
                "RenameRecord exception" << e.what();
        }

        return ret;
    }

    bool TableStatusDBSQLite::UpdateSyncDB(AccountID accountID, std::string TableName, std::string TableNameInDB)
    {
        bool ret = true;
        try {
            std::string Owner = to_string(accountID);
            auto db(mSyncTableStateDB->checkoutDb());
            soci::transaction tr(*db);

            static std::string updateVal(
                R"sql(UPDATE SyncTableState SET TableNameInDB = :TableNameInDB
                WHERE Owner = :Owner AND TableName = :TableName;)sql");

            *db << updateVal,
                soci::use(TableNameInDB),
                soci::use(Owner),
                soci::use(TableName);

            JLOG(journal_.trace()) << "update dataSync: " << updateVal
                << " TableNameInDB: " << TableNameInDB
                << " Owner: " << Owner
                << " TableName: " << TableName;

            tr.commit();
            ret = true;
        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error()) <<
                "UpdateSyncDBImpl exception" << e.what();
        }

        return ret;
    }

    bool TableStatusDBSQLite::DeleteRecord(AccountID accountID, std::string TableName)
    {
        bool ret = false;
      
        std::string Owner = to_string(accountID);
        try
        {
            auto db(mSyncTableStateDB->checkoutDb());
            soci::transaction tr(*db);

            static std::string deleteVal(
                R"sql(DELETE FROM  SyncTableState
                WHERE Owner = :Owner AND TableName = :TableName;)sql");

            *db << deleteVal,
                soci::use(Owner),
                soci::use(TableName);

            tr.commit();
            ret = true;

        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error()) <<
                "DeleteRecord exception" << e.what();
        }

        return ret;
    }

    bool TableStatusDBSQLite::IsExist(AccountID accountID, std::string TableNameInDB)
    {
        bool ret = false;
       
        std::string Owner = to_string(accountID);
        try
        {
            auto db(mSyncTableStateDB->checkoutDb());
            soci::transaction tr(*db);

            static std::string const prefix(
                R"(SELECT LedgerSeq from SyncTableState
            WHERE )");

            std::string sql = boost::str(boost::format(
                prefix +
                (R"(TableNameInDB = '%s' AND Owner = '%s';)"))
                % TableNameInDB
                % Owner);

            boost::optional<std::string> LedgerSeq;

            soci::statement st = (db->prepare << sql
                , soci::into(LedgerSeq));

            bool dbret = st.execute(true);

            if (dbret)//if have records
            {
                ret = true;
            }
        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error()) <<
                "isExist exception" << e.what();
        }

        return ret;
    }

    bool TableStatusDBSQLite::UpdateSyncDB(const std::string &Owner, const std::string &TableNameInDB, const std::string &TxnLedgerHash,
        const std::string &TxnLedgerSeq, const std::string &LedgerHash, const std::string &LedgerSeq, const std::string &TxnUpdateHash,
        const std::string &PreviousCommit)
    {
        bool ret = false;

        try
        {
            auto db(mSyncTableStateDB->checkoutDb());
            soci::transaction tr(*db);
            static std::string updateVal(
                R"sql(UPDATE SyncTableState SET TxnLedgerHash = :TxnLedgerHash, TxnLedgerSeq = :TxnLedgerSeq,LedgerHash = :LedgerHash, LedgerSeq = :LedgerSeq,TxnUpdateHash = :TxnUpdateHash,PreviousCommit = :PreviousCommit
                WHERE Owner = :Owner AND TableNameInDB = :TableNameInDB;)sql");

            *db << updateVal,
                soci::use(TxnLedgerHash),
                soci::use(TxnLedgerSeq),
                soci::use(LedgerHash),
                soci::use(LedgerSeq),
                soci::use(TxnUpdateHash),
                soci::use(PreviousCommit),
                soci::use(Owner),
                soci::use(TableNameInDB);

            JLOG(journal_.trace()) << "update dataSync(have tx): " << updateVal
                << " TxnLedgerHash: " << TxnLedgerHash
                << " TxnLedgerSeq: " << TxnLedgerSeq
                << " LedgerHash: " << LedgerHash
                << " LedgerSeq: " << LedgerSeq
                << " TxnFailHash: " << TxnUpdateHash
                << " PreviousCommit: " << PreviousCommit
                << " Owner: " << Owner
                << " TableName: " << TableNameInDB;

            tr.commit();
            ret = true;
        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error()) <<
                "UpdateSyncDB exception" << e.what();
        }

        return ret;
    }

    bool TableStatusDBSQLite::UpdateSyncDB(const std::string &Owner, const std::string &TableNameInDB, const std::string &LedgerHash,
        const std::string &LedgerSeq, const std::string &PreviousCommit)
    {
        bool ret = false;
        
        try
        {
            auto db(mSyncTableStateDB->checkoutDb());
            soci::transaction tr(*db);
            static std::string updateVal(
                R"sql(UPDATE SyncTableState SET LedgerHash = :LedgerHash, LedgerSeq = :LedgerSeq,PreviousCommit = :PreviousCommit
                WHERE Owner = :Owner AND TableNameInDB = :TableNameInDB;)sql");

            *db << updateVal,
                soci::use(LedgerHash),
                soci::use(LedgerSeq),
                soci::use(PreviousCommit),
                soci::use(Owner),
                soci::use(TableNameInDB);

            JLOG(journal_.trace()) << "update dataSync: " << updateVal
                << " LedgerHash: " << LedgerHash
                << " LedgerSeq: " << LedgerSeq
                << " PreviousCommit: " << PreviousCommit
                << " Owner: " << Owner
                << " TableName: " << TableNameInDB;

            tr.commit();
            ret = true;
        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error()) <<
                "UpdateSyncDB exception" << e.what();
        }

        return ret;
    }

    bool TableStatusDBSQLite::UpdateSyncDB(const std::string &Owner, const std::string &TableNameInDB,
        const std::string &TxnUpdateHash, const std::string &PreviousCommit)
    {
        bool ret = true;
  
        try {
            auto db(mSyncTableStateDB->checkoutDb());
            soci::transaction tr(*db);

            static std::string updateVal(
                R"sql(UPDATE SyncTableState SET TxnUpdateHash = :TxnUpdateHash
        WHERE Owner = :Owner AND TableNameInDB = :TableNameInDB;)sql");

            *db << updateVal,
                soci::use(TxnUpdateHash),
                soci::use(Owner),
                soci::use(TableNameInDB);

            JLOG(journal_.trace()) << "update dataSync: " << updateVal
                << " TableNameInDB: " << TableNameInDB
                << " Owner: " << Owner
                << " TxnUpdateHash: " << TxnUpdateHash;

            tr.commit();
            ret = true;
        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error()) <<
                "UpdateSyncDB exception" << e.what();
        }

        return ret;
    }

    bool TableStatusDBSQLite::UpdateSyncDB(const std::string &Owner, const std::string &TableNameInDB,
        bool bDel, const std::string &PreviousCommit)
    {
        
        bool ret = true;
        
        try {
            auto db(mSyncTableStateDB->checkoutDb());
            soci::transaction tr(*db);

            static std::string updateVal(
                R"sql(UPDATE SyncTableState SET deleted = :deleted
        WHERE Owner = :Owner AND TableNameInDB = :TableNameInDB;)sql");

            *db << updateVal,
                soci::use(to_string(bDel)),
                soci::use(Owner),
                soci::use(TableNameInDB);

            JLOG(journal_.trace()) << "update dataSync: " << updateVal
                << " TableNameInDB: " << TableNameInDB
                << " Owner: " << Owner
                << " deleted: " << to_string(bDel);

            tr.commit();
            ret = true;
        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error()) <<
                "UpdateSyncDB exception" << e.what();
        }
        
        return ret;
    }

}