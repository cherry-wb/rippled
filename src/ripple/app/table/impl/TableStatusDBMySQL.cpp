#include <ripple/app/table/TableStatusDBMySQL.h>
#include <ripple/app/table/TableSyncItem.h>

namespace ripple {

    TableStatusDBMySQL::TableStatusDBMySQL(DatabaseCon* dbconn, Application * app, beast::Journal& journal) : TableStatusDB(dbconn,app,journal)
    {
    }

    TableStatusDBMySQL::~TableStatusDBMySQL() {

    }

    bool TableStatusDBMySQL::InitDB(DatabaseCon::Setup setup)
    {        

        return CreateSnycDB(setup);
    }

    bool TableStatusDBMySQL::ReadSyncDB(std::string nameInDB, LedgerIndex &txnseq,
        uint256 &txnhash, LedgerIndex &seq, uint256 &hash, uint256 &txnupdatehash, bool &bDeleted)
    {
        bool ret = false;
        try
        {
            LockedSociSession sql_session = databasecon_->checkoutDb();

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
            boost::optional<std::string> TxnUpdatehash;
            boost::optional<std::string> deleted;
            boost::optional<std::string> status;            

            soci::statement st = (sql_session->prepare << sql
                , soci::into(TxnLedgerHash)
                , soci::into(TxnLedgerSeq)
                , soci::into(LedgerHash)
                , soci::into(LedgerSeq)
                , soci::into(TxnUpdatehash)
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
                if (TxnUpdatehash && !TxnUpdatehash.value().empty())
                    txnupdatehash = from_hex_text<uint256>(TxnUpdatehash.value());
                if (deleted && !deleted.value().empty())
                    bDeleted = std::stoi(deleted.value());
                ret = true;
            }
        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error()) <<
            "ReadSyncDB exception" << e.what();
        }

        return ret;
    }

    bool TableStatusDBMySQL::GetMaxTxnInfo(std::string TableName, std::string Owner, LedgerIndex &TxnLedgerSeq, uint256 &TxnLedgerHash)
    {
        bool ret = false;
        try
        {
            LockedSociSession sql_session = databasecon_->checkoutDb();

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

            soci::statement st = (sql_session->prepare << sql,
                    soci::into(txnLedgerHash_),
                    soci::into(txnLedgerSeq_));

            st.execute();

            while (st.fetch())
            {
                if (txnLedgerSeq_ != boost::none && !txnLedgerSeq_.value().empty())
                {
                    TxnLedgerSeq = std::stoi(txnLedgerSeq_.value());
                    TxnLedgerHash = from_hex_text<uint256>(txnLedgerHash_.value());
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

    bool TableStatusDBMySQL::InsertSnycDB(std::string TableName, std::string TableNameInDB, std::string Owner, LedgerIndex LedgerSeq, uint256 LedgerHash,bool IsAutoSync)
    {
        bool ret = false;
        try
        {
            LockedSociSession sql_session = databasecon_->checkoutDb();

            std::string sql(
                "INSERT INTO SyncTableState "
                "(Owner, TableName, TableNameInDB,LedgerSeq,LedgerHash,deleted,AutoSync) VALUES('");

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

            soci::statement st = (sql_session->prepare << sql);

            st.execute();
            ret = true;
        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error()) <<
                "InsertSnycDB exception" << e.what();
        }
        return ret;
    }

    bool TableStatusDBMySQL::CreateSnycDB(DatabaseCon::Setup setup)
    {
        bool ret = false;
        try
        {
            LockedSociSession sql_session = databasecon_->checkoutDb();

            std::string sql(
                "CREATE TABLE IF NOT EXISTS SyncTableState(" \
                "Owner             CHARACTER(64),          " \
                "TableName         CHARACTER(64),          " \
                "TableNameInDB     CHARACTER(64),          " \
                "TxnLedgerHash     CHARACTER(64),          " \
                "TxnLedgerSeq      CHARACTER(64),          " \
                "LedgerHash        CHARACTER(64),          " \
                "LedgerSeq         CHARACTER(64),          " \
                "TxnUpdateHash     CHARACTER(64),          " \
                "deleted           CHARACTER(64),          " \
                "AutoSync          CHARACTER(64),          " \
                "PreviousCommit    CHARACTER(64),          " \
                "PRIMARY KEY(Owner,TableNameInDB))         "
            );
            soci::statement st =
                (sql_session->prepare << sql);

            ret = st.execute();
        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error()) <<
            "CreateSnycDB exception" << e.what();
        }
        return ret;
    }
    bool TableStatusDBMySQL::isNameInDBExist(std::string TableName, std::string Owner, bool delCheck, std::string &TableNameInDB)
    {
        bool ret = false;
        try
        {
            LockedSociSession sql_session = databasecon_->checkoutDb();
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
           
            soci::statement st = (sql_session->prepare << sql
                , soci::into(tableNameInDB_));

            bool dbret = st.execute(true);
            
            if (dbret)
            {
                if (tableNameInDB_ && !tableNameInDB_.value().empty())
                    TableNameInDB = tableNameInDB_.value();

                ret = true;
            }
        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error()) <<
                "isNameInDBExist exception" << e.what();
        }

        return ret;
    }

    bool TableStatusDBMySQL::RenameRecord(AccountID accountID, std::string TableNameInDB, std::string TableName)
    {
        bool ret = false;
        std::string Owner = to_string(accountID);
        try
        {
            LockedSociSession sql_session = databasecon_->checkoutDb();
            std::string sql = boost::str(boost::format(
                (R"(UPDATE SyncTableState SET TableName = '%s'
                WHERE Owner = '%s' AND TableNameInDB = '%s';)"))
                % TableName
                % Owner
                % TableNameInDB);

            soci::statement st =
                (sql_session->prepare << sql);

            st.execute();
            ret = true;
        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error()) <<
            "RenameRecord exception" << e.what();
        }

        return ret;
    }

    bool TableStatusDBMySQL::UpdateSyncDB(AccountID accountID, std::string TableName, std::string TableNameInDB)
    {
        bool ret = true;
        try {
            std::string Owner = to_string(accountID);

            LockedSociSession sql_session = databasecon_->checkoutDb();

            std::string sql = boost::str(boost::format(
                (R"(UPDATE SyncTableState SET TableNameInDB = '%s'
                WHERE Owner = '%s' AND TableName = '%s';)"))
                % TableNameInDB
                % Owner
                % TableName);

            soci::statement st = sql_session->prepare << sql;

            bool dbret = st.execute(true);

            if (dbret)
                ret = true;
            ret = true;
        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error()) <<
            "UpdateSyncDB exception" << e.what();
        }

        return ret;
    }

    bool TableStatusDBMySQL::DeleteRecord(AccountID accountID, std::string TableName)
    {
        bool ret = false;
        std::string Owner = to_string(accountID);
        try
        {
            LockedSociSession sql_session = databasecon_->checkoutDb();

            std::string deleteVal = boost::str(boost::format(
                (R"(DELETE FROM  SyncTableState
                WHERE Owner = '%s' AND TableName = '%s';)"))
                % Owner
                % TableName);

            soci::statement st = sql_session->prepare << deleteVal;

            bool dbret = st.execute(true);

            if (dbret)
                ret = true;
            ret = true;
        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error()) <<
            "DeleteRecord exception" << e.what();
        }

        return ret;
    }

    bool TableStatusDBMySQL::IsExist(AccountID accountID, std::string TableNameInDB)
    {
        bool ret = false;
        std::string Owner = to_string(accountID);
        try
        {
            LockedSociSession sql_session = databasecon_->checkoutDb();
            static std::string const prefix(
                R"(SELECT LedgerSeq from SyncTableState
            WHERE )");

            std::string sql = boost::str(boost::format(
                prefix +
                (R"(TableNameInDB = '%s' AND Owner = '%s';)"))
                % TableNameInDB
                % Owner);

            boost::optional<std::string> LedgerSeq;

            soci::statement st = (sql_session->prepare << sql
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

    bool TableStatusDBMySQL::UpdateSyncDB(const std::string &Owner, const std::string &TableNameInDB, const std::string &TxnLedgerHash,
        const std::string &TxnLedgerSeq, const std::string &LedgerHash, const std::string &LedgerSeq, const std::string &TxnUpdateHash,
        const std::string &PreviousCommit)
    {
        bool ret = false;
        try
        {
            LockedSociSession sql_session = databasecon_->checkoutDb();
            std::string sql = boost::str(boost::format(
                (R"(UPDATE SyncTableState SET TxnLedgerHash = :TxnLedgerHash, TxnLedgerSeq = :TxnLedgerSeq,LedgerHash = :LedgerHash, LedgerSeq = :LedgerSeq,TxnUpdateHash = :TxnUpdateHash,PreviousCommit = :PreviousCommit
                WHERE Owner = :Owner AND TableNameInDB = :TableNameInDB;)")));

            soci::statement st = (sql_session->prepare << sql,
                soci::use(TxnLedgerHash),
                soci::use(TxnLedgerSeq),
                soci::use(LedgerHash),
                soci::use(LedgerSeq),
                soci::use(TxnUpdateHash),
                soci::use(PreviousCommit),
                soci::use(Owner),
                soci::use(TableNameInDB));

            bool dbret = st.execute(true);

            if (dbret)//if have records
            {
                ret = true;
            }
        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error()) <<
            "UpdateSyncDB exception" << e.what();
        }
        return ret;
    }

    bool TableStatusDBMySQL::UpdateSyncDB(const std::string &Owner, const std::string &TableNameInDB, const std::string &LedgerHash,
        const std::string &LedgerSeq, const std::string &PreviousCommit)
    {
        bool ret = false;
        try
        {
            LockedSociSession sql_session = databasecon_->checkoutDb();

            std::string sql = boost::str(boost::format(
                (R"(UPDATE SyncTableState SET LedgerHash = :LedgerHash, LedgerSeq = :LedgerSeq,PreviousCommit = :PreviousCommit
                WHERE Owner = :Owner AND TableNameInDB = :TableNameInDB;)")));

            soci::statement st = ((*sql_session).prepare << sql,
                soci::use(LedgerHash),
                soci::use(LedgerSeq),
                soci::use(PreviousCommit),
                soci::use(Owner),
                soci::use(TableNameInDB));

            bool dbret = st.execute(true);

            if (dbret)//if have records
            {
                ret = true;
            }
        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error()) <<
            "UpdateSyncDB exception" << e.what();
        }
        return ret;
    }

    bool TableStatusDBMySQL::UpdateSyncDB(const std::string &Owner, const std::string &TableNameInDB,
        const std::string &TxnUpdateHash, const std::string &PreviousCommit)
    {
        bool ret = true;
        try {            
            LockedSociSession sql_session = databasecon_->checkoutDb();
            std::string sql = boost::str(boost::format(
                (R"(UPDATE SyncTableState SET TxnUpdateHash = '%s'
                WHERE Owner = '%s' AND TableNameInDB = '%s';)"))
                % TxnUpdateHash
                % Owner
                % TableNameInDB);

            soci::statement st = (*sql_session).prepare << sql;

            bool dbret = st.execute(true);

            if (dbret)//if have records
            {
                ret = true;
            }
            ret = true;
        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error()) <<
            "UpdateSyncDB exception" << e.what();
        }

        return ret;
    }

    bool TableStatusDBMySQL::UpdateSyncDB(const std::string &Owner, const std::string &TableNameInDB,
        bool bDel, const std::string &PreviousCommit)
    {
        bool ret = true;
        try {
            LockedSociSession sql_session = databasecon_->checkoutDb();

            std::string sql = boost::str(boost::format(
                (R"(UPDATE SyncTableState SET deleted = '%s'
                WHERE Owner = '%s' AND TableNameInDB = '%s';)"))
                % to_string(bDel)
                % Owner
                % TableNameInDB);

            soci::statement st = ((*sql_session).prepare << sql);

            bool dbret = st.execute(true);

            if(dbret)
                ret = true;
        }
        catch (std::exception const& e)
        {
            JLOG(journal_.error()) <<
            "UpdateSyncDB exception" << e.what();
        }

        return ret;
    }

    bool TableStatusDBMySQL::GetAutoListFromDB(bool /*bAutoSunc*/, std::list<std::tuple<std::string, std::string, bool> > &list)
    {
        bool ret = false;
        try
        {
            LockedSociSession sql_session = databasecon_->checkoutDb();

            static std::string const sql(
                R"(select Owner,TableName from SyncTableState
            WHERE AutoSync = '1' ORDER BY TxnLedgerSeq DESC;)");

            boost::optional<std::string> Owner_;
            boost::optional<std::string> TableName_;

            soci::statement st =
                ((*sql_session).prepare << sql,
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

                    std::tuple<std::string, std::string,bool>tp = make_tuple(owner, tablename, isAutoSync);
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

    bool TableStatusDBMySQL::UpdateStateDB(const std::string & owner, const std::string & tablename, const bool &isAutoSync)
    {
        bool ret = false;
        try {
            LockedSociSession sql_session = databasecon_->checkoutDb();
            std::string sql = boost::str(boost::format(
                (R"(UPDATE SyncTableState SET AutoSync = '%s'
                WHERE Owner = '%s' AND TableName = '%s';)"))
                % to_string(isAutoSync)
                % owner
                % tablename);

            soci::statement st = ((*sql_session).prepare << sql);

            bool dbret = st.execute();

            if (dbret)
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