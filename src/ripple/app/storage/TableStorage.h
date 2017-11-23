#ifndef RIPPLE_APP_TABLE_TABLESTORAGE_H_INCLUDED
#define RIPPLE_APP_TABLE_TABLESTORAGE_H_INCLUDED

#include <ripple/app/storage/TableStorageItem.h>
#include <ripple/protocol/TableDefines.h>
namespace ripple {
class TableStorage
{
public:
    enum TableStorageFlag
    {
        STORAGE_SUCCESS,
        STORAGE_NORMALERROR,
        STORAGE_DBERROR
    };

public:
    TableStorage(Application& app, Config& cfg, beast::Journal journal);
    virtual ~TableStorage();

    void SetHaveSyncFlag(bool flag);
    void TryTableStorage();

    TableStorageFlag InitItem(STTx const&tx);
    void TableStorageThread();

    TxStore& GetTxStore(uint160 nameInDB);
private:
    Application&                                                                app_;
    beast::Journal                                                              journal_;
    Config&                                                                     cfg_;

    std::mutex                                                                  mutexMap_;
    std::map<uint160,std::shared_ptr<TableStorageItem> >                        m_map;
    bool                                                                        m_IsHaveStorage;
    bool                                                                        m_IsStorageOn;
    bool                                                                        bTableStorageThread_;
};

}
#endif

