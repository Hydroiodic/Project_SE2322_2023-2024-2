#pragma once

#include "kvstore_api.h"
#include "common/definitions.h"
#include "memTable/memTable.h"
#include "ssTable/ssTable.h"
#include "vLog/vLog.h"
#include "levelManager/levelManager.h"

using memtable::memTable;
using sstable::SSTable;
using vlog::vLog;
using vlog::garbage_unit;
using levelmanager::levelManager;
using def::key_type;
using def::value_type;
using def::ssTableContent;
using def::ssTableData;
using def::level_files;
using def::managerFileDetail;

class KVStore : public KVStoreAPI
{
private:
    std::string directory;

    vLog v_log;
    memTable mem_table;
    levelManager level_manager;

    void writeMemTableIntoFile();
    void flush();

    // get functions
    std::optional<std::pair<uint64_t, u_int32_t>> getPairFromSSTable(const key_type& key);
    std::optional<value_type> getFromMemTable(const key_type& key) const;
    std::optional<value_type> getFromSSTable(const key_type& key);

    // scan functions
    void scanFromMemTable(const key_type& key1, const key_type& key2, 
        std::map<key_type, value_type>& map) const;
    void scanFromSSTable(const key_type& key1, const key_type& key2, 
        std::map<key_type, value_type>& map);

public:
    KVStore(const std::string &dir, const std::string &vlog);

    ~KVStore();

    void put(key_type key, const value_type& value) override;

    value_type get(key_type key) override;

    bool del(key_type key) override;

    void reset() override;

    void scan(key_type key1, key_type key2, std::list<std::pair<key_type, value_type>>& list) override;

    void gc(uint64_t chunk_size) override;
};
