#pragma once

#include "../skipList/skipList.h"
#include "../bloomFilter/bloomFilter.h"
#include "../common/definitions.h"
#include "../ssTable/ssTable.h"
#include "../vLog/vLog.h"
#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>
#include <map>

namespace memtable {

    using def::value_type;
    using def::key_type;
    using def::ssTableContent;
    using sstable::SSTable;
    using bloomFilter = bloomfilter::bloomFilter<key_type>;

    class memTable
    {
    private:
        skiplist::skiplist_type data;
        uint64_t cur_timestamp = 0;

        // bloomFilter here
        bloomFilter filter;

    public:
        explicit memTable(const std::string& dir);
        ~memTable();

        bool insert(const key_type& key, const value_type& value);
        bool remove(const key_type& key);
        std::optional<value_type> get(const key_type& key) const;
        void scan(const key_type& key1, const key_type& key2, 
            std::map<key_type, value_type>& map) const;
        void clear();

        ssTableContent* getContent(vlog::vLog& v_log) const;

        size_t size() const { return data.size(); }
        bool empty() const { return data.size() == 0; }

        void setTimestamp(uint64_t new_timestamp) { cur_timestamp = new_timestamp; }
        uint64_t getTimestamp() const { return cur_timestamp; }
    };
}

