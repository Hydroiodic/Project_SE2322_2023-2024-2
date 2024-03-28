#pragma once

#include "../skipList/skipList.h"
#include "../common/definitions.h"
#include "../ssTable/ssTable.h"
#include "../vLog/vLog.h"
#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>

namespace memtable {

    using def::value_type;
    using def::key_type;
    using sstable::ssTableContent;

    class memTable
    {
    private:
        skiplist::skiplist_type data;
        uint64_t cur_timestamp = 0;   // TODO

    public:
        explicit memTable();
        ~memTable();

        bool insert(const key_type& key, const value_type& value);
        bool remove(const key_type& key);
        std::optional<value_type> get(const key_type& key) const;
        void scan(const key_type& key1, const key_type& key2, 
            skiplist::skiplist_type& skip_list) const;
        void clear();

        ssTableContent* getContent(vlog::vLog& v_log) const;

        size_t size() const { return data.size(); }
        bool empty() const { return data.size() == 0; }

        void setTimestamp(uint64_t new_timestamp) { cur_timestamp = new_timestamp; }
        uint64_t getTimestamp() const { return cur_timestamp; }
    };
}

