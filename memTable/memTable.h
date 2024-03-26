#pragma once

#include "../skipList/skipList.h"
#include "../common/definitions.h"
#include "../vLog/vLog.h"
#include <cstddef>
#include <cstdint>
#include <utility>

namespace memtable {

    using value_type = def::value_type;
    using key_type = def::key_type;

    using mem_table_content = std::pair<char*, size_t>;

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
        void clear();
        mem_table_content getContent(vlog::vLog& v_log) const;

        size_t size() const { return data.size(); }

        void setTimestamp(uint64_t new_timestamp) { cur_timestamp = new_timestamp; }
        uint64_t getTimestamp() const { return cur_timestamp; }
    };
}

