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
        memTable();
        ~memTable();

        bool insert(const key_type& key, const value_type& value, uint32_t offset);
        bool remove(const key_type& key);
        mem_table_content getContent(vlog::vLog& v_log) const;
    };
}


