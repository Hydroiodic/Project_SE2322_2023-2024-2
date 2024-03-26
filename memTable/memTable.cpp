#include "memTable.h"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <utility>

namespace memtable {

    memTable::memTable() {
        /* here a lot of work TODO */
    }

    memTable::~memTable() {
        /* here a lot of work TODO */
    }

    bool memTable::insert(const key_type &key, const value_type &value) {
        // insert the value
        data.put(key, value);

        // whether the size of the table allows more insertion
        return data.size() + 1 <= def::max_key_number;
    }

    bool memTable::remove(const key_type &key) {
        // insert the delete tag
        data.put(key, def::delete_tag);

        // whether the size of the table allows more insertion
        return data.size() + 1 <= def::max_key_number;
    }

    void memTable::clear() {
        data.clear();
    }

    mem_table_content memTable::getContent(vlog::vLog& v_log) const {
        // here we new an array of char, please remember to delete it
        char* content = new char[def::max_file_size];
        size_t cur_position = 0;
        
        // header of the SSTable file
        def::sstableHeader header {
            cur_timestamp,
            data.size(),
        };

        // bloom filter
        // TODO
        cur_position += def::sstable_header_size;
        memset(content + def::sstable_header_size, 0, def::bloom_filter_size);
        cur_position += def::bloom_filter_size;

        // all contents
        skiplist::skiplist_type::const_iterator it = data.cbegin(), 
            old_it = it, eit = data.cend();
        header.min_key = it.key();

        while (it != eit) {
            // fetch content and then write
            key_type key = it.key();
            value_type val = it.value();
            def::sstableData cur_data {
                it.key(),
                v_log.append(key, val),      // TODO
                static_cast<uint32_t>(val.length()),
            };
            memcpy(content + cur_position, &cur_data, def::sstable_data_size);

            // continue the next iteration
            cur_position += def::sstable_data_size;
            old_it = it;
            ++it;
        }

        // max key is the last one
        header.max_key = old_it.key();
        memcpy(content, &header, def::sstable_header_size);

        return std::make_pair(content, cur_position);
    }
}

