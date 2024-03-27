#include "memTable.h"
#include <cassert>
#include <cstdint>
#include <cstring>

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

    std::optional<value_type> memTable::get(const key_type& key) const {
        return data.get(key);
    }

    // ATTENTION! the content is allocated on the heap, so please remember to delete it
    ssTableContent* memTable::getContent(vlog::vLog& v_log) const {
        // here we new an array of char, please remember to delete it
        ssTableContent* content = new ssTableContent;
        
        // header of the SSTable file
        content->header.time = cur_timestamp;
        content->header.key_value_pair_number = data.size();

        // bloom filter
        // TODO
        memset(&content->bloomFilterContent, 0, def::bloom_filter_size);

        // all contents
        skiplist::skiplist_type::const_iterator it = data.cbegin(), 
            old_it = it, eit = data.cend();
        content->header.min_key = it.key();

        int index = 0;
        for (int i = 0; i < content->header.key_value_pair_number; ++i, old_it = it++) {
            // assertion
            assert(it != eit);

            // fetch content and then write
            key_type key = it.key();
            value_type val = it.value();

            // set content
            content->data[i].key = it.key();
            content->data[i].offset = v_log.append(key, val);
            content->data[i].value_length = static_cast<uint32_t>(val.length());
        }

        // assertion
        assert(it == eit);

        // max key is the last one
        content->header.max_key = old_it.key();

        return content;
    }
}

