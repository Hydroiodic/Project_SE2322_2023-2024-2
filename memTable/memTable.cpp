#include "memTable.h"
#include <cassert>
#include <cstring>
#include <map>
#include <optional>

namespace memtable {

    memTable::memTable(const std::string& dir) : filter(def::bloom_filter_size) {
        /* here's nothing to do??? */
    }

    memTable::~memTable() {
        /* here's nothing to do??? */
    }

    bool memTable::insert(const key_type &key, const value_type &value) {
        // insert the value
        data.put(key, value);
        filter.insert(key);

        // whether the size of the table allows more insertion
        return data.size() < def::max_key_number;
    }

    bool memTable::remove(const key_type &key) {
        // insert the delete tag
        data.put(key, def::delete_tag);
        filter.insert(key);

        // whether the size of the table allows more insertion
        return data.size() + 1 <= def::max_key_number;
    }

    void memTable::clear() {
        data.clear();
        filter.clear();
    }

    std::optional<value_type> memTable::get(const key_type& key) const {
        if (!filter.query(key)) return std::nullopt;
        return data.get(key);
    }

    void memTable::scan(const key_type& key1, const key_type& key2, 
        std::map<key_type, value_type>& map) const {
        // use iterator to scan
        skiplist::skiplist_type::const_iterator it = data.cbegin(), eit = data.cend();

        // scan through all contents
        for (; it != eit; ++it) {
            // assertion, not meet the end
            assert(it != eit);

            // fetch content and then write
            key_type key = it.key();

            // if the pair is ok
            if (key <= key2 && key >= key1) {
                // replace is not allowed
                map[key] = it.value();
            }
            // if those left keys are larger than key2
            else if (key > key2) {
                break;
            }
        }
    }

    // ATTENTION! the content is allocated on the heap, so please remember to delete it
    ssTableContent* memTable::getContent(vlog::vLog& v_log) const {
        // here we new an array of char, please remember to delete it
        ssTableContent* content = new ssTableContent;
        
        // header of the SSTable file
        content->header.time = cur_timestamp;
        content->header.key_value_pair_number = data.size();

        // bloom filter
        memcpy(&content->bloomFilterContent, filter.getContent(), def::bloom_filter_size);

        // all contents
        skiplist::skiplist_type::const_iterator it = data.cbegin(), 
            old_it = it, eit = data.cend();
        content->header.min_key = it.key();

        for (size_t i = 0; i < content->header.key_value_pair_number; ++i, old_it = it++) {
            // assertion, not meet the end
            assert(it != eit);

            // fetch content and then write
            key_type key = it.key();
            value_type val = it.value();

            // set content
            content->data[i].key = key;
            // if the pair is a deleted one
            if (val == def::delete_tag) {
                content->data[i].offset = 0;
                content->data[i].value_length = 0;
            }
            // insert the value into vlog and get offset
            else {
                content->data[i].offset = v_log.append(key, val);
                content->data[i].value_length = static_cast<uint32_t>(val.length());
            }
        }

        // assertion
        assert(it == eit);

        // max key is the last one
        content->header.max_key = old_it.key();

        return content;
    }
}

