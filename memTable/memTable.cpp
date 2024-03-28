#include "memTable.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <algorithm>

namespace memtable {

    memTable::memTable(const std::string& dir) {
        /* here a lot of work TODO */
        size_t level = 0;
        std::vector<std::string> files;

        // use a safer method to access directory
        std::filesystem::path path(dir);
        path.append(def::sstable_base_directory_name + std::to_string(level));
        if (!utils::dirExists(path.string())) return;

        // scan the directory to find the max timestamp
        // TODO: use data stored in file instead of names of files
        utils::scanDir(path.string(), files);
        auto max_it = std::max_element(files.begin(), files.end());

        // read data from SSTable
        SSTable table(path.string(), *max_it);
        this->cur_timestamp = table.tableContent()->header.time + 1;
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

    void memTable::scan(const key_type& key1, const key_type& key2, 
        skiplist::skiplist_type& skip_list) const {
        // use iterator to scan
        skiplist::skiplist_type::const_iterator it = data.cbegin(), eit = data.cend();
        size_t total_size = data.size();

        // scan through all contents
        for (int i = 0; i < total_size; ++i, ++it) {
            // assertion, not meet the end
            assert(it != eit);

            // fetch content and then write
            key_type key = it.key();

            // if the pair is ok
            if (key <= key2 && key >= key1) {
                // replace is not allowed
                skip_list.put(key, it.value(), false);
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
        // TODO
        memset(&content->bloomFilterContent, 0, def::bloom_filter_size);

        // all contents
        skiplist::skiplist_type::const_iterator it = data.cbegin(), 
            old_it = it, eit = data.cend();
        content->header.min_key = it.key();

        for (int i = 0; i < content->header.key_value_pair_number; ++i, old_it = it++) {
            // assertion, not meet the end
            assert(it != eit);

            // fetch content and then write
            key_type key = it.key();
            value_type val = it.value();

            // set content
            content->data[i].key = key;
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

