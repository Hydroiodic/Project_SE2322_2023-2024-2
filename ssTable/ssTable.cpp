#include <algorithm>
#include <cassert>
#include <cstddef>
#include <filesystem>
#include <string>
#include "ssTable.h"
#include "../utils.h"
#include "../common/exceptions.h"
#include "../common/definitions.h"

namespace sstable {

    SSTable::SSTable(const std::string& dir_name, uint64_t ts, size_t layer, 
        const std::string& name) : timestamp(ts), layer_number(layer) {
        // use a safer way to join directories
        std::filesystem::path directory(dir_name);
        directory.append(def::sstable_base_directory_name + std::to_string(layer));

        // start to create a new file
        if (!utils::dirExists(directory.string()) && utils::mkdir(directory.string()) == -1) {
            throw exception::create_directory_fail();
        }

        // a safer way to use directory
        directory.append(name);
        directory.replace_extension(def::sstable_extension_name);
        file_name = directory.string();

        // open file
        file_stream.open(file_name, std::ios::in | std::ios::out | std::ios::binary);
        if (!file_stream.is_open()) {
            file_stream.clear();
            file_stream.open(file_name, std::ios::out);
            file_stream.close();
            file_stream.open(file_name, std::ios::in | std::ios::out | std::ios::binary);
        }
    }

    SSTable::SSTable(const std::string& file_name) {
        // open file
        file_stream.open(file_name, std::ios::in | std::ios::binary);
        assert(file_stream.is_open());

        // initialization for SSTable
        initialize();
    }

    SSTable::~SSTable() {
        // close the file and write into it
        file_stream.close();
        if (content) delete content;
        if (filter) delete filter;
    }

    void SSTable::initialize() {
        // load ssTableContent from file system
        load();
        timestamp = content->header.time;
    }

    void SSTable::write(ssTableContent* content_to_write) {
        // no existing content allowed
        assert(!content);

        // directly write int file
        file_stream.seekp(0, std::ios::beg);
        file_stream.write((char*)content_to_write, def::sstable_header_size + def::bloom_filter_size
		    + def::sstable_data_size * content_to_write->header.key_value_pair_number);

        // assign content
        this->content = content_to_write;

        // TODO: actually I hope read could be separate from write
        // filter initialized here
        filter = new bloomFilter(def::bloom_filter_size);
        filter->set(content->bloomFilterContent);
    }

    void SSTable::load() {
        // no existing content allowed
        if (content) return;
        content = new ssTableContent;
        filter = new bloomFilter(def::bloom_filter_size);

        // directly read from file
        file_stream.seekg(0, std::ios::beg);
        file_stream.read((char*)content, sizeof(ssTableContent));

        // set bloomFilter
        filter->set(content->bloomFilterContent);
    }

    void SSTable::flush() {
        file_stream.flush();
    }

    // return offset of the key-value pair in vLog
    std::optional<std::pair<uint64_t, u_int32_t>> SSTable::get(const key_type& key) {
        // content shouldn't be equal to nullptr
        assert(content);

        // min_key and max_key check
        if (key < content->header.min_key || key > content->header.max_key) {
            return std::nullopt;
        }

        // bloomFilter
        if (!filter->query(key)) {
            return std::nullopt;
        }

        // find the key
        def::ssTableData* start = content->data, 
            * end = content->data + content->header.key_value_pair_number;
        auto it = std::lower_bound(start, end, key, 
            [](const def::ssTableData& dat, const key_type& key) -> bool 
            { return dat.key < key; });

        // key found
        if (it != end && it->key == key) {
            return std::make_pair(it->offset, it->value_length);
        }

        // key not found
        return std::nullopt;
    }

    std::vector<ssTableData> SSTable::scan(
        const key_type& key1, const key_type& key2) {
        // content shouldn't be equal to nullptr
        assert(content);

        // variable holding offset-vlen pair
        std::vector<ssTableData> vec;

        // min_key and max_key check
        if (key2 < content->header.min_key || key1 > content->header.max_key) {
            return vec;
        }

        // find the first key larger or equal than key1
        def::ssTableData* start = content->data, 
            * end = content->data + content->header.key_value_pair_number;
        auto it = std::lower_bound(start, end, key1, 
            [](const def::ssTableData& dat, const key_type& key) -> bool 
            { return dat.key < key; });

        // find all pairs with a smaller key than key2
        while (it != end && it->key <= key2) {
            vec.push_back(*it);
            ++it;
        }

        return vec;
    }
}
