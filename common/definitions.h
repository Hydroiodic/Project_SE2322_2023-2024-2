#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <string>
#include <cstring>

namespace sstable {
    // weak definition for sstable::SSTable
    class SSTable;
}

namespace def {

    // types of key and value
    using key_type = uint64_t;
    using value_type = std::string;

    // tag representing deleted element
    const value_type delete_tag = "~DELETED~";

    // charactor representing start
    const unsigned char start_sign = 0xff;

    // class vLog has a fixed-size part and a dynamic-size part
    const size_t v_log_fixed_size = sizeof(unsigned char) + sizeof(uint16_t) 
        + sizeof(key_type) + sizeof(uint32_t);

    // extension name of SSTable
    const std::string sstable_extension_name = ".sst";

    // base name of directories storing sstable
    const std::string sstable_base_directory_name = "level-";

    // the header of SSTable
    struct ssTableHeader {
        uint64_t time;
        uint64_t key_value_pair_number;
        def::key_type min_key, max_key;
    };

    // the data of SSTable
    struct ssTableData {
        key_type key;
        uint64_t offset;
        uint32_t value_length;
    };

    // max number of keys stored in the memory
    const size_t max_file_size = 16 * 1024;
    const size_t sstable_header_size = sizeof(ssTableHeader);
    const size_t bloom_filter_size = 8192;
    const size_t sstable_data_size = sizeof(ssTableData);
    const size_t max_key_number = (max_file_size - sstable_header_size - 
        bloom_filter_size) / sstable_data_size;

    // the number of bytes scanned once when initializing vLog
    const size_t v_log_initialization_check_size = 1000;

    // the content of one SSTable
    struct ssTableContent {
        def::ssTableHeader header;
        unsigned char bloomFilterContent[bloom_filter_size];
        def::ssTableData data[max_key_number];
    };

    // the entry content of vLog
    struct vLogEntry {
        unsigned char start;
        uint16_t cycSum;
        key_type key;
        uint32_t value_length;
        value_type value;
    };

    // a function to read something from buffer
    inline void read_from_buffer(char* dst, char* src, size_t size, size_t& pos) {
        memcpy((char*)dst, src + pos, size);
        pos += size;
    }

    // the detail of a SSTable used by levelManager
    struct managerFileDetail {
        std::string file_name;
        def::ssTableHeader header;
        sstable::SSTable* table_cache = nullptr;
    };
    using level_files = std::deque<managerFileDetail>;

    inline bool compare_file_detail_zero_level(const managerFileDetail& a, const managerFileDetail& b) {
        return a.header.time > b.header.time || 
            a.header.time == b.header.time && a.header.min_key < b.header.min_key;
    }

    inline bool compare_file_detail_other_level(const managerFileDetail& a, const managerFileDetail& b) {
        return a.header.min_key < b.header.min_key;
    }

    inline std::string getLevelDirectoryPath(const std::string& dir, size_t level) {
        // use a safer way to deal with path
        std::filesystem::path path(dir);
        path.append(sstable_base_directory_name + std::to_string(level));

        return path.string();
    }

    // the function used to judge whether a level is full
    inline size_t maxLevelSize(size_t level) {
        return 1 << (level + 1);
    }

    // the type used in priority_queue when merging SSTable
    using pq_type = std::pair<ssTableData, size_t>;

    // the structure used to build a less-rooted heap
    struct pq_greater {
        bool operator()(const pq_type& a, const pq_type& b) {
            return a.first.key > b.first.key || a.first.key == b.first.key && a.second > b.second;
        }
    };

    // the number of levels that allow cache exists
    const size_t cached_levels = 4;
}
