#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <cstring>

namespace def {

    // types of key and value
    using key_type = uint64_t;
    using value_type = std::string;

    struct sstableHeader {
        uint64_t time;
        uint64_t key_value_pair_number;
        def::key_type min_key, max_key;
    };

    struct sstableData {
        key_type key;
        uint64_t offset;
        uint32_t value_length;
    };

    struct vLogEntry {
        unsigned char start;
        uint16_t cycSum;
        key_type key;
        uint32_t value_length;
        value_type value;
    };

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

    // max number of keys stored in the memory
    const size_t max_file_size = 16 * 1024;
    const size_t sstable_header_size = sizeof(sstableHeader);
    const size_t bloom_filter_size = 8192;
    const size_t sstable_data_size = sizeof(sstableData);
    const size_t max_key_number = (max_file_size - sstable_header_size - 
        bloom_filter_size) / sstable_data_size;

    inline bool compare_filename_greater(const std::string& a, const std::string& b) {
        return (a.length() > b.length()) || (a.length() == b.length() && a > b);
    }

    const size_t v_log_initialization_check_size = 1000;

    inline void read_from_buffer(char* dst, char* src, size_t size, size_t& pos) {
        memcpy((char*)dst, src + pos, size);
        pos += size;
    };
}
