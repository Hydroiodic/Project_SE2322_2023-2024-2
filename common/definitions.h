#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

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
        key_type key;
        uint32_t value_length;
        value_type value;
    };

    // tag representing deleted element
    const value_type delete_tag = "~DELETED~";

    // max number of keys stored in the memory
    const size_t max_file_size = 16 * 1024;
    const size_t sstable_header_size = sizeof(sstableHeader);
    const size_t bloom_filter_size = 8192;
    const size_t sstable_data_size = sizeof(sstableData);
    const size_t max_key_number = (max_file_size - sstable_header_size - 
        bloom_filter_size) / sstable_data_size;
}
