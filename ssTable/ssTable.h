#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <fstream>
#include <optional>
#include "../common/definitions.h"

namespace sstable {

    using def::key_type;
    using def::value_type;

    struct ssTableContent {
        def::sstableHeader header;
        char bloomFilterContent[def::bloom_filter_size];
        def::sstableData data[def::sstable_data_size * def::max_key_number];
    };

    class SSTable
    {
    private:
        std::string file_name;
        std::fstream file_stream;

        uint64_t timestamp = 0;
        size_t layer_number = 0;

        // read once upon a time
        ssTableContent* content = nullptr;

    public:
        explicit SSTable(const std::string& dir_name, uint64_t ts, size_t layer);
        explicit SSTable(const std::string& dir_name, const std::string& file);
        ~SSTable();

        void initialize();

        void write(ssTableContent* content);
        void load();
        void flush();

        std::optional<std::pair<uint64_t, u_int32_t>> get(const key_type& key);
    };

}


