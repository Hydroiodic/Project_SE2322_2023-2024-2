#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <fstream>
#include "../memTable/memTable.h"

namespace sstable {

    using memtable::mem_table_content;

    class SSTable
    {
    private:
        std::string file_name;
        std::fstream file_stream;

        uint64_t timestamp = 0;
        size_t layer_number = 0;

    public:
        explicit SSTable(const std::string& dir_name, uint64_t ts, size_t layer, bool create);
        ~SSTable();

        void initialize();

        void append(const char* data, size_t length);
        void flush();
    };

}


