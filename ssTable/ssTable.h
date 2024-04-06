#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <fstream>
#include <optional>
#include "../common/definitions.h"
#include "../skipList/skipList.h"
#include "../bloomFilter/bloomFilter.h"

namespace sstable {

    using def::key_type;
    using def::value_type;
    using def::ssTableContent;
    using bloomFilter = bloomfilter::bloomFilter<key_type>;

    class SSTable
    {
    private:
        std::string file_name;
        std::fstream file_stream;

        uint64_t timestamp = 0;
        size_t layer_number = 0;

        // read once upon a time
        ssTableContent* content = nullptr;

        // bloomFilter here
        bloomFilter* filter = nullptr;

    public:
        explicit SSTable(const std::string& dir_name, uint64_t ts, size_t layer, 
            const std::string& name);
        explicit SSTable(const std::string& file_name);
        ~SSTable();

        void initialize();

        void write(ssTableContent* content);
        void load();
        void flush();

        std::optional<std::pair<uint64_t, uint32_t>> get(const key_type& key);
        std::vector<std::pair<uint64_t, uint32_t>> scan(
            const key_type& key1, const key_type& key2);

        const ssTableContent* tableContent() const { return content; }
        const std::string& getFileName() const { return file_name; }
    };

}


