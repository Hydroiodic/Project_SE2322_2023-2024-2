#pragma once

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <string>
#include "../common/definitions.h"
#include "../utils.h"

namespace vlog {

    using def::key_type;
    using def::value_type;
    using garbage_unit = std::pair<def::vLogEntry, uint64_t>;

    class vLog
    {
    private:
        std::string file_name;
        std::fstream file_stream;

        // tail and head of LSMTree
        std::uint64_t tail = 0, head = 0;
        void initialize();

        void createAndOpenFile();

        // use this function to deal with different types of key-value pair
        uint64_t writeIntoFile(def::vLogEntry& entry);
        std::pair<key_type, value_type> readFromFile(uint64_t offset, uint32_t vlen);

        // some variables for garbage collection under multi-process
        size_t garbage_to_collect = 0;

    public:
        explicit vLog(const std::string& name);
        ~vLog();

        uint64_t append(const key_type& key, const value_type& val);
        std::pair<key_type, value_type> get(uint64_t offset, uint32_t vlen);
        void flush();

        void clear();

        std::vector<garbage_unit> getGCReinsertion(uint64_t chunk_size);
        void garbageCollection();
    };

}

