#pragma once

#include <cstdint>
#include <fstream>
#include <string>
#include "../common/definitions.h"
#include "../utils.h"

namespace vlog {

    using key_type = def::key_type;
    using value_type = def::value_type;

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
        value_type readFromFile(uint64_t offset, uint32_t vlen);

    public:
        explicit vLog(const std::string& name);
        ~vLog();

        uint64_t append(const key_type& key, const value_type& val);
        value_type get(uint64_t offset, uint32_t vlen);

        void clear();
    };

}


