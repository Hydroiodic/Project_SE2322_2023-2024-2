#pragma once

#include <cstdint>
#include <fstream>
#include <string>

namespace vlog {

    class vLog
    {
    private:
        std::string file_name;
        std::fstream file_stream;

        // tail and head of LSMTree
        std::uint64_t tail, head;
        void initialize();

    public:
        vLog();
        ~vLog();

    };

}


