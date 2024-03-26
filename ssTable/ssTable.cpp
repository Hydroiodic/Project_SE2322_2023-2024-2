#include <ios>
#include <cstddef>
#include <filesystem>
#include <string>
#include "ssTable.h"
#include "../utils.h"
#include "../common/exceptions.h"
#include "../common/definitions.h"

namespace sstable {

    SSTable::SSTable(const std::string& dir_name, uint64_t ts, size_t layer, bool create) 
        : timestamp(ts), layer_number(layer) {
        // use a safer way to join directories
        std::filesystem::path directory(dir_name);
        directory.append(def::sstable_base_directory_name + std::to_string(layer));

        // start to create a new file
        if (utils::mkdir(directory.string()) == -1) {
            throw exception::create_directory_fail();
        }

        // a safer way to use directory
        directory.append(std::to_string(ts));
        directory.replace_extension(def::sstable_extension_name);
        file_name = directory.string();

        // open file
        file_stream.open(file_name, std::ios::in | std::ios::out | std::ios::app);

        if (!create) initialize();
    }

    SSTable::~SSTable() {
        file_stream.close();
    }

    void SSTable::initialize() {
        /* maybe there's lots of things TODO? */
    }

    void SSTable::append(const char* data, size_t length) {
        file_stream.write(data, length);
    }

    void SSTable::flush() {
        file_stream.flush();
    }

}
