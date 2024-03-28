#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include <ios>
#include "vLog.h"
#include "../common/exceptions.h"

namespace vlog {

    vLog::vLog(const std::string& name) : file_name(name) {
        // use a safer way to manage file path
        std::filesystem::path path(name);
        if (!path.has_filename()) {
            throw exception::vlog_path_error();
        }

        // create directory first
        if (utils::mkdir(path.parent_path().string()) != 0) {
            throw exception::create_directory_fail();
        }

        // open file and initialize vlog
        createAndOpenFile();
        initialize();
    }

    vLog::~vLog() {
        file_stream.close();
    }

    void vLog::initialize() {
        /* there's a lot of things TODO */
    }

    void vLog::createAndOpenFile() {
        // the file shouldn't be opened now
        assert(!file_stream.is_open());

        // try to open first, if failing, then create it
        file_stream.open(file_name, std::ios::in | std::ios::out | std::ios::binary);
        if (!file_stream.is_open()) {
            file_stream.clear();
            file_stream.open(file_name, std::ios::out);
            file_stream.close();
            file_stream.open(file_name, std::ios::in | std::ios::out | std::ios::binary);
        }
    }

    uint64_t vLog::writeIntoFile(def::vLogEntry& entry) {
        file_stream.seekp(0, std::ios::end);
        uint64_t start_pos = file_stream.tellp();

        size_t dynamic_size = entry.value_length;
        char* write_buffer = new char[def::v_log_fixed_size + dynamic_size];

        // key, vlen and value
        size_t buffer_start = sizeof(unsigned char) + sizeof(uint16_t);
        memcpy(write_buffer + buffer_start, &entry.key, sizeof(key_type) + sizeof(uint32_t));
        size_t buffer_end = buffer_start + sizeof(key_type) + sizeof(uint32_t);
        memcpy(write_buffer + buffer_end, entry.value.c_str(), dynamic_size);
        buffer_end += dynamic_size;

        // cycSum calculation
        std::vector<unsigned char> united_entry(write_buffer + buffer_start, write_buffer + buffer_end);
        entry.cycSum = utils::crc16(united_entry);

        // start tag and cycSum
        memcpy(write_buffer, &entry, sizeof(unsigned char) + sizeof(uint16_t));

        // write into file using fstream and release memory
        file_stream.write(write_buffer, def::v_log_fixed_size + dynamic_size);
        delete [] write_buffer;

        // head = file_stream.tellp();
        head = start_pos + def::v_log_fixed_size + dynamic_size;

        return start_pos;
    }

    std::pair<key_type, value_type> vLog::readFromFile(uint64_t offset, uint32_t vlen) {
        // find the position
        file_stream.seekg(offset, std::ios::beg);
        def::vLogEntry entry{};

        // read from file
        size_t length = def::v_log_fixed_size + vlen;
        char* read_buffer = new char[length + 1];
        file_stream.read(read_buffer, length);
        read_buffer[length] = '\0';

        // assign read data to the entry
        size_t cur_pos = 0;
        auto read_from_buffer = [&cur_pos, &read_buffer](char* pointer, size_t size) {
            memcpy((char*)pointer, read_buffer + cur_pos, size);
            cur_pos += size;
        };

        // read each part of content from the file
        read_from_buffer((char*)&entry.start, sizeof(entry.start));
        read_from_buffer((char*)&entry.cycSum, sizeof(entry.cycSum));
        read_from_buffer((char*)&entry.key, sizeof(entry.key));
        read_from_buffer((char*)&entry.value_length, sizeof(entry.value_length));
        entry.value = std::string(read_buffer + def::v_log_fixed_size);
        delete [] read_buffer;

        // check if ok (cycSum TODO)
        if (entry.start != def::start_sign) {
            // TODO
        }

        return std::make_pair(entry.key, entry.value);
    }

    uint64_t vLog::append(const key_type& key, const value_type& val) {
        def::vLogEntry entry {
            def::start_sign,
            0,  // calculated later
            key,
            static_cast<uint32_t>(val.length()),
            val,
        };

        return writeIntoFile(entry);
    }

    std::pair<key_type, value_type> vLog::get(uint64_t offset, uint32_t vlen) {
        return readFromFile(offset, vlen);
    }

    void vLog::clear() {
        // close and then delete the file
        file_stream.close();
        utils::rmfile(file_name);

        // truncate and re-open the file
        createAndOpenFile();
        assert(file_stream.is_open());

        // related variables
        head = tail = 0;
    }
    
}
