#include "vLog.h"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <ios>

namespace vlog {

    vLog::vLog(const std::string& name) : file_name(name) {
        file_stream.open(file_name, std::ios::in | std::ios::app | std::ios::binary);
        initialize();
    }

    vLog::~vLog() {
        file_stream.close();
    }

    void vLog::initialize() {
        /* there's a lot of things TODO */
    }

    uint64_t vLog::writeIntoFile(def::vLogEntry& entry) {
        file_stream.seekp(std::ios::end);
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

    value_type vLog::readFromFile(uint64_t offset) {
        // find the position
        file_stream.seekg(std::ios::beg + offset);
        def::vLogEntry entry;

        file_stream.read((char*)&entry, def::v_log_fixed_size);
        
        // check if ok (cycSum TODO)
        if (entry.start != def::start_sign) {
            // TODO
        }

        // read from file
        char* value_buffer = new char[entry.value_length + 1];
        value_buffer[entry.value_length] = '\0';
        file_stream.read(value_buffer, entry.value_length);

        // assign, delete and return
        entry.value = value_buffer;
        delete [] value_buffer;
        return entry.value;
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

    value_type vLog::get(uint64_t offset) {
        return readFromFile(offset);
    }
    
}
