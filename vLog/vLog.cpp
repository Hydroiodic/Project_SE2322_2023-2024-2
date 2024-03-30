#include <algorithm>
#include <cassert>
#include <filesystem>
#include <iostream>
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
        // prepare to assign for head
        size_t cur_pos = utils::seek_data_block(file_name);
        def::vLogEntry entry;
        unsigned char* read_buffer = new unsigned char[def::v_log_fixed_size];
        unsigned char* check_buffer = new unsigned char[def::v_log_initialization_check_size];

        // head here
        file_stream.seekp(0, std::ios::end);
        head = file_stream.tellp();

        // read from file
        file_stream.seekg(0, std::ios::beg);

        while (cur_pos < head) {
            // read bytes shouldn't exceed all left bytes
            size_t bound = std::min(head - cur_pos, def::v_log_initialization_check_size);
            file_stream.seekg(cur_pos, std::ios::beg);
            file_stream.read((char*)check_buffer, bound);

            bool found = false;
            for (int i = 0; i < bound; ++i, ++cur_pos) {
                if (check_buffer[i] == def::start_sign) {
                    // read from file into buffer
                    file_stream.seekg(cur_pos, std::ios::beg);
                    file_stream.read((char*)read_buffer, def::v_log_fixed_size);

                    // read from buffer into entry
                    size_t accumulated_count = 0;
                    def::read_from_buffer((char*)&entry.start, (char*)read_buffer, 
                        sizeof(entry.start), accumulated_count);
                    def::read_from_buffer((char*)&entry.cycSum, (char*)read_buffer, 
                        sizeof(entry.cycSum), accumulated_count);
                    size_t start_pos = accumulated_count;
                    def::read_from_buffer((char*)&entry.key, (char*)read_buffer, 
                        sizeof(entry.key), accumulated_count);
                    def::read_from_buffer((char*)&entry.value_length, (char*)read_buffer, 
                        sizeof(entry.value_length), accumulated_count);

                    // value here
                    char* val = new char[entry.value_length + 1];
                    val[entry.value_length] = '\0';
                    file_stream.read(val, entry.value_length);

                    // try to validate the entry
                    std::vector<unsigned char> vec(read_buffer + start_pos, 
                        read_buffer + accumulated_count);
                    vec.insert(vec.cend(), val, val + entry.value_length);
                    uint16_t calculated_cycSum = utils::crc16(vec);
                    delete [] val;

                    // find the right entry
                    if (entry.cycSum == calculated_cycSum) {
                        found = true;
                        break;
                    }
                }
            }

            // the right entry has been found
            if (found) break;
        }

        // assign tail and release memory
        tail = cur_pos;
        delete [] read_buffer;
        delete [] check_buffer;
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
        size_t buffer_start = sizeof(entry.start) + sizeof(entry.cycSum);
        memcpy(write_buffer + buffer_start, &entry.key, sizeof(entry.key));
        size_t buffer_end = buffer_start + sizeof(entry.key);
        memcpy(write_buffer + buffer_end, &entry.value_length, sizeof(entry.value_length));
        buffer_end += sizeof(entry.value_length);
        memcpy(write_buffer + buffer_end, entry.value.c_str(), dynamic_size);
        buffer_end += dynamic_size;

        // cycSum calculation
        std::vector<unsigned char> united_entry(write_buffer + buffer_start, write_buffer + buffer_end);
        entry.cycSum = utils::crc16(united_entry);

        // start tag and cycSum
        memcpy(write_buffer, &entry.start, sizeof(entry.start));
        memcpy(write_buffer + sizeof(entry.start), &entry.cycSum, sizeof(entry.cycSum));

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
        // read each part of content from the file
        def::read_from_buffer((char*)&entry.start, (char*)read_buffer, 
            sizeof(entry.start), cur_pos);
        def::read_from_buffer((char*)&entry.cycSum, (char*)read_buffer, 
            sizeof(entry.cycSum), cur_pos);
        // variable for validating data
        size_t buffer_start = cur_pos;
        def::read_from_buffer((char*)&entry.key, (char*)read_buffer, 
            sizeof(entry.key), cur_pos);
        def::read_from_buffer((char*)&entry.value_length, (char*)read_buffer, 
            sizeof(entry.value_length), cur_pos);
        entry.value = std::string(read_buffer + def::v_log_fixed_size);

        // check if ok
        std::vector<unsigned char> united_entry(read_buffer + buffer_start, read_buffer + length);
        delete [] read_buffer;

        // start sign
        assert(entry.start == def::start_sign);
        // cycSum
        assert(entry.cycSum == utils::crc16(united_entry));

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

    void vLog::flush() {
        file_stream.flush();
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

    std::vector<garbage_unit> vLog::getGCReinsertion(uint64_t chunk_size) {
        // read one more vLog entry
        uint64_t read_buffer_size = std::min(chunk_size + def::v_log_fixed_size, head - tail);
        uint64_t max_pos_allowed = std::min(chunk_size, head - tail);

        // read from file
        char* read_buffer = new char[read_buffer_size];
        file_stream.seekg(tail, std::ios::beg);
        file_stream.read(read_buffer, read_buffer_size);

        // prepare to get key-offset pairs
        std::vector<garbage_unit> vec;
        size_t cur_pos = 0;

        // start to get key-offset pairs
        while (cur_pos < max_pos_allowed) {
            // preserve offset here
            uint64_t offset = cur_pos;
            def::vLogEntry entry;

            // read each part of content from the file
            def::read_from_buffer((char*)&entry.start, (char*)read_buffer, 
                sizeof(entry.start), cur_pos);
            def::read_from_buffer((char*)&entry.cycSum, (char*)read_buffer, 
                sizeof(entry.cycSum), cur_pos);
            def::read_from_buffer((char*)&entry.key, (char*)read_buffer, 
                sizeof(entry.key), cur_pos);
            def::read_from_buffer((char*)&entry.value_length, (char*)read_buffer, 
                sizeof(entry.value_length), cur_pos);

            // read value of the entry
            char* val = new char[entry.value_length + 1];
            val[entry.value_length] = '\0';

            if (cur_pos + entry.value_length <= read_buffer_size) {
                // read from buffer
                memcpy(val, read_buffer + cur_pos, entry.value_length);
            }
            else {
                // read from file
                file_stream.seekg(tail + cur_pos, std::ios::beg);
                file_stream.read(val, entry.value_length);
            }

            // assign value and update cur_pos
            entry.value = val;
            delete [] val;
            cur_pos += entry.value_length;

            // TODO: assertion may be needed? however I don't wanna do it now

            // push pair into the vector
            vec.push_back(std::make_pair(std::move(entry), tail + offset));
        }

        // record garbage to be collected
        delete [] read_buffer;
        garbage_to_collect += cur_pos;

        return vec;
    }

    void vLog::garbageCollection() {
        // separate this function apart to prevent the hazard caused accidental interruption
        utils::de_alloc_file(file_name, tail, garbage_to_collect);
        tail += garbage_to_collect;
        garbage_to_collect = 0;
    }
}
