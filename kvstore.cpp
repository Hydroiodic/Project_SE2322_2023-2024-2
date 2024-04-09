#include "kvstore.h"
#include "common/definitions.h"
#include <algorithm>
#include <iterator>
#include <vector>

KVStore::KVStore(const std::string &dir, const std::string &vlog) : KVStoreAPI(dir, vlog), 
    directory(dir), v_log(vlog), mem_table(dir), level_manager(dir) {
    // get the max timestamp for memTable to use
    size_t cur_level_number = level_manager.size();
    for (int i = 0; i < cur_level_number; ++i) {
        // get details of all files in this level
        auto file_details = level_manager.getLevelFiles(i);
        // the level shouldn't be empty
        if (!file_details.empty()) {
            // scan for max timestamp
            uint64_t max_timestamp = 0;
            for (managerFileDetail detail : file_details) {
                max_timestamp = std::max(max_timestamp, detail.header.time);
            }

            // set timestamp for memTable
            mem_table.setTimestamp(max_timestamp + 1);
            break;
        }
    }
}

KVStore::~KVStore() {
    // write MemTable into file when the instance is destroyed
    writeMemTableIntoFile();
}

void KVStore::writeMemTableIntoFile() {
    // if empty, no need to write into file
    if (mem_table.empty()) return;

    // write vLog and memTable into disk
    ssTableContent* content_to_write = mem_table.getContent(v_log);
    // v_log must be flushed before table is written for multi-process
    v_log.flush();		// flush into vlog file

    // write content_to_write into file system with the format of SSTable
    level_manager.writeIntoSSTableFile(content_to_write);

    // update mem_table
    mem_table.setTimestamp(mem_table.getTimestamp() + 1);
    mem_table.clear();
}

void KVStore::flush() {
    // directly flush all contents into disk
    writeMemTableIntoFile();
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(key_type key, const value_type& value) {
    // insert key-value pair into the mem_table
    if (!mem_table.insert(key, value)) {
        writeMemTableIntoFile();
    }
}

std::optional<std::pair<uint64_t, u_int32_t>> KVStore::getPairFromSSTable(const key_type& key) {
    // iterate through all levels from zero to level_manager.size() - 1
    for (size_t level = 0; level < level_manager.size(); ++level) {
        // start scaning
        if (level >= level_manager.size()) return std::nullopt;
        level_files files = level_manager.getLevelFiles(level);
        if (files.empty()) continue;

        // iterate through all files in each level
        if (level) [[likely]] {
            // if the level number is larger than 0, all files is ordered and unique
            // use binary_search to find the first file detail whose max_key is larger than key
            auto file_detail_less = [](const managerFileDetail& file, const key_type& key) -> bool {
                return file.header.max_key < key;
            };
            auto it = std::lower_bound(files.begin(), files.end(), key, file_detail_less);

            // if not found
            if (it == files.end()) continue;

            // there's only one situation, so try to find it in the file
            // search for the key in SSTable
            std::optional<std::pair<uint64_t, uint32_t>> result;
            if (it->table_cache) {
                // get in cache
                result = it->table_cache->get(key);
            }
            else {
                // get in file system
                SSTable table(it->file_name);
                result = table.get(key);
            }

            // if the key is found
            if (result.has_value()) {
                return result.value();
            }
        }
        else {
            for (managerFileDetail file : files) {
                // search for the key in SSTable
                std::optional<std::pair<uint64_t, uint32_t>> result;
                if (file.table_cache) {
                    // get in cache
                    result = file.table_cache->get(key);
                }
                else {
                    // get in file system
                    SSTable table(file.file_name);
                    result = table.get(key);
                }

                // if the key is found
                if (result.has_value()) {
                    return result.value();
                }
            }
        }
    }

    // not found
    return std::nullopt;
}

std::optional<value_type> KVStore::getFromMemTable(const key_type& key) const {
    return mem_table.get(key);
}

std::optional<value_type> KVStore::getFromSSTable(const key_type& key) {
    auto pair_result = getPairFromSSTable(key);

    // if found
    if (pair_result.has_value()) {
        // if pair_result represents a deleted pair
        if (!pair_result->second) return std::nullopt;

        // get the value from vlog file
        return v_log.get(pair_result->first, pair_result->second).second;
    }

    // not found
    return std::nullopt;
}

void KVStore::scanFromMemTable(const key_type& key1, const key_type& key2, 
    std::map<key_type, value_type>& map) const {
    mem_table.scan(key1, key2, map);
}

void KVStore::scanFromSSTable(const key_type& key1, const key_type& key2, 
    std::map<key_type, value_type>& map) {
    // iterate through all levels from zero to level_manager.size() - 1
    for (size_t level = 0; level < level_manager.size(); ++level) {
        // path to scan files
        std::filesystem::path path(directory);
        path.append(def::sstable_base_directory_name + std::to_string(level));

        // if the directory doesn't exist
        if (!utils::dirExists(path.string())) {
            return;
        }

        // start scaning
        level_files files = level_manager.getLevelFiles(level);
        if (files.empty()) continue;

        // this vector is used to store all ssTableData in current level
        std::vector<ssTableData> vec;
        level_files files_to_scan;

        // iterate through all files in each level
        if (level) [[likely]] {
            // if the level number is larger than 0, all files is ordered and unique
            // use binary_search to find the first file detail whose max_key is larger than key1
            auto file_detail_less = [](const managerFileDetail& file, const key_type& key) -> bool {
                return file.header.max_key < key;
            };
            auto it_begin = std::lower_bound(files.begin(), files.end(), 
                key1, file_detail_less);
            auto eit = files.end(), it_end = it_begin;

            // if not found
            if (it_begin == eit) continue;

            // get all files needed to be scanned into a vector
            while (it_end != eit && it_end->header.min_key <= key2) {
                ++it_end;
            }
            files_to_scan = level_files(it_begin, it_end);
        }
        else {
            files_to_scan = files;
        }

        // iterate through all files in level 0
        for (managerFileDetail file : files) {
            // search for the key in SSTable
            SSTable* table;
            bool from_cache = false;

            if (file.table_cache) {
                // get in cache
                table = file.table_cache;
                from_cache = true;
            }
            else {
                // get in file system
                table = new SSTable(file.file_name);
            }

            // scan for results
            std::vector<ssTableData> temp_vec = table->scan(key1, key2);
            // here use move function to reduce cost
            vec.insert(vec.end(), std::make_move_iterator(temp_vec.begin()), 
                std::make_move_iterator(temp_vec.end()));

            // release memory if we get the table not from cache
            if (!from_cache) delete table;
        }

        // insert ssTableData into the map
        for (ssTableData data : vec) {
            // check whether the key appeared in the map or not
            if (map.find(data.key) == map.end()) {
                // if the pair represents a deleted pair
                if (!data.value_length) {
                    map[data.key] = def::delete_tag;
                }
                else {
                    // get value from vlog file and then put it into a skiplist
                    auto result_pair = v_log.get(data.offset, data.value_length);
                    map[result_pair.first] = result_pair.second;
                }
            }
        }
    }
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
value_type KVStore::get(key_type key) {
    // find from memory
    auto result_mem = getFromMemTable(key);
    if (result_mem.has_value()) {
        // already deleted
        if (result_mem.value() == def::delete_tag) return value_type();
        return result_mem.value();
    }

    // find from storage
    auto result_sto = getFromSSTable(key);
    if (result_sto.has_value()) {
        // already deleted
        if (result_sto.value() == def::delete_tag) return value_type();
        return result_sto.value();
    }

    // not found
    return value_type();
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(key_type key) {
    // not found
    if (get(key) == value_type()) return false;

    // insert a delete pair
    put(key, def::delete_tag);
    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
    // clear mem_table
    mem_table.clear();
    mem_table.setTimestamp(0);

    // delete sstable files
    level_manager.clear();

    // clear the vlog file
    v_log.clear();
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty list indicates not found.
 */
void KVStore::scan(key_type key1, key_type key2, std::list<std::pair<key_type, value_type>>& list) {
    // the list should be empty initially
    assert(list.empty());

    // key2 should be larger than or equal to key1
    if (key1 > key2) {
        return;
    }

    // use skipList to store all values found
    std::map<key_type, value_type> map;

    // scan from memory
    scanFromMemTable(key1, key2, map);

    // scan from storage
    scanFromSSTable(key1, key2, map);

    // append all contents in the skiplist to the list
    auto it = map.cbegin(), eit = map.cend();
    while (it != eit) {
        // if the pair isn't a deleted one
        if (it->second != def::delete_tag) {
            // add to the list
            list.push_back(*it);
        }

        // iterate for the next
        ++it;
    }
}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size) {
    // check collected vLog entries here
    std::vector<garbage_unit> garbage_to_validate = v_log.getGCReinsertion(chunk_size);

    for (auto garbage : garbage_to_validate) {
        def::vLogEntry entry = garbage.first;

        // not in mem_table
        if (!getFromMemTable(entry.key).has_value()) {
            auto pair_result = getPairFromSSTable(entry.key);
            if (pair_result.has_value() && pair_result->first == garbage.second) {
                put(entry.key, entry.value);
            }
        }
    }

    // flush before collecting garbage for multi-process
    flush();
    v_log.garbageCollection();
}

