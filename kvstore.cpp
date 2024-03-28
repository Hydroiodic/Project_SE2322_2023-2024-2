#include <algorithm>
#include <cassert>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <utility>
#include <vector>
#include "kvstore.h"
#include "common/definitions.h"
#include "skipList/skipList.h"
#include "utils.h"

KVStore::KVStore(const std::string &dir, const std::string &vlog)
	: KVStoreAPI(dir, vlog), v_log(vlog), directory(dir) {
	/* maybe there's a lot of things to do but now it's empty */
}

KVStore::~KVStore() {
	/* maybe there's a lot of things to do but now it's empty */
	writeMemTableIntoFile();
}

void KVStore::writeMemTableIntoFile() {
	// if empty, no need to write into file
	if (mem_table.empty()) return;

	// if the mem_table is full, create a sstable
	SSTable table(directory, mem_table.getTimestamp(), 0);

	// write and then release memory
	ssTableContent* content_to_write = mem_table.getContent(v_log);
	table.write(content_to_write);

	// TODO: check and compaction

	// update mem_table
	mem_table.setTimestamp(mem_table.getTimestamp() + 1);
	mem_table.clear();
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

std::optional<value_type> KVStore::getFromMemTable(const key_type& key) const {
	return mem_table.get(key);
}

std::optional<value_type> KVStore::getFromSSTable(const key_type& key) {
	// TODO: iterate through all levels
	size_t level = 0;

	// path to scan files
	std::filesystem::path path(directory);
	path.append(def::sstable_base_directory_name + std::to_string(level));

	// if the directory doesn't exist
	if (!utils::dirExists(path.string())) {
		return std::nullopt;
	}

	// start scaning
	std::vector<std::string> files;
	utils::scanDir(path.string(), files);
	std::sort(files.begin(), files.end(), std::greater());

	// iterate through all files in level 0
	// TODO: when the number of level is larger than 0, we should use a quicker way to search
	for (std::string file : files) {
		// create SSTable and search for the key
		// TODO: use cache to optimize
		SSTable table(path.string(), file);
		auto result = table.get(key);

		// if the key is found
		if (result.has_value()) {
			auto result_pair = result.value();
			return v_log.get(result_pair.first, result_pair.second).second;
		}
	}

	// not found
	return std::nullopt;
}

void KVStore::scanFromMemTable(const key_type& key1, const key_type& key2, 
	skiplist::skiplist_type& skip_list) const {
	mem_table.scan(key1, key2, skip_list);
}

void KVStore::scanFromSSTable(const key_type& key1, const key_type& key2, 
	skiplist::skiplist_type& skip_list) {
	// TODO: iterate through all levels
	size_t level = 0;

	// path to scan files
	std::filesystem::path path(directory);
	path.append(def::sstable_base_directory_name + std::to_string(level));

	// if the directory doesn't exist
	if (!utils::dirExists(path.string())) {
		return;
	}

	// start scaning
	std::vector<std::string> files;
	utils::scanDir(path.string(), files);
	std::sort(files.begin(), files.end(), std::greater());

	// iterate through all files in level 0
	// TODO: when the number of level is larger than 0, we should use a quicker way to search
	for (std::string file : files) {
		// create SSTable and search for the key
		// TODO: use cache to optimize
		SSTable table(path.string(), file);
		std::vector<std::pair<uint64_t, uint32_t>> vec = table.scan(key1, key2);

		// scan for corresponding values
		for (auto pair : vec) {
			auto result_pair = v_log.get(pair.first, pair.second);
			skip_list.put(result_pair.first, result_pair.second, false);
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
	// TODO: there's something wrong
	mem_table.clear();

	// delete sstable files
	size_t level_num = 0;
	std::filesystem::path directory_name(directory);
	directory_name.append(def::sstable_base_directory_name + std::to_string(level_num));
	while (utils::dirExists(directory_name.string())) {
		// basic variables
		std::vector<std::string> files;
		std::filesystem::path path(directory_name);
		utils::scanDir(directory_name, files);

		// remove files
		for (auto file : files) {
			// use a safer method to deal with path
			std::filesystem::path cur_file_path(path);
			cur_file_path.append(file);
			utils::rmfile(cur_file_path.string());
		}

		// remove the directory
		utils::rmdir(path.string());

		// next directory
		++level_num;
		directory_name = directory_name.parent_path()
			.append(def::sstable_base_directory_name + std::to_string(level_num));
	}

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
	skiplist::skiplist_type skip_list;

	// scan from memory
	scanFromMemTable(key1, key2, skip_list);

	// scan from storage
	scanFromSSTable(key1, key2, skip_list);

	// append all contents in the skiplist to the list
	auto it = skip_list.cbegin(), eit = skip_list.cend();
    while (it != eit) {
		// if the pair is a deleted one
        if (it.value() == def::delete_tag) {
			++it; continue;
		}

		// add to the list
		list.push_back(std::make_pair(it.key(), it.value()));
		++it;
    }
}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(key_type chunk_size) {

}
