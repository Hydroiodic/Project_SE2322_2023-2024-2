#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <functional>
#include <optional>
#include <string>
#include <vector>
#include "kvstore.h"
#include "common/definitions.h"
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

std::optional<value_type> KVStore::getFromMemTable(const key_type& key) {
	return mem_table.get(key);
}

std::optional<value_type> KVStore::getFromSSTable(const key_type& key) {
	// TODO: iterate through all levels
	size_t level = 0;

	// path to scan files
	std::filesystem::path path(directory);
	path.append(def::sstable_base_directory_name + std::to_string(level));

	// start scaning
	std::vector<std::string> files;
	utils::scanDir(path.string(), files);
	std::sort(files.begin(), files.end(), std::greater());

	for (std::string file : files) {
		// create SSTable and search for the key
		// TODO: use cache to optimize
		SSTable table(path.string(), file);
		auto result = table.get(key);

		// if the key is found
		if (result.has_value()) {
			auto result_pair = result.value();
			return v_log.get(result_pair.first, result_pair.second);
		}
	}

	// not found
	return std::nullopt;
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
value_type KVStore::get(key_type key) {
	// find from memory
	auto result_mem = getFromMemTable(key);
	if (result_mem.has_value()) return result_mem.value();

	// find from storage
	auto result_sto = getFromSSTable(key);
	if (result_sto.has_value()) return result_sto.value();
	
	// not found
	return "";
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(key_type key) {
	return false;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {

}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(key_type key1, key_type key2, std::list<std::pair<key_type, value_type>>& list) {

}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(key_type chunk_size) {

}
