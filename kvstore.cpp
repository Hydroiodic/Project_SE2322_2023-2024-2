#include "kvstore.h"
#include "common/definitions.h"
#include "skipList/skipList.h"
#include "utils.h"

KVStore::KVStore(const std::string &dir, const std::string &vlog) : KVStoreAPI(dir, vlog), 
	directory(dir), v_log(vlog), mem_table(dir), level_manager(dir) {
	/* maybe there's a lot of things TODO */
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
	/* maybe there's a lot of things TODO */
	writeMemTableIntoFile();
}

void KVStore::writeMemTableIntoFile() {
	// if empty, no need to write into file
	if (mem_table.empty()) return;

	// write vLog and memTable into disk
	ssTableContent* content_to_write = mem_table.getContent(v_log);
	// v_log must be flushed before table is written for multi-process
	v_log.flush();		// flush into vlog file

	// TODO: check and compaction
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
	// TODO: iterate through all levels
	size_t level = 0;

	// start scaning
	if (level >= level_manager.size()) return std::nullopt;
	level_files files = level_manager.getLevelFiles(level);
	if (files.empty()) return std::nullopt;

	// iterate through all files in level 0
	// TODO: when the number of level is larger than 0, we should use a quicker way to search
	for (managerFileDetail file : files) {
		// create SSTable and search for the key
		// TODO: use cache to optimize
		SSTable table(file.file_name);
		auto result = table.get(key);

		// if the key is found
		if (result.has_value()) {
			return result.value();
		}
	}

	return std::nullopt;
}


std::optional<value_type> KVStore::getFromMemTable(const key_type& key) const {
	return mem_table.get(key);
}

std::optional<value_type> KVStore::getFromSSTable(const key_type& key) {
	auto pair_result = getPairFromSSTable(key);

	// if found
	if (pair_result.has_value()) {
		return v_log.get(pair_result->first, pair_result->second).second;
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
	level_files files = level_manager.getLevelFiles(level);
	if (files.empty()) return;

	// iterate through all files in level 0
	// TODO: when the number of level is larger than 0, we should use a quicker way to search
	for (managerFileDetail file : files) {
		// create SSTable and search for the key
		// TODO: use cache to optimize
		SSTable table(file.file_name);
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
void KVStore::gc(uint64_t chunk_size) {
	// check collected vLog entries here
	// TODO: prevent the potential hazard caused by interrupt after gc but before insertion
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

