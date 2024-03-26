#include <string>
#include "kvstore.h"

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
	SSTable table(directory, mem_table.getTimestamp(), 0, true);

	// write and then release memory
	mem_table_content content_to_write = mem_table.getContent(v_log);
	table.append(content_to_write.first, content_to_write.second);
	delete [] content_to_write.first;

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

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(key_type key) {
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
