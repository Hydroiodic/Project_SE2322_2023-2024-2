#pragma once

#include <string>
#include "kvstore_api.h"
#include "./common/definitions.h"
#include "./memTable/memTable.h"
#include "./ssTable/ssTable.h"
#include "./vLog/vLog.h"

using memtable::memTable;
using memtable::mem_table_content;
using sstable::SSTable;
using vlog::vLog;
using def::key_type;
using def::value_type;

class KVStore : public KVStoreAPI
{
private:
	std::string directory;

	vLog v_log;
	memTable mem_table;

	void writeMemTableIntoFile();

public:
	KVStore(const std::string &dir, const std::string &vlog);

	~KVStore();

	void put(key_type key, const value_type& value) override;

	std::string get(key_type key) override;

	bool del(key_type key) override;

	void reset() override;

	void scan(key_type key1, key_type key2, std::list<std::pair<key_type, value_type>>& list) override;

	void gc(key_type chunk_size) override;
};
