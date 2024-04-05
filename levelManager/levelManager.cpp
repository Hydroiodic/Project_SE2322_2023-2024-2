#include "levelManager.h"
#include "../utils.h"
#include <algorithm>
#include <cassert>
#include <queue>

namespace levelmanager {

    levelManager::levelManager(const std::string& dir) : directory_name(dir) {
        /* maybe there's a lot of things TODO */
        scanLevels();
    }

    levelManager::~levelManager() {
        /* maybe there's a lot of things TODO */
    }

    level_files levelManager::sortFiles(const std::vector<std::string>& files, size_t level) const {
        // use a vector to store all file details
        level_files current_level;

        // scan all files in this level
        for (std::string file : files) {
            // use a safer way to get directory
            std::filesystem::path path(directory_name);
            path.append(def::sstable_base_directory_name + std::to_string(level));
            path.append(file);

            // open file
            std::fstream stream;
            stream.open(path.string(), std::ios::in | std::ios::binary);
            assert(stream.is_open());

            // read header from file
            managerFileDetail detail { path.string() };
            stream.read((char*)&detail.header, sizeof(detail.header));
            stream.close();

            current_level.push_back(detail);
        }

        // sort with details read above
        // the method to sort differs from zero level and other levels
        if (level) {
            std::sort(current_level.begin(), current_level.end(), 
                def::compare_file_detail_other_level);
        }
        else {
            std::sort(current_level.begin(), current_level.end(), 
                def::compare_file_detail_zero_level);
        }

        return current_level;
    }

    void levelManager::scanLevels() {
        // start from zero to scan all levels
        level_number = 0;
        levels.clear();

        // use a safer way to process path
        std::filesystem::path path(directory_name);
        path.append(def::sstable_base_directory_name + std::to_string(level_number));

        // start to scan
        while (utils::dirExists(path.string())) {
            // scan all files in the current level
            std::vector<std::string> current_level;
            utils::scanDir(path.string(), current_level);

            // push into vector
            levels.push_back(sortFiles(current_level, level_number));

            // next iteration
            path = path.parent_path().append(def::sstable_base_directory_name + 
                std::to_string(++level_number));
        }
    }

    const level_files& levelManager::getLevelFiles(size_t level) const {
        // the value of level must be less than level_number
        assert(level < level_number);

        return levels[level];
    }

    size_t levelManager::size() const {
        return level_number;
    }

    void levelManager::clear() {
        // remove files from each level
        for (level_files current_level : levels) {
            for (managerFileDetail file_detail : current_level) {
                utils::rmfile(file_detail.file_name);
            }
        }

        // use a safer way to process path
        std::filesystem::path path(directory_name);

        // remove each directory
        for (int i = 0; i < level_number; ++i) {
            path.append(def::sstable_base_directory_name + std::to_string(i));
            utils::rmdir(path.string());

            // reset path to its parent directory
            path = path.parent_path();
        }

        // reset level_number and levels
        level_number = 0;
        levels.clear();
    }

    std::vector<ssTableContent*> levelManager::mergeSSTable(
        const std::vector<managerFileDetail>& files) const {
        // some definitions
        std::vector<ssTableContent*> merged_contents;
        std::vector<SSTable*> contents;
        std::vector<size_t> index;
        size_t file_number = files.size();

        // use std::greater to build a less-root heap
        // ATTENTION! if a SSTable is newer, it should be more front in "files"
        std::priority_queue<pq_type, std::vector<pq_type>, def::pq_greater> pq;

        uint64_t max_time = 0;
        for (size_t i = 0; i < file_number; ++i) {
            // create table instance and push the first data element into pq
            SSTable* table = new SSTable(files[i].file_name);
            contents.push_back(table);
            pq.push(std::make_pair(table->tableContent()->data[0], i));
            index.push_back(1);

            // get the time
            max_time = std::max(max_time, table->tableContent()->header.time);
        }

        // some variables used in later merging
        ssTableContent* current_content = new ssTableContent;
        bloomFilter filter(def::bloom_filter_size);
        size_t index_in_content_data = 0;
        key_type current_key{};
        bool initialized = false;

        // a lambda function to set data and then push current_content into the vector
        auto collect_data_for_content = [&]() {
            memcpy(&current_content->bloomFilterContent, filter.getContent(), 
                def::bloom_filter_size);
            current_content->header.key_value_pair_number = index_in_content_data;
            current_content->header.min_key = current_content->data[0].key;
            current_content->header.max_key = current_content->data[index_in_content_data - 1].key;
            current_content->header.time = max_time;
            merged_contents.push_back(current_content);
        };

        // start merging
        while (!pq.empty()) {
            // get the top element
            pq_type front_element = pq.top();
            pq.pop();

            // some variables used later
            size_t table_index = front_element.second;
            auto content_of_current_table = contents[table_index]->tableContent();

            if (index[table_index] < content_of_current_table->header.key_value_pair_number) [[likely]] {
                // push the next element into priority_queue
                pq.push(std::make_pair(content_of_current_table->data[index[table_index]], table_index));
                ++index[table_index];
            }

            // if same keys encountered
            if (initialized && current_key == front_element.first.key) [[unlikely]] continue;

            // insert into these structures
            current_content->data[index_in_content_data++] = front_element.first;
            filter.insert(front_element.first.key);
            current_key = front_element.first.key;
            initialized = true;

            // if the table is full, push it into the vector
            if (index_in_content_data >= def::max_key_number) {
                // prepare header and bloomfilter
                collect_data_for_content();

                // reset the state
                filter.clear();
                index_in_content_data = 0;
                current_content = new ssTableContent;
            }
        }

        // the last data
        if (index_in_content_data) {
            collect_data_for_content();
        }
        else {
            delete current_content;
        }

        // release memory
        for (auto table : contents) {
            delete table;
        }

        return merged_contents;
    }

    void levelManager::checkZeroLevelCompaction() {
        // because level-0 is unordered, use another function to manage it
        assert(level_number);
        if (levels[0].size() <= def::maxLevelSize(0)) return;

        // TODO: compaction of level-0
        std::vector<managerFileDetail> files;
        for (int index = levels[0].size() - 1; index >= 0; --index) {
            // files in level 0
            files.push_back(levels[0][index]);

            // deal with files in level 1
            size_t i = 0, j = 0;
            if (level_number > 1) [[likely]] {
                size_t second_level_size = levels[1].size();

                // find the first managerFileDetail ...
                for (; i < second_level_size; ++i) {
                    if (levels[1][i].header.max_key >= levels[0][index].header.min_key) {
                        break;
                    }
                }

                // ... until the last one
                // the range is [i, j)
                j = i;
                while (j < second_level_size && 
                    levels[1][j].header.min_key <= levels[0][index].header.max_key) {
                    files.push_back(levels[1][j]);
                    ++j;
                }
            }

            // write these SSTables into storage
            std::vector<ssTableContent*> contents_to_insert = mergeSSTable(files);
            size_t merged_file_size = contents_to_insert.size();
            for (size_t no = merged_file_size; no; --no) {
                writeIntoLevel(contents_to_insert[no - 1], 1, no, i);
            }

            // delete files in level 1
            i += merged_file_size; j += merged_file_size;
            for (size_t no = i; no < j; ++no) {
                utils::rmfile(levels[1][no].file_name);
            }
            levels[1].erase(levels[1].begin() + i, levels[1].begin() + j);
            utils::rmfile(levels[0][index].file_name);
            files.clear();
        }

        // clear level 0
        levels[0].clear();
    }

    void levelManager::checkCompaction(size_t level) {
        /* here very very very many things TODO */
        assert(level_number > level);
        if (levels[level].size() <= def::maxLevelSize(level)) return;

        // TODO: the level is full, compaction needed

    }

    void levelManager::writeIntoLevel(ssTableContent* content, size_t level, size_t no, size_t pos) {
        // new level will be created
        if (level_number <= level) {
            // only one more level once is allowed
            assert(level_number == level);
            utils::mkdir(def::getLevelDirectoryPath(directory_name, level_number++));
            levels.push_back(level_files());
        }

        // the size of the vector should be equal to level_number
        assert(levels.size() == level_number);

        // if the mem_table is full, create a sstable
        SSTable table(directory_name, content->header.time, level, no);
        table.write(content);

        managerFileDetail new_file_detail { table.getFileName(), content->header };
        // TODO: those level with an index larger than 0 should be ordered
        levels[level].insert(levels[level].begin() + pos, new_file_detail);
    }

    void levelManager::writeIntoSSTableFile(ssTableContent* content) {
        // write the content into the first level
        writeIntoLevel(content, 0);

        // check compaction for the level
        checkZeroLevelCompaction();
    }

    void levelManager::removeSSTableFile(const std::string& file_name, size_t level) {
        // level_number must be larger than level
        assert(level_number > level);

        // scan all files to find the one to delete
        auto it = levels[level].begin(), eit = levels[level].end();
        for (; it != eit; ++it) {
            if (it->file_name == file_name) {
                break;
            }
        }

        // it shouldn't reach the end
        assert(it != eit);

        // remove from deque
        levels[level].erase(it);
    }

}

