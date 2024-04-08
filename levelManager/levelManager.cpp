#include "levelManager.h"
#include "../utils.h"
#include <algorithm>
#include <cstddef>
#include <queue>
#include <sys/time.h>

namespace levelmanager {

    levelManager::levelManager(const std::string& dir) : directory_name(dir) {
        // update file_prefix for levelManager
        updatePrefix();

        // scan for files in each level
        scanLevels();
    }

    levelManager::~levelManager() {
        // release memory allocated before for SSTable
        for (size_t i = 0; i < level_number && i < def::cached_levels; ++i) {
            for (auto file : levels[i]) {
                // delete [] is a safer way to release memory
                if (file.table_cache) delete file.table_cache;
            }
        }
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

            // open SSTable and create managerFileDetail for it
            SSTable* table = new SSTable(path.string());
            managerFileDetail detail { path.string(), table->tableContent()->header };

            // store SSTable into cache or delete it
            if (level < def::cached_levels) {
                detail.table_cache = table;
            }
            else {
                delete table;
            }

            current_level.push_back(detail);
        }

        // sort with details read above
        // the method to sort differs from zero level and other levels
        if (level) [[likely]] {
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
            levels_time.push_back(0);

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
        levels_time.clear();
    }

    std::vector<ssTableContent*> levelManager::mergeSSTable(
        const std::vector<managerFileDetail>& files, bool remove_deleted_pair) const {
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
            // create table instance while checking cache
            SSTable* table;
            if (files[i].table_cache) {
                table = files[i].table_cache;
            }
            else {
                table = new SSTable(files[i].file_name);
            }

            // push the first data element into pq
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
            if (front_element.first.value_length || !remove_deleted_pair) [[likely]] {
                // if the pair is a deleted one and remove_deleted_pair is specified
                current_content->data[index_in_content_data++] = front_element.first;
                filter.insert(front_element.first.key);
            }
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

    void levelManager::checkCompaction(size_t level) {
        assert(level_number > level);
        if (levels[level].size() <= def::maxLevelSize(level)) return;

        // find those SSTable files which have the smallest timestamp
        // here some variables prepared in advance
        std::deque<managerFileDetail> current_level_files = levels[level];
        std::deque<managerFileDetail> files_to_be_merged;

        // if the level is among the last two, we should remove deleted pair during compaction
        bool remove_deleted_pair = level_number <= level + 2;

        // for different levels, there're different methods to deal with them
        if (level) {
            std::sort(current_level_files.begin(), current_level_files.end(), 
                def::compare_file_detail_zero_level);
            files_to_be_merged = std::deque<managerFileDetail>(current_level_files.begin() + 
                def::maxLevelSize(level), current_level_files.end());
        }
        else {
            files_to_be_merged = levels[level];
            std::reverse(files_to_be_merged.begin(), files_to_be_merged.end());
        }

        // if the next level doesn't exist, create it
        size_t next_level = level + 1;
        createNewLevelIfNonexist(next_level);

        // iterate through these files and merge them with files in the following level
        std::vector<managerFileDetail> files;
        for (auto file_detail : files_to_be_merged) {
            // files in current level
            files.push_back(file_detail);

            // deal with files in level 1
            size_t i = 0, j = 0, next_level_size = levels[next_level].size();
            assert(level_number > next_level);

            // find the first managerFileDetail ...
            for (; i < next_level_size; ++i) {
                if (levels[next_level][i].header.max_key >= file_detail.header.min_key) {
                    break;
                }
            }

            // ... until the last one
            // the range is [i, j)
            j = i;
            while (j < next_level_size && 
                levels[next_level][j].header.min_key <= file_detail.header.max_key) {
                files.push_back(levels[next_level][j]);
                ++j;
            }

            // write these SSTables into storage
            std::vector<ssTableContent*> contents_to_insert = mergeSSTable(files, remove_deleted_pair);
            size_t merged_file_size = contents_to_insert.size();
            for (size_t no = merged_file_size; no; --no) {
                writeIntoLevel(contents_to_insert[no - 1], next_level, i);
            }

            // delete files in level 1
            i += merged_file_size; j += merged_file_size;
            for (size_t no = i; no < j; ++no) {
                utils::rmfile(levels[next_level][no].file_name);
            }
            levels[next_level].erase(levels[next_level].begin() + i, levels[next_level].begin() + j);
            utils::rmfile(file_detail.file_name);
            files.clear();
        }

        // erase files in the deque of current level
        auto removed_file_function = [&files_to_be_merged](const managerFileDetail& file) -> bool {
            auto it = files_to_be_merged.begin(), eit = files_to_be_merged.end();
            for (; it != eit; ++it) {
                if (it->file_name == file.file_name) return true;
            }
            return false;
        };
        auto it = std::remove_if(levels[level].begin(), levels[level].end(), 
            removed_file_function);
        levels[level].erase(it, levels[level].end());

        // continue to check the next level
        checkCompaction(next_level);
    }

    void levelManager::writeIntoLevel(ssTableContent* content, size_t level, size_t pos) {
        // the size of the vector should be equal to level_number
        assert(level_number > level);
        assert(levels.size() == level_number);
        assert(levels_time.size() == level_number);

        // if the mem_table is full, create a sstable
        std::string file_name = file_prefix + '-' + std::to_string(levels_time[level]++);
        SSTable* table = new SSTable(directory_name, content->header.time, level, file_name);
        table->write(content);

        // create a instance of managerFileDetail
        managerFileDetail new_file_detail { table->getFileName(), content->header };

        // determine whether the SSTable is to be cached
        if (level < def::cached_levels) {
            // if yes, assign it to new_file_detail.table_cache
            new_file_detail.table_cache = table;
        }
        else {
            // if not, just deleting it is ok
            delete table;
        }

        // insert into the vector
        levels[level].insert(levels[level].begin() + pos, new_file_detail);
    }

    void levelManager::createNewLevelIfNonexist(size_t level) {
        // if the last level is full, new level will be created
        if (level_number <= level) [[unlikely]] {
            // only one more level once is allowed
            assert(level_number == level);
            utils::mkdir(def::getLevelDirectoryPath(directory_name, level_number++));
            levels.push_back(level_files());
            levels_time.push_back(0);
        }
        assert(levels.size() == level_number);
        assert(levels_time.size() == level_number);
    }

    void levelManager::writeIntoSSTableFile(ssTableContent* content) {
        // if the level doesn't exist, create it
        createNewLevelIfNonexist(0);

        // write the content into the first level
        writeIntoLevel(content, 0);

        // check compaction for the level
        checkCompaction(0);
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
        if (it->table_cache) delete it->table_cache;
        levels[level].erase(it);
    }

    void levelManager::updatePrefix() {
        // use timestamp and pid to construct a unique prefix for levelManager
        timeval time_structure;
        gettimeofday(&time_structure, nullptr);
        file_prefix = std::to_string(time_structure.tv_usec) + std::to_string(getpid());
    }

}

