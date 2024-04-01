#include "levelManager.h"
#include "../utils.h"
#include <algorithm>

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
        std::sort(current_level.begin(), current_level.end(), def::compare_file_detail_great);
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
        for (const level_files& current_level : levels) {
            for (const managerFileDetail& file_detail : current_level) {
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

    void levelManager::checkCompaction(size_t level) {
        /* here very very very many things TODO */
    }

    void levelManager::writeIntoLevel(ssTableContent* content, size_t level) {
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
        SSTable table(directory_name, content->header.time, 0);
        table.write(content);

        managerFileDetail new_file_detail { table.getFileName(), content->header };
        // TODO: those level with an index larger than 0 should be ordered
        levels[level].push_front(new_file_detail);
    }

    void levelManager::writeIntoSSTableFile(ssTableContent* content) {
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
        levels[level].erase(it);
    }

}

