#pragma once

#include <cstddef>
#include <string>
#include <vector>
#include "../common/definitions.h"
#include "../ssTable/ssTable.h"

namespace levelmanager {

    using def::level_files;
    using def::managerFileDetail;
    using def::ssTableContent;
    using def::sstableData;
    using def::pq_type;
    using def::key_type;
    using sstable::SSTable;
    using bloomFilter = bloomfilter::bloomFilter<key_type>;

    class levelManager
    {
    private:
        // the number of levels
        size_t level_number = 0;

        // store all names of files in each level
        std::vector<level_files> levels;

        // the parent directory of each level
        std::string directory_name;

        // function to sort files
        level_files sortFiles(const std::vector<std::string>& files, size_t level) const;

        // deal with compaction
        void checkZeroLevelCompaction();
        void checkCompaction(size_t level);

        // compaction among some managerFileDetail
        std::vector<ssTableContent*> mergeSSTable(const std::vector<managerFileDetail>& files) const;

        // internal funtion to write SSTable into a specific level
        void writeIntoLevel(ssTableContent* content, size_t level, size_t no = 0, size_t pos = 0);

    public:
        levelManager(const std::string& dir);
        ~levelManager();

        void scanLevels();
        const level_files& getLevelFiles(size_t level) const;

        size_t size() const;
        void clear();

        void writeIntoSSTableFile(ssTableContent* content);
        void removeSSTableFile(const std::string& file_name, size_t level);

        // TODO: use levelManager to deal with compaction
    };

}
