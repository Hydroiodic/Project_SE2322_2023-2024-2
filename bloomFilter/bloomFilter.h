#pragma once

#include <cstddef>
#include <cstdint>

namespace bloomfilter {

    typedef unsigned int uint;

    template <typename T>
    class bloomFilter
    {
    private:
        size_t m, k;
        uint* hash_array;

    public:
        bloomFilter(size_t m, size_t k);
        ~bloomFilter();

        void insert(const T& key);
        bool remove(const T& key);
        bool query(const T& key) const;
    };

}

