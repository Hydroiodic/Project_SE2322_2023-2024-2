#pragma once

#include <cstddef>
#include <cstdint>

namespace bloomfilter {

    template <typename T>
    class bloomFilter
    {
    private:
        size_t byte_count, bit_count, k;
        unsigned char* hash_array;

    public:
        bloomFilter(size_t m, size_t k = 1);
        ~bloomFilter();

        void insert(const T& key);
        bool query(const T& key) const;
        void clear();
        void set(const unsigned char* src);

        const unsigned char* getContent() const { return hash_array; }
    };

}

// this line of code has to be added because of the usage of template class
#include "bloomFilter.cpp"
