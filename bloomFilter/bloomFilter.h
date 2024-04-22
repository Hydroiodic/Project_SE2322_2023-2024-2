#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include "MurmurHash3.h"

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

// below is the implementation of bloomFilter
namespace bloomfilter {

    template <typename T>
    bloomFilter<T>::bloomFilter(size_t m, size_t k) : byte_count(m), bit_count(m << 3), k(k) {
        this->hash_array = new unsigned char[byte_count]{};
    }

    template <typename T>
    bloomFilter<T>::~bloomFilter<T>() {
        delete [] hash_array;
    }

    template <typename T>
    void bloomFilter<T>::insert(const T &key) {
        for (uint32_t i = 0; i < k; ++i) {
            uint32_t hash[4];
            // calculate hash value
            MurmurHash3_x64_128(&key, sizeof(key), i, hash);
            for (uint32_t j = 0; j < 4; ++j) {
                // calculate for the position
                size_t bits = hash[j] % bit_count;
                size_t byte_no = bits / 8, bit_no = bits % 8;
                // insert into the array
                hash_array[byte_no] |= 1 << bit_no;
            }
        }
    }

    template <typename T>
    bool bloomFilter<T>::query(const T &key) const {
        for (uint32_t i = 0; i < k; ++i) {
            uint32_t hash[4];
            // calculate hash value
            MurmurHash3_x64_128(&key, sizeof(key), i, hash);
            for (uint32_t j = 0; j < 4; ++j) {
                // calculate for the position
                size_t bits = hash[j] % bit_count;
                size_t byte_no = bits / 8, bit_no = bits % 8;
                // the value in the hash array is 0
                if (!((hash_array[byte_no] >> bit_no) & 1)) return false;
            }
        }

        // all is 1, but may be mistaken
        return true;
    }

    template <typename T>
    void bloomFilter<T>::clear() {
        memset(hash_array, 0, byte_count);
    }

    template <typename T>
    void bloomFilter<T>::set(const unsigned char* src) {
        memcpy(hash_array, src, byte_count);
    }

}
