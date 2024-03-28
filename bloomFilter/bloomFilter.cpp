#pragma once

#include <cstdint>
#include <cstddef>
#include "bloomFilter.h"
#include "MurmurHash3.h"

namespace bloomfilter {

    typedef unsigned int uint;

    template <typename T>
    bloomFilter<T>::bloomFilter(size_t m, size_t k) : m(m), k(k) {
        this->hash_array = new uint[m]{};
    }

    template <typename T>
    bloomFilter<T>::~bloomFilter<T>() {
        delete [] hash_array;
    }

    template <typename T>
    void bloomFilter<T>::insert(const T &key) {
        for (uint32_t i = 0; i < k; ++i) {
            uint64_t hash[2]{};
            // calculate hash value
            MurmurHash3_x64_128(&key, sizeof(key), i, hash);
            // insert into the array
            ++hash_array[hash[0] % m];
        }
    }

    template <typename T>
    bool bloomFilter<T>::remove(const T &key) {
        // if key isn't in the bloom filter
        if (!query(key)) return false;

        // remove the key by decrease each value by 1
        for (uint32_t i = 0; i < k; ++i) {
            uint64_t hash[2]{};
            // calculate hash value
            MurmurHash3_x64_128(&key, sizeof(key), i, hash);
            // the value in the hash array is 0
            --hash_array[hash[0] % m];
        }
        return true;
    }

    template <typename T>
    bool bloomFilter<T>::query(const T &key) const {
        for (uint32_t i = 0; i < k; ++i) {
            uint64_t hash[2]{};
            // calculate hash value
            MurmurHash3_x64_128(&key, sizeof(key), i, hash);
            // the value in the hash array is 0
            if (!hash_array[hash[0] % m]) return false;
        }

        // all is 1, but may be mistaken
        return true;
    }

}
