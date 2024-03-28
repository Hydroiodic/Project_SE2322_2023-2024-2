// main.cpp
// ATTENTION! this file is only targeted at testing

#include "kvstore.h"
#include "./common/definitions.h"
#include <cstddef>
#include <iostream>
#include <list>
#include <string>
#include <utility>

using def::key_type;
using def::value_type;

void sequent_query(KVStore& store, const key_type& max_key) {
    for (key_type i = key_type(); i < max_key; ++i) {
        std::cout << i << ": " << store.get(i) << '\n';
    }
}

int main() {
    const size_t max = 1000;
    KVStore store("./data", "./data/vlog");

    for (key_type i = 0; i < max; ++i) {
        store.put(i, std::to_string(i));
    }

    // for (key_type i = 1; i < max; ++i) {
    //     store.put(i, std::to_string(max - i));
    // }

    std::list<std::pair<key_type, value_type>> list;
    store.scan(900, 910, list);

    for (auto i : list) {
        std::cout << i.first << ": " << i.second << '\n';
    }

    return 0;
}
