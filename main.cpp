// main.cpp
// ATTENTION! this file is only targeted at testing

#include "kvstore.h"
#include "./common/definitions.h"
#include <iostream>
#include <string>

using def::key_type;
using def::value_type;

void sequent_query(KVStore& store, const key_type& max_key) {
    for (key_type i = key_type(); i < max_key; ++i) {
        std::cout << i << ": " << store.get(i) << '\n';
    }
}

int main() {
    KVStore store("./data", "./data/vlog");

    for (key_type i = 0; i < 1000; ++i) {
        store.put(i, std::to_string(i));
    }

    for (key_type i = 1; i < 1000; i += 2) {
        store.del(i);
    }

    store.reset();
    sequent_query(store, 1000);

    return 0;
}
