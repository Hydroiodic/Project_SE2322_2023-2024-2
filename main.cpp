// main.cpp
// ATTENTION! this file is only targeted at testing

#include "kvstore.h"
#include "./common/definitions.h"
#include <cstdlib>
#include <iostream>
#include <string>

using def::key_type;
using def::value_type;

int main() {
    KVStore store("./data", "./data/vlog");

    for (key_type i = 0; i < 1000; ++i) {
        store.put(i, std::to_string(i));
    }

    for (key_type i = 1; i < 1000; ++i) {
        key_type random_key = rand() % 1000;
        std::cout << random_key << ": " << store.get(random_key) << '\n';
    }

    return 0;
}
