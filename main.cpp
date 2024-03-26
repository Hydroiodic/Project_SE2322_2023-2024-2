// main.cpp
// ATTENTION! this file is only targeted at testing

#include "kvstore.h"
#include "./common/definitions.h"
#include <string>

using def::key_type;
using def::value_type;

int main() {
    KVStore store("./data", "./data/vlog");

    for (key_type i = 0; i < 1000; ++i) {
        store.put(i, std::to_string(i));
    }

    return 0;
}
