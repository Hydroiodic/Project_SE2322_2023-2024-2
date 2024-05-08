#include "../kvstore.h"
#include "utils.h"
#include <cstdlib>
#include <functional>
#include <iostream>
#include <unistd.h>

using namespace HI;

const size_t TEST_MAX = 1e6;
const size_t STRING_LEN_MAX = 1e4;

std::string randomString(size_t length) {
    auto randchar = []() -> char {
        const char charset[] = "0123456789"
                               "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                               "abcdefghijklmnopqrstuvwxyz";
        const size_t max_index = (sizeof(charset) - 1);
        return charset[rand() % max_index];
    };
    std::string str(length, 0);
    std::generate_n(str.begin(), length, randchar);
    return str;
}

double testPut(KVStore &tree, size_t MAX) {
    double result = 0;

    for (size_t i = 0; i < MAX; ++i) {
        // generate random data
        uint64_t key = rand();
        std::string val = randomString(rand() % STRING_LEN_MAX + 1);

        // time for the operation
        result += timeSeconds(std::bind(&KVStore::put, &tree, key, val));
    }

    return result / MAX;
}

double testGet(KVStore &tree, size_t MAX) {
    double result = 0;

    for (size_t i = 0; i < MAX; ++i) {
        // generate random data
        uint64_t key = rand();

        // time for the operation
        result += timeSeconds(std::bind(&KVStore::get, &tree, key));
    }

    return result / MAX;
}

double testDel(KVStore &tree, size_t MAX) {
    double result = 0;

    for (size_t i = 0; i < MAX; ++i) {
        // generate random data
        uint64_t key = rand();

        // time for the operation
        result += timeSeconds(std::bind(&KVStore::del, &tree, key));
    }

    return result / MAX;
}

double testScan(KVStore &tree, size_t MAX) {
    double result = 0;

    for (size_t i = 0; i < MAX; ++i) {
        // generate random data
        uint64_t key1 = rand(), key2 = rand();
        if (key2 < key1)
            std::swap(key1, key2);
        std::list<std::pair<key_type, value_type>> list;

        // time for the operation
        result +=
            timeSeconds(std::bind(&KVStore::scan, &tree, key1, key2, list));

        std::cout << i << '\n';
    }

    return result / MAX;
}

static const size_t TEST_NUM = 4;
static double (*const func[TEST_NUM])(KVStore &, size_t) = {
    testPut,
    testGet,
    testScan,
    testDel,
};
static const size_t test_size[TEST_NUM] = {
    TEST_MAX,
    TEST_MAX,
    TEST_MAX / 10000,
    TEST_MAX,
};
static const std::string func_str[TEST_NUM] = {
    "/******************* Testing Put *******************/",
    "/******************* Testing Get *******************/",
    "/******************* Testing Scan ******************/",
    "/******************* Testing Del *******************/",
};

int main() {
    // randomize seed
    srand(time(nullptr));

    // create instance for LSMTree
    KVStore tree("./data", "./data/vlog");

    // start testing
    for (size_t i = 0; i < TEST_NUM; ++i) {
        std::cout << func_str[i] << '\n';
        std::cout << "Average time: " << func[i](tree, test_size[i]) << "ns\n";
    }
    tree.reset();

    return 0;
}
