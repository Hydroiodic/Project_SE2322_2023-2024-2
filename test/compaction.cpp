#include "../kvstore.h"
#include "utils.h"
#include <functional>
#include <iostream>
#include <unistd.h>

using namespace HI;

const size_t TEST_MAX = 100;
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

void insertRandomly(KVStore *tree) {
    // generate random data
    uint64_t key = rand();
    std::string val = randomString(rand() % STRING_LEN_MAX + 1);
    tree->put(key, val);
}

void testPut(KVStore &tree, size_t MAX) {
    for (size_t i = 0; i < MAX; ++i) {
        // time for the operation
        std::cout << operationPerSecond(std::bind(&insertRandomly, &tree), 5e3)
                  << "\n";
        std::cout.flush();
    }
}

static const size_t TEST_NUM = 1;
static void (*const func[TEST_NUM])(KVStore &, size_t) = {
    testPut,
};
static const size_t test_size[TEST_NUM] = {
    TEST_MAX,
};
static const std::string func_str[TEST_NUM] = {
    "/******************* Testing Put *******************/",
};

int main() {
    // randomize seed
    srand(time(nullptr));

    // create instance for LSMTree
    KVStore tree("./data", "./data/vlog");

    // start testing
    for (size_t i = 0; i < TEST_NUM; ++i) {
        std::cout << func_str[i] << '\n';
        std::cout.flush();
        func[i](tree, test_size[i]);
    }

    tree.reset();

    return 0;
}
