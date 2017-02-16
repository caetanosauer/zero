#include "mem_mgmt.h"

#include <fstream>
#include <cstdlib>
#include <list>
#include <map>

#undef NDEBUG
#include <assert.h>  // force assertions

typedef mem_mgmt_t::slot_t slot_t;

void test(mem_mgmt_t* mem)
{
    int MAX = 100000;
    std::list<slot_t> slots;
    // randomly allocate and free blocks between 24 and 8192 bytes
    for (int i = 0; i < MAX; i++) {
        // flip a coin
        if (rand() % 2) {
           slot_t s(NULL,0);
           mem->allocate(24 + (rand() % 16000), s);
           if (s.address) {
               slots.push_front(s);
           }
        }
        else {
            std::list<slot_t>::iterator iter = slots.begin();
            size_t size = slots.size();
            if (size > 0) {
                size_t r = rand() % size;
                for (size_t j = 0; j < r; j++) {
                    iter++;
                }
                slots.erase(iter);
                mem->free(*iter);
            }
        }
        if (i % 1000 == 0) { std::cout << "." << std::endl; }
    }
}

void test_trace(mem_mgmt_t* mem)
{
    std::map<size_t, std::list<char*>> slots;
    long count = 0;
    std::ifstream in("allocs.txt");
    char type;
    size_t len;
    while (in >> type >> len) {
        if (type == 'A') {
            slot_t s(nullptr, 0);
            W_COERCE(mem->allocate(len, s));
            if (s.address) {
                slots[s.length].push_back(s.address);
            }
        }
        else if (type == 'D') {
            auto& l = slots[len];
            auto pos = rand() % l.size();
            auto iter = l.begin();
            for (size_t i = 0; i < pos; i++) {
                iter++;
            }
            char* addr = *iter;
            l.erase(iter);
            W_COERCE(mem->free(slot_t{addr, len}));
        }
        if (count++ % 100000 == 0) {
            std::cout << count / 100000 << std::endl;
        }
    }
    std::cout << "Processed " << count << " lines" << std::endl;
}

void usage(char** argv)
{
    std::cerr << "Usage: " << argv[0] << " <test-number>" << std::endl;
    std::cerr << "test-number can be 0 or 1 for now" << std::endl;
    std::cerr << "test 1 replays a trace file alloc.txt" << std::endl;
}

int main(int argc, char** argv)
{
    if (argc != 2) {
        usage(argv);
        return 1;
    }

    mem_mgmt_t * mem;
    int test_number = atoi(argv[1]);
    switch (test_number) {
        case 0:
            mem = new fixed_lists_mem_t();
            test(mem);
            break;
        case 1:
            mem = new fixed_lists_mem_t();
            test_trace(mem);
            break;
        default:
            usage(argv);
            return 1;
    }
}
