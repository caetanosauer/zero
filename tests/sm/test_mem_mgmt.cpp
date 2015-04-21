#include "mem_mgmt.h"

#include <iostream>
#include <cstdlib>
#include <list>

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
           mem->allocate(24 + (rand() % 8169), s);
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
    }
}

void usage(char** argv)
{
    std::cerr << "Usage: " << argv[0] << " <test-number>" << std::endl;
    std::cerr << "test-number can be only 0 for now" << std::endl;
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
            break;
        default:
            usage(argv);
            return 1;
    }

    test(mem);
}
