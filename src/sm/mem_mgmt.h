#ifndef MEM_MGMT_H
#define MEM_MGMT_H

#include "w_defines.h"
#include "sm_base.h"

#undef MM_TEST

/**
 * Memory management algorithm as proposed in:
 *
 * P. Larson: External sorting: run formation revisited
 * IEEE Transactions on Knowledge and Data Engineering
 * (Volume:15 ,  Issue: 4 )
 *
 * Memory blocks have fixed lengths ranging from a minimum
 * to a maximum size in fixed increments, for example from
 * 32 to 8192 bytes. Free blocks are managed in linked lists,
 * one for each block size. The metadata of each block, such
 * as the prev/next pointers (valid only if free) and the block
 * size, are stored within the block itself (see list_header_t).
 *
 * Coalescence of free blocks is implemented by using boundary
 * tags, as suggested in the paper. While it adds more complexity
 * to the algorithm, we have found it essential to maintain a
 * robust behavior in terms of usable space over time.
 *
 * @author: Caetano Sauer
 */
class fixed_lists_mem_t
{
public:
    struct slot_t {
        char* address;
        size_t length;
        slot_t(char* a, size_t l)
            : address(a), length(l)
        {}
    };
private:
    class list_header_t {
    private:
        uint16_t tag;

        void update_footer() {
            char* p = (char*) this + block_size() - 2;
            memcpy(p, &tag, 2);
        }


    public:
        /*
         * next and prev could be given also in 2 or 4 bytes is we use
         * offsets relative to the memory manager buffer (_buf)
         */
        list_header_t* next;
        list_header_t* prev;

        uint16_t block_size() {
            return tag & 0x7FFF;
        }


        void init(size_t block_size) {
            // max length is 32K (15 bits)
            uint16_t len = block_size;
            // set new length value but maintain ms bit of tag
            tag = (len & 0x7FFF) | (tag & 0x8000);
            update_footer();
            prev = NULL;
            next = NULL;
        }

        void set_free() {
            tag &= 0x7FFF;
            update_footer();
        }

        void set_occupied() {
            tag |= 0x8000;
            update_footer();
        }

        bool is_free() {
            return !(tag & 0x8000);
        }

        size_t slot_length() {
            return block_size() - sizeof(list_header_t) - 2;
        }

        list_header_t* get_left_neighbor() {
            uint16_t left_tag;
            memcpy(&left_tag, (char*) this - 2, 2);
            return (list_header_t*) ((char*) this - (left_tag & 0x7FFF));
        }

        list_header_t* get_right_neighbor() {
            return (list_header_t*)
                ((char*) this + block_size());
        }

        uint16_t footer() {
            return *((uint16_t*) ((char*) this + block_size() - 2));
        }
        uint16_t header() {
            return *((uint16_t*) this);
        }

        static size_t get_best_fit(size_t length, size_t incr) {
            size_t needed = length + sizeof(list_header_t) + 2;
            size_t index = needed / incr;
            if (needed % incr != 0) {
                index++; // take ceil
            }
            return index * incr;
        }
    };

    const size_t _incr;
    const size_t _max;
    size_t _bufsize;
    list_header_t ** _lists;
    char * _buf;
    size_t _first_non_empty;
    size_t _last_non_empty;
    size_t _alloc;
    size_t _used;

    void add_to_list(size_t block_size, char* address);
    char* remove_from_list(size_t block_size);
    void remove_from_list(list_header_t* p);
    bool is_list_empty(size_t block_size);

#ifdef MM_TEST
    void verify_lists();
    void verify_block(void*);
    void verify_neighbor(list_header_t*, list_header_t*, bool);
    void verify_blocks();
#endif

public:
    fixed_lists_mem_t(
            size_t bufsize = 8192 * 10240,
            size_t incr = 32,
            size_t max = 16384);
    ~fixed_lists_mem_t();
    rc_t allocate(size_t length, slot_t& slot);
    rc_t free(slot_t slot);
    rc_t defrag();
};


#endif
