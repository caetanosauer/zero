#include "mem_mgmt.h"

#include "w_debug.h"

#ifdef MM_TEST
    #undef NDEBUG
    #include <assert.h>  // force assertions
#endif

#ifdef MM_TEST
#define MM_VERIFY(v) v
#else
#define MM_VERIFY(v)
#endif

typedef fixed_lists_mem_t::slot_t slot_t;

/**
 * Adds a new free block (located at 'address') to the list of blocks
 * with 'block_size'.
 */
void fixed_lists_mem_t::add_to_list(size_t block_size, char* address)
{
    size_t index = block_size / _incr - 1;
    DBG(<< "Adding block of " << block_size);

    list_header_t* p = (list_header_t*) address;
    p->init(block_size);

    MM_VERIFY(verify_block(address));
    assert(block_size > 0);
    assert(block_size % _incr == 0);

    if (_lists[index]) {
        _lists[index]->prev = p;
    }
    else {
        if (block_size < _first_non_empty) {
            _first_non_empty = block_size;
        }
        if (block_size > _last_non_empty) {
            _last_non_empty = block_size;
        }
    }

    p->next = _lists[index];
    p->prev = NULL;
    p->set_free();
    _lists[index] = p;

    MM_VERIFY(verify_lists());
}

/**
 * Removes a free block from the middle of a linked lists. This version of the
 * method is used by block coalescence, which transforms 2 or 3 free blocks
 * into one of larger size. This requires removing the smaller blocks from
 * their free lists using this method.
 */
void fixed_lists_mem_t::remove_from_list(list_header_t* p)
{
    size_t index = p->block_size() / _incr - 1;
    DBG(<< "Removing coalesced block of " << p->block_size());
    assert(p->prev || p == _lists[index]);
    assert(p != _lists[index] || !p->prev);

    if (p->prev) {
        p->prev->next = p->next;
    }
    if (p->next) {
        p->next->prev = p->prev;
    }
    p->set_occupied();

    // check if p is the list head and update it if necessary
    if (p == _lists[index]) {
        _lists[index] = p->next;
    }

    MM_VERIFY(verify_lists());
}

/**
 * Removes a block of a given size from its linked list. Here we do not care
 * which block is returned, since the method is invoked inside the allocation
 * method. Thus, the list head is always removed, allowing allocation to happen
 * in constant time.
 */
char* fixed_lists_mem_t::remove_from_list(size_t block_size)
{
    size_t index = block_size / _incr - 1;
    DBG(<< "Removing block of " << block_size);
    if (!_lists[index]) {
        DBG(<< "Cannot remove! Empty list for block size " << index * _incr);
        return NULL;
    }

    list_header_t* p = _lists[index];
    if (p->next) {
        p->next->prev = NULL;
    }
    _lists[index] = p->next;
    p->set_occupied();

    MM_VERIFY(verify_lists());

    return (char*) p;
}

bool inline fixed_lists_mem_t::is_list_empty(size_t block_size)
{
    size_t index = block_size / _incr - 1;
    return _lists[index] == NULL;
}

/**
 * Initialize the buffer with all blocks of the maximum size
 */
fixed_lists_mem_t::fixed_lists_mem_t(size_t bufsize, size_t incr, size_t max)
    : _incr(incr), _max(max)
{
    // make sure bufsize is multiple of max
    _bufsize = (bufsize / max) * max;
    _buf = new char[_bufsize];
    _lists = new list_header_t*[max/incr];
    ::memset(_lists, 0, max/incr * sizeof(list_header_t*));

    _first_non_empty = max;
    _last_non_empty = max;

    size_t maxblock_count = _bufsize / max;
    for (size_t i = 0; i < maxblock_count; i++) {
        char* add = _buf + (max * i);
        add_to_list(max, add);
    }

    MM_VERIFY(verify_blocks());
}

fixed_lists_mem_t::~fixed_lists_mem_t()
{
    delete _lists;
    delete _buf;
}

/**
 * Allocate space for a record/object of size 'length'. The address and the
 * actual block size are returned in 'slot'. If the slot is larger than the
 * best fit (i.e., the smallest block size larger than 'length'), the block
 * is split into one with size equal to the best fit, and the remaining part
 * is readded to the lists as a smaller free block.
 *
 * If there is no block available with the requested size, the method returns
 * an invalid slot (containing a null pointer). The calles should then attempt
 * to free more blocks before trying again.
 */
rc_t fixed_lists_mem_t::allocate(size_t length, slot_t& slot)
{
    // ERROUT(<< "ALLOC " << length);
    if (length > _max) {
        ERROUT(<< "Cannot allocate block of " << length);
        return RC(eINTERNAL);
    }

    size_t best_fit = list_header_t::get_best_fit(length, _incr);
    size_t fit = max(best_fit, _first_non_empty);
    DBG(<< "Looking for block of " << fit
            << " for " << length << " bytes");

    char* addr = NULL;
    while (fit <= _last_non_empty) {
        if (!is_list_empty(fit)) {
            DBG(<< "FOUND in list of " << fit);
            addr = remove_from_list(fit);
            break;
        }
        fit += _incr;
//        DBG(<< "NOT FOUND! Trying list of " << fit);
    }

    if (fit > _last_non_empty) {
        // full -- caller must consume to try freeing space
        DBG(<< "NOT FOUND -- No space for " << length << " bytes");
        slot.address = NULL;
        slot.length = 0;
        return RCOK;
    }

    MM_VERIFY(verify_block(addr));

    // readd remainder piece if block retrieved is larger than necessary
    size_t diff = fit - best_fit;
    if (diff > 0) {
        add_to_list(diff, addr + best_fit);
        // update block structure for new smaller size
        ((list_header_t*) (addr))->init(best_fit);

        assert(!((list_header_t*) (addr))->is_free());
        assert(diff % _incr == 0);
        assert(best_fit > 0);
        assert(best_fit % _incr == 0);
    }


    slot.address = addr + sizeof(list_header_t);
    slot.length = best_fit;
    return RCOK;
}

/*
 * Returns a slot to the memory pool (i.e., frees it). Before readding the
 * block to the linked lists, we check if it can be coalesced (merged) with
 * any of the neighboring blocks. This is done by reading the boundary tag
 * and checking whether the occupied bit is set (see get_right_neighbor and
 * get_left_neighbor). If coalesence is possible, the block size to be readded
 * is incremented accordingly, and the block pointer updated if the block to
 * the left is coalesced.
 */
rc_t fixed_lists_mem_t::free(slot_t slot)
{
    // ERROUT(<< "DEALLOC " << slot.length);
    DBG(<< "Freeing block of " << slot.length);
    assert(slot.length % _incr == 0);

    // check if coalescence is possible
    char* p_addr = slot.address - sizeof(list_header_t);
    list_header_t* p = (list_header_t*) p_addr;
    size_t block_size = slot.length;

    MM_VERIFY(verify_block(p));

    // right neighbor only exists if p is not the last block in the buffer
    list_header_t* right = p->get_right_neighbor();
    if ((char*) right < _buf + _bufsize) {
        MM_VERIFY(verify_neighbor(p, right, false));
        if (right->is_free()
                && (block_size + right->block_size() <= _max))
        {
            remove_from_list(right);
            block_size += right->block_size();
        }
    }

    MM_VERIFY(verify_block(p));
    MM_VERIFY(verify_blocks());
    MM_VERIFY(verify_lists());

    // left neighbor only exists if p is not the first block in the buffer
    if ((char*) p != _buf) {
        list_header_t* left = p->get_left_neighbor();
        MM_VERIFY(verify_neighbor(p, left, true));
        if (left->is_free()
                && (block_size + left->block_size() <= _max))
        {
            remove_from_list(left);
            block_size += left->block_size();
            p_addr = (char*) left;
        }
    }

    add_to_list(block_size, p_addr);
    return RCOK;
}

/**
 * WARNING: This defrag method assumes that all blocks were freed
 * beforehand (e.g. the overlying heap is empty)
 */
rc_t fixed_lists_mem_t::defrag()
{
    for (size_t i = 0; i <= _max/_incr; i++) {
        _lists[i] = NULL;
    }
    size_t maxblock_count = _bufsize/_max;
    for (size_t i = 0; i < maxblock_count; i++) {
        char* add = _buf + (_max * i);
        add_to_list(_max, add);
    }
    return RCOK;
}

#ifdef MM_TEST
/**
 * Different consistency checks with assertions -- used for testing and
 * debugging. To enable, the flag MM_TEST must be set (see mem_mgmt.h)
 * TODO: Integrate this with the shore debug mechanism (e.g., w_assert1)
 */
void fixed_lists_mem_t::verify_block(void* addr)
{
    list_header_t* p = (list_header_t*) addr;
    assert(p->header() == p->footer());
    assert(p->block_size() != 0);
}

void fixed_lists_mem_t::verify_neighbor
        (list_header_t* p, list_header_t* neighbor, bool left)
{
    assert(p != neighbor);
    verify_block(p);
    verify_block(neighbor);
    size_t delta = left ?
        ((char*) p - (char*) neighbor) :
        ((char*) neighbor - (char*) p);
    assert(delta % _incr == 0);
    assert(!left || delta == neighbor->block_size());
    assert(left || delta == p->block_size());

    assert(!left || (char*) neighbor >= _buf);
    assert(left || (char*) neighbor <= _buf + _bufsize);
}

void fixed_lists_mem_t::verify_lists()
{
    for (int i = 0; i < _max / _incr; i++) {
        list_header_t* head = _lists[i];
        list_header_t* p = _lists[i];
        list_header_t* prev = NULL;
        while (p) {
            assert(p->block_size() == (i + 1) * _incr);
            assert(p->is_free());
            assert(p->prev == prev);
            assert(p->header() == p->footer());
            assert(p->next != head); // finds circles
            prev = p;
            p = p->next;
        }
    }
}

void fixed_lists_mem_t::verify_blocks()
{
    list_header_t* p = (list_header_t*) _buf;
    while(true) {
        verify_block(p);
        p = (list_header_t*) ((char*) p + p->block_size());
        if ((char*) p >= _buf + _bufsize) {
            break;
        }
    }
}

#endif
