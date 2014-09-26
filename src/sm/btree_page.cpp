/*
 * (c) Copyright 2013-2014, Hewlett-Packard Development Company, LP
 */

#include "btree_page.h"

#include <algorithm>
#include <memory>
#include "w_debug.h"
#include "w_key.h"


void btree_page_data::init_items() {
    w_assert1(btree_level >= 1);

    nitems          = 0;
    nghosts         = 0;
    first_used_body = max_bodies;

    w_assert3(_items_are_consistent());
}

void btree_page_data::remove_items(
                      const int item_count,    // In: Number of records to remove
                      const w_keystr_t &high)  // In: high fence after record removal
{
    // Use this function with caution

    // A special helper function to remove 'item_count' largest items from the storage
    // this function is only used by full logging page rebalance restart operation
    // to recover the source page after a system crash
    // the caller resets the fence keys on source page which eliminate some
    // of the records from source page
    // this function removes the largest 'item_count' items from the page
    // because they belong to destination page after the rebalance
    // After the removal, item count changed but no change to ghost count

    w_assert1(btree_level >= 1);
    w_assert1(nitems > item_count);          // Must have at least one record which is the fency key record
    w_assert3(_items_are_consistent());

    if ((0 == item_count) || (1 == nitems))  // If 1 == nitems, we only have a fence key record
        return;

    DBGOUT3( << "btree_page_data::reset_item_count - before deletion item count: " << nitems
             << ", new high fence key: " << high);

    int remaining = item_count;
    char* high_key_p = (char *)high.buffer_as_keystr();
    size_t high_key_length = (size_t)high.get_length_as_keystr();
    while (0 < remaining)
    {
        w_assert1(1 < nitems);
        // Find the records with key >= new high fence key and delete them
        int item_index = 1;  // Start with index 1 since 0 is for the fence key record
        uint16_t* key_length;;
        size_t item_len;

        int cmp;
        const int data_offset = sizeof(uint16_t);  // To skipover the portion which contains the size of variable data
        for (int i = item_index; i < nitems; ++i)
        {
            key_length = (uint16_t*)item_data(i);
            item_len = *key_length++;       

            cmp = ::memcmp(high_key_p, item_data(i)+data_offset, (high_key_length<=item_len)? high_key_length : item_len);
            if ((0 > cmp) || ((0 == cmp) && (high_key_length <= item_len)))
            {
                // The item is larger than the new high fence key or the same as high fence key (high fence is ghost)
                DBGOUT3( << "btree_page_data::reset_item_count - delete record index: " << i);

                // Delete the item, which changes nitems but no change to nghosts
                // therefore break out the loop and start the loop again if we have more items to remove
                delete_item(i);
                break;
            }
        }

        --remaining;
    }

    DBGOUT3( << "btree_page_data::reset_item_count - after deletion item count: " << nitems);
    w_assert3(_items_are_consistent());
}


void btree_page_data::set_ghost(int item) {
    w_assert1(item>=0 && item<nitems);

    body_offset_t offset = head[item].offset;
    w_assert1(offset != 0);
    if (offset >= 0) {
        head[item].offset = -offset;
        nghosts++;
    }
}

void btree_page_data::unset_ghost(int item) {
    w_assert1(item>=0 && item<nitems);

    body_offset_t offset = head[item].offset;
    w_assert1(offset != 0);
    if (offset < 0) {
        head[item].offset = -offset;
        nghosts--;
    }
}



bool btree_page_data::insert_item(int item, bool ghost, poor_man_key poor,
                shpid_t child, size_t data_length) {
    w_assert1(item>=0 && item<=nitems);  // use of <= intentional
    w_assert3(_items_are_consistent());

    size_t body_length = data_length + _item_body_overhead();
    if ((size_t)usable_space() < sizeof(item_head) + _item_align(body_length)) {
        return false;
    }

    // shift item array up to insert a item so it is head[item]:
    ::memmove(&head[item+1], &head[item], (nitems-item)*sizeof(item_head));
    nitems++;
    if (ghost) {
        nghosts++;
    }

    first_used_body -= _item_align(body_length)/sizeof(item_body);
    head[item].offset = ghost ? -first_used_body : first_used_body;
    head[item].poor = poor;

    if (!is_leaf()) {
        body[first_used_body].interior.child = child;
    } else {
        w_assert1(child == 0);
    }
    _item_body_length(first_used_body) = body_length;

    w_assert3(_items_are_consistent());
    return true;
}

bool btree_page_data::insert_item(int item, bool ghost, poor_man_key poor,
                             shpid_t child, const cvec_t& data) {
    if (!insert_item(item, ghost, poor, child, data.size())) {
        return false;
    }
    data.copy_to(item_data(item));
    return true;
}


bool btree_page_data::resize_item(int item, size_t new_length, size_t keep_old) {
    w_assert1(item>=0 && item<nitems);
    w_assert1(keep_old <= new_length);
    w_assert3(_items_are_consistent());

    body_offset_t offset = head[item].offset;
    bool ghost = false;
    if (offset < 0) {
        offset = -offset;
        ghost = true;
    }

    size_t old_length  = _item_body_length(offset);
    size_t body_length = new_length + _item_body_overhead();
    if (body_length <= _item_align(old_length)) {
        _item_body_length(offset) = body_length;
        w_assert3(_items_are_consistent()); 
        return true;
    }

    if (_item_align(body_length) > (size_t) usable_space()) {
        return false;
    }

    char* old_p = item_data(item);
    first_used_body -= _item_align(body_length)/sizeof(item_body);
    head[item].offset = ghost ? -first_used_body : first_used_body;
    _item_body_length(first_used_body) = body_length;
    if (!is_leaf()) {
        body[first_used_body].interior.child = body[offset].interior.child;
    }

    if (keep_old > 0) {
        char* new_p = item_data(item);
        w_assert1(old_p+keep_old <= (char*)&body[offset]+old_length);
        ::memcpy(new_p, old_p, keep_old); 
    }

    w_assert3(_items_are_consistent()); 
    return true;
}

bool btree_page_data::replace_item_data(int item, size_t offset, const cvec_t& new_data) {
    if (!resize_item(item, offset+new_data.size(), offset))
        return false;

    new_data.copy_to(item_data(item) + offset);
    return true;
}


void btree_page_data::delete_item(int item) {
    w_assert1(item>=0 && item<nitems);
    w_assert3(_items_are_consistent());

    body_offset_t offset = head[item].offset;
    if (offset < 0) {
        offset = -offset;
        nghosts--;
    }

    if (offset == first_used_body) {
        // Then, we are pushing down the first_used_body.  lucky!
        first_used_body += _item_bodies(offset);
    }

    // shift item array down to remove head[item]:
    ::memmove(&head[item], &head[item+1], (nitems-(item+1))*sizeof(item_head));
    nitems--;

    w_assert3(_items_are_consistent());
}


// <<<>>>
#include <boost/scoped_array.hpp>

bool btree_page_data::_items_are_consistent() const {
    // This is not a part of check; should be always true:
    w_assert1(first_used_body*sizeof(item_body) >= nitems*sizeof(item_head));

    
    // check overlapping records.
    // rather than using std::map, use array and std::sort for efficiency.
    // high 16 bits=offset, low 16 bits=length
    //static_assert(sizeof(item_length_t) <= 2, 
    //              "item_length_t doesn't fit in 16 bits; adjust this code");
    BOOST_STATIC_ASSERT(sizeof(item_length_t) <= 2);
    //std::unique_ptr<uint32_t[]> sorted_items(new uint32_t[nitems]);
    boost::scoped_array<uint32_t> sorted_items(new uint32_t[nitems]); // <<<>>>
    int ghosts_seen = 0;
    for (int item = 0; item<nitems; ++item) {
        int offset = head[item].offset;
        if (offset < 0) {
            offset = -offset;
            ghosts_seen++;
        }
        sorted_items[item] = (offset << 16) + _item_bodies(offset);
    }
    std::sort(sorted_items.get(), sorted_items.get() + nitems);

    bool error = false;
    if (nghosts != ghosts_seen) {
        DBGOUT1(<<"Actual number of ghosts, " << ghosts_seen <<  ", differs from nghosts, " << nghosts);
        error = true;
    }

    // all offsets and lengths here are in terms of item bodies 
    // (e.g., 1 length unit = sizeof(item_body) bytes):
    size_t prev_end = 0;
    for (int item = 0; item<nitems; ++item) {
        size_t offset = sorted_items[item] >> 16;
        size_t len    = sorted_items[item] & 0xFFFF;

        if (offset < (size_t) first_used_body) {
            DBGOUT1(<<"The item starting at offset " << offset <<  " is located before first_used_body " << first_used_body);
            error = true;
        }
        if (len == 0) {
            DBGOUT1(<<"The item starting at offset " << offset <<  " has zero length");
            error = true;
        }
        if (offset >= max_bodies) {
            DBGOUT1(<<"The item starting at offset " << offset <<  " starts beyond the end of data area (" << max_bodies << ")!");
            error = true;
        }
        if (offset + len > max_bodies) {
            DBGOUT1(<<"The item starting at offset " << offset 
                    << " (length " << len << ") goes beyond the end of data area (" << max_bodies << ")!");
            error = true;
        }
        if (item != 0 && prev_end > offset) {
            DBGOUT1(<<"The item starting at offset " << offset <<  " overlaps with another item ending at " << prev_end);
            error = true;
        }

        prev_end = offset + len;
    }

#if W_DEBUG_LEVEL >= 1
    if (error) {
        DBGOUT1(<<"nitems=" << nitems << ", nghosts="<<nghosts);
        for (int i=0; i<nitems; i++) {
            int offset = head[i].offset;
            if (offset < 0) offset = -offset;
            size_t len = _item_bodies(offset);
            DBGOUT1(<<"  item[" << i << "] body @ offsets " << offset << " to " << offset+len-1
                    << "; ghost? " << is_ghost(i) << " poor: " << item_poor(i));
        }
    }
#endif

    //w_assert1(!error);
    return !error;
}


void btree_page_data::compact() {
    w_assert3(_items_are_consistent());

    item_body scratch_body[max_bodies];
    int       scratch_head = max_bodies;
#ifdef ZERO_INIT
    ::memset(&scratch_body, 0, sizeof(scratch_body));
#endif // ZERO_INIT

    int j = 0;
    for (int i=0; i<nitems; i++) {
        body_offset_t offset = head[i].offset;
        if (offset < 0) {
            nghosts--;
        } else {
            int length = _item_bodies(offset);
            scratch_head -= length;
            head[j].poor   = head[i].poor;
            head[j].offset = scratch_head;
            ::memcpy(&scratch_body[scratch_head], &body[offset], length*sizeof(item_body));
            j++;
        }
    }
    nitems = j;
    first_used_body = scratch_head;
    ::memcpy(&body[first_used_body], &scratch_body[scratch_head], (max_bodies-scratch_head)*sizeof(item_body));
    
    w_assert1(nghosts == 0);
    w_assert3(_items_are_consistent());
}


char* btree_page_data::unused_part(size_t& length) {
    char* start_gap = (char*)&head[nitems];
    char* after_gap = (char*)&body[first_used_body];
    length = after_gap - start_gap;
    return start_gap;
}


const size_t btree_page_data::max_item_overhead = sizeof(item_head) + sizeof(item_length_t) + sizeof(shpid_t) + _item_align(1)-1;
