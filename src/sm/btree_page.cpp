/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "btree_page.h"

#include <algorithm>
#include <memory>
#include "w_debug.h"


void btree_page_data::init_items() {
    w_assert1(btree_level >= 1);
    const size_t max_offset = data_sz/sizeof(slot_body);

    nitems       = 0;
    nghosts      = 0;
    record_head8 = max_offset;

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

    size_t length = data_length + sizeof(item_length_t);
    if (item != 0 && btree_level != 1) {
        length += sizeof(shpid_t);
    }
    if ((size_t)usable_space() < sizeof(slot_head) + align(length)) {
        return false;
    }

    // shift item array up to insert a item so it is head[item]:
    ::memmove(&head[item+1], &head[item], (nitems-item)*sizeof(slot_head));
    nitems++;
    if (ghost) {
        nghosts++;
    }

    record_head8 -= (length-1)/8+1;
    head[item].offset = ghost ? -record_head8 : record_head8;
    head[item].poor = poor;

    if (btree_level != 1) {
        body[record_head8].interior.child = child;
    } else {
        w_assert1(child == 0);
    }
    set_slot_length(item, length);

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

    size_t old_length = slot_length(item);
    size_t length = new_length + sizeof(item_length_t);
    if (item != 0 && btree_level != 1) {
        length += sizeof(shpid_t);
    }

    if (length <= align(old_length)) {
        set_slot_length(item, length);
        w_assert3(_items_are_consistent()); 
        return true;
    }

    if (align(length) > (size_t) usable_space()) {
        return false;
    }

    record_head8 -= (length-1)/8+1;
    head[item].offset = ghost ? -record_head8 : record_head8;
    set_slot_length(item, length);
    if (item != 0 && btree_level != 1) {
        body[record_head8].interior.child = body[offset].interior.child;
    }

    if (keep_old > 0) {
        char* new_p = item_data(item);
        char* old_p = (char*)&body[offset] + (new_p - (char*)&body[record_head8]);
        w_assert1(old_p+keep_old <= (char*)&body[offset]+old_length);
        ::memcpy(new_p, old_p, keep_old); 
    }

#if W_DEBUG_LEVEL>0
    ::memset((char*)&body[offset], 0xef, align(old_length)); // overwrite old item
#endif // W_DEBUG_LEVEL>0

    w_assert3(_items_are_consistent()); 
    return true;
}

bool btree_page_data::replace_item_data(int item, const cvec_t& new_data, size_t keep_old) {
    if (!resize_item(item, keep_old+new_data.size(), keep_old))
        return false;

    new_data.copy_to(item_data(item) + keep_old);
    return true;
}


void btree_page_data::delete_item(int item) {
    w_assert1(item>=0 && item<nitems);
    w_assert1(item != 0); // deleting item 0 makes no sense because it has a special format <<<>>>
    w_assert3(_items_are_consistent());

    body_offset_t offset = head[item].offset;
    if (offset < 0) {
        offset = -offset;
        nghosts--;
    }

    if (offset == record_head8) {
        // Then, we are pushing down the record_head8.  lucky!
        record_head8 += slot_length8(item);
    }

    // shift item array down to remove head[item]:
    ::memmove(&head[item], &head[item+1], (nitems-(item+1))*sizeof(slot_head));
    nitems--;

    w_assert3(_items_are_consistent());
}


// <<<>>>
#include <boost/scoped_array.hpp>

bool btree_page_data::_items_are_consistent() const {
    // This is not a part of check; should be always true:
    w_assert1(record_head8*8 - nitems*4 >= 0);
    
    // check overlapping records.
    // rather than using std::map, use array and std::sort for efficiency.
    // high 16 bits=offset, low 16 bits=length
    //static_assert(sizeof(item_length_t) <= 2, 
    //              "item_length_t doesn't fit in 16 bits; adjust this code");
    BOOST_STATIC_ASSERT(sizeof(item_length_t) <= 2);
    //std::unique_ptr<uint32_t[]> sorted_slots(new uint32_t[nitems]);
    boost::scoped_array<uint32_t> sorted_slots(new uint32_t[nitems]); // <<<>>>
    int ghosts_seen = 0;
    for (int slot = 0; slot<nitems; ++slot) {
        int offset = head[slot].offset;
        int length = slot_length8(slot);
        if (offset < 0) {
            ghosts_seen++;
            sorted_slots[slot] = ((-offset) << 16) + length;
        } else {
            sorted_slots[slot] =  (offset << 16)   + length;
        }
    }
    std::sort(sorted_slots.get(), sorted_slots.get() + nitems);

    bool error = false;
    if (nghosts != ghosts_seen) {
        DBGOUT1(<<"Actual number of ghosts, " << ghosts_seen <<  ", differs from nghosts, " << nghosts);
        error = true;
    }

    // all offsets and lengths here are in terms of slot bodies 
    // (e.g., 1 length unit = sizeof(slot_body) bytes):
    const size_t max_offset = data_sz/sizeof(slot_body);
    size_t prev_end = 0;
    for (int slot = 0; slot<nitems; ++slot) {
        size_t offset = sorted_slots[slot] >> 16;
        size_t len    = sorted_slots[slot] & 0xFFFF;

        if (offset < (size_t) record_head8) {
            DBGOUT1(<<"The slot starting at offset " << offset <<  " is located before record_head " << record_head8);
            error = true;
        }
        if (len == 0) {
            DBGOUT1(<<"The slot starting at offset " << offset <<  " has zero length");
            error = true;
        }
        if (offset >= max_offset) {
            DBGOUT1(<<"The slot starting at offset " << offset <<  " starts beyond the end of data area (" << max_offset << ")!");
            error = true;
        }
        if (offset + len > max_offset) {
            DBGOUT1(<<"The slot starting at offset " << offset 
                    << " (length " << len << ") goes beyond the end of data area (" << max_offset << ")!");
            error = true;
        }
        if (slot != 0 && prev_end > offset) {
            DBGOUT1(<<"The slot starting at offset " << offset <<  " overlaps with another slot ending at " << prev_end);
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
            size_t len    = slot_length8(i);
            DBGOUT1(<<"  slot[" << i << "] body @ offsets " << offset << " to " << offset+len-1
                    << "; ghost? " << is_ghost(i) << " poor: " << item_poor(i));
        }
    }
#endif

    //w_assert1(!error);
    return !error;
}


void btree_page_data::compact() {
    w_assert3(_items_are_consistent());

    const size_t max_offset = data_sz/sizeof(slot_body);
    slot_body scratch_body[data_sz/sizeof(slot_body)];
    int       scratch_head = max_offset;
#ifdef ZERO_INIT
    ::memset(&scratch_body, 0, sizeof(scratch_body));
#endif // ZERO_INIT

    int reclaimed = 0;
    int j = 0;
    for (int i=0; i<nitems; i++) {
        if (head[i].offset<0) {
            reclaimed++;
            nghosts--;
        } else {
            int length = slot_length8(i);
            scratch_head -= length;
            head[j].poor = head[i].poor;
            head[j].offset = scratch_head;
            ::memcpy(scratch_body[scratch_head].raw, slot_start(i), length*sizeof(slot_body));
            j++;
        }
    }
    nitems = j;
    record_head8 = scratch_head;
    ::memcpy(body[record_head8].raw, scratch_body[scratch_head].raw, (max_offset-scratch_head)*sizeof(slot_body));
    
    w_assert1(nghosts == 0);
    w_assert3(_items_are_consistent());
}


char* btree_page_data::unused_part(size_t& length) {
    char* start_gap = (char*)&head[nitems];
    char* after_gap = (char*)&body[record_head8];
    length = after_gap - start_gap;
    return start_gap;
}


size_t btree_page_data::predict_item_space(size_t data_length) {
    size_t size = data_length + sizeof(item_length_t);
    int item = 1; // <<<>>>
    if (item != 0 && btree_level != 1) {
        size += sizeof(shpid_t);
    }

    return align(size) + sizeof(slot_head);
}


size_t btree_page_data::item_space(int item) const {
    return align(slot_length(item)) + sizeof(slot_head);
}


const size_t btree_page_data::max_item_overhead = sizeof(slot_head) + sizeof(item_length_t) + sizeof(shpid_t) + align(1)-1;
