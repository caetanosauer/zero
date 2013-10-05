/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "btree_page.h"

#include <algorithm>
#include <memory>
#include "w_debug.h"


void btree_page::init_slots() {
    const size_t max_offset = data_sz/sizeof(slot_body);

    nslots       = 0;
    nghosts      = 0;
    record_head8 = max_offset;

    w_assert3(_slots_are_consistent());
}


void btree_page::set_ghost(slot_index_t slot) {
    w_assert1(slot>=0 && slot<nslots);
    w_assert1(slot != 0); // fence slot cannot be a ghost

    w_assert1(!is_ghost(slot));
    slot_offset8_t offset = head[slot].offset;
    w_assert1(offset != 0);
    if (offset >= 0) {
        head[slot].offset = -offset;
        nghosts++;
    }
}

void btree_page::unset_ghost(slot_index_t slot) {
    w_assert1(slot>=0 && slot<nslots);
    w_assert1(slot != 0); // fence slot cannot be a ghost

    w_assert1(is_ghost(slot));
    slot_offset8_t offset = head[slot].offset;
    w_assert1(offset != 0);
    if (offset < 0) {
        head[slot].offset = -offset;
        nghosts--;
    }
}


bool btree_page::resize_slot(slot_index_t slot, size_t length, bool keep_old) {
    w_assert1(slot>=0 && slot<nslots);
    w_assert3(_slots_are_consistent());

    slot_offset8_t offset = head[slot].offset;
    bool ghost = false;
    if (offset < 0) {
        offset = -offset;
        ghost = true;
    }

    size_t old_length = slot_length(slot);
    if (length <= align(old_length)) {
        return true;
    }

    if (align(length) > (size_t) usable_space()) {
        return false;
    }

    record_head8 -= (length-1)/8+1;
    head[slot].offset = ghost ? -record_head8 : record_head8;

    if (keep_old) {
        char* old_p = (char*)&body[offset];
        char* new_p = (char*)&body[record_head8];
        ::memcpy(new_p, old_p, length); // later don't copy length?
    }

#if W_DEBUG_LEVEL>0
    ::memset((char*)&body[offset], 0, align(old_length)); // clear old slot
#endif // W_DEBUG_LEVEL>0

    //w_assert3(_slots_are_consistent()); // only consistent once length is set by caller
    return true;
}


void btree_page::delete_slot(slot_index_t slot) {
    w_assert1(slot>=0 && slot<nslots);
    w_assert1(slot != 0); // deleting slot 0 makes no sense because it has a special format
    w_assert3(_slots_are_consistent());

    slot_offset8_t offset = head[slot].offset;
    if (offset < 0) {
        offset = -offset;
        nghosts--;
    }

    if (offset == record_head8) {
        // Then, we are pushing down the record_head8.  lucky!
        record_head8 += slot_length8(slot);
    }

    // shift slot array down to remove head[slot]:
    ::memmove(&head[slot], &head[slot+1], (nslots-(slot+1))*sizeof(slot_head));
    nslots--;

    w_assert3(_slots_are_consistent());
}


bool btree_page::insert_slot(slot_index_t slot, bool ghost, size_t length, 
                             poor_man_key poor_key) {
    w_assert1(slot>=0 && slot<=nslots);  // use of <= intentional
    w_assert3(_slots_are_consistent());

    if ((size_t)usable_space() < sizeof(slot_head) + align(length)) {
        return false;
    }

    // shift slot array up to insert a slot so it is head[slot]:
    ::memmove(&head[slot+1], &head[slot], (nslots-slot)*sizeof(slot_head));
    nslots++;
    if (ghost) {
        nghosts++;
    }

    record_head8 -= (length-1)/8+1;
    head[slot].offset = ghost ? -record_head8 : record_head8;
    head[slot].poor = poor_key;

    //w_assert3(_slots_are_consistent()); // only consistent once length is set by caller
    return true;
}


bool btree_page::_slots_are_consistent() const {
    // This is not a part of check; should be always true:
    w_assert1(usable_space() >= 0);
    
    // check overlapping records.
    // rather than using std::map, use array and std::sort for efficiency.
    // high 16 bits=offset, low 16 bits=length
    static_assert(sizeof(slot_length_t) <= 2, 
                  "slot_length_t doesn't fit in 16 bits; adjust this code");
    std::unique_ptr<uint32_t[]> sorted_slots(new uint32_t[nslots]);
    int ghosts_seen = 0;
    for (int slot = 0; slot<nslots; ++slot) {
        int offset = head[slot].offset;
        int length = slot_length8(slot);
        if (offset < 0) {
            ghosts_seen++;
            sorted_slots[slot] = ((-offset) << 16) + length;
        } else {
            sorted_slots[slot] =  (offset << 16)   + length;
        }
    }
    std::sort(sorted_slots.get(), sorted_slots.get() + nslots);

    bool error = false;
    if (nghosts != ghosts_seen) {
        DBGOUT1(<<"Actual number of ghosts, " << ghosts_seen <<  ", differs from nghosts, " << nghosts);
        error = true;
    }

    // all offsets and lengths here are in terms of slot bodies 
    // (e.g., 1 length unit = sizeof(slot_body) bytes):
    const size_t max_offset = data_sz/sizeof(slot_body);
    size_t prev_end = 0;
    for (int slot = 0; slot<nslots; ++slot) {
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
        DBGOUT1(<<"nslots=" << nslots << ", nghosts="<<nghosts);
        for (int i=0; i<nslots; i++) {
            int offset = head[i].offset;
            if (offset < 0) offset = -offset;
            size_t len    = slot_length8(i);
            DBGOUT1(<<"  slot[" << i << "] body @ offsets " << offset << " to " << offset+len-1
                    << "; ghost? " << is_ghost(i) << " poor: " << poor(i));
        }
    }
#endif

    return !error;
}


void btree_page::compact() {
    w_assert3(_slots_are_consistent());

    const size_t max_offset = data_sz/sizeof(slot_body);
    slot_body scratch_body[data_sz/sizeof(slot_body)];
    int       scratch_head = max_offset;
#ifdef ZERO_INIT
    ::memset(&scratch_body, 0, sizeof(scratch_body));
#endif // ZERO_INIT

    int reclaimed = 0;
    int j = 0;
    for (int i=0; i<nslots; i++) {
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
    nslots = j;
    record_head8 = scratch_head;
    ::memcpy(body[record_head8].raw, scratch_body[scratch_head].raw, (max_offset-scratch_head)*sizeof(slot_body));
    
    w_assert1(nghosts == 0);
    w_assert3(_slots_are_consistent());
}
