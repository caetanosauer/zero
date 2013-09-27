/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "btree_page.h"


void btree_page::set_ghost(slot_index_t slot) {
    w_assert1(slot>=0 && slot<nslots);
    w_assert1(slot != 0); // fence slot cannot be a ghost

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

    slot_offset8_t offset = head[slot].offset;
    w_assert1(offset != 0);
    if (offset < 0) {
        head[slot].offset = -offset;
        nghosts--;
    }
}


void btree_page::delete_slot(slot_index_t slot) {
    w_assert1(slot>=0 && slot<nslots);
    w_assert1(slot != 0); // deleting slot 0 makes no sense because it has a special format

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
}


bool btree_page::resize_slot(slot_index_t slot, size_t length, bool keep_old) {
    w_assert1(slot>=0 && slot<nslots);

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

    if (align(length) > usable_space()) {
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

    return true;
}
