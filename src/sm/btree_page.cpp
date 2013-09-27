/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "btree_page.h"


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
