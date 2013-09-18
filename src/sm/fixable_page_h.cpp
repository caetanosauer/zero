/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#define SM_SOURCE
#include "sm_int_1.h"

#include "fixable_page_h.h"



rc_t fixable_page_h::set_to_be_deleted (bool log_it) {
    if ((_pp->page_flags & t_to_be_deleted) == 0) {
        if (log_it) {
            W_DO(log_page_set_to_be_deleted (*this));
        }
        _pp->page_flags ^= t_to_be_deleted;
        set_dirty();
    }
    return RCOK;
}

// <<<>>> use virtual methods later?

#include "btree_page.h"

int fixable_page_h::max_child_slot() const {
    btree_page_h downcast(get_generic_page());

    if (downcast.level()<=1)
        return -1;
    return downcast.nslots() - 1;
}

shpid_t* fixable_page_h::child_slot_address(int child_slot) const {
    btree_page_h downcast(get_generic_page());

    if (child_slot == -1) {
        return &downcast.foster_pointer();
    }

    w_assert1( downcast.level()>1 && child_slot < downcast.nslots() );

    if (child_slot == 0) {
        return &downcast.pid0_pointer();
    }

    void* addr = downcast.tuple_addr(child_slot);
    return reinterpret_cast<shpid_t*>(addr);
}
