/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#define SM_SOURCE
#define PAGE_C

#include "sm_int_1.h"
#include "generic_page_h.h"
#include "page_bf_inline.h"
#include "btree_p.h"
#include "w_key.h"
#include "bf_tree.h"


bool generic_page_h::check_space_for_insert(size_t rec_size) {
    size_t contiguous_free_space = usable_space();
    return contiguous_free_space >= align(rec_size) + slot_sz;
}

rc_t generic_page_h::set_tobedeleted (bool log_it) {
    if ((_pp->page_flags & t_tobedeleted) == 0) {
        if (log_it) {
            W_DO(log_page_set_tobedeleted (*this));
        }
        _pp->page_flags ^= t_tobedeleted;
        set_dirty();
    }
    return RCOK;
}
