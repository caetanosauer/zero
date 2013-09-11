/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#define SM_SOURCE
#define PAGE_C

#include "sm_int_1.h"
#include "fixable_page_h.h"
#include "page_bf_inline.h"
#include "btree_page.h"
#include "w_key.h"
#include "bf_tree.h"


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
