/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#define SM_SOURCE
#define LOGREC_C
#include "sm_int_2.h"
#include "logdef_gen.cpp"

#include "log_spr.h"

page_evict_log::page_evict_log (const btree_page_h& p, slotid_t child_slot, lsn_t child_lsn) {
    new (_data) page_evict_t(child_lsn, child_slot);
    fill(&p.pid(), p.tag(), sizeof(page_evict_t));
}

void page_evict_log::redo(fixable_page_h* page) {
    borrowed_btree_page_h bp(page);
    page_evict_t *dp = (page_evict_t*) _data;
    bp.set_emlsn_general(dp->_child_slot, dp->_child_lsn);
}
