/*

<std-header orig-src='shore' genfile='true'>

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#ifndef LOGDEF_GEN_H
#define LOGDEF_GEN_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include "alloc_page.h"
#include "btree_page_h.h"
#include "stnode_page.h"
#include "w_base.h"
#include "logrec.h"

    class comment_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_undo|t_logical, header._type = t_comment;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_undo|t_logical, header._type = t_comment;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_undo|t_logical, header._type = t_comment;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_undo|t_logical, header._type = t_comment;
      logrec_t::fill(pid, tag, l);
    }
      public:
    comment_log (const char* msg);
    comment_log (logrec_t*)   {};

    void redo(fixable_page_h*);
    void undo(fixable_page_h*);
    };

    class compensate_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_logical, header._type = t_compensate;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_logical, header._type = t_compensate;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_logical, header._type = t_compensate;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_logical, header._type = t_compensate;
      logrec_t::fill(pid, tag, l);
    }
      public:
    compensate_log (const lsn_t& rec_lsn);
    compensate_log (logrec_t*)   {};



    };

    class skip_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_skip;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = t_status, header._type = t_skip;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = t_status, header._type = t_skip;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_skip;
      logrec_t::fill(pid, tag, l);
    }
      public:
    skip_log ();
    skip_log (logrec_t*)   {};



    };

    class chkpt_begin_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_chkpt_begin;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = t_status, header._type = t_chkpt_begin;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = t_status, header._type = t_chkpt_begin;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_chkpt_begin;
      logrec_t::fill(pid, tag, l);
    }
      public:
    chkpt_begin_log (const lsn_t &lastMountLSN);
    chkpt_begin_log (logrec_t*)   {};



    };

    class chkpt_bf_tab_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_chkpt_bf_tab;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = t_status, header._type = t_chkpt_bf_tab;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = t_status, header._type = t_chkpt_bf_tab;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_chkpt_bf_tab;
      logrec_t::fill(pid, tag, l);
    }
      public:
    chkpt_bf_tab_log (int cnt, const PageID* pid, const lsn_t* rec_lsn, const lsn_t* page_lsn);
    chkpt_bf_tab_log (logrec_t*)   {};



    };

    class chkpt_xct_tab_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_chkpt_xct_tab;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = t_status, header._type = t_chkpt_xct_tab;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = t_status, header._type = t_chkpt_xct_tab;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_chkpt_xct_tab;
      logrec_t::fill(pid, tag, l);
    }
      public:
    chkpt_xct_tab_log (const tid_t& youngest, int cnt, const tid_t* tid, const smlevel_0::xct_state_t* state, const lsn_t* last_lsn, const lsn_t* first_lsn);;
    chkpt_xct_tab_log (logrec_t*)   {};



    };

    class chkpt_xct_lock_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_chkpt_xct_lock;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = t_status, header._type = t_chkpt_xct_lock;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = t_status, header._type = t_chkpt_xct_lock;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_chkpt_xct_lock;
      logrec_t::fill(pid, tag, l);
    }
      public:
    chkpt_xct_lock_log (const tid_t& tid, int cnt, const okvl_mode* lock_mode, const uint32_t* lock_hash);
    chkpt_xct_lock_log (logrec_t*)   {};



    };

    class chkpt_restore_tab_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo, header._type = t_chkpt_restore_tab;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo, header._type = t_chkpt_restore_tab;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo, header._type = t_chkpt_restore_tab;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo, header._type = t_chkpt_restore_tab;
      logrec_t::fill(pid, tag, l);
    }
      public:
    chkpt_restore_tab_log ();
    chkpt_restore_tab_log (logrec_t*)   {};

    void redo(fixable_page_h*);

    };

    class chkpt_backup_tab_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo, header._type = t_chkpt_backup_tab;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo, header._type = t_chkpt_backup_tab;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo, header._type = t_chkpt_backup_tab;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo, header._type = t_chkpt_backup_tab;
      logrec_t::fill(pid, tag, l);
    }
      public:
    chkpt_backup_tab_log (int cnt, const string* paths);
    chkpt_backup_tab_log (logrec_t*)   {};

    void redo(fixable_page_h*);

    };

    class chkpt_end_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_chkpt_end;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = t_status, header._type = t_chkpt_end;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = t_status, header._type = t_chkpt_end;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_chkpt_end;
      logrec_t::fill(pid, tag, l);
    }
      public:
    chkpt_end_log (const lsn_t& master, const lsn_t& min_rec_lsn, const lsn_t& min_xct_lsn);
    chkpt_end_log (logrec_t*)   {};



    };

    class add_backup_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_add_backup;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_add_backup;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_add_backup;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_add_backup;
      logrec_t::fill(pid, tag, l);
    }
      public:
    add_backup_log (const string& path, lsn_t backupLSN);
    add_backup_log (logrec_t*)   {};

    void redo(fixable_page_h*);

    };

    class xct_abort_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_xct_abort;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = t_status, header._type = t_xct_abort;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = t_status, header._type = t_xct_abort;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_xct_abort;
      logrec_t::fill(pid, tag, l);
    }
      public:
    xct_abort_log ();
    xct_abort_log (logrec_t*)   {};



    };

    class xct_freeing_space_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_xct_freeing_space;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = t_status, header._type = t_xct_freeing_space;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = t_status, header._type = t_xct_freeing_space;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_xct_freeing_space;
      logrec_t::fill(pid, tag, l);
    }
      public:
    xct_freeing_space_log ();
    xct_freeing_space_log (logrec_t*)   {};



    };

    class xct_end_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_xct_end;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = t_status, header._type = t_xct_end;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = t_status, header._type = t_xct_end;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_xct_end;
      logrec_t::fill(pid, tag, l);
    }
      public:
    xct_end_log ();
    xct_end_log (logrec_t*)   {};



    };

    class xct_end_group_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_xct_end_group;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = t_status, header._type = t_xct_end_group;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = t_status, header._type = t_xct_end_group;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_xct_end_group;
      logrec_t::fill(pid, tag, l);
    }
      public:
    xct_end_group_log (const xct_t** l, int llen);
    xct_end_group_log (logrec_t*)   {};



    };

    class xct_latency_dump_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_xct_latency_dump;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = t_status, header._type = t_xct_latency_dump;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = t_status, header._type = t_xct_latency_dump;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_xct_latency_dump;
      logrec_t::fill(pid, tag, l);
    }
      public:
    xct_latency_dump_log (unsigned long nsec);
    xct_latency_dump_log (logrec_t*)   {};



    };

    class alloc_page_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_alloc_page;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_alloc_page;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_alloc_page;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_alloc_page;
      logrec_t::fill(pid, tag, l);
    }
      public:
    alloc_page_log (PageID pid);
    alloc_page_log (logrec_t*)   {};

    void redo(fixable_page_h*);

    };

    class dealloc_page_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_dealloc_page;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_dealloc_page;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_dealloc_page;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_dealloc_page;
      logrec_t::fill(pid, tag, l);
    }
      public:
    dealloc_page_log (PageID pid);
    dealloc_page_log (logrec_t*)   {};

    void redo(fixable_page_h*);

    };

    class create_store_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_create_store;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_create_store;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_create_store;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_create_store;
      logrec_t::fill(pid, tag, l);
    }
      public:
    create_store_log (PageID root_pid, StoreID snum);
    create_store_log (logrec_t*)   {};

    void redo(fixable_page_h*);

    };

    class append_extent_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_append_extent;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_append_extent;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_append_extent;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_append_extent;
      logrec_t::fill(pid, tag, l);
    }
      public:
    append_extent_log (extent_id_t ext);
    append_extent_log (logrec_t*)   {};

    void redo(fixable_page_h*);

    };

    class loganalysis_begin_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_loganalysis_begin;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = t_status, header._type = t_loganalysis_begin;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = t_status, header._type = t_loganalysis_begin;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_loganalysis_begin;
      logrec_t::fill(pid, tag, l);
    }
      public:
    loganalysis_begin_log ();
    loganalysis_begin_log (logrec_t*)   {};



    };

    class loganalysis_end_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_loganalysis_end;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = t_status, header._type = t_loganalysis_end;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = t_status, header._type = t_loganalysis_end;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_loganalysis_end;
      logrec_t::fill(pid, tag, l);
    }
      public:
    loganalysis_end_log ();
    loganalysis_end_log (logrec_t*)   {};



    };

    class redo_done_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_redo_done;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = t_status, header._type = t_redo_done;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = t_status, header._type = t_redo_done;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_redo_done;
      logrec_t::fill(pid, tag, l);
    }
      public:
    redo_done_log ();
    redo_done_log (logrec_t*)   {};



    };

    class undo_done_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_undo_done;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = t_status, header._type = t_undo_done;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = t_status, header._type = t_undo_done;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_undo_done;
      logrec_t::fill(pid, tag, l);
    }
      public:
    undo_done_log ();
    undo_done_log (logrec_t*)   {};



    };

    class restore_begin_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_logical|t_single_sys_xct, header._type = t_restore_begin;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_logical|t_single_sys_xct, header._type = t_restore_begin;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_logical|t_single_sys_xct, header._type = t_restore_begin;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_logical|t_single_sys_xct, header._type = t_restore_begin;
      logrec_t::fill(pid, tag, l);
    }
      public:
    restore_begin_log ();
    restore_begin_log (logrec_t*)   {};

    void redo(fixable_page_h*);

    };

    class restore_segment_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_logical|t_single_sys_xct, header._type = t_restore_segment;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_logical|t_single_sys_xct, header._type = t_restore_segment;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_logical|t_single_sys_xct, header._type = t_restore_segment;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_logical|t_single_sys_xct, header._type = t_restore_segment;
      logrec_t::fill(pid, tag, l);
    }
      public:
    restore_segment_log (uint32_t segment);
    restore_segment_log (logrec_t*)   {};

    void redo(fixable_page_h*);

    };

    class restore_end_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_logical|t_single_sys_xct, header._type = t_restore_end;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_logical|t_single_sys_xct, header._type = t_restore_end;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_logical|t_single_sys_xct, header._type = t_restore_end;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_logical|t_single_sys_xct, header._type = t_restore_end;
      logrec_t::fill(pid, tag, l);
    }
      public:
    restore_end_log ();
    restore_end_log (logrec_t*)   {};

    void redo(fixable_page_h*);

    };

    class page_set_to_be_deleted_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_undo, header._type = t_page_set_to_be_deleted;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_undo, header._type = t_page_set_to_be_deleted;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_undo, header._type = t_page_set_to_be_deleted;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_undo, header._type = t_page_set_to_be_deleted;
      logrec_t::fill(pid, tag, l);
    }
      public:
    page_set_to_be_deleted_log (const fixable_page_h& page);
    page_set_to_be_deleted_log (logrec_t*)   {};

    void redo(fixable_page_h*);
    void undo(fixable_page_h*);
    };

    class page_img_format_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_undo, header._type = t_page_img_format;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_undo, header._type = t_page_img_format;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_undo, header._type = t_page_img_format;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_undo, header._type = t_page_img_format;
      logrec_t::fill(pid, tag, l);
    }
      public:
    page_img_format_log (const btree_page_h& page);
    page_img_format_log (logrec_t*)   {};

    void redo(fixable_page_h*);
    void undo(fixable_page_h*);
    };

    class page_evict_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_page_evict;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_page_evict;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_page_evict;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_page_evict;
      logrec_t::fill(pid, tag, l);
    }
      public:
    page_evict_log (const btree_page_h& page, general_recordid_t child_slot, lsn_t child_lsn);
    page_evict_log (logrec_t*)   {};

    void redo(fixable_page_h*);

    };

    class btree_norec_alloc_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_norec_alloc;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_norec_alloc;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_norec_alloc;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_norec_alloc;
      logrec_t::fill(pid, tag, l);
    }
      public:
    btree_norec_alloc_log (const btree_page_h& page, const btree_page_h& page2, PageID new_page_id, const w_keystr_t& fence, const w_keystr_t& chain_fence_high);
    btree_norec_alloc_log (logrec_t*)   {};

    void redo(fixable_page_h*);

    };

    class btree_insert_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_undo|t_logical, header._type = t_btree_insert;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_undo|t_logical, header._type = t_btree_insert;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_undo|t_logical, header._type = t_btree_insert;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_undo|t_logical, header._type = t_btree_insert;
      logrec_t::fill(pid, tag, l);
    }
      public:
    btree_insert_log (const btree_page_h& page, const w_keystr_t& key, const cvec_t& el, const bool sys_txn);
    btree_insert_log (logrec_t*)   {};

    void redo(fixable_page_h*);
    void undo(fixable_page_h*);
    };

    class btree_insert_nonghost_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_undo|t_logical, header._type = t_btree_insert_nonghost;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_undo|t_logical, header._type = t_btree_insert_nonghost;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_undo|t_logical, header._type = t_btree_insert_nonghost;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_undo|t_logical, header._type = t_btree_insert_nonghost;
      logrec_t::fill(pid, tag, l);
    }
      public:
    btree_insert_nonghost_log (const btree_page_h& page, const w_keystr_t& key, const cvec_t& el, const bool sys_txn);
    btree_insert_nonghost_log (logrec_t*)   {};

    void redo(fixable_page_h*);
    void undo(fixable_page_h*);
    };

    class btree_update_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_undo|t_logical, header._type = t_btree_update;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_undo|t_logical, header._type = t_btree_update;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_undo|t_logical, header._type = t_btree_update;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_undo|t_logical, header._type = t_btree_update;
      logrec_t::fill(pid, tag, l);
    }
      public:
    btree_update_log (const btree_page_h& page, const w_keystr_t& key, const char* old_el, int old_elen, const cvec_t& new_el);
    btree_update_log (logrec_t*)   {};

    void redo(fixable_page_h*);
    void undo(fixable_page_h*);
    };

    class btree_overwrite_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_undo|t_logical, header._type = t_btree_overwrite;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_undo|t_logical, header._type = t_btree_overwrite;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_undo|t_logical, header._type = t_btree_overwrite;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_undo|t_logical, header._type = t_btree_overwrite;
      logrec_t::fill(pid, tag, l);
    }
      public:
    btree_overwrite_log (const btree_page_h& page, const w_keystr_t& key, const char* old_el, const char* new_el, size_t offset, size_t elen);
    btree_overwrite_log (logrec_t*)   {};

    void redo(fixable_page_h*);
    void undo(fixable_page_h*);
    };

    class btree_ghost_mark_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_undo|t_logical, header._type = t_btree_ghost_mark;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_undo|t_logical, header._type = t_btree_ghost_mark;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_undo|t_logical, header._type = t_btree_ghost_mark;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_undo|t_logical, header._type = t_btree_ghost_mark;
      logrec_t::fill(pid, tag, l);
    }
      public:
    btree_ghost_mark_log (const btree_page_h& page, const vector<slotid_t>& slots, const bool sys_txn);
    btree_ghost_mark_log (logrec_t*)   {};

    void redo(fixable_page_h*);
    void undo(fixable_page_h*);
    };

    class btree_ghost_reclaim_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_btree_ghost_reclaim;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_btree_ghost_reclaim;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_btree_ghost_reclaim;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_btree_ghost_reclaim;
      logrec_t::fill(pid, tag, l);
    }
      public:
    btree_ghost_reclaim_log (const btree_page_h& page, const vector<slotid_t>& slots);
    btree_ghost_reclaim_log (logrec_t*)   {};

    void redo(fixable_page_h*);

    };

    class btree_ghost_reserve_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_btree_ghost_reserve;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_btree_ghost_reserve;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_btree_ghost_reserve;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_btree_ghost_reserve;
      logrec_t::fill(pid, tag, l);
    }
      public:
    btree_ghost_reserve_log (const btree_page_h& page, const w_keystr_t& key, int element_length);
    btree_ghost_reserve_log (logrec_t*)   {};

    void redo(fixable_page_h*);

    };

    class btree_foster_adopt_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_foster_adopt;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_foster_adopt;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_foster_adopt;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_foster_adopt;
      logrec_t::fill(pid, tag, l);
    }
      public:
    btree_foster_adopt_log (const btree_page_h& page, const btree_page_h& page2, PageID new_child_pid, lsn_t child_emlsn, const w_keystr_t& new_child_key);
    btree_foster_adopt_log (logrec_t*)   {};

    void redo(fixable_page_h*);

    };

    class btree_foster_merge_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_foster_merge;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_foster_merge;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_foster_merge;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_foster_merge;
      logrec_t::fill(pid, tag, l);
    }
      public:
    btree_foster_merge_log (const btree_page_h& page, const btree_page_h& page2, const w_keystr_t& high, const w_keystr_t& chain_high, PageID foster_pid0, lsn_t foster_emlsn, const int16_t prefix_len, const int32_t move_count, const smsize_t record_buffer_len, const cvec_t& record_data);
    btree_foster_merge_log (logrec_t*)   {};

    void redo(fixable_page_h*);

    };

    class btree_foster_rebalance_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_foster_rebalance;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_foster_rebalance;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_foster_rebalance;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_foster_rebalance;
      logrec_t::fill(pid, tag, l);
    }
      public:
    btree_foster_rebalance_log (const btree_page_h& page, const btree_page_h& page2, const w_keystr_t& fence, PageID new_pid0, lsn_t pid0_emlsn, const w_keystr_t& high, const w_keystr_t& chain_high, const int16_t prefix_len, const int32_t move_count, const smsize_t record_data_len, const cvec_t& record_data);
    btree_foster_rebalance_log (logrec_t*)   {};

    void redo(fixable_page_h*);

    };

    class btree_foster_rebalance_norec_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_foster_rebalance_norec;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_foster_rebalance_norec;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_foster_rebalance_norec;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_foster_rebalance_norec;
      logrec_t::fill(pid, tag, l);
    }
      public:
    btree_foster_rebalance_norec_log (const btree_page_h& page, const btree_page_h& page2, const w_keystr_t& fence);
    btree_foster_rebalance_norec_log (logrec_t*)   {};

    void redo(fixable_page_h*);

    };

    class btree_foster_deadopt_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_foster_deadopt;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_foster_deadopt;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_foster_deadopt;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_foster_deadopt;
      logrec_t::fill(pid, tag, l);
    }
      public:
    btree_foster_deadopt_log (const btree_page_h& page, const btree_page_h& page2, PageID deadopted_pid, lsn_t deadopted_emlsn, int32_t foster_slot, const w_keystr_t& low, const w_keystr_t& high);
    btree_foster_deadopt_log (logrec_t*)   {};

    void redo(fixable_page_h*);

    };

    class btree_split_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_split;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_split;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_split;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_multi|t_single_sys_xct, header._type = t_btree_split;
      logrec_t::fill(pid, tag, l);
    }
      public:
    btree_split_log (const btree_page_h& page, const btree_page_h& page2, uint16_t move_count, const w_keystr_t& new_high_fence, const w_keystr_t& new_chain);
    btree_split_log (logrec_t*)   {};

    void redo(fixable_page_h*);

    };

    class btree_compress_page_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_btree_compress_page;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_btree_compress_page;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_btree_compress_page;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = 0|t_redo|t_single_sys_xct, header._type = t_btree_compress_page;
      logrec_t::fill(pid, tag, l);
    }
      public:
    btree_compress_page_log (const btree_page_h& page, const w_keystr_t& low, const w_keystr_t& high, const w_keystr_t& chain);
    btree_compress_page_log (logrec_t*)   {};

    void redo(fixable_page_h*);

    };

    class tick_sec_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_tick_sec;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = t_status, header._type = t_tick_sec;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = t_status, header._type = t_tick_sec;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_tick_sec;
      logrec_t::fill(pid, tag, l);
    }
      public:
    tick_sec_log ();
    tick_sec_log (logrec_t*)   {};



    };

    class tick_msec_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_tick_msec;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = t_status, header._type = t_tick_msec;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = t_status, header._type = t_tick_msec;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_tick_msec;
      logrec_t::fill(pid, tag, l);
    }
      public:
    tick_msec_log ();
    tick_msec_log (logrec_t*)   {};



    };

    class benchmark_start_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_benchmark_start;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = t_status, header._type = t_benchmark_start;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = t_status, header._type = t_benchmark_start;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_benchmark_start;
      logrec_t::fill(pid, tag, l);
    }
      public:
    benchmark_start_log ();
    benchmark_start_log (logrec_t*)   {};



    };

    class page_write_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_page_write;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = t_status, header._type = t_page_write;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = t_status, header._type = t_page_write;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_page_write;
      logrec_t::fill(pid, tag, l);
    }
      public:
    page_write_log ();
    page_write_log (logrec_t*)   {};



    };

    class page_read_log : public logrec_t {
    void fill(const PageID p, StoreID store, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_page_read;
      logrec_t::fill(p, store, tag, l);
    }
    void fill(const PageID pid, int l) {
      header._cat = t_status, header._type = t_page_read;
      logrec_t::fill(pid, 0, 0, l);
    }
    void fill(const generic_page_h& p, int l) {
      header._cat = t_status, header._type = t_page_read;
      logrec_t::fill(p.pid(), p.store(), p.tag(), l);
    }
    void fill(PageID pid, uint16_t tag, int l) {
      header._cat = t_status, header._type = t_page_read;
      logrec_t::fill(pid, tag, l);
    }
      public:
    page_read_log ();
    page_read_log (logrec_t*)   {};



    };

/**
 * This is a special way of logging the creation of a new page.
 * New page creation is usually a page split, so the new page has many
 * records in it. To simplify and to avoid many log entries in that case,
 * we log ALL bytes from the beginning to the end of slot vector,
 * and from the record_head8 to the end of page.
 * We can assume totally defragmented page image because this is page creation.
 * We don't need UNDO (again, this is page creation!), REDO is just two memcpy().
 */
struct page_img_format_t {
    size_t      beginning_bytes;
    size_t      ending_bytes;
    char        data[logrec_t::max_data_sz - 2 * sizeof(size_t)];
    int size()        { return 2 * sizeof(size_t) + beginning_bytes + ending_bytes; }
    page_img_format_t (const btree_page_h& page);

    void apply(fixable_page_h* p);
};

struct chkpt_bf_tab_t {
    struct brec_t {
    PageID    pid;      // +8 -> 8
    /*
     *  CS: store is required to mark as in-doubt on buffer pool.
     *  Perhaps we can remove the store number from buffer control blocks
     *  (bf_tree_cb_t), provided that they are not required. (TODO)
     */
    lsn_t    rec_lsn;   // +8 -> 16, this is the minimum (earliest) LSN
    lsn_t    page_lsn;  // +8 -> 24, this is the latest (page) LSN
    };

    // max is set to make chkpt_bf_tab_t fit in logrec_t::data_sz
    enum { max = (logrec_t::max_data_sz - 2 * sizeof(uint32_t)) / sizeof(brec_t) };
    uint32_t              count;
    fill4              filler;
    brec_t             brec[max];

    NORET            chkpt_bf_tab_t(
    int                 cnt,
    const PageID*             p,
    const lsn_t*             l,
    const lsn_t*             pl);

    int                size() const;
};

struct prepare_stores_to_free_t
{
    enum { max = (logrec_t::max_data_sz - sizeof(uint32_t)) / sizeof(StoreID) };
    uint32_t            num;
    StoreID            stids[max];

    prepare_stores_to_free_t(uint32_t theNum, const StoreID* theStids)
    : num(theNum)
    {
        w_assert3(theNum <= max);
        for (uint32_t i = 0; i < num; i++)
        stids[i] = theStids[i];
    };

    int size() const  { return sizeof(uint32_t) + num * sizeof(StoreID); };
};

struct chkpt_xct_tab_t {
    struct xrec_t {
    tid_t                 tid;
    lsn_t                last_lsn;
    lsn_t                first_lsn;
    smlevel_0::xct_state_t        state;
    };

    // max is set to make chkpt_xct_tab_t fit in logrec_t::data_sz
    enum {     max = ((logrec_t::max_data_sz - sizeof(tid_t) -
            2 * sizeof(uint32_t)) / sizeof(xrec_t))
    };
    tid_t            youngest;    // maximum tid in session
    uint32_t            count;
    fill4            filler;
    xrec_t             xrec[max];

    NORET            chkpt_xct_tab_t(
    const tid_t&             youngest,
    int                 count,
    const tid_t*             tid,
    const smlevel_0::xct_state_t* state,
    const lsn_t*             last_lsn,
    const lsn_t*             first_lsn);
    int             size() const;
};

struct chkpt_xct_lock_t {
    struct lockrec_t {
    okvl_mode            lock_mode;
    uint32_t             lock_hash;
    };

    // max is set to make chkpt_xct_lock_t fit in logrec_t::data_sz
    enum {     max = ((logrec_t::max_data_sz - sizeof(tid_t) -
            2 * sizeof(uint32_t)) / sizeof(lockrec_t))
    };

    tid_t            tid;    // owning transaction tid
    uint32_t         count;
    fill4            filler;
    lockrec_t        xrec[max];

    NORET            chkpt_xct_lock_t(
    const tid_t&        tid,
    int                 count,
    const okvl_mode*    lock_mode,
    const uint32_t*     lock_hash);
    int             size() const;
};

struct chkpt_backup_tab_t
{
    uint32_t count;
    uint32_t data_size;
    char     data[logrec_t::max_data_sz];

    enum {
        max = (logrec_t::max_data_sz - 2 * sizeof(uint32_t))
                / (smlevel_0::max_devname)
    };

    chkpt_backup_tab_t(
        const std::vector<string>& paths);

    chkpt_backup_tab_t(int cnt, const string* paths);

    int size() const {
        return data_size + sizeof(uint32_t) * 2;
    }

    void read(std::vector<string>& paths);
};

struct chkpt_restore_tab_t
{
    enum {
        maxBitmapSize = logrec_t::max_data_sz - 2*sizeof(PageID)
            - sizeof(uint32_t),
        // one segment for each bit in the bitmap
        maxSegments = maxBitmapSize * 8
    };

    PageID firstNotRestored;
    uint32_t bitmapSize;
    char bitmap[maxBitmapSize];

    chkpt_restore_tab_t()
        : firstNotRestored(0), bitmapSize(0)
    {}

    size_t length()
    {
        return sizeof(PageID)
            + sizeof(uint32_t)
            + bitmapSize;
    }
};

struct xct_list_t {
    struct xrec_t {
        tid_t                 tid;
    };

    // max is set to make chkpt_xct_tab_t fit in logrec_t::data_sz
    enum {     max = ((logrec_t::max_data_sz - sizeof(tid_t) -
            2 * sizeof(uint32_t)) / sizeof(xrec_t))
    };
    uint32_t            count;
    fill4              filler;
    xrec_t             xrec[max];

    NORET             xct_list_t(const xct_t* list[], int count);
    int               size() const;
};

inline int
chkpt_bf_tab_t::size() const
{
    return (char*) &brec[count] - (char*) this;
}

inline int
chkpt_xct_tab_t::size() const
{
    return (char*) &xrec[count] - (char*) this;
}

inline int
chkpt_xct_lock_t::size() const
{
    return (char*) &xrec[count] - (char*) this;
}

inline int
xct_list_t::size() const
{
    return (char*) &xrec[count] - (char*) this;
}

#endif
