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

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include "alloc_page.h"
#include "btree_page_h.h"
#include "stnode_page.h"
#include "w_base.h"
#include "logrec.h"

	case t_comment :
		return "comment";
	case t_compensate :
		return "compensate";
	case t_skip :
		return "skip";
	case t_chkpt_begin :
		return "chkpt_begin";
	case t_chkpt_bf_tab :
		return "chkpt_bf_tab";
	case t_chkpt_xct_tab :
		return "chkpt_xct_tab";
	case t_chkpt_xct_lock :
		return "chkpt_xct_lock";
	case t_chkpt_restore_tab :
		return "chkpt_restore_tab";
	case t_chkpt_backup_tab :
		return "chkpt_backup_tab";
	case t_chkpt_end :
		return "chkpt_end";
	case t_add_backup :
		return "add_backup";
	case t_xct_abort :
		return "xct_abort";
	case t_xct_freeing_space :
		return "xct_freeing_space";
	case t_xct_end :
		return "xct_end";
	case t_xct_end_group :
		return "xct_end_group";
	case t_xct_latency_dump :
		return "xct_latency_dump";
	case t_alloc_page :
		return "alloc_page";
	case t_dealloc_page :
		return "dealloc_page";
	case t_create_store :
		return "create_store";
	case t_append_extent :
		return "append_extent";
	case t_loganalysis_begin :
		return "loganalysis_begin";
	case t_loganalysis_end :
		return "loganalysis_end";
	case t_redo_done :
		return "redo_done";
	case t_undo_done :
		return "undo_done";
	case t_restore_begin :
		return "restore_begin";
	case t_restore_segment :
		return "restore_segment";
	case t_restore_end :
		return "restore_end";
	case t_page_set_to_be_deleted :
		return "page_set_to_be_deleted";
	case t_page_img_format :
		return "page_img_format";
	case t_page_evict :
		return "page_evict";
	case t_btree_norec_alloc :
		return "btree_norec_alloc";
	case t_btree_insert :
		return "btree_insert";
	case t_btree_insert_nonghost :
		return "btree_insert_nonghost";
	case t_btree_update :
		return "btree_update";
	case t_btree_overwrite :
		return "btree_overwrite";
	case t_btree_ghost_mark :
		return "btree_ghost_mark";
	case t_btree_ghost_reclaim :
		return "btree_ghost_reclaim";
	case t_btree_ghost_reserve :
		return "btree_ghost_reserve";
	case t_btree_foster_adopt :
		return "btree_foster_adopt";
	case t_btree_foster_merge :
		return "btree_foster_merge";
	case t_btree_foster_rebalance :
		return "btree_foster_rebalance";
	case t_btree_foster_rebalance_norec :
		return "btree_foster_rebalance_norec";
	case t_btree_foster_deadopt :
		return "btree_foster_deadopt";
	case t_btree_split :
		return "btree_split";
	case t_btree_compress_page :
		return "btree_compress_page";
	case t_tick_sec :
		return "tick_sec";
	case t_tick_msec :
		return "tick_msec";
	case t_benchmark_start :
		return "benchmark_start";
	case t_page_write :
		return "page_write";
	case t_page_read :
		return "page_read";
