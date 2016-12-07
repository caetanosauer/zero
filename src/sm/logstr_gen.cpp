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
	case t_stnode_format :
		return "stnode_format";
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
