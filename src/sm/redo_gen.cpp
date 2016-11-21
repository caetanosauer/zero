	case t_comment :
		W_FATAL(eINTERNAL);
		break;
	case t_compensate :
		W_FATAL(eINTERNAL);
		break;
	case t_skip :
		W_FATAL(eINTERNAL);
		break;
	case t_chkpt_begin :
		W_FATAL(eINTERNAL);
		break;
	case t_chkpt_bf_tab :
		W_FATAL(eINTERNAL);
		break;
	case t_chkpt_xct_tab :
		W_FATAL(eINTERNAL);
		break;
	case t_chkpt_xct_lock :
		W_FATAL(eINTERNAL);
		break;
	case t_chkpt_restore_tab :
		((chkpt_restore_tab_log *) this)->redo(page);
		break;
	case t_chkpt_backup_tab :
		((chkpt_backup_tab_log *) this)->redo(page);
		break;
	case t_chkpt_end :
		W_FATAL(eINTERNAL);
		break;
	case t_add_backup :
		((add_backup_log *) this)->redo(page);
		break;
	case t_xct_abort :
		W_FATAL(eINTERNAL);
		break;
	case t_xct_freeing_space :
		W_FATAL(eINTERNAL);
		break;
	case t_xct_end :
		W_FATAL(eINTERNAL);
		break;
	case t_xct_end_group :
		W_FATAL(eINTERNAL);
		break;
	case t_xct_latency_dump :
		W_FATAL(eINTERNAL);
		break;
	case t_alloc_page :
		((alloc_page_log *) this)->redo(page);
		break;
	case t_dealloc_page :
		((dealloc_page_log *) this)->redo(page);
		break;
	case t_create_store :
		((create_store_log *) this)->redo(page);
		break;
	case t_append_extent :
		((append_extent_log *) this)->redo(page);
		break;
	case t_loganalysis_begin :
		W_FATAL(eINTERNAL);
		break;
	case t_loganalysis_end :
		W_FATAL(eINTERNAL);
		break;
	case t_redo_done :
		W_FATAL(eINTERNAL);
		break;
	case t_undo_done :
		W_FATAL(eINTERNAL);
		break;
	case t_restore_begin :
		((restore_begin_log *) this)->redo(page);
		break;
	case t_restore_segment :
		((restore_segment_log *) this)->redo(page);
		break;
	case t_restore_end :
		((restore_end_log *) this)->redo(page);
		break;
	case t_page_set_to_be_deleted :
		((page_set_to_be_deleted_log *) this)->redo(page);
		break;
	case t_page_img_format :
		((page_img_format_log *) this)->redo(page);
		break;
	case t_page_evict :
		((page_evict_log *) this)->redo(page);
		break;
	case t_btree_norec_alloc :
		((btree_norec_alloc_log *) this)->redo(page);
		break;
	case t_btree_insert :
		((btree_insert_log *) this)->redo(page);
		break;
	case t_btree_insert_nonghost :
		((btree_insert_nonghost_log *) this)->redo(page);
		break;
	case t_btree_update :
		((btree_update_log *) this)->redo(page);
		break;
	case t_btree_overwrite :
		((btree_overwrite_log *) this)->redo(page);
		break;
	case t_btree_ghost_mark :
		((btree_ghost_mark_log *) this)->redo(page);
		break;
	case t_btree_ghost_reclaim :
		((btree_ghost_reclaim_log *) this)->redo(page);
		break;
	case t_btree_ghost_reserve :
		((btree_ghost_reserve_log *) this)->redo(page);
		break;
	case t_btree_foster_adopt :
		((btree_foster_adopt_log *) this)->redo(page);
		break;
	case t_btree_foster_merge :
		((btree_foster_merge_log *) this)->redo(page);
		break;
	case t_btree_foster_rebalance :
		((btree_foster_rebalance_log *) this)->redo(page);
		break;
	case t_btree_foster_rebalance_norec :
		((btree_foster_rebalance_norec_log *) this)->redo(page);
		break;
	case t_btree_foster_deadopt :
		((btree_foster_deadopt_log *) this)->redo(page);
		break;
	case t_btree_split :
		((btree_split_log *) this)->redo(page);
		break;
	case t_btree_compress_page :
		((btree_compress_page_log *) this)->redo(page);
		break;
	case t_tick_sec :
		W_FATAL(eINTERNAL);
		break;
	case t_tick_msec :
		W_FATAL(eINTERNAL);
		break;
	case t_benchmark_start :
		W_FATAL(eINTERNAL);
		break;
	case t_page_write :
		W_FATAL(eINTERNAL);
		break;
	case t_page_read :
		W_FATAL(eINTERNAL);
		break;
