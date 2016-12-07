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
		W_FATAL(eINTERNAL);
		break;
	case t_chkpt_backup_tab :
		W_FATAL(eINTERNAL);
		break;
	case t_chkpt_end :
		W_FATAL(eINTERNAL);
		break;
	case t_add_backup :
		W_FATAL(eINTERNAL);
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
		W_FATAL(eINTERNAL);
		break;
	case t_dealloc_page :
		W_FATAL(eINTERNAL);
		break;
	case t_stnode_format :
		W_FATAL(eINTERNAL);
		break;
	case t_create_store :
		W_FATAL(eINTERNAL);
		break;
	case t_append_extent :
		W_FATAL(eINTERNAL);
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
		W_FATAL(eINTERNAL);
		break;
	case t_restore_segment :
		W_FATAL(eINTERNAL);
		break;
	case t_restore_end :
		W_FATAL(eINTERNAL);
		break;
	case t_page_img_format :
		((page_img_format_log *) this)->undo(page);
		break;
	case t_page_evict :
		W_FATAL(eINTERNAL);
		break;
	case t_btree_norec_alloc :
		W_FATAL(eINTERNAL);
		break;
	case t_btree_insert :
		((btree_insert_log *) this)->undo(page);
		break;
	case t_btree_insert_nonghost :
		((btree_insert_nonghost_log *) this)->undo(page);
		break;
	case t_btree_update :
		((btree_update_log *) this)->undo(page);
		break;
	case t_btree_overwrite :
		((btree_overwrite_log *) this)->undo(page);
		break;
	case t_btree_ghost_mark :
		((btree_ghost_mark_log *) this)->undo(page);
		break;
	case t_btree_ghost_reclaim :
		W_FATAL(eINTERNAL);
		break;
	case t_btree_ghost_reserve :
		W_FATAL(eINTERNAL);
		break;
	case t_btree_foster_adopt :
		W_FATAL(eINTERNAL);
		break;
	case t_btree_split :
		W_FATAL(eINTERNAL);
		break;
	case t_btree_compress_page :
		W_FATAL(eINTERNAL);
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
