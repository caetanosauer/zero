/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

/*<std-header orig-src='shore'>

 $Id: smstats.cpp,v 1.22 2010/11/08 15:07:06 nhall Exp $

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

#include "sm_base.h"
// smstats_info_t is the collected stats from various
// sm parts.  Each part is separately-generate from .dat files.
#include "smstats.h"

// the strings:
const char* get_stat_name(sm_stat_id s)
{
    switch (s) {
        case sm_stat_id::rwlock_r_wait: return "rwlock_r_wait";
        case sm_stat_id::rwlock_w_wait: return "rwlock_w_wait";
        case sm_stat_id::needs_latch_condl: return "needs_latch_condl";
        case sm_stat_id::needs_latch_uncondl: return "needs_latch_uncondl";
        case sm_stat_id::latch_condl_nowait: return "latch_condl_nowait";
        case sm_stat_id::latch_uncondl_nowait: return "latch_uncondl_nowait";
        case sm_stat_id::bf_one_page_write: return "bf_one_page_write";
        case sm_stat_id::bf_two_page_write: return "bf_two_page_write";
        case sm_stat_id::bf_three_page_write: return "bf_three_page_write";
        case sm_stat_id::bf_four_page_write: return "bf_four_page_write";
        case sm_stat_id::bf_five_page_write: return "bf_five_page_write";
        case sm_stat_id::bf_six_page_write: return "bf_six_page_write";
        case sm_stat_id::bf_seven_page_write: return "bf_seven_page_write";
        case sm_stat_id::bf_eight_page_write: return "bf_eight_page_write";
        case sm_stat_id::bf_more_page_write: return "bf_more_page_write";
        case sm_stat_id::cleaned_pages: return "cleaned_pages";
        case sm_stat_id::cleaner_time_cpu: return "cleaner_time_cpu";
        case sm_stat_id::cleaner_time_io: return "cleaner_time_io";
        case sm_stat_id::cleaner_time_copy: return "cleaner_time_copy";
        case sm_stat_id::cleaner_single_page_read: return "cleaner_single_page_read";
        case sm_stat_id::bf_already_evicted: return "bf_already_evicted";
        case sm_stat_id::bf_eviction_attempts: return "bf_eviction_attempts";
        case sm_stat_id::bf_dirty_page_cleaned: return "bf_dirty_page_cleaned";
        case sm_stat_id::bf_flushed_OHD_page: return "bf_flushed_OHD_page";
        case sm_stat_id::bf_kick_full: return "bf_kick_full";
        case sm_stat_id::bf_kick_replacement: return "bf_kick_replacement";
        case sm_stat_id::bf_kick_threshold: return "bf_kick_threshold";
        case sm_stat_id::bf_sweep_page_hot_skipped: return "bf_sweep_page_hot_skipped";
        case sm_stat_id::bf_discarded_hot: return "bf_discarded_hot";
        case sm_stat_id::bf_log_flush_all: return "bf_log_flush_all";
        case sm_stat_id::bf_log_flush_lsn: return "bf_log_flush_lsn";
        case sm_stat_id::bf_write_out: return "bf_write_out";
        case sm_stat_id::bf_sleep_await_clean: return "bf_sleep_await_clean";
        case sm_stat_id::bf_invoked_spr: return "bf_invoked_spr";
        case sm_stat_id::bf_fg_scan_cnt: return "bf_fg_scan_cnt";
        case sm_stat_id::bf_unfix_cleaned: return "bf_unfix_cleaned";
        case sm_stat_id::bf_evict: return "bf_evict";
        case sm_stat_id::bf_evict_duration: return "bf_evict_duration";
        case sm_stat_id::rwlock_r_waits: return "rwlock_r_waits";
        case sm_stat_id::rwlock_w_waits: return "rwlock_w_waits";
        case sm_stat_id::need_latch_condl: return "need_latch_condl";
        case sm_stat_id::latch_condl_nowaits: return "latch_condl_nowaits";
        case sm_stat_id::need_latch_uncondl: return "need_latch_uncondl";
        case sm_stat_id::latch_uncondl_nowaits: return "latch_uncondl_nowaits";
        case sm_stat_id::latch_uncondl_waits: return "latch_uncondl_waits";
        case sm_stat_id::btree_latch_wait: return "btree_latch_wait";
        case sm_stat_id::io_latch_wait: return "io_latch_wait";
        case sm_stat_id::bf_look_cnt: return "bf_look_cnt";
        case sm_stat_id::bf_grab_latch_failed: return "bf_grab_latch_failed";
        case sm_stat_id::bf_hit_cnt: return "bf_hit_cnt";
        case sm_stat_id::bf_hit_wait: return "bf_hit_wait";
        case sm_stat_id::bf_hit_wait_any_p: return "bf_hit_wait_any_p";
        case sm_stat_id::bf_hit_wait_btree_p: return "bf_hit_wait_btree_p";
        case sm_stat_id::bf_hit_wait_alloc_p: return "bf_hit_wait_alloc_p";
        case sm_stat_id::bf_hit_wait_stnode_p: return "bf_hit_wait_stnode_p";
        case sm_stat_id::bf_hit_wait_scan: return "bf_hit_wait_scan";
        case sm_stat_id::bf_replace_out: return "bf_replace_out";
        case sm_stat_id::bf_replaced_dirty: return "bf_replaced_dirty";
        case sm_stat_id::bf_replaced_clean: return "bf_replaced_clean";
        case sm_stat_id::bf_replaced_unused: return "bf_replaced_unused";
        case sm_stat_id::bf_awaited_cleaner: return "bf_awaited_cleaner";
        case sm_stat_id::bf_no_transit_bucket: return "bf_no_transit_bucket";
        case sm_stat_id::bf_prefetch_requests: return "bf_prefetch_requests";
        case sm_stat_id::bf_prefetches: return "bf_prefetches";
        case sm_stat_id::bf_upgrade_latch_unconditional: return "bf_upgrade_latch_unconditional";
        case sm_stat_id::bf_upgrade_latch_race: return "bf_upgrade_latch_race";
        case sm_stat_id::bf_upgrade_latch_changed: return "bf_upgrade_latch_changed";
        case sm_stat_id::restart_repair_rec_lsn: return "restart_repair_rec_lsn";
        case sm_stat_id::vol_reads: return "vol_reads";
        case sm_stat_id::vol_writes: return "vol_writes";
        case sm_stat_id::vol_blks_written: return "vol_blks_written";
        case sm_stat_id::need_vol_lock_r: return "need_vol_lock_r";
        case sm_stat_id::need_vol_lock_w: return "need_vol_lock_w";
        case sm_stat_id::nowait_vol_lock_r: return "nowait_vol_lock_r";
        case sm_stat_id::nowait_vol_lock_w: return "nowait_vol_lock_w";
        case sm_stat_id::await_vol_lock_r: return "await_vol_lock_r";
        case sm_stat_id::await_vol_lock_w: return "await_vol_lock_w";
        case sm_stat_id::io_m_lsearch: return "io_m_lsearch";
        case sm_stat_id::vol_cache_primes: return "vol_cache_primes";
        case sm_stat_id::vol_cache_prime_fix: return "vol_cache_prime_fix";
        case sm_stat_id::vol_cache_clears: return "vol_cache_clears";
        case sm_stat_id::vol_lock_noalloc: return "vol_lock_noalloc";
        case sm_stat_id::log_dup_sync_cnt: return "log_dup_sync_cnt";
        case sm_stat_id::log_daemon_wait: return "log_daemon_wait";
        case sm_stat_id::log_daemon_work: return "log_daemon_work";
        case sm_stat_id::log_fsync_cnt: return "log_fsync_cnt";
        case sm_stat_id::log_chkpt_cnt: return "log_chkpt_cnt";
        case sm_stat_id::log_chkpt_wake: return "log_chkpt_wake";
        case sm_stat_id::log_fetches: return "log_fetches";
        case sm_stat_id::log_buffer_hit: return "log_buffer_hit";
        case sm_stat_id::log_inserts: return "log_inserts";
        case sm_stat_id::log_full: return "log_full";
        case sm_stat_id::log_full_old_xct: return "log_full_old_xct";
        case sm_stat_id::log_full_old_page: return "log_full_old_page";
        case sm_stat_id::log_full_wait: return "log_full_wait";
        case sm_stat_id::log_full_force: return "log_full_force";
        case sm_stat_id::log_full_giveup: return "log_full_giveup";
        case sm_stat_id::log_file_wrap: return "log_file_wrap";
        case sm_stat_id::log_bytes_generated: return "log_bytes_generated";
        case sm_stat_id::log_bytes_written: return "log_bytes_written";
        case sm_stat_id::log_bytes_rewritten: return "log_bytes_rewritten";
        case sm_stat_id::log_bytes_generated_rb: return "log_bytes_generated_rb";
        case sm_stat_id::log_bytes_rbfwd_ratio: return "log_bytes_rbfwd_ratio";
        case sm_stat_id::log_flush_wait: return "log_flush_wait";
        case sm_stat_id::log_short_flush: return "log_short_flush";
        case sm_stat_id::log_long_flush: return "log_long_flush";
        case sm_stat_id::lock_deadlock_cnt: return "lock_deadlock_cnt";
        case sm_stat_id::lock_false_deadlock_cnt: return "lock_false_deadlock_cnt";
        case sm_stat_id::lock_dld_call_cnt: return "lock_dld_call_cnt";
        case sm_stat_id::lock_dld_first_call_cnt: return "lock_dld_first_call_cnt";
        case sm_stat_id::lock_dld_false_victim_cnt: return "lock_dld_false_victim_cnt";
        case sm_stat_id::lock_dld_victim_self_cnt: return "lock_dld_victim_self_cnt";
        case sm_stat_id::lock_dld_victim_other_cnt: return "lock_dld_victim_other_cnt";
        case sm_stat_id::nonunique_fingerprints: return "nonunique_fingerprints";
        case sm_stat_id::unique_fingerprints: return "unique_fingerprints";
        case sm_stat_id::rec_pin_cnt: return "rec_pin_cnt";
        case sm_stat_id::rec_unpin_cnt: return "rec_unpin_cnt";
        case sm_stat_id::rec_repin_cvt: return "rec_repin_cvt";
        case sm_stat_id::bt_find_cnt: return "bt_find_cnt";
        case sm_stat_id::bt_insert_cnt: return "bt_insert_cnt";
        case sm_stat_id::bt_remove_cnt: return "bt_remove_cnt";
        case sm_stat_id::bt_traverse_cnt: return "bt_traverse_cnt";
        case sm_stat_id::bt_partial_traverse_cnt: return "bt_partial_traverse_cnt";
        case sm_stat_id::bt_restart_traverse_cnt: return "bt_restart_traverse_cnt";
        case sm_stat_id::bt_posc: return "bt_posc";
        case sm_stat_id::bt_scan_cnt: return "bt_scan_cnt";
        case sm_stat_id::bt_splits: return "bt_splits";
        case sm_stat_id::bt_cuts: return "bt_cuts";
        case sm_stat_id::bt_grows: return "bt_grows";
        case sm_stat_id::bt_shrinks: return "bt_shrinks";
        case sm_stat_id::bt_links: return "bt_links";
        case sm_stat_id::bt_upgrade_fail_retry: return "bt_upgrade_fail_retry";
        case sm_stat_id::bt_clr_smo_traverse: return "bt_clr_smo_traverse";
        case sm_stat_id::bt_pcompress: return "bt_pcompress";
        case sm_stat_id::bt_plmax: return "bt_plmax";
        case sm_stat_id::any_p_fix_cnt: return "any_p_fix_cnt";
        case sm_stat_id::alloc_p_fix_cnt: return "alloc_p_fix_cnt";
        case sm_stat_id::stnode_p_fix_cnt: return "stnode_p_fix_cnt";
        case sm_stat_id::page_fix_cnt: return "page_fix_cnt";
        case sm_stat_id::bf_fix_cnt: return "bf_fix_cnt";
        case sm_stat_id::bf_refix_cnt: return "bf_refix_cnt";
        case sm_stat_id::bf_unfix_cnt: return "bf_unfix_cnt";
        case sm_stat_id::vol_check_owner_fix: return "vol_check_owner_fix";
        case sm_stat_id::page_alloc_cnt: return "page_alloc_cnt";
        case sm_stat_id::page_dealloc_cnt: return "page_dealloc_cnt";
        case sm_stat_id::ext_lookup_hits: return "ext_lookup_hits";
        case sm_stat_id::ext_lookup_misses: return "ext_lookup_misses";
        case sm_stat_id::alloc_page_in_ext: return "alloc_page_in_ext";
        case sm_stat_id::vol_free_page: return "vol_free_page";
        case sm_stat_id::vol_next_page: return "vol_next_page";
        case sm_stat_id::vol_find_free_exts: return "vol_find_free_exts";
        case sm_stat_id::xct_log_flush: return "xct_log_flush";
        case sm_stat_id::begin_xct_cnt: return "begin_xct_cnt";
        case sm_stat_id::commit_xct_cnt: return "commit_xct_cnt";
        case sm_stat_id::abort_xct_cnt: return "abort_xct_cnt";
        case sm_stat_id::log_warn_abort_cnt: return "log_warn_abort_cnt";
        case sm_stat_id::prepare_xct_cnt: return "prepare_xct_cnt";
        case sm_stat_id::rollback_savept_cnt: return "rollback_savept_cnt";
        case sm_stat_id::internal_rollback_cnt: return "internal_rollback_cnt";
        case sm_stat_id::s_prepared: return "s_prepared";
        case sm_stat_id::sdesc_cache_miss: return "sdesc_cache_miss";
        case sm_stat_id::mpl_attach_cnt: return "mpl_attach_cnt";
        case sm_stat_id::anchors: return "anchors";
        case sm_stat_id::compensate_in_log: return "compensate_in_log";
        case sm_stat_id::compensate_in_xct: return "compensate_in_xct";
        case sm_stat_id::compensate_records: return "compensate_records";
        case sm_stat_id::compensate_skipped: return "compensate_skipped";
        case sm_stat_id::log_switches: return "log_switches";
        case sm_stat_id::get_logbuf: return "get_logbuf";
        case sm_stat_id::await_1thread_xct: return "await_1thread_xct";
        case sm_stat_id::lock_query_cnt: return "lock_query_cnt";
        case sm_stat_id::unlock_request_cnt: return "unlock_request_cnt";
        case sm_stat_id::lock_request_cnt: return "lock_request_cnt";
        case sm_stat_id::lock_acquire_cnt: return "lock_acquire_cnt";
        case sm_stat_id::lock_head_t_cnt: return "lock_head_t_cnt";
        case sm_stat_id::lock_await_alt_cnt: return "lock_await_alt_cnt";
        case sm_stat_id::lock_extraneous_req_cnt: return "lock_extraneous_req_cnt";
        case sm_stat_id::lock_conversion_cnt: return "lock_conversion_cnt";
        case sm_stat_id::lk_vol_acq: return "lk_vol_acq";
        case sm_stat_id::lk_store_acq: return "lk_store_acq";
        case sm_stat_id::lk_key_acq: return "lk_key_acq";
        case sm_stat_id::lock_wait_cnt: return "lock_wait_cnt";
        case sm_stat_id::lock_block_cnt: return "lock_block_cnt";
        case sm_stat_id::lk_vol_wait: return "lk_vol_wait";
        case sm_stat_id::lk_store_wait: return "lk_store_wait";
        case sm_stat_id::lk_key_wait: return "lk_key_wait";
        case sm_stat_id::bf_fix_nonroot_count: return "bf_fix_nonroot_count";
        case sm_stat_id::bf_fix_nonroot_swizzled_count: return "bf_fix_nonroot_swizzled_count";
        case sm_stat_id::bf_fix_nonroot_miss_count: return "bf_fix_nonroot_miss_count";
        case sm_stat_id::bf_fix_adjusted_parent: return "bf_fix_adjusted_parent";
        case sm_stat_id::bf_batch_wait_time: return "bf_batch_wait_time";
        case sm_stat_id::restart_log_analysis_time: return "restart_log_analysis_time";
        case sm_stat_id::restart_redo_time: return "restart_redo_time";
        case sm_stat_id::restart_dirty_pages: return "restart_dirty_pages";
        case sm_stat_id::restore_sched_seq: return "restore_sched_seq";
        case sm_stat_id::restore_sched_queued: return "restore_sched_queued";
        case sm_stat_id::restore_sched_random: return "restore_sched_random";
        case sm_stat_id::restore_time_read: return "restore_time_read";
        case sm_stat_id::restore_time_replay: return "restore_time_replay";
        case sm_stat_id::restore_time_openscan: return "restore_time_openscan";
        case sm_stat_id::restore_time_write: return "restore_time_write";
        case sm_stat_id::restore_skipped_segs: return "restore_skipped_segs";
        case sm_stat_id::restore_backup_reads: return "restore_backup_reads";
        case sm_stat_id::restore_async_write_time: return "restore_async_write_time";
        case sm_stat_id::restore_log_volume: return "restore_log_volume";
        case sm_stat_id::restore_multiple_segments: return "restore_multiple_segments";
        case sm_stat_id::restore_segment_count: return "restore_segment_count";
        case sm_stat_id::restore_invocations: return "restore_invocations";
        case sm_stat_id::restore_preempt_queue: return "restore_preempt_queue";
        case sm_stat_id::restore_preempt_bitmap: return "restore_preempt_bitmap";
        case sm_stat_id::la_log_slow: return "la_log_slow";
        case sm_stat_id::la_activations: return "la_activations";
        case sm_stat_id::la_read_volume: return "la_read_volume";
        case sm_stat_id::la_read_count: return "la_read_count";
        case sm_stat_id::la_open_count: return "la_open_count";
        case sm_stat_id::la_read_time: return "la_read_time";
        case sm_stat_id::la_block_writes: return "la_block_writes";
        case sm_stat_id::la_merge_heap_time: return "la_merge_heap_time";
        case sm_stat_id::la_index_probes: return "la_index_probes";
        case sm_stat_id::la_img_compressed_bytes: return "la_img_compressed_bytes";
        case sm_stat_id::log_img_format_bytes: return "log_img_format_bytes";
        case sm_stat_id::la_skipped_bytes: return "la_skipped_bytes";
        case sm_stat_id::la_img_trimmed: return "la_img_trimmed";
        case sm_stat_id::backup_not_prefetched: return "backup_not_prefetched";
        case sm_stat_id::backup_evict_segment: return "backup_evict_segment";
        case sm_stat_id::backup_eviction_stuck: return "backup_eviction_stuck";
        case sm_stat_id::la_wasted_read: return "la_wasted_read";
        case sm_stat_id::la_avoided_probes: return "la_avoided_probes";
    }
    return "UNKNOWN_STAT";
}

const char* get_stat_expl(sm_stat_id s)
{
    switch (s) {
        case sm_stat_id::rwlock_r_wait: return "Number waits for read lock on srwlock";
        case sm_stat_id::rwlock_w_wait: return "Number waits for write lock on srwlock";
        case sm_stat_id::needs_latch_condl: return "Conditional latch requests";
        case sm_stat_id::needs_latch_uncondl: return "Unconditional latch requests";
        case sm_stat_id::latch_condl_nowait: return "Conditional requests satisfied immediately";
        case sm_stat_id::latch_uncondl_nowait: return "Unconditional requests satisfied immediately";
        case sm_stat_id::bf_one_page_write: return "Single page written to volume";
        case sm_stat_id::bf_two_page_write: return "Two-page writes to volume";
        case sm_stat_id::bf_three_page_write: return "Three-page writes to volume";
        case sm_stat_id::bf_four_page_write: return "Four-page writes to volume";
        case sm_stat_id::bf_five_page_write: return "Five-page writes to volume";
        case sm_stat_id::bf_six_page_write: return "Six-page writes to volume";
        case sm_stat_id::bf_seven_page_write: return "Seven-page writes to volume";
        case sm_stat_id::bf_eight_page_write: return "Eight-page writes to volume";
        case sm_stat_id::bf_more_page_write: return "Over-eight-page writes to volume";
        case sm_stat_id::cleaned_pages: return "Number of pages cleaned by bf_cleaner thread";
        case sm_stat_id::cleaner_time_cpu: return "Time spent manipulating cleaner candidate lists";
        case sm_stat_id::cleaner_time_io: return "Time spent flushing the cleaner workspace";
        case sm_stat_id::cleaner_time_copy: return "Time spent latching and copy page images into workspace";
        case sm_stat_id::cleaner_single_page_read: return "number of single-page reads performed by decoupled cleaner";
        case sm_stat_id::bf_already_evicted: return "Could not find page to copy for flushing (evicted)";
        case sm_stat_id::bf_eviction_attempts: return "Total number of frames inspected for eviction";
        case sm_stat_id::bf_dirty_page_cleaned: return "Found page already cleaned (hot)";
        case sm_stat_id::bf_flushed_OHD_page: return "Non-cleaner thread had to flush an old-hot-dirty page synchronously";
        case sm_stat_id::bf_kick_full: return "Kicks because pool is full of dirty pages";
        case sm_stat_id::bf_kick_replacement: return "Kicks because doing page replacement";
        case sm_stat_id::bf_kick_threshold: return "Kicks because dirty page threshold met";
        case sm_stat_id::bf_sweep_page_hot_skipped: return "Page swept was not flushed because it was hot ";
        case sm_stat_id::bf_discarded_hot: return "Discarded a page from the bp when someone was waiting to latch it";
        case sm_stat_id::bf_log_flush_all: return "Number of whole-log flushes by bf_cleaner";
        case sm_stat_id::bf_log_flush_lsn: return "Number of partial log flushes by bf_cleaner";
        case sm_stat_id::bf_write_out: return "Pages written out in background or forced";
        case sm_stat_id::bf_sleep_await_clean: return "Times slept awaiting cleaner to clean a page for fix()";
        case sm_stat_id::bf_invoked_spr: return "Number of single-page recovery invocations due to stale LSN";
        case sm_stat_id::bf_fg_scan_cnt: return "Foreground scans of buffer pool";
        case sm_stat_id::bf_unfix_cleaned: return "Unfix-clean cleaned a page that had a rec_lsn";
        case sm_stat_id::bf_evict: return "Evicted page from buffer pool";
        case sm_stat_id::bf_evict_duration: return "Duration of eviction calls in nanosecond";
        case sm_stat_id::rwlock_r_waits: return "Number of waits for read lock on srwlock";
        case sm_stat_id::rwlock_w_waits: return "Number of waits for write lock on srwlock";
        case sm_stat_id::need_latch_condl: return "Conditional latch requests ";
        case sm_stat_id::latch_condl_nowaits: return "Conditional latch requests immediately granted ";
        case sm_stat_id::need_latch_uncondl: return "Unconditional latch requests ";
        case sm_stat_id::latch_uncondl_nowaits: return "Uncondl latch requests immediately granted ";
        case sm_stat_id::latch_uncondl_waits: return "Uncondl latch requests not immediately granted ";
        case sm_stat_id::btree_latch_wait: return "Waited on btree store latch (not in buffer pool)";
        case sm_stat_id::io_latch_wait: return "Waited on io store latch (not in buffer pool)";
        case sm_stat_id::bf_look_cnt: return "Calls to find/grab";
        case sm_stat_id::bf_grab_latch_failed: return "Page found but could not acquire latch ";
        case sm_stat_id::bf_hit_cnt: return "Found page in buffer pool in find/grab";
        case sm_stat_id::bf_hit_wait: return "Found page in buffer pool but awaited latch";
        case sm_stat_id::bf_hit_wait_any_p: return "Found page in b pool but awaited latch";
        case sm_stat_id::bf_hit_wait_btree_p: return "Found page in b pool but awaited latch";
        case sm_stat_id::bf_hit_wait_alloc_p: return "Found page in b pool but awaited latch";
        case sm_stat_id::bf_hit_wait_stnode_p: return "Found page in b pool but awaited latch";
        case sm_stat_id::bf_hit_wait_scan: return "Found any_p page in b pool but awaited latch in scan";
        case sm_stat_id::bf_replace_out: return "Pages written out to free a frame for fixing";
        case sm_stat_id::bf_replaced_dirty: return "Victim for page replacement is dirty";
        case sm_stat_id::bf_replaced_clean: return "Victim for page replacement is clean";
        case sm_stat_id::bf_replaced_unused: return "Victim for page replacement is unused frame";
        case sm_stat_id::bf_awaited_cleaner: return "Had to wait for page cleaner to be done with page";
        case sm_stat_id::bf_no_transit_bucket: return "Wanted in-transit-out bucket was full ";
        case sm_stat_id::bf_prefetch_requests: return "Requests to prefetch a page ";
        case sm_stat_id::bf_prefetches: return "Prefetches performed";
        case sm_stat_id::bf_upgrade_latch_unconditional: return "Unconditional latch upgrade";
        case sm_stat_id::bf_upgrade_latch_race: return "Dropped and reqacquired latch to upgrade";
        case sm_stat_id::bf_upgrade_latch_changed: return "A page changed during a latch upgrade race";
        case sm_stat_id::restart_repair_rec_lsn: return "Cleared rec_lsn on a page dirtied by unlogged changes";
        case sm_stat_id::vol_reads: return "Data volume read requests (from disk)";
        case sm_stat_id::vol_writes: return "Data volume write requests (to disk)";
        case sm_stat_id::vol_blks_written: return "Data volume pages written (to disk)";
        case sm_stat_id::need_vol_lock_r: return "Times requested vol lock for read";
        case sm_stat_id::need_vol_lock_w: return "Times requested vol lock for write";
        case sm_stat_id::nowait_vol_lock_r: return "Times vol read lock acquired immediately";
        case sm_stat_id::nowait_vol_lock_w: return "Times vol write lock acquired immediately";
        case sm_stat_id::await_vol_lock_r: return "Requests not acquired immediately";
        case sm_stat_id::await_vol_lock_w: return "Requests not acquired immediately";
        case sm_stat_id::io_m_lsearch: return "Times a linear search was started in io manager";
        case sm_stat_id::vol_cache_primes: return "Caches primed";
        case sm_stat_id::vol_cache_prime_fix: return "Fixes due to cache primes";
        case sm_stat_id::vol_cache_clears: return "Caches cleared (dismounts)";
        case sm_stat_id::vol_lock_noalloc: return "Failed to allocate from an extent due to lock contention";
        case sm_stat_id::log_dup_sync_cnt: return "Times the log was flushed superfluously";
        case sm_stat_id::log_daemon_wait: return "Times the log daemon waited for a kick";
        case sm_stat_id::log_daemon_work: return "Times the log daemon flushed something";
        case sm_stat_id::log_fsync_cnt: return "Times the fsync system call was used";
        case sm_stat_id::log_chkpt_cnt: return "Checkpoints taken";
        case sm_stat_id::log_chkpt_wake: return "Checkpoints requested by kicking the chkpt thread";
        case sm_stat_id::log_fetches: return "Log records fetched from log (read)";
        case sm_stat_id::log_buffer_hit: return "Log fetches that were served from in-memory fetch buffers";
        case sm_stat_id::log_inserts: return "Log records inserted into log (written)";
        case sm_stat_id::log_full: return "A transaction encountered log full";
        case sm_stat_id::log_full_old_xct: return "An old transaction had to abort";
        case sm_stat_id::log_full_old_page: return "A transaction had to abort due to holding a dirty old page";
        case sm_stat_id::log_full_wait: return "A log full was resolved by waiting for space";
        case sm_stat_id::log_full_force: return "A log full was resolved by forcing the buffer pool";
        case sm_stat_id::log_full_giveup: return "A transaction aborted because neither waiting nor forcing helped";
        case sm_stat_id::log_file_wrap: return "Log file numbers wrapped around";
        case sm_stat_id::log_bytes_generated: return "Bytes of log records inserted ";
        case sm_stat_id::log_bytes_written: return "Bytes written to log including skip and padding";
        case sm_stat_id::log_bytes_rewritten: return "Bytes written minus generated    ";
        case sm_stat_id::log_bytes_generated_rb: return "Bytes of log records inserted during rollback";
        case sm_stat_id::log_bytes_rbfwd_ratio: return "Ratio of rollback: forward log bytes inserted";
        case sm_stat_id::log_flush_wait: return "Flushes awaited log flush daemon";
        case sm_stat_id::log_short_flush: return "Log flushes <= 1 block";
        case sm_stat_id::log_long_flush: return "Log flushes > 1 block";
        case sm_stat_id::lock_deadlock_cnt: return "Deadlocks detected";
        case sm_stat_id::lock_false_deadlock_cnt: return "False positive deadlocks";
        case sm_stat_id::lock_dld_call_cnt: return "Deadlock detector total calls";
        case sm_stat_id::lock_dld_first_call_cnt: return "Deadlock detector first called for one lock";
        case sm_stat_id::lock_dld_false_victim_cnt: return "Deadlock detector victim not blocked";
        case sm_stat_id::lock_dld_victim_self_cnt: return "Deadlock detector picked self as victim ";
        case sm_stat_id::lock_dld_victim_other_cnt: return "Deadlock detector picked other as victim ";
        case sm_stat_id::nonunique_fingerprints: return "Smthreads created a non-unique fingerprint";
        case sm_stat_id::unique_fingerprints: return "Smthreads created a unique fingerprint";
        case sm_stat_id::rec_pin_cnt: return "Times records were pinned in the buffer pool";
        case sm_stat_id::rec_unpin_cnt: return "Times records were unpinned";
        case sm_stat_id::rec_repin_cvt: return "Converted latch-lock to lock-lock deadlock";
        case sm_stat_id::bt_find_cnt: return "Btree lookups (find_assoc())";
        case sm_stat_id::bt_insert_cnt: return "Btree inserts (create_assoc())";
        case sm_stat_id::bt_remove_cnt: return "Btree removes (destroy_assoc())";
        case sm_stat_id::bt_traverse_cnt: return "Btree traversals";
        case sm_stat_id::bt_partial_traverse_cnt: return "Btree traversals starting below root";
        case sm_stat_id::bt_restart_traverse_cnt: return "Restarted traversals";
        case sm_stat_id::bt_posc: return "POSCs established";
        case sm_stat_id::bt_scan_cnt: return "Btree scans started";
        case sm_stat_id::bt_splits: return "Btree pages split (interior and leaf)";
        case sm_stat_id::bt_cuts: return "Btree pages removed (interior and leaf)";
        case sm_stat_id::bt_grows: return "Btree grew a level";
        case sm_stat_id::bt_shrinks: return "Btree shrunk a level";
        case sm_stat_id::bt_links: return "Btree links followed";
        case sm_stat_id::bt_upgrade_fail_retry: return "Failure to upgrade a latch forced a retry";
        case sm_stat_id::bt_clr_smo_traverse: return "Cleared SMO bits on traverse";
        case sm_stat_id::bt_pcompress: return "Prefixes compressed";
        case sm_stat_id::bt_plmax: return "Maximum prefix levels encountered";
        case sm_stat_id::any_p_fix_cnt: return "Fix method called for unknown type";
        case sm_stat_id::alloc_p_fix_cnt: return "Extlink_p fix method called";
        case sm_stat_id::stnode_p_fix_cnt: return "Stnode_p fix method called";
        case sm_stat_id::page_fix_cnt: return "Times fixable_page_h::_fix was called (even if page already fixed)";
        case sm_stat_id::bf_fix_cnt: return "Times bp fix called  (conditional or unconditional)";
        case sm_stat_id::bf_refix_cnt: return "Times pages were refixedn in bp (cheaper than fix)";
        case sm_stat_id::bf_unfix_cnt: return "Times pages were unfixed in bp";
        case sm_stat_id::vol_check_owner_fix: return "Fixes to check page allocation-to-store status";
        case sm_stat_id::page_alloc_cnt: return "Pages allocated";
        case sm_stat_id::page_dealloc_cnt: return "Pages deallocated";
        case sm_stat_id::ext_lookup_hits: return "Hits in extent lookups in cache ";
        case sm_stat_id::ext_lookup_misses: return "Misses in extent lookups in cache ";
        case sm_stat_id::alloc_page_in_ext: return "Requests to allocate a page in a given extent";
        case sm_stat_id::vol_free_page: return "Extents fixed to free a page ";
        case sm_stat_id::vol_next_page: return "Next-page requests (might fix more than one ext map page)";
        case sm_stat_id::vol_find_free_exts: return "Free extents requested";
        case sm_stat_id::xct_log_flush: return "Log flushes by xct for commit/prepare";
        case sm_stat_id::begin_xct_cnt: return "Transactions started";
        case sm_stat_id::commit_xct_cnt: return "Transactions committed";
        case sm_stat_id::abort_xct_cnt: return "Transactions aborted";
        case sm_stat_id::log_warn_abort_cnt: return "Transactions aborted due to log space warning";
        case sm_stat_id::prepare_xct_cnt: return "Transactions prepared";
        case sm_stat_id::rollback_savept_cnt: return "Rollbacks to savepoints (not incl aborts)";
        case sm_stat_id::internal_rollback_cnt: return "Internal partial rollbacks ";
        case sm_stat_id::s_prepared: return "Externally coordinated prepares";
        case sm_stat_id::sdesc_cache_miss: return "Times sdesc_cache missed altogether";
        case sm_stat_id::mpl_attach_cnt: return "Times a thread was not the only one attaching to a transaction";
        case sm_stat_id::anchors: return "Log Anchors grabbed";
        case sm_stat_id::compensate_in_log: return "Compensations written in log buffer";
        case sm_stat_id::compensate_in_xct: return "Compensations written in xct log buffer";
        case sm_stat_id::compensate_records: return "Compensations written as own log record ";
        case sm_stat_id::compensate_skipped: return "Compensations would be a no-op";
        case sm_stat_id::log_switches: return "Times log turned off";
        case sm_stat_id::get_logbuf: return "Times acquired log buf for xct";
        case sm_stat_id::await_1thread_xct: return "Times blocked on 1thread mutex for xct (mcs_lock only)";
        case sm_stat_id::lock_query_cnt: return "High-level query for lock information";
        case sm_stat_id::unlock_request_cnt: return "High-level unlock requests";
        case sm_stat_id::lock_request_cnt: return "High-level lock requests";
        case sm_stat_id::lock_acquire_cnt: return "Acquires to satisfy high-level requests";
        case sm_stat_id::lock_head_t_cnt: return "Locks heads put in table for chains of requests";
        case sm_stat_id::lock_await_alt_cnt: return "Transaction had a waiting thread in the lock manager and had to wait on alternate resource";
        case sm_stat_id::lock_extraneous_req_cnt: return "Extraneous requests (already granted)";
        case sm_stat_id::lock_conversion_cnt: return "Requests requiring conversion";
        case sm_stat_id::lk_vol_acq: return "Volume locks acquired";
        case sm_stat_id::lk_store_acq: return "Store locks acquired";
        case sm_stat_id::lk_key_acq: return "Key locks acquired";
        case sm_stat_id::lock_wait_cnt: return "Lock acquires that waited in smthread_block";
        case sm_stat_id::lock_block_cnt: return "Times lock acquire called smthread_block";
        case sm_stat_id::lk_vol_wait: return "Volume locks waited";
        case sm_stat_id::lk_store_wait: return "Store locks waited";
        case sm_stat_id::lk_key_wait: return "Key locks waited";
        case sm_stat_id::bf_fix_nonroot_count: return "Fix a non-root page";
        case sm_stat_id::bf_fix_nonroot_swizzled_count: return "Fix a non-root page, which is already swizzled";
        case sm_stat_id::bf_fix_nonroot_miss_count: return "Cache miss when fixing a non-root page";
        case sm_stat_id::bf_fix_adjusted_parent: return "Parent pointer adjusted in hash table while performing a fix";
        case sm_stat_id::bf_batch_wait_time: return "Time spent waiting for batch warmup when ficing pages (usec; nodb mode only)";
        case sm_stat_id::restart_log_analysis_time: return "Time spend with log analysis (usec)";
        case sm_stat_id::restart_redo_time: return "Time spend with non-concurrent REDO (usec)";
        case sm_stat_id::restart_dirty_pages: return "Number of dirty pages computed in restart log analysis";
        case sm_stat_id::restore_sched_seq: return "Restore scheduled a page in single-pass restore";
        case sm_stat_id::restore_sched_queued: return "Restore scheduled a page which was queued (on-demand)";
        case sm_stat_id::restore_sched_random: return "Restore scheduled a page at random";
        case sm_stat_id::restore_time_read: return "Time spent by restore reading backup segments (usec)";
        case sm_stat_id::restore_time_replay: return "Time spent by restore replaying log archive (usec)";
        case sm_stat_id::restore_time_openscan: return "Time spent by restore opening archive scan (usec)";
        case sm_stat_id::restore_time_write: return "Time spent by restore writing restored segments (usec)";
        case sm_stat_id::restore_skipped_segs: return "Number of segments on which no log replay was performed";
        case sm_stat_id::restore_backup_reads: return "Number of segment reads on backup file";
        case sm_stat_id::restore_async_write_time: return "Time spend writing segments in async writer";
        case sm_stat_id::restore_log_volume: return "Amount of log replayed during restore (bytes)";
        case sm_stat_id::restore_multiple_segments: return "How often multiple segments were restored with a single log scan";
        case sm_stat_id::restore_segment_count: return "Total number of segments restored";
        case sm_stat_id::restore_invocations: return "How often the restore segment procedure was invoked";
        case sm_stat_id::restore_preempt_queue: return "How often sequential restore was preempted due to queued request";
        case sm_stat_id::restore_preempt_bitmap: return "How often sequential restore was preempted due to bitmap state";
        case sm_stat_id::la_log_slow: return "Log archiver activated with small window due to slow log growth";
        case sm_stat_id::la_activations: return "How often log archiver was activated";
        case sm_stat_id::la_read_volume: return "Number of bytes read during log archive scans";
        case sm_stat_id::la_read_count: return "Number of read operations performed on the log archive";
        case sm_stat_id::la_open_count: return "Number of open calls on run files of the log archive scanner";
        case sm_stat_id::la_read_time: return "Time spent reading blocks from log archive (usec)";
        case sm_stat_id::la_block_writes: return "Number of blocks appended to the log archive";
        case sm_stat_id::la_merge_heap_time: return "Time spent with log archiver merger operations (usec)";
        case sm_stat_id::la_index_probes: return "Number of probes in runs of the log archive index";
        case sm_stat_id::la_img_compressed_bytes: return "Bytes saved by applying page image compression";
        case sm_stat_id::log_img_format_bytes: return "Bytes added to transaction log by generating page images";
        case sm_stat_id::la_skipped_bytes: return "Bytes skipped in open method of archive index probes";
        case sm_stat_id::la_img_trimmed: return "Log archive lookups trimmed off thanks to page_img logrecs";
        case sm_stat_id::backup_not_prefetched: return "How often a segment was fixed without being prefetched first";
        case sm_stat_id::backup_evict_segment: return "A buffered segment had to be evicted in the brackup prefetcher";
        case sm_stat_id::backup_eviction_stuck: return "Backup prefetcher could not find a segment to evict";
        case sm_stat_id::la_wasted_read: return "Wasted log archive reads, i.e., that didn't use any logrec";
        case sm_stat_id::la_avoided_probes: return "Log archive prbves that were avoided thanks to run filters";
    }
    return "UNKNOWN_STAT";
}

void print_sm_stats(sm_stats_t& stats, std::ostream& out)
{
    for (size_t i = 0; i < stats.size(); i++) {
        out << get_stat_name(static_cast<sm_stat_id>(i)) << " "
            << stats[i]
            << std::endl;
    }
}

/*
 * One static stats structure for collecting
 * statistics that might otherwise be lost:
 */
namespace local_ns {
    sm_stats_t _global_stats_;
    static queue_based_block_lock_t _global_stats_mutex;
    static bool _global_stat_init = false;
}
void
smlevel_0::add_to_global_stats(const sm_stats_t &from)
{
    CRITICAL_SECTION(cs, local_ns::_global_stats_mutex);
    if (!local_ns::_global_stat_init) {
        local_ns::_global_stats_.fill(0);
        local_ns::_global_stat_init = true;
    }

    for (size_t i = 0; i < from.size(); i++) {
        local_ns::_global_stats_[i] += from[i];
    }
}
void
smlevel_0::add_from_global_stats(sm_stats_t &to)
{
    CRITICAL_SECTION(cs, local_ns::_global_stats_mutex);
    if (!local_ns::_global_stat_init) {
        local_ns::_global_stats_.fill(0);
        local_ns::_global_stat_init = true;
    }
    for (size_t i = 0; i < to.size(); i++) {
        to[i] += local_ns::_global_stats_[i];
    }
}
