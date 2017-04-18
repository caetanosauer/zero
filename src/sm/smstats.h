/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

/*<std-header orig-src='shore' incl-file-exclusion='SMSTATS_H'>

 $Id: smstats.h,v 1.36 2010/09/21 14:26:20 nhall Exp $

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

#ifndef SMSTATS_H
#define SMSTATS_H

#include "w_defines.h"
#include <array>

/*  -- do not edit anything above this line --   </std-header>*/

// This file is included in sm.h in the middle of the class ss_m
// declaration.  Member functions are defined in sm.cpp


/**\addtogroup SSMSTATS
 *
 * The storage manager API allows a server to gather statistics on
 * a per-transaction basis or on a global basis.
 * These counters are not segregated by the semantics of the counters. Rather,
 * they are segregated by the thread that performed the action being measured.
 * In other words, an action is attributed to a transaction if it was
 * performed by a thread while attached to that transaction.
 * This means that some actions, such as writing pages to disk, might not
 * be attributable to a transaction even though they are in some
 * sense logically
 * associated with that transaction.  If the write is performed by a
 * page writer (background thread), it will show up in the global statistics
 * but not in any per-transaction statistics. On the other hand, if a write
 * is performed by ss_m::set_store_property (which flushes to disk
 * all pages for the store thus changed) it will be attributed to the
 * transaction.
 *
 * All statistics are collected on a per-smthread_t basis
 * (thus avoiding expensive atomic updates on a per-counter basis).
 * Each smthread has its own
 * local sm_stats_info_t structure for these statistics.
 * Any time this structure is cleared,
 * its contents are added to a single global statistics structure
 * (protected by a mutex) before it is cleared.
 * The clearing happens in two circumstances:
 * - when an smthread_t is destroyed (in its destructor)
 * - when an attached instrumented transaction collects the statistics from
 *   the thread (see below).
 *
 * Thus, the single global statistics structure's contents reflect the
 * activities of finished threads and of instrumented transactions' collected
 * statistics, and all other activities are reflected in per-thread
 * statistics structures.
 *
 * A value-added server may collect the global statistics with the
 * ss_m::gather_stats method.  This method first adds together all the
 * per-thread statistics, adds in the global statistics, and returns.
 * The global statistics cannot be reset, and, indeed, they survive
 * the storage manager so that they can be gathered after the
 * storage manager shuts down. This means that to determine incremental
 * statistics, the value-added server has to keep the prior copy of
 * statistics and diff the current statistics from the prior statistics.
 * The sm_stats_info_t has a difference operator to make this easy.
 * \attention Gathering the per-thread statistics from running threads is
 * not atomic; in other words, if threads are updating their counters
 * while the gathering of their counters is going on, some counts may
 * be missed (become stale). (In any case, they will be stale soon
 * after the statistics are gathered.)
 *
 * A transaction must be instrumented to collect its statistics.
 *
 * Instrumenting a transaction
 * consists in allocating a structure in which to store the collected
 * statistics, and passing in that structure to the storage manager using
 * using the variants of begin_xct, commit_xct, etc that take
 * an argument of this type.
 *
 * When a transaction is detached from a thread,
 * the statistics it gathered up to that point by the thread are
 * added to the per-transaction statistics, and the thread statistics are
 * cleared so they don't get over-counted.
 *
 * A server may gather the per-transaction statistics for the
 * attached running transaction with
 * ss_m::gather_xct_stats.
 *
 * A server may choose to reset the per-transaction statistics when it
 * gathers them; this facilitates gathering incremental statistics.
 * These counters aren't lost to the world, since their values were
 * added to the global statistics before they were gathered in the first place.
 *
 * \attention The per-transaction statistics structure is not
 * protected against concurrently-attached threads, so
 * its values are best collected and reset when the server
 * knows that only one thread is attached to the
 * transaction when making the call.
 */


template<typename E>
constexpr auto enum_to_base(E e) -> typename std::underlying_type<E>::type
{
       return static_cast<typename std::underlying_type<E>::type>(e);
}

enum class sm_stat_id : size_t
{
    rwlock_r_wait,
    rwlock_w_wait,
    needs_latch_condl,
    needs_latch_uncondl,
    latch_condl_nowait,
    latch_uncondl_nowait,
    bf_one_page_write,
    bf_two_page_write,
    bf_three_page_write,
    bf_four_page_write,
    bf_five_page_write,
    bf_six_page_write,
    bf_seven_page_write,
    bf_eight_page_write,
    bf_more_page_write,
    cleaned_pages,
    cleaner_time_cpu,
    cleaner_time_io,
    cleaner_time_copy,
    bf_already_evicted,
    bf_eviction_stuck,
    bf_dirty_page_cleaned,
    bf_flushed_OHD_page,
    bf_kick_full,
    bf_kick_replacement,
    bf_kick_threshold,
    bf_sweep_page_hot_skipped,
    bf_discarded_hot,
    bf_log_flush_all,
    bf_log_flush_lsn,
    bf_write_out,
    bf_sleep_await_clean,
    bf_invoked_spr,
    bf_fg_scan_cnt,
    bf_unfix_cleaned,
    bf_evict,
    rwlock_r_waits,
    rwlock_w_waits,
    need_latch_condl,
    latch_condl_nowaits,
    need_latch_uncondl,
    latch_uncondl_nowaits,
    latch_uncondl_waits,
    btree_latch_wait,
    io_latch_wait,
    bf_look_cnt,
    bf_grab_latch_failed,
    bf_hit_cnt,
    bf_hit_wait,
    bf_hit_wait_any_p,
    bf_hit_wait_btree_p,
    bf_hit_wait_alloc_p,
    bf_hit_wait_stnode_p,
    bf_hit_wait_scan,
    bf_replace_out,
    bf_replaced_dirty,
    bf_replaced_clean,
    bf_replaced_unused,
    bf_awaited_cleaner,
    bf_no_transit_bucket,
    bf_prefetch_requests,
    bf_prefetches,
    bf_upgrade_latch_unconditional,
    bf_upgrade_latch_race,
    bf_upgrade_latch_changed,
    restart_repair_rec_lsn,
    vol_reads,
    vol_writes,
    vol_blks_written,
    need_vol_lock_r,
    need_vol_lock_w,
    nowait_vol_lock_r,
    nowait_vol_lock_w,
    await_vol_lock_r,
    await_vol_lock_w,
    io_m_lsearch,
    vol_cache_primes,
    vol_cache_prime_fix,
    vol_cache_clears,
    vol_lock_noalloc,
    log_dup_sync_cnt,
    log_daemon_wait,
    log_daemon_work,
    log_fsync_cnt,
    log_chkpt_cnt,
    log_chkpt_wake,
    log_fetches,
    log_inserts,
    log_full,
    log_full_old_xct,
    log_full_old_page,
    log_full_wait,
    log_full_force,
    log_full_giveup,
    log_file_wrap,
    log_bytes_generated,
    log_bytes_written,
    log_bytes_rewritten,
    log_bytes_generated_rb,
    log_bytes_rbfwd_ratio,
    log_flush_wait,
    log_short_flush,
    log_long_flush,
    lock_deadlock_cnt,
    lock_false_deadlock_cnt,
    lock_dld_call_cnt,
    lock_dld_first_call_cnt,
    lock_dld_false_victim_cnt,
    lock_dld_victim_self_cnt,
    lock_dld_victim_other_cnt,
    nonunique_fingerprints,
    unique_fingerprints,
    rec_pin_cnt,
    rec_unpin_cnt,
    rec_repin_cvt,
    bt_find_cnt,
    bt_insert_cnt,
    bt_remove_cnt,
    bt_traverse_cnt,
    bt_partial_traverse_cnt,
    bt_restart_traverse_cnt,
    bt_posc,
    bt_scan_cnt,
    bt_splits,
    bt_cuts,
    bt_grows,
    bt_shrinks,
    bt_links,
    bt_upgrade_fail_retry,
    bt_clr_smo_traverse,
    bt_pcompress,
    bt_plmax,
    any_p_fix_cnt,
    alloc_p_fix_cnt,
    stnode_p_fix_cnt,
    page_fix_cnt,
    bf_fix_cnt,
    bf_refix_cnt,
    bf_unfix_cnt,
    vol_check_owner_fix,
    page_alloc_cnt,
    page_dealloc_cnt,
    ext_lookup_hits,
    ext_lookup_misses,
    alloc_page_in_ext,
    vol_free_page,
    vol_next_page,
    vol_find_free_exts,
    xct_log_flush,
    begin_xct_cnt,
    commit_xct_cnt,
    abort_xct_cnt,
    log_warn_abort_cnt,
    prepare_xct_cnt,
    rollback_savept_cnt,
    internal_rollback_cnt,
    s_prepared,
    sdesc_cache_miss,
    mpl_attach_cnt,
    anchors,
    compensate_in_log,
    compensate_in_xct,
    compensate_records,
    compensate_skipped,
    log_switches,
    get_logbuf,
    await_1thread_xct,
    lock_query_cnt,
    unlock_request_cnt,
    lock_request_cnt,
    lock_acquire_cnt,
    lock_head_t_cnt,
    lock_await_alt_cnt,
    lock_extraneous_req_cnt,
    lock_conversion_cnt,
    lk_vol_acq,
    lk_store_acq,
    lk_key_acq,
    lock_wait_cnt,
    lock_block_cnt,
    lk_vol_wait,
    lk_store_wait,
    lk_key_wait,
    bf_fix_nonroot_count,
    bf_fix_nonroot_swizzled_count,
    bf_fix_nonroot_miss_count,
    bf_fix_adjusted_parent,
    bf_batch_wait_time,
    restart_log_analysis_time,
    restart_redo_time,
    restore_sched_seq,
    restore_sched_queued,
    restore_sched_random,
    restore_time_read,
    restore_time_replay,
    restore_time_openscan,
    restore_time_write,
    restore_skipped_segs,
    restore_backup_reads,
    restore_async_write_time,
    restore_log_volume,
    restore_multiple_segments,
    restore_segment_count,
    restore_invocations,
    restore_preempt_queue,
    restore_preempt_bitmap,
    la_log_slow,
    la_activations,
    la_read_volume,
    la_read_count,
    la_open_count,
    la_read_time,
    la_block_writes,
    la_merge_heap_time,
    la_index_probes,
    la_img_compressed_bytes,
    log_img_format_bytes,
    la_skipped_bytes,
    backup_not_prefetched,
    backup_evict_segment,
    backup_eviction_stuck,
    la_wasted_read,
    la_avoided_probes,
    stat_max // Leave this one here to count the number of stats!
};

using sm_stats_t = std::array<long, enum_to_base(sm_stat_id::stat_max)>;

// CS TODO: move this into some static class instead of global functions
void print_sm_stats(sm_stats_t& stats, std::ostream& out);
const char* get_stat_name(sm_stat_id s);
const char* get_stat_expl(sm_stat_id s);

/**\brief Configuration Information
 * \details
 * The method ss_m::config_info lets a server to pick up some useful
 * configuration information from the storage manager.  Several of
 * these data depend on the compile-time page size; some depend on
 * run-time options.
 */
struct sm_config_info_t {
    /**\brief compile-time constant. Settable in
     * \code shore.def \endcode.
     * Default is 8K.
     */
    u_long page_size;         // bytes in page, including all headers

    /**\brief Data space available on a page of a large record */
    //TODO: SHORE-KITS-API
    // shore-kits needs max_small_rec; shore-sm-6.0.1 initializes this field at
    // several places. Make sure Zero similarly initializes max_small_rec
    u_long max_small_rec;      // maximum number of bytes in a "small"
                // (ie. on one page) record.  This is
                // align(header_len)+align(body_len).
    u_long lg_rec_page_space;

    /**\brief Size in KB of buffer pool */
    u_long buffer_pool_size;    // buffer pool size in kilo-bytes

    /**\brief Largest permissible size in bytes of an index entry
     * (key,value pair) */
    u_long max_btree_entry_size;

    /**\brief Number of extent links on an extent page */
    u_long exts_on_page;

    /**\brief Number of pages per extent (compile-time constant)
     * \note The storage manager has not been tested with any value but 8.
     */
    u_long pages_per_ext;

    /**\brief True if logging is on.
     * \note The multi-threaded storage manager has not been
     * tested with logging turned off, so turning off logging is
     * not supported in this release.
     */
    bool   logging;

    friend ostream& operator<<(ostream&, const sm_config_info_t& s);
};

/*<std-footer incl-file-exclusion='SMSTATS_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
