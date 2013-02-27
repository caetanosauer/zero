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

 /**\brief Statistics (counters) for most of the storage manager.
  * \details
  * This structure holds most of the storage manager's statictics, 
  * those not specific to the buffer-manager's hash table.
  * Those counters are in bf_htab_stats_t.
  */
class sm_stats_t {
public:
    void    compute();
#include "sm_stats_t_struct_gen.h"
};

 /**\brief Statistics (counters) for the buffer-manager hash table.
  * \details
  * This structure holds counters
  * specific to the buffer-manager's hash table.
  * Although it is not necessary,
  * they are separated from the rest for ease of unit-testing.
  */
class bf_htab_stats_t {
public:
    void    compute();
#include "bf_htab_stats_t_struct_gen.h"
};

/**\brief Storage Manager Statistics 
 *
 * The storage manager is instrumented; it collects the statistics
 * (mostly counters) that are described in *_stats.dat files (input 
 * files to Perl scripts).
 * These statistics are incremented in per-thread structures, which
 * are gathered and available to the value-added server
 * under various circumstances, described in \ref SSMSTATS.
 */
class sm_stats_info_t {
public:
    bf_htab_stats_t  bfht;
    sm_stats_t       sm;
    void    compute() { 
        bfht.compute(); 
        sm.compute(); 
    }
    friend ostream& operator<<(ostream&, const sm_stats_info_t& s);
    sm_stats_info_t() {
        memset(this, '\0', sizeof (*this));
    }
};

extern sm_stats_info_t &operator+=(sm_stats_info_t &s, const sm_stats_info_t &t);
extern sm_stats_info_t &operator-=(sm_stats_info_t &s, const sm_stats_info_t &t);

/**\brief Configuration Information
 * \details
 * The method ss_m::config_info lets a server to 
 * pick up some useful configuration
 * information from the storage manager.
 * Several of these data depend on the compile-time page size; some
 * depend on run-time options.
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
