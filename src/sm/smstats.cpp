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
#include "sm_stats_t_inc_gen.cpp"
#include "sm_stats_t_dec_gen.cpp"
#include "sm_stats_t_out_gen.cpp"
#include "bf_htab_stats_t_inc_gen.cpp"
#include "bf_htab_stats_t_dec_gen.cpp"
#include "bf_htab_stats_t_out_gen.cpp"

// the strings:
const char *sm_stats_t ::stat_names[] = {
#include "bf_htab_stats_t_msg_gen.h"
#include "sm_stats_t_msg_gen.h"
   ""
};

void bf_htab_stats_t::compute()
{
}

void sm_stats_t::compute()
{
    latch_uncondl_waits = need_latch_uncondl - latch_uncondl_nowaits;

    await_vol_lock_r = need_vol_lock_r - nowait_vol_lock_r;
    await_vol_lock_w = need_vol_lock_w - nowait_vol_lock_w;

    if(log_bytes_written > 0) {
        // skip-log and padding bytes -- actually,
        // anything flushed more than once, although inserted
        // bytes not yet flushed will tend to warp this number
        // if the log wasn't recently flushed.
        log_bytes_rewritten = log_bytes_written - log_bytes_generated;
    }
    if(log_bytes_generated_rb > 0) {
        // get the # bytes generated during forward processing.
        double x = log_bytes_generated - log_bytes_generated_rb;
        w_assert0(x >= 0.0);
        // should always be > 0, since the log_bytes_generated is 
        // the total of fwd and rollback bytes.
        if(x>0.0) {
            log_bytes_rbfwd_ratio = double(log_bytes_generated_rb) / x;
        }else {
            log_bytes_rbfwd_ratio = 0.0;
        }
    }
}

sm_stats_info_t &operator+=(sm_stats_info_t &s, const sm_stats_info_t &t)
{
    s.bfht += t.bfht;
    s.sm += t.sm;
    return s;
}

sm_stats_info_t &operator-=(sm_stats_info_t &s, const sm_stats_info_t &t)
{
    s.bfht -= t.bfht;
    s.sm -= t.sm;
    return s;
}


sm_stats_info_t &operator-=(sm_stats_info_t &s, const sm_stats_info_t &t);

/*
 * One static stats structure for collecting
 * statistics that might otherwise be lost:
 */
namespace local_ns {
    sm_stats_info_t _global_stats_;
    static queue_based_block_lock_t _global_stats_mutex;
}
void
smlevel_0::add_to_global_stats(const sm_stats_info_t &from)
{
    CRITICAL_SECTION(cs, local_ns::_global_stats_mutex);
    local_ns::_global_stats_ += from;
}
void
smlevel_0::add_from_global_stats(sm_stats_info_t &to)
{
    CRITICAL_SECTION(cs, local_ns::_global_stats_mutex);
    to += local_ns::_global_stats_;
}
