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
#include "sm_stats_t_msg_gen.h"
    }
    return "UNKNOWN_STAT";
}

void print_sm_stats(sm_stats_t& stats, std::ostream& out)
{
    for (size_t i = 0; i < stats.size() - 1; i++) {
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
}
void
smlevel_0::add_to_global_stats(const sm_stats_t &from)
{
    CRITICAL_SECTION(cs, local_ns::_global_stats_mutex);
    for (size_t i = 0; i < from.size(); i++) {
        local_ns::_global_stats_[i] += from[i];
    }
}
void
smlevel_0::add_from_global_stats(sm_stats_t &to)
{
    CRITICAL_SECTION(cs, local_ns::_global_stats_mutex);
    for (size_t i = 0; i < to.size(); i++) {
        to[i] += local_ns::_global_stats_[i];
    }
}
