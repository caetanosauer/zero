/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

/*<std-header orig-src='shore'>

 $Id: lid.cpp,v 1.155 2010/06/23 23:44:29 nhall Exp $

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

#define SM_SOURCE
#define LID_C

#ifdef __GNUG__
#pragma implementation "lid.h"
#endif

#include <sm_int_4.h>
#include <btcursor.h>

#ifdef HAVE_UTSNAME
#        include <sys/utsname.h>
#else
#        include <hostname.h>
#endif

#include <netdb.h>        /* XXX really should be included for all */

rc_t
lid_m::generate_new_volid(lvid_t& lvid)
{
    FUNC(lid_m::_generate_new_volid);
    /*
     * For now the long volume ID will consists of
     * the machine network address and the current time-of-day.
     *
     * Since the time of day resolution is in seconds,
     * we protect this function with a mutex to guarantee we
     * don't generate duplicates.
     */
    static long  last_time = 0;
    const int    max_name = 100;
    char         name[max_name+1];

    // Mutex only for generating new volume ids.
    static queue_based_block_lock_t lidmgnrt_mutex;
    CRITICAL_SECTION(cs, lidmgnrt_mutex);

#ifdef HAVE_UTSNAME
    struct        utsname uts;
    if (uname(&uts) == -1) return RC(eOS);
    strncpy(name, uts.nodename, max_name);
#else
    if (gethostname(name, max_name)) return RC(eOS);
#endif

    struct hostent* hostinfo = gethostbyname(name);

    if (!hostinfo)
        W_FATAL(eINTERNAL);

    memcpy(&lvid.high, hostinfo->h_addr, sizeof(lvid.high));
    DBG( << "lvid " << lvid );

    /* XXXX generating ids fast enough can create a id time sequence
       that grows way faster than real time!  This could be a problem!
       Better time resolution than seconds does exist, might be worth
       using it.  */
    stime_t curr_time = stime_t::now();

    if (curr_time.secs() > last_time)
            last_time = curr_time.secs();
    else
            last_time++;

    lvid.low = last_time;

    return RCOK;
}
