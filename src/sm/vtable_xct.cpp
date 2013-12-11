/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

/*<std-header orig-src='shore'>

 $Id: vtable_xct.cpp,v 1.3 2010/06/21 20:39:39 nhall Exp $

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
#define XCT_C

#include <new>
#include "sm_int_1.h"
#include "tls.h"

#include "lock_x.h"
#include "xct_dependent.h"

#include <vtable.h>
#include <sm_vtable_enum.h>

#ifdef __GNUG__ 
template class vtable_func<xct_t>;
#endif /* __GNUG__ */

const char *xct_vtable_attr_names[] = {
    "Threads attached",
    "Gtid",
    "Tid",
    "State",
    "Coord",
    "Force-readonly"
};

static vtable_names_init_t names_init(xct_last, xct_vtable_attr_names);

int
xct_t::collect( vtable_t& v, bool names_too)
{
    int n=0;
    W_COERCE(acquire_xlist_mutex());
    {
        w_list_i<xct_t, queue_based_lock_t> i(_xlist);
        while (i.next())  { n++; }
    }

    if(names_too) n++;
    // n: number of rows
    // xct_last: number of attributes
    // names_init.max_size(): maximum attribute length
    if(v.init(n, xct_last, names_init.max_size())) {
        release_xlist_mutex();
        return -1;
    }
    vtable_func<xct_t> f(v);
    if(names_too) f.insert_names();

    {
        w_list_i<xct_t, queue_based_lock_t> i(_xlist);
        while (i.next())  { 
            f( *i.curr());
        }
    }
    release_xlist_mutex();
    return 0; //no error
}

void        
xct_t::vtable_collect(vtable_row_t &t)
{
    // xct_nthreads_attr
    t.set_int(xct_nthreads_attr, num_threads());

    // xct_gtid_attr
    if(is_extern2pc()) {
        w_ostrstream        s;
        s << *gtid() << ends;
        t.set_string(xct_gtid_attr, s.c_str());
    } else {
        t.set_string(xct_gtid_attr, "");
    }
    // xct_tid_attr
    {
        w_ostrstream o;
        o << tid() << ends;
        t.set_string(xct_tid_attr, o.c_str());
    }

    // xct_state_attr
    {
        w_ostrstream o;
        o << state() << ends;
        t.set_string(xct_state_attr, o.c_str());
    }

    // xct_coordinator_attr
    if(state() == xct_prepared) {
        w_ostrstream        s;
        s << get_coordinator() << ends;
        t.set_string(xct_coordinator_attr, s.c_str());
    } else {
        t.set_string(xct_coordinator_attr, "");
    }

    // xct_forced_readonly_attr
    t.set_string(xct_forced_readonly_attr, 
            (forced_readonly()?"true":"false"));
}

void        
xct_t::vtable_collect_names(vtable_row_t &t)
{
    names_init.collect_names(t);
}

