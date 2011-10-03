/*<std-header orig-src='shore'>

 $Id: vtable_smthread.cpp,v 1.2 2010/05/26 01:20:48 nhall Exp $

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
#define SMTHREAD_C
#include <w.h>
#include <sm_int_1.h>
#include <sthread_vtable_enum.h>
#include <sm_vtable_enum.h>
#include "smthread.h"

const char *smthread_vtable_attr_names[] = {
    /* NB: Make this match the order in sthread.cpp
     * (sthread_vtable_attr_names) so that we can just
     * descend to sthread_t to collect part of a row.
     */
    "Sthread id",
    "Sthread name",
    "Sthread status",
#include "sthread_stats_msg_gen.h"
    "Smthread name", 
    "Smthread thread type", 
    "Smthread pin count", 
    "Is in SM", 
    "Tid",
    0
};

static vtable_names_init_t names_init(smthread_last, 
        smthread_vtable_attr_names);

int
smthread_t::collect(vtable_t &v, bool names_too)
{

    int num_threads=100;  // a max
    if(v.init(num_threads, smthread_last, names_init.max_size())) return -1;

    if(names_too) num_threads++;

    vtable_func<smthread_t> f(v);

    if(names_too) f.insert_names();

    class NothingAdded : public SmthreadFunc 
    {
        vtable_func<smthread_t> &_f;
        int _max;
        int _sofar;
    public:
        NothingAdded(vtable_func<smthread_t> &f, int maxx) : _f(f),
            _max(maxx), _sofar(0) {}
        ~NothingAdded(){}
    
        void operator()(const smthread_t& smthread)  { 
            if(_sofar++ < _max) _f(smthread); 
        }

    } F(f, num_threads);

    smthread_t::for_each_smthread(F);

    return 0; // no error
}

void                
smthread_t::vtable_collect_names(vtable_row_t& t) 
{
    names_init.collect_names(t);
}

void                
smthread_t::vtable_collect(vtable_row_t& t) 
{
    sthread_t::vtable_collect(t);

    t.set_string(smthread_name_attr, name());
    t.set_int(smthread_thread_type_attr, thread_type());
    t.set_int(smthread_pin_count_attr, pin_count());
    t.set_int(smthread_is_in_sm_attr, is_in_sm());
    if(tcb().xct) {
      w_ostrstream o;
      o << tcb().xct->tid() << ends;
      t.set_string(smthread_tid_attr, o.c_str());
    } else {
       t.set_string(smthread_tid_attr, "");
    }
}
