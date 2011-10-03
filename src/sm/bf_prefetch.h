/*<std-header orig-src='shore' incl-file-exclusion='BF_PREFETCH_H'>

 $Id: bf_prefetch.h,v 1.10 2010/05/26 01:20:36 nhall Exp $

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

#ifndef BF_PREFETCH_H
#define BF_PREFETCH_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

class bf_prefetch_thread_t : public smthread_t 
{

public:
    NORET            bf_prefetch_thread_t(int i=1);
    NORET            ~bf_prefetch_thread_t();
    w_rc_t            request(
                    const lpid_t&       pid,
                    latch_mode_t    mode
                );
                // request prefetch of page from thread

    w_rc_t            fetch(
                    const lpid_t&       pid,
                    page_p&        page        
                );
                // fetch prefetched page from thread
                // if it matches pid; refix() it in given
                // frame; return true if it worked, false
                // if not (e.g., wrong page)

    void            retire();
    virtual void        run();

private:
    bool            get_error();
    void            _init(int i);
public:
    enum prefetch_status_t {
    pf_init=0, pf_requested, pf_in_transit, pf_available, pf_grabbed,
    pf_failure, 
    pf_fatal,
    pf_max_unused_status 
    };
private:
    enum prefetch_event_t {
    pf_request=0, pf_get_error, pf_start_fix, pf_end_fix, pf_fetch,
    pf_error, 
    pf_destructor, 
    pf_max_unused_event
    };

    w_rc_t            _fix_error;
    int                _fix_error_i;
    int                _n;
    void            new_state(int i, prefetch_event_t e);
    struct frame_info {
public:
    NORET frame_info():_pid(lpid_t::null),_status(pf_init),
        _mode(LATCH_NL){}

    NORET ~frame_info() {}

    page_p                _page;
    lpid_t               _pid;
        prefetch_status_t       _status; // indicates request satisfied
    latch_mode_t        _mode;
    };
    struct frame_info        *_info;
    int                _f; // index being fetched
                // _page[_f] is in use by this thread; 
                // _pid[_f] is being fetched by this thread
                // other _page[] may be in use by scan

    bool                       _retire;

    pthread_mutex_t            _prefetch_mutex; // paired with _activate
    pthread_cond_t             _activate; // paired with _prefetch_mutex

    // disabled
    NORET            bf_prefetch_thread_t(
                    const bf_prefetch_thread_t&);
    bf_prefetch_thread_t&    operator=(const bf_prefetch_thread_t&);

    static prefetch_status_t    
        _table[pf_max_unused_status][pf_max_unused_event];
};

/*<std-footer incl-file-exclusion='BF_PREFETCH_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
