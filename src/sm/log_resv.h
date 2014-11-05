/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager
   
                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne
   
                         All Rights Reserved.
   
   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.
   
   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

/*<std-header orig-src='shore' incl-file-exclusion='SRV_LOG_H'>

 $Id: log_core.h,v 1.11 2010/09/21 14:26:19 nhall Exp $

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

#ifndef LOG_RESV_H
#define LOG_RESV_H
#include "w_defines.h"

#include <deque>

#include "log_storage.h"

typedef smlevel_0::fileoff_t fileoff_t;

class PoorMansOldestLsnTracker;

class log_resv {
private:
    struct waiting_xct {
        fileoff_t* needed;
        pthread_cond_t* cond;
        NORET waiting_xct(fileoff_t *amt, pthread_cond_t* c)
            : needed(amt), cond(c)
        {
        }
    };
    std::deque<waiting_xct*> _log_space_waiters;

public:
    log_resv(log_storage* storage);
    virtual ~log_resv();

    fileoff_t           reserve_space(fileoff_t howmuch);
    long                max_chkpt_size() const;
    bool                verify_chkpt_reservation();
    rc_t            scavenge(const lsn_t &min_rec_lsn, const lsn_t&min_xct_lsn);
    void            release_space(fileoff_t howmuch);
    void            activate_reservations(const lsn_t& curr_lsn) ;
    fileoff_t       consume_chkpt_reservation(fileoff_t howmuch);
    rc_t            wait_for_space(fileoff_t &amt, timeout_in_ms timeout);
    bool            reservations_active() const { return _reservations_active; }
    rc_t            file_was_archived(const char * /*file*/);

    /*
     * STATIC METHODS
     */
    static fileoff_t    take_space(fileoff_t *ptr, int amt) ;

    /*
     * INLINED
     */

    /**\brief  Return the amount of space left in the log.
     * \details
     * Used by xct_impl for error-reporting. 
     */
    fileoff_t           space_left() const { return *&_space_available; }
    fileoff_t           space_for_chkpt() const { return *&_space_rsvd_for_chkpt ; }

    PoorMansOldestLsnTracker* get_oldest_lsn_tracker() { return _oldest_lsn_tracker; }

protected:
    log_storage*    _storage;
    bool            _reservations_active;
    fileoff_t       _space_available; // how many unreserved bytes left
    fileoff_t       _space_rsvd_for_chkpt; // can we run a chkpt now?

    bool _waiting_for_space; // protected by log_m::_insert_lock/_wait_flush_lock

    pthread_mutex_t         _space_lock; // tied to _space_cond
    pthread_cond_t          _space_cond; // tied to _space_lock

    PoorMansOldestLsnTracker* _oldest_lsn_tracker;
};


#endif
