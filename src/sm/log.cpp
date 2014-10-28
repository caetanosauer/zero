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

/*<std-header orig-src='shore'>

 $Id: log.cpp,v 1.137 2010/12/08 17:37:42 nhall Exp $

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
#define LOG_C

#include "sm_int_1.h"
#include "logdef_gen.cpp"
#include "log.h"
#include "log_core.h"
#include "log_lsn_tracker.h"
#include "crash.h"
#include "lock_raw.h"
#include "bf_tree.h"
#include <algorithm> // for std::swap
#include <stdio.h> // snprintf
#include <boost/static_assert.hpp>
#include <vector>

typedef smlevel_0::fileoff_t fileoff_t;

/*********************************************************************
 *
 *  log_i::xct_next(lsn, r)
 *
 *  Read the next record into r and return its lsn in lsn.
 *  Return false if EOF reached. true otherwise.
 *
 *********************************************************************/
bool log_i::xct_next(lsn_t& lsn, logrec_t*& r)  
{
    // Initially (before the first xct_next call, 
    // 'cursor' is set to the starting point of the scan
    // After each xct_next call, 
    // 'cursor' is set to the lsn of the next log record if forward scan
    // or the lsn of the previous log record if backward scan
    
    bool eof = (cursor == lsn_t::null);

    if (! eof) {
        lsn = cursor;
        rc_t rc = log.fetch(lsn, r, &cursor, forward_scan);  // Either forward or backward scan
        
        // release right away, since this is only
        // used in recovery.
        log.release();

        if (rc.is_error())  {
            last_rc = RC_AUGMENT(rc);
            RC_APPEND_MSG(last_rc, << "trying to fetch lsn " << cursor);
            
            if (last_rc.err_num() == eEOF)
                eof = true;
            else  {
                smlevel_0::errlog->clog << fatal_prio 
                << "Fatal error : " << last_rc << flushl;
            }
        }
    }

    return ! eof;
}

// virtual
void  log_m::shutdown() 
    { log_core::THE_LOG->shutdown(); }


NORET  log_m::~log_m() 
{
    // This cannot call log_core destructor or we'll get into
    // an inf loop, thus we have to do this with virtual methods.
}

// Used by sm options-handling:
// static
fileoff_t  log_m::segment_size() 
   { return log_core::THE_LOG->segment_size(); }
// static
fileoff_t log_m::partition_size(long psize)
   { return log_core::partition_size(psize); }
// static
fileoff_t log_m::min_partition_size()
   { return log_core::min_partition_size(); }

fileoff_t log_m::max_partition_size()
   { return log_core::max_partition_size(); }

log_m::log_m()
{
}

void log_m::start_log_corruption()
    { log_core::THE_LOG->start_log_corruption(); }


/*********************************************************************
 * 
 *  log_core::set_master(master_lsn, min_rec_lsn, min_xct_lsn)
 *
 *********************************************************************/
void log_m::set_master(const lsn_t& mlsn, const lsn_t  & min_rec_lsn, 
        const lsn_t &min_xct_lsn) 
{
    log_core::THE_LOG->set_master(mlsn, min_rec_lsn, min_xct_lsn);
}

rc_t  
log_m::new_log_m(log_m   *&the_log,
                         const char *path,
                         int wrbufsize,
                         bool  reformat,
                         int carray_active_slot_count)
{
    FUNC(log_m::new_log_m);

    rc_t rc = log_core::new_log_m(path, the_log, wrbufsize, reformat, carray_active_slot_count);

    w_assert1(the_log != NULL);

    if(!rc.is_error())  {
        log_core::THE_LOG->start_flush_daemon();
    }
    return RCOK;
}

const char *
log_m::make_log_name(uint32_t idx, char* buf, int bufsz)
{
    return log_core::THE_LOG->make_log_name(idx, buf, bufsz);
}

rc_t                
log_m::scavenge(const lsn_t& min_rec_lsn, const lsn_t & min_xct_lsn) 
    { return log_core::THE_LOG->scavenge(min_rec_lsn, min_xct_lsn); }

void                
log_m:: release()  
    { log_core::THE_LOG->release(); }

rc_t 
log_m::fetch(lsn_t &lsn, logrec_t* &rec, lsn_t* nxt, bool forward) 
    { return log_core::THE_LOG->fetch(lsn, rec, nxt, forward); }

rc_t 
log_m::insert(logrec_t &r, lsn_t* ret)
{ 
    return log_core::THE_LOG->insert(r, ret); 
}

rc_t 
log_m::flush(const lsn_t &lsn, bool block, bool signal, bool *ret_flushed)
    { return log_core::THE_LOG->flush(lsn, block, signal, ret_flushed); }

rc_t 
log_m::compensate(const lsn_t & orig_lsn, const lsn_t& undo_lsn) 
    { return log_core::THE_LOG->compensate(orig_lsn, undo_lsn); }


PoorMansOldestLsnTracker*
log_m::get_oldest_lsn_tracker()
    { return log_core::THE_LOG->get_oldest_lsn_tracker(); }

/*********************************************************************
 * 
 *  log_m::_make_master_name(master_lsn, min_chkpt_rec_lsn, buf, bufsz)
 *
 *  Make up the name of a master record in buf.
 *
 *********************************************************************/
void
log_m::_make_master_name(
    const lsn_t&         master_lsn, 
    const lsn_t&        min_chkpt_rec_lsn,
    char*                 buf,
    int                        bufsz,
    bool                old_style)
{
    log_core::THE_LOG->_make_master_name(master_lsn, min_chkpt_rec_lsn, buf, bufsz, old_style);
}

void
log_m::_write_master(const lsn_t &l, const lsn_t &min) 
   { log_core::THE_LOG->_write_master(l, min); }

/*********************************************************************
 *
 * log_m::_create_master_chkpt_string
 *
 * writes a string which parse_master_chkpt_string expects.
 * includes the version, and master and min chkpt rec lsns.
 *
 *********************************************************************/

void
log_m::_create_master_chkpt_string(
                ostream&        s,
                int                arraysize,
                const lsn_t*        array,
                bool                old_style)
{
    log_core::THE_LOG->_create_master_chkpt_string(s, arraysize, array, old_style);
}

/*********************************************************************
 *
 * log_m::_check_version
 *
 * returns LOGVERSIONTOONEW or LOGVERSIONTOOOLD if the passed in
 * version is incompatible with this sources version
 *
 *********************************************************************/

rc_t
log_m::_check_version(uint32_t major, uint32_t minor)
{
    return log_core::THE_LOG->_check_version(major, minor);
}


void
log_m::_create_master_chkpt_contents(
                ostream&        s,
                int                arraysize,
                const lsn_t*        array
                )
{
    log_core::THE_LOG->_create_master_chkpt_contents(s, arraysize, array);
}


rc_t
log_m::_parse_master_chkpt_contents(
                istream&            s,
                int&                    listlength,
                lsn_t*                    lsnlist
                )
{
    return log_core::THE_LOG->_parse_master_chkpt_contents(s, listlength, lsnlist);
}


/*********************************************************************
 *
 * log_m::_parse_master_chkpt_string
 *
 * parse and return the master_lsn and min_chkpt_rec_lsn.
 * return an error if the version is out of sync.
 *
 *********************************************************************/
rc_t
log_m::_parse_master_chkpt_string(
                istream&            s,
                lsn_t&              master_lsn,
                lsn_t&              min_chkpt_rec_lsn,
                int&                    number_of_others,
                lsn_t*                    others,
                bool&                    old_style)
{
    return log_core::THE_LOG->_parse_master_chkpt_string(s, master_lsn,
            min_chkpt_rec_lsn, number_of_others, others, old_style);
}


/* Compute size of the biggest checkpoint we ever risk having to take...
 */
long log_m::max_chkpt_size() const 
{
    return log_core::THE_LOG->max_chkpt_size();
}

w_rc_t
log_m::_read_master( 
        const char *fname,
        int prefix_len,
        lsn_t &tmp,
        lsn_t& tmp1,
        lsn_t* lsnlist,
        int&   listlength,
        bool&  old_style
)
{
    return log_core::THE_LOG->_read_master(fname, prefix_len, tmp, tmp1,
            lsnlist, listlength, old_style);
}

fileoff_t log_m::take_space(fileoff_t *ptr, int amt) 
{
    return log_core::THE_LOG->take_space(ptr, amt);
}

extern "C" void log_stop()
{
}

fileoff_t log_m::reserve_space(fileoff_t amt) 
{
    return log_core::THE_LOG->reserve_space(amt);
}

fileoff_t log_m::consume_chkpt_reservation(fileoff_t amt)
{
    return log_core::THE_LOG->consume_chkpt_reservation(amt);
}

bool log_m::verify_chkpt_reservation() 
{
    return log_core::THE_LOG->verify_chkpt_reservation();
}

void log_m::release_space(fileoff_t amt) 
{
    log_core::THE_LOG->release_space(amt);
}

rc_t log_m::wait_for_space(fileoff_t &amt, timeout_in_ms timeout) 
{
    return log_core::THE_LOG->wait_for_space(amt, timeout);
}

void            
log_m::activate_reservations()
{
    log_core::THE_LOG->activate_reservations();
}

bool                
log_m::squeezed_by(const lsn_t &self)  const 
{
    return log_core::THE_LOG->squeezed_by(self);
}

rc_t                
log_m::file_was_archived(const char *file)
{
    // TODO: should check that this is the oldest, 
    // and that we indeed asked for it to be archived.
    return log_core::THE_LOG->file_was_archived(file);
}

const char * log_m::dir_name()
    { return log_core::THE_LOG->dir_name(); }

fileoff_t log_m::space_left() const 
    { return log_core::THE_LOG->space_left(); }

fileoff_t log_m::space_for_chkpt() const 
    { return log_core::THE_LOG->space_for_chkpt(); }

lsn_t log_m::curr_lsn() const
    { return log_core::THE_LOG->curr_lsn(); }

lsn_t log_m::min_chkpt_rec_lsn() const
    { return log_core::THE_LOG->min_chkpt_rec_lsn(); }

fileoff_t log_m::limit() const
    { return log_core::THE_LOG->limit(); }

lsn_t log_m::durable_lsn() const { return log_core::THE_LOG->master_lsn(); }
lsn_t log_m::master_lsn() const { return log_core::THE_LOG->master_lsn(); }
