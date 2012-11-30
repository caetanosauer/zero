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

// -*- mode:c++; c-basic-offset:4 -*-
/*<std-header orig-src='shore'>

 $Id: bf.cpp,v 1.248 2010/12/17 19:36:26 nhall Exp $

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
#define BF_C


#ifdef __GNUG__
#pragma implementation "bf.h"
#endif

#include <sm_int_0.h>
#include "bf_core.h"
#include "chkpt.h"
#include "lock.h" // TODO for dummy transaction to delete page. need to reconsider
#include "xct.h" // TODO for dummy transaction to delete page. need to reconsider

#ifdef EXPLICIT_TEMPLATE
template class w_list_t<bf_cleaner_thread_t, queue_based_block_lock_t>;
template class w_list_i<bf_cleaner_thread_t, queue_based_block_lock_t>;
#endif

// These are here because bf_s.h doesn't know structure of *_frame
void  bfcb_t::set_storeflags(uint32_t f) { 
    _store_flags = _frame->set_page_storeflags(f); 
}

uint32_t  bfcb_t::read_page_storeflags() {
    uint32_t f = _frame->get_page_storeflags(); 
    _store_flags = f;
    /* Now this might not be accurate 
     * just after recovery
    w_assert1(_store_flags == f || 
            smlevel_0::operating_mode == smlevel_0::t_in_redo);
            */
    return f;
}

uint32_t  bfcb_t::get_storeflags() const {
    uint32_t f = _frame->get_page_storeflags(); 
    // w_assert1(_store_flags == f); doesn't work right after recovery. the all storeflag stuff should go away!
    return f;
}

typedef class w_list_t<bf_cleaner_thread_t, queue_based_block_lock_t> cleaner_thread_list_t;

struct bf_page_writer_control_t
{
    pthread_mutex_t _pwc_lock; // paired with _wake_master and _wake_slaves
    pthread_cond_t _wake_master; // page_write_threads signal this to
                                 // when they have finished a run, then
                                 // they (might) sleep on the following:
    pthread_cond_t _wake_slaves; // bf_cleaner_thread signals this to
                                 // awaken page_writer_threads to do something

    lpid_t* pids;
    int pages_submitted;  // # pages the master asked the slaves to write
    int pages_written;    // running total of # pages written by slaves
    int pages_claimed;    // index of last page for which a single writer thread has
    bool* retire;         // pointer to master's retire flag
    bool cancelslaves;    // cancel all slaves due to error by some slave
    w_rc_t thread_rc;     // setting this causes cancelslaves to be set

    lsn_t                 _min_copy_rec_lsn;

    NORET bf_page_writer_control_t(bool *bptr) :
        pids(0), pages_submitted(0), pages_written(0), 
        pages_claimed(0), 
        retire(bptr), 
        cancelslaves(false),
        _min_copy_rec_lsn(lsn_t::max)
    {
        DO_PTHREAD(pthread_mutex_init(&_pwc_lock, NULL));
        DO_PTHREAD(pthread_cond_init(&_wake_slaves, NULL));
        DO_PTHREAD(pthread_cond_init(&_wake_master, NULL));
    }
    NORET ~bf_page_writer_control_t() {
        DO_PTHREAD(pthread_mutex_destroy(&_pwc_lock));
        DO_PTHREAD(pthread_cond_destroy(&_wake_slaves));
        DO_PTHREAD(pthread_cond_destroy(&_wake_master));
    }

    bf_page_writer_control_t volatile* vthis() { return this; }
};

/*********************************************************************
 *
 *  bf_m::npages()
 *
 *  Return the size of the buffer pool in pages.
 *
 *********************************************************************/
int
bf_m::npages()
{
    return _core->_num_bufs;
}

/* a hack to allow cleaner threads to pass
 * info to bf_m::_clean_buf  w/o adding all the arguments
 */

void
bf_m::_incr_page_write(int number, bool bg)
{
    switch(number) {
    case 1:
        INC_TSTAT(bf_one_page_write);
        break;
    case 2:
        INC_TSTAT(bf_two_page_write);
        break;
    case 3:
        INC_TSTAT(bf_three_page_write);
        break;
    case 4:
        INC_TSTAT(bf_four_page_write);
        break;
    case 5:
        INC_TSTAT(bf_five_page_write);
        break;
    case 6:
        INC_TSTAT(bf_six_page_write);
        break;
    case 7:
        INC_TSTAT(bf_seven_page_write);
        break;
    case 8:
        INC_TSTAT(bf_eight_page_write);
        break;
    default:
        INC_TSTAT(bf_more_page_write);
        break;
    }
    if(bg) {
        ADD_TSTAT(bf_write_out, number);
    } else {
        ADD_TSTAT(bf_replace_out, number);
    }
}

/*********************************************************************
 *
 *  cmp_lpid(x, y)
 *
 *  Used for qsort(). Compare two lpids x and y ... disregard
 *  store id in comparison.
 *
 *********************************************************************/
static int
cmp_lpid(const void* x, const void* y)
{
    register const lpid_t* p1 = (lpid_t*) x;
    register const lpid_t* p2 = (lpid_t*) y;
    if (p1->vol() - p2->vol())
        return p1->vol() - p2->vol();
#ifdef SOLARIS2
    return (p1->page > p2->page ? 1 :
            p2->page > p1->page ? -1 :
            0);
#else
    return (p1->page > p2->page) ? 1 :
            ((p1->page < p2->page) ? -1 :
            0);
#endif
}


/*
 * update_rec_lsn
 * SEE COMMENTS IN restart.cpp - look for repair_rec_lsn
 *
 * Every fix has to set the rec_lsn to indicate a lower bound
 * on recovery for this page.  At fix time, fixes in SH mode
 * won't update the page so they don't affect the rec_lsn,
 * but fixes in EX mode could apply updates; those future updates' log
 * records necessarily will be after the current tail of the log.
 * (Ok, that is in the forward-processing case.  Redo is
 * different.  The aforementioned comments expound on this.)
 * Multiple EX fixes in a row won't increase the rec_lsn; the
 * rec_lsn can only be increased when the page is known to be
 * durable, that is, the buffer frame is clean.
 */

void
bfcb_t::update_rec_lsn(latch_mode_t mode, bool check)
{
    DBGTHRD(<<"update_rec_lsn pid " << pid() <<" mode=" 
                    << int(mode) << " rec_lsn=" << this->curr_rec_lsn()
            << "frame " << ::hex << (unsigned long) (frame()) << ::dec
            << " frame's pid " << frame()->pid);
    // Sanity check:
    w_assert1(latch.mode() >= mode);
    w_assert1(latch.is_latched());


    // Determine if this page is holding up scavenging of logs by (being 
    // presumably hot, and) having a rec_lsn that's in the oldest open
    // log partition and that oldest partition being sufficiently aged....
    if(mode == LATCH_EX && smlevel_0::log && curr_rec_lsn().valid()) 
    {
        w_assert1(latch.is_mine()); 

        if( smlevel_0::log->squeezed_by(this->curr_rec_lsn())
                && !old_rec_lsn().valid() // I don't have an old_rec_lsn so I'm
                // not in the process of being flushed by a cleaner thread
                )
        {
            /* grab the page lock and make sure we should really do
               the I/O... if we can't get it we conclude someone else
               is already dealing with the problem and just continue.
             */
            pthread_mutex_t* pm = page_write_mutex_t::locate(pid());
            int rval = pthread_mutex_trylock(pm);
            if(rval != EBUSY) {
                w_assert1(!rval);
                w_assert0(!old_rec_lsn().valid()); // never set when the mutex is free!
                // recheck
                if(smlevel_0::log->squeezed_by(this->curr_rec_lsn())) {
                    /* FRJ: We've stumbled across a hot-old-dirty
                       page. This beast is notorious for preventing the
                       system from reclaiming log space because all dirty
                       pages mentioned in a log segment must be written
                       out before it can be recycled, and this page has
                       not yet been written out.
                       
                       To avoid log-full problems and nasty corner cases
                       later, we'll just force the page out now.
                    */
                    INC_TSTAT(bf_flushed_OHD_page);
                    W_COERCE(bf_m::_write_out(_frame, 1));
                    
                    // these will be set appropriately below (before releasing the latch)
                    mark_clean();
                }
                pthread_mutex_unlock(pm);
            }
        }
    }
    
    if (mode == LATCH_EX && this->curr_rec_lsn() == lsn_t::null)  {
        /*
         *  intent to modify
         *  page would be dirty later than this lsn
         */
        if(smlevel_0::log) {
            this->set_rec_lsn ( smlevel_0::log->curr_lsn() );

            // This is the first fix with EX; noone else can
            // fix with EX while we have it fixed so noone else 
            // should be able to change the frame lsn between
            // the time we grabbed the log's curr lsn and the
            // time we set the rec lsn.  
            // Unfortunately, if we are fixing for the purpose of
            // a format, all bets are off here. We might not have
            // a frame lsn yet.
            if(check) {
                // leave code in this form to avoid compiler complaint
                // about unused function argument in level-0-debug build
                w_assert1(frame()->lsn <= curr_rec_lsn());
            }

            // Once we dirty the pages, the rec_lsn() is <= the
            // page/frame lsn until the page is cleaned.
            // The ONLY time the frame lsn1 is < rec_lsn is in these
            // short periods when the page is clean but the
            // frame is fixed in EX mode and before the update is logged.
            //
            // If this assertion fails, then one of the following
            // must have gone wrong: 
            // 1: the log curr_lsn is jacked.
            // 2: the frame lsn1 got updated for a non-EX-latched page. ??
            // 3: an update got discarded but we somehow slipped in 
            // and fixed the page before the discard is complete.
            // 4: We are fixing a frame in the bpool for an uninitialized
            // page.  The page on the disk is ok per the volume format
            // but the buffer pool frame is still trash if we didn't
            // actually read the page.
        } 
    }
    DBGTHRD(<<"pid " << pid() <<" mode=" 
                    << int(mode) << " rec_lsn=" << this->curr_rec_lsn());
}


/*********************************************************************
 *
 *  class bf_filter_t
 *
 *  Abstract base class 
 *********************************************************************/
class bf_filter_t : public smlevel_0 {
public:
    bf_filter_t() {}
    virtual        ~bf_filter_t();

    virtual bool                is_good(const bfcb_t&) const = 0;
};


bf_filter_t::~bf_filter_t()
{
}


/*********************************************************************
 *  
 *  Filters for use with bf_m::_scan()
 *
 *********************************************************************/
// bf_filter_store_t : used by discard_store, force_store
class bf_filter_store_t : public bf_filter_t {
public:
    NORET                       bf_filter_store_t(const stid_t& stid);
    bool                        is_good(const bfcb_t& p) const;
private:
    stid_t                      _stid;
};

// bf_filter_vol_t : used by discard_volume, force_volume
class bf_filter_vol_t : public bf_filter_t {
public:
    NORET                       bf_filter_vol_t(const vid_t&);
    bool                        is_good(const bfcb_t& p) const;
private:
    vid_t                       _vid;
};

// bf_filter_none_t:: used by discard_all, force_all
class bf_filter_none_t : public bf_filter_t {
public:
    NORET                       bf_filter_none_t();
    bool                        is_good(const bfcb_t& p) const;
};

// bf_filter_lsn_t: used by force_until_lsn
// is_good for every page whose lsn is valid && <= that given
class bf_filter_lsn_t : public bf_filter_t {
public:
    NORET                       bf_filter_lsn_t(const lsn_t& lsn);
    bool                        is_good(const bfcb_t& p) const;
private:
    lsn_t                       _lsn;
};

// bf_filter_sweep_t : used by page cleaner
// is_good for non-zero dirty pages whose volume matches that given and
// either:
//  - page is not hot
//  - page is hot and given urgent flag is true
class bf_filter_sweep_t : public bf_filter_t {
public:
  NORET                         bf_filter_sweep_t(bool urgent, vid_t v);
    bool                        is_good(const bfcb_t& p) const;
private:
  bool                          _urgent;
    vid_t                       _vol;
};

// bf_filter_sweep_old_t : used by page cleaner
// is_good for non-zero dirty pages whose volume matches that given and
// its rec_lsn matches the given segment and
// either:
//  - page is not hot
//  - page is hot and given urgent flag is true
//
// In other words, it's the same as bf_filter_sweep_t with one add'l
// criterion: that the page_lsn is in the given segment.
// This is used to ignore the other pages and thereby give priority 
// to old pages; when the number of open log segments is > 2.
class bf_filter_sweep_old_t : public bf_filter_t {
public:
    NORET                        bf_filter_sweep_old_t( 
                                    int log_segment, 
                                    bool urgent, vid_t v);
    bool                         is_good(const bfcb_t& p) const;
private:
    int                          _segment;
    vid_t                        _vol;
    bool _urgent;
};

class page_writer_thread_t : public smthread_t 
{
    bf_page_writer_control_t* _pwc;
public:
    page_writer_thread_t() : 
             smthread_t(t_regular, "page_writer", WAIT_NOT_USED), _pwc(0) 
    { }
    void init(bf_page_writer_control_t* pwc) { _pwc = pwc; }
    virtual void run();
};


/*********************************************************************
 *
 *  class bf_cleaner_thread_t
 *
 *  Thread that flushes managers multiple slave threads, which
 *  do the writing of pages.  It shares a control buffer with the
 *  slaves, and the take responsibility for runs of pages when they
 *  are able.
 *
 *********************************************************************/

class bf_cleaner_thread_t : public smthread_t 
{
    friend class bf_m;

public:
    NORET             bf_cleaner_thread_t(vid_t);
    NORET             ~bf_cleaner_thread_t();

    void              activate(bool aggressive);
    void              _activate_impl(bool retire, bool aggressive);
    void              retire();
    virtual void      run();
    const vid_t&      vol() { return _vol; }
private:
    vid_t             _vol;
    bool              _is_running;
    bool              _retire;
    bf_page_writer_control_t    _pwc;
    page_writer_thread_t*       _page_writers;

    // set from the api
    static int        _page_writer_count;

    static int *      _histogram;

    // We won't even go to sleep on the condition variable _activate
    // if the count exceeds the cleaner's threshold
    // We dont' bother to protect this counter by the mutex: it's 
    // not needed for correctness
    int                         _kick_count;
    int                         _cleaner_threshold; // (my own)
    int                         _aggressive; // if true, do "urgent" cleaning
    pthread_mutex_t             _cleaner_mutex; // paired with _activate_cond
    pthread_cond_t              _activate_cond; // paired with _cleaner_mutex
protected:
    w_link_t                    _link;

    // disabled
    NORET              bf_cleaner_thread_t( const bf_cleaner_thread_t&);
    bf_cleaner_thread_t&  operator=(const bf_cleaner_thread_t&);

protected:
    /* shared with bf_m: */
    static int                  _dirty_threshold;
    static int                  _ndirty;
};

/*********************************************************************
 *
 *  bf_cleaner_thread_t 
 *
 *********************************************************************/

int *bf_cleaner_thread_t::_histogram = 0;

// changed at configuration
int bf_cleaner_thread_t::_page_writer_count = 1;

// Changed in constructor for bf_m:
int bf_cleaner_thread_t::_dirty_threshold = 20; 
int bf_cleaner_thread_t::_ndirty = 0;

/*********************************************************************
 *
 *  bf_cleaner_thread_t::bf_cleaner_thread_t()
 *
 *********************************************************************/
NORET
bf_cleaner_thread_t::bf_cleaner_thread_t(vid_t v)
    : smthread_t(t_time_critical, "bf_cleaner", WAIT_NOT_USED),
      _vol(v),
      _is_running(false),
      _retire(false),
      _pwc(&_retire),
      _kick_count(0),
      _cleaner_threshold(0)
{
    _is_running = true;
    _cleaner_threshold = _dirty_threshold >> 1; // just a guess for now
    if (_cleaner_threshold == 0)
        _cleaner_threshold = 1;
    DO_PTHREAD(pthread_cond_init(&_activate_cond, NULL));
    DO_PTHREAD(pthread_mutex_init(&_cleaner_mutex, NULL));

}

NORET
bf_cleaner_thread_t::~bf_cleaner_thread_t() 
{
    DO_PTHREAD(pthread_cond_destroy(&_activate_cond));
    DO_PTHREAD(pthread_mutex_destroy(&_cleaner_mutex));
}

/*********************************************************************
 *
 *  bf_cleaner_thread_t::activate(aggressive)
 *
 *  Signal the cleaner thread to wake up and do work.
 *
 *********************************************************************/
void
bf_cleaner_thread_t::activate(bool aggressive)
{
  // There are some deadlocks that come up if we grab the mutex...
  //CRITICAL_SECTION(cs, _mutex);
    _activate_impl(false, aggressive);
}

void
bf_cleaner_thread_t::_activate_impl(bool retire, bool aggressive)
{
    if(retire)
        _retire = retire;
    _aggressive = aggressive;
    _kick_count++;
    INC_TSTAT(bf_cleaner_signalled);
    DO_PTHREAD(pthread_cond_signal(&_activate_cond));
}


/*********************************************************************
 *
 *  bf_cleaner_thread_t::retire()
 *
 *  Signal the cleaner thread to wake up, stop its
 *  page cleaner threads, and exit
 *
 *********************************************************************/
void
bf_cleaner_thread_t::retire()
{
    CRITICAL_SECTION(cs, _cleaner_mutex);
    _retire = true;
    while (_is_running)  {
        _activate_impl(true, true);

        cs.pause();

        rc_t rc = join(1000); // Time is just a guess -- but it retries
        if (rc.err_num() != stTIMEOUT) {
            W_COERCE(rc);
        }
        cs.resume();
    }
}

/*********************************************************************
 *
 *  bf_cleaner_thread_t::run()
 *
 *  Body of cleaner thread. Repeatedly wait for wakeup signal,
 *  get array of dirty pages from bf, and call bf_m::_clean_buf()
 *  to flush the pages.
 *
 *********************************************************************/
void
bf_cleaner_thread_t::run()
{
    FUNC(bf_cleaner_thread_t::run);

    // fire up page writers
    // NOTE: _pwc.retire = &_retire;
    // The pointer to my retire flag is set in _pwc in the
    // constructor for this thread, so the page cleaners can check
    // it whenever they are looking for more work.

    // Check for being retired before we even begin:
   {
    CRITICAL_SECTION(cs, _cleaner_mutex);
   
    bool retire = _retire;
    if (retire)  {
        _is_running = false;
    }
     if(retire)
          return;
   } // end critical section

    if(_page_writer_count > 0) {
        _page_writers = new page_writer_thread_t[_page_writer_count];
        for(int i=0; i < _page_writer_count; i++) {
            // Make all page writers share my_page_writer_control with me
            _page_writers[i].init(&_pwc);
            // is virgin or is blocked, awaiting fork
            w_assert1( (_page_writers[i].status() == t_virgin)
                     || (_page_writers[i].status() == t_blocked));
            _page_writers[i].fork();
        }
    } else {
        _page_writers = NULL;
    }
    
#ifdef W_TRACE
    // for use with DBG()
    sthread_base_t::id_t _id = me()->id;
    DBGTHRD( << " cleaner " << _id << " activated" << endl );
#endif

    int ntimes = 0;

    // do this even if no page writers; we'll see sweeps but nothing should
    // be flushed by page writers.
    _ndirty = 0;
    while ( !_retire )  // BUG_BF_CLEANER_FIX
    {
        INC_TSTAT(bf_cleaner_sweeps);
        {
            // give other threads a chance at the mutex
            usleep(100*1000);
            CRITICAL_SECTION(cs, _cleaner_mutex); 
            /*
             *  Wait for wakeup signal
             */
            while( !_kick_count && !_retire) {
                // Not enough to warrant re-sweeping.. go to sleep
                struct timespec when;
                // 10 seconds
                sthread_t::timeout_to_timespec(10000, when);
                DO_PTHREAD_TIMED(pthread_cond_timedwait( 
                        &_activate_cond, &_cleaner_mutex, &when));
            } // else don't go to sleep - just re-sweep

            _kick_count = 0;
            if(_retire)
              break;
        }


        /*
          Figure out how much of a hurry we're in, 
          based on the number of full log segments:
          0-1: don't even bother
          2-6: take non-hot pages
          7-8: flush everything
         */
        bf_filter_t* filter = 0;
        bool prioritize_old_pages = false;
        if(smlevel_0::log) 
        {
          int oldest_segment = smlevel_0::log->global_min_lsn().file();
          int open_segments = 
                          smlevel_0::log->curr_lsn().file() - 
                          oldest_segment + 1;

          if(_aggressive || open_segments > 2) {
              prioritize_old_pages = open_segments > 4;
              filter = new bf_filter_sweep_old_t(oldest_segment, 
                                        prioritize_old_pages, vol());
          }
        }

        if(!filter && (_aggressive || _ndirty > _dirty_threshold)) 
        {
            // no log... just try to keep the pool from overfilling
            bool urgent = _ndirty > 3*smlevel_0::bf->npages()/4;
            if (_aggressive)
                urgent = true;
            filter = new bf_filter_sweep_t(urgent, vol());
        }
        if(!filter)
          continue; // never mind

        /*
         *  Get list of dirty pids
         */
        std::vector<uint32_t> bufidxes;
        {
            for (long i = 0; i < smlevel_0::bf->npages(); i++)  
            {
                bfcb_t &cb = bf_core_m::_buftab[i];
                /* 
                 * Use the refbit as an indicator of how hot it is
                 * Set it before we apply the filter.
                 * NOTE: this is racy.
                 */
                if (cb.set_hotbit(cb.refbit())) {
                    cb.decr_hotbit();
                }

                // Q: why decrement it here?
                // find() default is 0
                // unfix() default is 1
                // A: the only time it will be > 0 here is if
                // some caller of unfix or unfix_dirty  or
                // fix  explicitly gave a larger-than-one hint
                // This happens by default with certain page types (see
                // MAKEPAGE macro in *.h) like extent pages.
                // The pin API allows a vas to set the refbit on
                // a pinned page as well.
                // In any case, this bit is only a hint. 
                // It has nothing to do with the actual latch count.
                w_assert3( cb.hotbit() >= 0);

                if ( filter->is_good(cb))
                {
                    // We're not expecting null pids:
                    w_assert9(cb.pid().page != 0);
                    bufidxes.push_back(i);
                }
            }
            delete filter;
        }
        // Retire does NOT require the background flushing to finish its
        // job before it stops, but it DOES require that all its
        // subordinate threads are cleaned up before it returns.
        {
            if(_retire)
              break;
        }

        w_assert9(bufidxes.size() <= bf_m::npages());
        _histogram[bufidxes.size()]++;

        if (bufidxes.size() > 0)
        {
            /*
             * Sync the log in case any lazy transactions are pending.
             * Plus this is more efficient than small syncs when
             * pages are flushed below.
             */
            if (smlevel_0::log) { // log manager is running
                DBGTHRD(<< "flushing log");
                INC_TSTAT(bf_log_flush_all);
                W_IGNORE(smlevel_0::log->flush_all());
            }

            ++ntimes;

            // Sort the pages and delegate to slave page cleaners to
            // clean the given pages.
            w_rc_t rc = bf_m::_clean_buf( &_pwc, 
                            // smlevel_0::max_many_pages, 
                            bufidxes, WAIT_FOREVER, &_retire);
            if(rc.is_error()
               && rc.err_num() != smlevel_0::eBPFORCEFAILED) {
                // Unexpected error. Report it and choke.
                w_ostrstream trouble;
                trouble << "in bf_m::_clean_segment " << rc ;
                fprintf(stderr, "%s\n", trouble.c_str());
                W_COERCE(rc);
            }
            lintel::unsafe::atomic_fetch_sub(&_ndirty, bufidxes.size());
        }
        
    } // while !_retire
    // We exited the loop above because we were told to retire.

#ifdef W_TRACE
    DBGTHRD( << " cleaner " << _id << " retired" << endl
         << "\tswept " << ntimes << " times " << endl );
    DBGTHRD( << endl );
#endif

    // clean up page writers
    {
          CRITICAL_SECTION(cs, _pwc._pwc_lock); 
          DO_PTHREAD(pthread_cond_broadcast(&_pwc._wake_slaves));
    }

    for(int i=0; i < _page_writer_count; i++) {
          _page_writers[i].join();
          w_assert1(_page_writers[i].status() == t_defunct);
    }

    delete [] _page_writers;    
    _page_writers = NULL;
    {
        _is_running = false;
    }
}


/*********************************************************************
 *
 *  bf_m class static variables
 *
 *  _cleaner_threads : list of background thread to write dirty bf pages 
 *
 *  _core            : bf core essentially rsrc_m that manages bfcb_t
 *
 *********************************************************************/
queue_based_block_lock_t           bf_m::_cleaner_threads_list_mutex;  
cleaner_thread_list_t*             bf_m::_cleaner_threads = 0;
bf_core_m*                         bf_m::_core = 0;


/*********************************************************************
 *
 *  bf_m::bf_m(max, extra, pg_writer_cnt)
 *
 *  Constructor. Allocates shared memory and internal structures to
 *  manage "max" number of frames.
 *
 *********************************************************************/
bf_m::bf_m(uint32_t max, char *bp, uint32_t pg_writer_cnt)
{
    _core = new bf_core_m(max, bp);
    if (! _core) W_FATAL(eOUTOFMEMORY);

    // If we don't have any page writers, there's no sense in
    // creating any cleaner threads. They are fired off per-volume
    // upon mount and stopped on dismount.
    // If we don't create the list into which to keep track of them,
    // that's a signal not to fire off a thread in 
    // enable_background_flushing().
    //
    // set number of page writers at the cleaner threads
    if (pg_writer_cnt>0) {
      bf_cleaner_thread_t::_page_writer_count = pg_writer_cnt;


        _cleaner_threads = new 
            cleaner_thread_list_t(W_LIST_ARG(bf_cleaner_thread_t, _link),
                &_cleaner_threads_list_mutex    );
        if (! _cleaner_threads) W_FATAL(eOUTOFMEMORY);

        bf_cleaner_thread_t::_histogram = new int[npages()+1];
        for(int i=0; i< npages()+1; i++) bf_cleaner_thread_t::_histogram[i]=0;

        /* XXXX magic numbers.  The dirty threshold is set to a minimum
           of 100 pages OR of 1/8th of the buffer pool of pages. */

        bf_cleaner_thread_t::_dirty_threshold = npages()/8;
        if(bf_cleaner_thread_t::_dirty_threshold < 4) {
            W_FATAL_MSG(OPT_BadValue, << " Buffer pool too small. " << endl);
        }
    }
}


/*********************************************************************
 *
 *  bf_m::~bf_m()
 *
 *  Clean up
 *
 *********************************************************************/
bf_m::~bf_m()
{
    /*
     * disable_background_flushing removes all the
     * threads from the list
     */
    W_COERCE( disable_background_flushing() );
    {
        CRITICAL_SECTION(cs, _cleaner_threads_list_mutex);
        if(_cleaner_threads) {
            delete _cleaner_threads;
            _cleaner_threads = NULL;
        }
    }

    /*
     *  clean up frames
     */
    if (_core)  {
        W_COERCE( (shutdown_clean ? force_all(true) : _discard_all()) );
        delete _core;
        _core = 0;
    }

    if(bf_cleaner_thread_t::_histogram) {
        delete[] bf_cleaner_thread_t::_histogram;
        bf_cleaner_thread_t::_histogram = 0;
    }

}


/*********************************************************************
 *
 *  bf_m::mem_needed(int npages)
 *
 *  Return the amount of shared memory needed (in bytes)
 *  for the given number of pages.
 *
 *********************************************************************/
long
bf_m::mem_needed(int n)
{
    return sizeof(page_s) * n;
}

/*********************************************************************
 *
 *  bf_m::force_my_dirty_old_pages(wal_page)
 *
 *  For now, this is just a hook -- see 
 *  comments in xct_impl.cpp where this is called.
 *
 *********************************************************************/
bool bf_m::force_my_dirty_old_pages(lpid_t const* wal_page) const
{
    return _core->force_my_dirty_old_pages(wal_page);
}

/*********************************************************************
 *
 *  bf_m::get_cb(page)
 *
 *  Given a frame, compute and return a pointer to its
 *  buffer control block.
 *  NB: this does NOT indicate whether the frame is in the hash table.
 *
 *********************************************************************/
bfcb_t* bf_m::get_cb(const page_s* p) 
{
    int idx = p - bf_core_m::_bufpool;
    return (idx<0 || idx>=bf_core_m::_num_bufs) ? 0 : bf_core_m::_buftab + idx;
}


/*********************************************************************
 *
 *  bf_m::is_bf_page(const page_s* p, bool and_in_htab = true)
 *   and_in_htab = true means we want to return true iff it's
 *   a legit frame *and* it's in the hash table.  OW return true
 *   if it's a legit frame, even if in transit or free.
 *
 *********************************************************************/
bool
bf_m::is_bf_page(const page_s* p, bool and_in_htab /* = true */)
{
    bfcb_t *b = get_cb(p);
    return b ? (and_in_htab ?  _core->_in_htab(b) : true) : false;
}



/*********************************************************************
 * bf_m::is_cached(b)
 **********************************************************************/
bool
bf_m::is_cached(const bfcb_t* b)
{
        return _core->_in_htab(b);
}


/*********************************************************************
 *
 *  bf_m::_fix(ret_page, pid, tag, mode, no_read, ret_store_flags, 
 *             ignore_store_id, store_flags)
 *
 *  Fix a frame for "pid" in buffer pool in latch "mode". "No_read"
 *  indicates whether bf_m should read the page from disk if it is
 *  not cached. "Ignore_store_id" indicates whether the store ID
 *  on the page can be trusted to match pid.store; usually it can,
 *  but if not, then passing in true avoids an extra assert check.
 *  The frame is returned in "ret_page".
 *
 *********************************************************************/
extern "C" void bfstophere() {
}
#if W_DEBUG_LEVEL > 0
extern "C" void bfstophere1(int line) {
    cerr << "PRINT MY LATCHES at line " << line << endl;
    print_my_latches();
    cerr << "PRINT ALL LATCHES at line " << line << endl;
    print_all_latches();
    bfstophere();
}
#else
extern "C" void bfstophere1(int /*line*/) {
    bfstophere();
}
#endif
rc_t
bf_m::_fix(
    timeout_in_ms        timeout,
    page_s*&             ret_page,
    const lpid_t&        pid,
    uint16_t              tag,            // page_t::tag_t
    latch_mode_t         mode,
    bool                 no_read,
    store_flag_t&        return_store_flags,
    bool                 ignore_store_id, // default = false
    store_flag_t         store_flags    // page_t::store_flag_t (for case no_read==true)
  )
{
    FUNC(bf_m::_fix);
    DBGTHRD( << "about to fix " << pid 
            << " mode " <<  int(mode)  );
/* this is not true any more because we call btree_p::init_fix_steal() in redo
#if W_DEBUG_LEVEL > 2
    if(smlevel_0::operating_mode == smlevel_0::t_in_redo) {
        w_assert3(!no_read);
        w_assert3(store_flags == st_bad);
        //Note that in redo, we always read the page.
        //We need not for formatting, but we don't know whether
        //to apply the format log record until we know the page's lsn,
        //so we fix the page first, and at that point, we don't know
        //the store flags.
        
        
    }
#endif 
*/
    ret_page = 0;

    bool         found=false; 
    bfcb_t*      b;
    rc_t         rc;

    /* 
     * the whole point of this section is to
     * avoid replacing dirty pages.  If there aren't
     * any clean pages left for replacement, we wait
     * until the cleaner has done its job
     */
    {
#if defined(EXPENSIVE_LATCH_COUNTS) && EXPENSIVE_LATCH_COUNTS>0
        base_stat_t *stat(NULL);
        switch(tag) {
        case page_p::t_btree_p: stat = &(GET_TSTAT(bf_hit_wait_btree_p)); break;
        case page_p::t_alloc_p: stat = &(GET_TSTAT(bf_hit_wait_alloc_p));break;
        case page_p::t_stnode_p: stat=&(GET_TSTAT(bf_hit_wait_stnode_p)); break;

        default:
        case page_p::t_any_p: stat = & GET_TSTAT(bf_hit_wait_any_p ); break;
        }
        int before = *((volatile base_stat_t *volatile) stat);

        rc = _core->find(b, bfpid_t(pid), mode, timeout, 0/*ref_bit*/, stat);
        int after = *((volatile base_stat_t *volatile) stat);
        if(after > before && tag == page_p::t_any_p) {
            bfstophere1(__LINE__);
        }
#else
        rc = _core->find(b, bfpid_t(pid), mode, timeout);
#endif
        DBGTHRD( << "fix : find " << pid << " returns " << rc); 
        if(!rc.is_error()) {
            // latch already acquired
            w_assert1(b);
            w_assert1(b->latch.is_mine() || 
                    (mode == LATCH_SH && b->latch.held_by_me())
                    );
            found=true; 
            DBGTHRD(
            << "seek pid " << pid
            << "found frame " << ::hex 
                << (unsigned long) (b->frame()) << ::dec
            << " frame's pid " << b->frame()->pid);
            // NOTE: the store could have changed, so we compare pages here.
            //
            w_assert2(no_read || (pid.page == b->frame()->pid.page) 
             || smlevel_0::operating_mode == t_in_redo);

        } else if( _cleaner_threads && ! _cleaner_threads->is_empty() ) {
            DBGTHRD( << "fix : kick cleaner " ); 
            // We have at least one cleaner thread -- kick it/them
            if(bf_cleaner_thread_t::_ndirty == npages()) {
                INC_TSTAT(bf_kick_full);
                activate_background_flushing();
            }
            // and await a clean page to grab -- try this only once
            if (bf_cleaner_thread_t::_ndirty == npages()) {
                INC_TSTAT(bf_sleep_await_clean);
                // If we're using preemptive threads,
                // we have to 
                // give the cleaner time to work:
                me()->sleep(10);
            }
        }
    }

    DBGTHRD( << "middle of fix " << pid 
            << " in mode " <<  int(mode)  
            << " found " <<  int(found)  
            );

    if(!found) {
        bfcb_t* v = _core->replacement(); 
        if(!v) return RC(fcFULL);

        // Now replacement() gives us the latch  and we hold it through
        // _replace_out.
        w_assert1(v->latch.is_mine() == true); 
        w_assert1(v->latch.held_by_me() == true); 
        w_assert1(v->latch.held_by_me() == 1);  // had better not be > 1 at this point

        /* No lock held on v.  V is not in the hash table now.
         * It came from the free list (old_pid_valid() == false) 
         *    (is a yet-unused frame, as opposed to a replacement)
         * or has old_pid_valid() == true (is an in-use frame,
         *    a.k.a. replacement) and
         *    a) is now on the in-transit-out list (if it is dirty)
         *    or
         *    b) is not on the in-transit-out list (if it is clean)
         */
        if(v->old_pid_valid()) {
            /*
             *  v is a replacement (in-use frame). If it's  dirty,
             *  it's on the in-transit-out list and we need to
             *  get it to disk, then publish_partial()
             *  to inform bf_core_m that the old page has been 
             *  flushed out.
             */
            if (v->dirty())  {
                INC_TSTAT(bf_kick_replacement);
                vid_t vid = v->pid().vol();
                activate_background_flushing(&vid);
                // Grab the page_write_mutex and force the frame to disk.
                CRITICAL_SECTION(cs, page_write_mutex_t::locate(v->pid()));
                w_assert0(!v->old_rec_lsn().valid());//never set if mutex free!
                // Don't need to re-check the page status here because 
                // replacement() removed  it from the hash table.
                W_COERCE(_replace_out(v));
            } else {
                // not dirty but could be a replacement (clean frame) or
                // an unused frame. If the first case, we need to wait for
                // any cleaner that's writing the page to be done with it.
                // NOTE 1: replacement() tried to avoid picking up such
                // pages, but it's possible that a cleaner slipped in in
                // the meantime and started to clean it.  
                // (TODO: Is it indeed possible,
                // what with our holding the latch?)
                // NOTE 2: it's marked clean but a cleaner could be
                // writing it anyway, since the cleaner copies the page
                // and immediately marks it clean, even before the copy
                // gets to disk. We can tell that the copy made it to
                // disk by the old_rec_lsn, which gets invalidated by
                // the cleaner when the page is durable.
                if(v->old_rec_lsn().valid()) {
                    // grab just long enough to make sure no page 
                    // cleaning is going
                    INC_TSTAT(bf_awaited_cleaner);
                    CRITICAL_SECTION(cs, page_write_mutex_t::locate(v->pid()));
                }
                INC_TSTAT(bf_replaced_clean);
            }
            // Now the frame is cleaned, we can tell the bf_core_m that
            // it's no longer on the in-transit list.
            _core->publish_partial(v);
        } // old_pid_valid

        w_assert1(v->latch.is_mine()); // EX-mode.
        w_assert1(v->latch.held_by_me() == 1);  // had better not be > 1 at this point

        b = v;
        
        // replacement frame. Its pid is possibly garbage in this debug trace:
        DBGTHRD( 
            << " frame " << ::hex << (unsigned long) (b->frame()) << ::dec
            << " frame pid " << b->frame()->pid 
            << " is a replacement ");

        rc = _core->grab(v, pid, found, mode, timeout); 
        // Grab acquires an EX latch on the frame v;
        // if the replacement (v) is not used, 
        // the EX latch is released and the given-mode-latch is 
        // acquired on the page that it finds in the pool.
        // On the other hand, if the replacement 
        // is to be used (read into...), the EX latch is still held.
        // Consequently, we may have mode-latch or an EX-latch on the frame v.
        DBGTHRD( << "middle of fix: grab " << pid << " returns rc=" << rc 
                << " found " << found
            << " frame " << ::hex << (unsigned long) (v->frame()) << ::dec
            << " frame pid " << v->frame()->pid  );

        if (rc.is_error()) {
            return rc.reset();
        }
        /* sanity checks on the meaning of grab:
         */
        w_assert2( found? (b != v) : (b == v) );
        w_assert2(
            found? (v->latch.mode() == mode) : (b->latch.mode() == LATCH_EX)
        ); 
        
        if(found) {
            // the victim (the value of v passed to grab())
            // got added back into the freelist and v is the 
            // existing frame in the buffer pool.
            w_assert2( b != v) ;
            w_assert2( v->latch.mode() == mode) ;
            w_assert2( v->latch.held_by_me() > 0) ; // could be > 1 :
            // we have it latched more than once, even if in EX mode.
            b = v;
        }
        else  {
            w_assert0(b == v);
            w_assert0(b->latch.mode() == LATCH_EX);
        }
    }
    /* Whether b is a replacement or came off the
     * in-transit-out list, it is not in the hash table now, nor
     * is it in the unused list.
     */

    w_assert1(b);

    /* We have a latch (given mode, or possibly EX) on frame in v */

    if (found)  {
        /*
         * Page was found.  Store id might not be correct, but
         * page and volume match.
         * Well, page and volume match in the control block. If the
         * frame is a replacement, the page itself could contain anything
         * for a pid, as we might be about to format it.
         */
        DBGTHRD( << "found page " << pid << " b->pid=" << ((lpid_t)b->pid())
             << " no_read " << no_read 
             << " store_flags " << store_flags 
             << " operating_mode " << smlevel_0::operating_mode
             << " page lsn=" << b->frame()->lsn
            << " found frame " << ::hex << (unsigned long) (b->frame()) << ::dec
            << " frame pid " << b->frame()->pid 
        );
        /*
         *  set store flag and perhaps store id.
         *  
         */
        if( ((lpid_t)b->pid() != pid) || no_read ) {

            // If we're not in redo:
            // Copy the store flags from the store head to the frame
            // 
            // The page could have changed stores. Get the correct store
            // and update the store flags.
            // We really only need to do this copy if it's a "virgin" page
            // (in which case, it's called with no_read == true)
            //
            // The page might have changed from a temp to a non-temp file, 
            // by virtue of the pages having been discarded, 
            // but not actually modified. 
            //
            // Use the store_flags given, if any.
            //
            // NOTE: We cannot trust the store flags in the store node to
            // be correct in redo. In redo, we have to trust the flags
            // on the page.
            //
            if(smlevel_0::operating_mode != smlevel_0::t_in_redo) {

                if (store_flags == st_bad)  // we just "read" a zeroed page
                {
                    DBGTHRD( << "get store flags for pid " << pid);
                    W_DO( io->get_store_flags(pid.stid(), store_flags) );
                    if(smlevel_0::operating_mode == t_forward_processing) {
                        w_assert1(store_flags != st_bad);
                    }
                }

                w_assert9(store_flags  <= 0xF); // for now -- see page.h

                if (store_flags & st_insert_file)  {
                    // WHY ARE WE DOING THIS???
                    /* Because for most page types, there is no such
                     * thing as st_insert_file.  It only makes sense
                     * for files, hence for t_file_p
                     * pages.  For all others, only t_load_file and t_tmp
                     * and t_regular make sense. Furthermore, insert_file
                     * pages get converted to st_regular eventually...
                     * So rather than special-case the page types here
                     * (where we don't have those types #included), we
                     * let the page-type-specific fix code make the change.
                     */
                    b->set_storeflags(st_regular);
                }  else {
                    DBGTHRD( << "set store flags  to " << store_flags 
                                        << " on pid " << pid);
                    b->set_storeflags(store_flags);
                }
            } else {
                // Just trust what's on the page.  Stuff these flags into
                // the bfcb_t.
                store_flags =  (store_flag_t)b->read_page_storeflags();
            }

            b->set_pid(pid); // to set the store id as well as the page id
            w_assert2(pid.page != 0); // should never try to fix page 0
        }
    } else {
        /*
         * Page not found, have to read it or get a new frame 
         */

        DBGTHRD( << "not found; read page " << pid << " in mode " << int(mode));
        /*
         *  Read the page and update store_flags. If error occurs,
         *  call _core->publish( .. error) with error flag to inform
         *  bf_core_m that the frame is bad.
         */
        rc = get_page(pid, b, tag, no_read, ignore_store_id);
        DBGTHRD( << "get_page " << pid << " returns " << rc );
        if (rc.is_error()) {
            // At this writing, the only error could be eBADVOL
            w_assert1(rc.err_num() == eBADVOL);

            // publish will leave us with the given latch mode 
            // LATCH_NL means it'll be released.
            _core->publish(b, LATCH_NL, rc);
            return rc.reset();
        }
            
        DBGTHRD( << "got " << pid 
            << "found frame " << ::hex << (unsigned long) (b->frame()) << ::dec
            << " frame pid " << b->frame()->pid 
            << " store_flags " << store_flags
            );

        /*
         * Deal with store flags.
         * In redo, the store flags in the stnode_p might be wrong;
         * it all depends on in what order pages made it to disk.
         * By the time we are done with redo though, the stnode_p should
         * be correct
         */
        if(smlevel_0::operating_mode != smlevel_0::t_in_redo) 
        {
            if (store_flags == st_bad)  // we just "read" a zeroed page
            {
                DBGTHRD( << "getting store flags for store  " << pid.stid()
                        << "{");

                // fixes the store node page:
                rc = io->get_store_flags(pid.stid(), store_flags);
                DBGTHRD( << "get_store_flags returned " << rc);
                if (rc.is_error() ) {
                    // At this writing, the only error could be eBADVOL or
                    // eBADSTID. The latter can happen if we recently
                    // remove this page from the store and the page hasn't
                    // been discarded from the bpool (yet).
                    // An application could be trying to fix a now-obsolete
                    // record in the page.  The eBADSTID should propogate
                    // up to the app.
                    w_assert0(rc.err_num() == eBADVOL || 
                              rc.err_num() == eBADSTID ||
                              rc.err_num() == fcFULL);

                    // publish will leave us with the given latch mode 
                    // LATCH_NL means it'll be released.
                    // The page remains in the htab.
                    // Presumably the page will get re-used by a no-read
                    // fix().
                    _core->publish(b, LATCH_NL, rc);
                    DBGTHRD( << "error } " << pid );
                    return rc.reset();
                }
                DBGTHRD( << "} store_flag " << store_flags );
                // NOTE: this page/store node might not even be 
                // allocated now.
            }

            w_assert9(store_flags  <= 0xF); // for now -- see page.h
            // The following assert does not hold because when we
            // fix prior to a format, we invariably have st_bad.
            // w_assert1(store_flags != st_bad);

            if (!no_read && store_flags & st_insert_file)  {
               /* Convert to regular because this is !no_read
                * case, i.e., it's not a virgin page, i.e., it
                * was once written to disk as a tmp but is now
                * to become regular.  Granted, this might be
                * changed too soon, as it might have been flushed
                * to disk in the same tx that allocated the page,
                * but there's not much we can do about that without
                * keeping the tx id on the page.
                */
                store_flags = st_regular;
            }
            DBGTHRD( 
                << "set store flags to " << store_flags << " for pid " << pid
                << "frame " << ::hex << (unsigned long) (b->frame()) << ::dec
                << " frame's pid " << b->frame()->pid
                    );

            b->set_storeflags(store_flags);
        } else {
            store_flags = (store_flag_t)b->read_page_storeflags();
        }

        b->set_pid(pid);
        w_assert2(pid.page != 0); // should never try to fix page 0
        w_assert2(!b->dirty());                 // dirty flag and rec_lsn are
        w_assert2(b->curr_rec_lsn() == lsn_t::null);// cleared inside ::_replace_out

        // publish will leave us with the given latch mode,
        // downgrading or releasing the latch as necessary
        _core->publish(b, mode, RCOK /* no error occurred */);
    }

    /*
     * At this point, we should have the called-for pid in the
     * control block.  The store should be correct, also.  The
     * page, however, might not contain that pid.  There are 2
     * possible reasons:  
     *
     *   1) we're getting a new frame and we
     * plan to format the page (called from page_p::fix()), in
     * which case no_read is true.
     * 
     *   2) we're reading a page during recovery and we're
     * going to have to apply log records to get it back into
     * its correct state.  (The log record we're applying
     * is probably a format_page.) In this case, ignore_store_id
     * is true.
     */
    DBGTHRD( << "success " << pid
            << " frame " << ::hex << (unsigned long) (b->frame()) << ::dec
            << " frame's pid " << b->frame()->pid
            );

    w_assert2(((lpid_t)b->pid()) == pid); // compares stores too
    w_assert2( (b->frame()->pid == pid) || 
            no_read || ignore_store_id); // compares stores too

    w_assert1(b->latch.mode() >= mode);
    b->update_rec_lsn(mode, !no_read);

    DBGTHRD(<<"pid " << pid <<" mode=" 
                << int(mode) << " rec_lsn=" << b->curr_rec_lsn());

    INC_TSTAT(bf_fix_cnt);

    ret_page = b->frame_nonconst();
    return_store_flags = (store_flag_t)b->get_storeflags();

    w_assert9(_core->latch_mode(b) >= mode);

    w_assert1(b->pin_cnt() > 0);

    DBGTHRD( << "fixed " << pid 
            << " noread " << no_read
            << " frame " << ::hex << (unsigned long) (b->frame()) << ::dec
            << " frame's pid " << b->frame()->pid
            << " mode " <<  int(mode)  );
    return RCOK;
}



/*********************************************************************
 *
 *  bf_m::refix(buf, mode)
 *
 *  Fix "buf" again in "mode".
 *
 *********************************************************************/
w_rc_t
bf_m::refix(const page_s* buf, latch_mode_t mode)
{
    FUNC(bf_m::refix);
    DBGTHRD(<<"about to refix " << buf->pid 
            << " frame " << ::hex << (unsigned long) (buf) << ::dec
            << " mode" << int(mode));

    bfcb_t* b = get_cb(buf);
    w_assert1(b && b->frame() == buf);

    // Since we're taking one fixed frame and
    // fixing it again, pin() should NEVER return eFRAMENOTFOUND
    W_COERCE( _core->pin(b, mode));

    b->update_rec_lsn(mode, true);
    w_assert9(_core->latch_mode(b) >= mode);
    DBGTHRD(<<"mode=" <<  int(mode) << " rec_lsn=" << b->curr_rec_lsn());

    INC_TSTAT(bf_refix_cnt);
    return RCOK;
}

/*********************************************************************
 *
 *  latch_mode_t bf_m::latch_mode(buf)
 *
 *  returns latch mode
 *
 *********************************************************************/
latch_mode_t
bf_m::latch_mode(const page_s* buf)
{
    bfcb_t* b = get_cb(buf);
    w_assert1(b && b->frame() == buf);
    return _core->latch_mode(b);
}

void
bf_m::downgrade_latch(page_s*& buf)
{
    bfcb_t* b = get_cb(buf);
    w_assert1(b && b->frame() == buf);
    _core->downgrade_latch(b);
}

/*********************************************************************
 *
 *  bf_m::upgrade_latch(buf, mode)
 *
 *  Upgrade the latch on buf.
 *
 *********************************************************************/
void
bf_m::upgrade_latch(page_s*& buf, latch_mode_t        m)
{
    FUNC(bf_m::upgrade_latch);
    INC_TSTAT(bf_upgrade_latch_unconditional);
    DBGTHRD(<<"about to upgrade latch on " << buf->pid << " to mode " 
            <<  int(m));

    bool would_block;
    bfcb_t* b = get_cb(buf);
    w_assert1(b && b->frame() == buf);
    _core->upgrade_latch_if_not_block(b, would_block);
    if(!would_block) {
        w_assert9(b->latch.mode() >= m);
    }
    if(would_block) {
        INC_TSTAT(bf_upgrade_latch_race);
#ifndef __GNUC__
#warning we release the latch on upgrade without warning our caller;
#warning if they depend on the latch not changing they will break
// This is noted as BUG_LATCH_RACE
#endif
                
        w_assert9(!b->latch.is_mine());
        const lpid_t&       pid = buf->pid;
        uint16_t             tag = buf->tag;
        lsn_t old_page_lsn = buf->lsn;
        unfix(buf, false, 1);

        // This can actually happen (we could hold the latch more than once), 
        // but we can't safely continue because it would risk the very 
        // deadlock that unfix() was supposed to avoid...
        // So if we hit this assert, we had better find the source of
        // the multiple-latching.
        w_assert1(!b->latch.held_by_me());
        
        // possibly block here:
        store_flag_t        store_flags;
        W_COERCE(fix(buf, pid, tag, m, false, store_flags, false));
        w_assert9(b->latch.mode() == m);
        if(buf->lsn != old_page_lsn) {
            INC_TSTAT(bf_upgrade_latch_changed);
        }
        
    }
    w_assert9(b->latch.mode() >= m);

    DBGTHRD(<<"mode of latch on " << buf->pid 
        << " is " << int(b->latch.mode()));

    w_assert9( _core->latch_mode(b) >= m );
    DBGTHRD(<<"core mode of latch on " << buf->pid << " is " 
        <<  int(_core->latch_mode(b)));

    b->update_rec_lsn(m, true);
    DBGTHRD(<<"mode=" <<  int(m) << " rec_lsn=" << b->curr_rec_lsn());
}
/*********************************************************************
 *
 *  bf_m::upgrade_latch_if_not_block(buf, would_block)
 *
 *  Upgrade the latch on buf, only if it would not block.
 *  Set would_block to true if upgrade would block
 *
 *********************************************************************/
void
bf_m::upgrade_latch_if_not_block(const page_s* buf, bool& would_block)
{
    DBGTHRD(<<"about to upgrade latch on " << buf->pid );
    bfcb_t* b = get_cb(buf);
    w_assert1(b && b->frame() == buf);
    _core->upgrade_latch_if_not_block(b, would_block);

    // Have to update the rec_lsn so that we maintain
    // the invariant that if this is an EX lock, the
    // rec_lsn isn't null
    latch_mode_t m = _core->latch_mode(b);
    if(m == LATCH_EX ) {
        b->update_rec_lsn(m, true);
    }
}



/*********************************************************************
 *
 *  bf_m::fixed_by_me(buf) 
 *  return true if the given frame is fixed by this thread
 *  bf_m::is_mine(buf) 
 *  return true if the given frame is latched in EX mode  by this thread
 *
 *********************************************************************/
bool
bf_m::fixed_by_me(const page_s* buf) 
{
    FUNC(bf_m::fixed_by_me);
    bfcb_t* b = get_cb(buf);
    w_assert1(b && b->frame() == buf);
    return _core->latched_by_me(b);
}

bool
bf_m::is_mine(const page_s* buf) 
{
    FUNC(bf_m::is_mine);
    bfcb_t* b = get_cb(buf);
    if(b==NULL) return false;
    w_assert1(b && b->frame() == buf);
    return _core->is_mine(b);
}

const latch_t*             
bf_m::my_latch(const page_s* buf) 
{
    FUNC(bf_m::my_latch);
    bfcb_t* b = get_cb(buf);
    w_assert1(b && b->frame() == buf);
    return _core->my_latch(b);
}

/*********************************************************************
 *
 *  bf_m::unfix(buf, dirty, ref_bit)
 *
 *  Unfix the buffer "buf". If "dirty" is true, set the dirty bit.
 *  "Refbit" is a page-replacement policy hint to rsrc_m. It indicates
 *  the importance of keeping the page in the buffer pool. If a page
 *  is marked with a 0 ref_bit, rsrc_m will mark it as the next
 *  replacement candidate.
 *
 *  The only place dirty== true is by virtue of unfix_dirty /
 *  page_p::unfix_dirty, which is when called from xct_impl::give_logbuf
 *  after a log record is written. Since st_tmp pages don't
 *  get a log record written, they instead get a set_dirty() call.
 *
 *********************************************************************/
void
bf_m::unfix(const page_s* buf, bool dirty, int ref_bit)
{
    FUNC(bf_m::unfix);
    bool    kick_cleaner = false;
    bfcb_t* b = get_cb(buf);
    w_assert1(b && b->frame() == buf);
    w_assert1(b->pin_cnt() > 0);
    w_assert1(b->latch.held_by_me()); 

    DBGTHRD( << "about to unfix " << b->pid() 
            << " frame " << ::hex << (unsigned long) (buf) << ::dec
            << " frame's pid " << b->frame()->pid
            << " dirty arg " << dirty
            << " refbit arg " << ref_bit
            << " w/ curr_rec_lsn " << b->curr_rec_lsn() 
            << " w/frame dirty " << b->dirty() );

    // Calling this with dirty==true overrides any notion of whether
    // the page's lsn is recent. We can mark a page as dirty in the
    // control block even if the lsn was never updated, say, if 
    // logging is turned off or temporarily disabled or the page is st_tmp.
    // However the only time this is called with dirty==true is
    // after some logging, which should have set the lsns on the pages.
    if (dirty)  {
        if( _set_dirty(b) ) kick_cleaner = true;
        w_assert2( b->dirty() );
    } else {
        /* not setting dirty with this unfix, but it might be dirty
         * because it was dirtied earlier.
         */
        if (! b->dirty()) {
            /*
             * Don't clear the lsn if we're not unfixing for the last
             * time. Note that we don't care about pin count so much
             * as latch count -- threads waiting on the latch will
             * have pinned the page but should not be allowed to
             * impact our decision now.
             */
            /* Even if this thread "owns" the page, i.e., has it
             * latched exclusively, it could have pinned the page 
             * multiple times, and one of the others could have set the
             * rec_lsn and will unfix with dirty=true. We mustn't
             * clobber that rec_lsn unless we are the last release.
             *
             * If two or more threads have it latched, they
             * all have it in shared mode, and then it should already 
             * have a null lsn anyway.
             *
             * *************************************************************
             * FRJ NOTE: Checking latches instead of pin counts avoids
             * the CLEAN_REC_LSN_RACE mentioned in bf_core.cpp because
             * the check depends only on the calling thread and
             * therefore can't race.
             * *************************************************************
             *
             * Note: there's yet another scenario, which is that
             * pin_cnt > 1 because another thread has pinned it and is
             * waiting for the latch which we have in EX mode,
             * or has unlatched it (SH) and hasn't yet updated the pin count.
             * So there was a race here, in which we find the pin count > 1
             * even though it's about to become 1, in which case,
             * we missed a chance to clean the page, but a page cleaner
             * would eventually take care of that.
             * GNATS 64. See also GNATS 64 in bf_core.cpp and
             * CLEAN_REC_LSN_RACE in bf_core.cpp.
             * orig code: if(b->pin_cnt()) < = 1 .. 
             * now: check latches. 
             */
            if(b->latch.is_mine() && b->latch.held_by_me() == 1) {
                // If dirty should be fixed in EX mode.
                { w_assert0(dirty ? b->latch.is_mine(): true); }
                
                // mark_clean clears the lsn as well as the dirty bit
                b->mark_clean();
                DBGTHRD(<<"marked clean "
                    << " frame " << ::hex << (unsigned long) (buf) << ::dec
                    << " frame's pid " << b->frame()->pid
                    );
                INC_TSTAT(bf_unfix_cleaned);
            }
        }

        // See if it's really clean despite what the control block says.
        // All bets are off if we're not logging b/c the
        // lsn on the page tells us nothing unless we're logging.
        if (b->dirty() && (!b->frame()->lsn.valid())) {
            if( smlevel_0::log && smlevel_0::logging_enabled ) {
            /*
            * Control block marked dirty but page isn't
            * really dirty. 
            * Fix the control block's
            * idea of dirty/not dirty so that we don't run into
            * the otherwise-legit situation at XXXYYY below.
            */
            /* Note that when a page is formatted (st_tmp or
             * otherwise), the lsn on the page is the xct's latest
             * lsn (which should be non-null since the page allocations
             * are formatted, and anyway, before the give_logbuf returns,
             * the page_lsn is updated)
             * So we no longer see lsn_t(0,1) as originally implemented,
             * in the page_format case for a transaction.
             *
             * Pages may be formatted (e.g., the extent pages) when a
             * volume is formatted, and they will have lsn_t(0,1), which
             * really should not fail, and certainly should not give us
             * tmp pages.
             *
             * That should mean that we only get here when a clean page
             * was fixed in EX mode and unfixed w/o any change.
             * And that should probably NEVER be a tmp page.
             */

            // Temp pages don't get logged (this is enforced in the
            // generated log functions), so their page lsns don't
            // get automagically updated (that happens after the
            // log record with that lsn is written).
            // However, temp pages should start out life (formatted) with
            // a valid lsn, the xct's latest lsn. For that reason,
            // we shouldn't get here if the page is tmp (invalid page/frame lsn)
            w_assert1 ((b->get_storeflags() & smlevel_0::st_tmp) == 0);

            // Thus, we must have the case where the page is not
            // really dirty even though it was marked dirty in the
            // control block.
            w_assert0(b->latch.is_mine());
            if(b->pin_cnt() <= 1) {
                // Make it clean.
                b->mark_clean();
                // If dirty should be fixed in EX mode.
                { w_assert0(dirty ? b->latch.is_mine(): true); }
            }
            } 
        } // b->dirty() but page not really dirty


#if W_DEBUG_LEVEL > 2
        if(b->dirty()) {
            // see comments in similar assert in set_dirty()
            if(log) w_assert3( b->curr_rec_lsn() != lsn_t::null || b->pin_cnt() > 0); 
        }
#endif 

    }

    DBGTHRD( << "about to unfix " << b->pid() 
            << " frame " << ::hex << (unsigned long) (buf) << ::dec
            << " frame's pid " << b->frame()->pid
            << " w/lsn " << b->curr_rec_lsn() );
    w_assert1(b->pin_cnt() > 0);

    vid_t        v = b->pid().vol();
    _core->unpin(b, ref_bit);
    // b is invalid now
    INC_TSTAT(bf_unfix_cnt);

    buf = 0;
    if (kick_cleaner) {
        activate_background_flushing(&v);
    }
}


/*********************************************************************
 *
 *  bf_m::discard_pinned_page(page)
 *
 *  Remove page "pid" from the buffer pool. Note that the page
 *  is not flushed even if it is dirty.
 *
 *********************************************************************/
void
bf_m::discard_pinned_page(const page_s* buf)
{
    FUNC(bf_m::discard_pinned_page);
    bfcb_t* b = get_cb(buf);
    w_assert1(b && b->frame() == buf);
    // Can only discard ex-latched pages.
    // For one thing, that's all you want to discard,
    // for another, you must be the only thread with
    // a latch on this page if you're going to discard it,
    // so you don't discard out from under another thread.
    // latch.is_mine() returns true IFF you have an EX latch
    w_assert2(b->latch.is_mine());
#if W_DEBUG_LEVEL > 0
    // Let's trash the frame in a recognizable way.
    // We need to flush out bogus assertions; this will help.
    (void) memset(const_cast<page_s *>(buf), 'e', SM_PAGESIZE); // 'e' is 0x65
#endif
    {
        bfcb_t* tmp = b; // so we can check asserts below
        shpid_t pagenum = b->pid().page;
        w_rc_t rc = _core->remove(tmp);
        if (rc.is_error())  { // releases the latch
            fprintf(stderr, "page %d remove failed with err num %d\n",
                    pagenum,
                    rc.err_num());

            /* ignore */ ;
            w_assert0(!b->latch.is_mine());
            // racy w_assert0(b->pid() == lpid_t::null);
        } // there really is no else for this
    }
}

void page_writer_thread_t::run() 
{
    // The largest number of pages I'll claim at any time.
    // This limits the size of the buffer that I'll need.
    const int largest_claim = 8*smlevel_0::max_many_pages;

    // pbuf is for copies of the buffer pool pages.
    page_s* pbuf = new page_s[largest_claim];

    w_auto_delete_array_t<page_s> delete_pbuf(pbuf);

    int count = 0;
    w_rc_t rc;
    w_assert1(_pwc); 
    while(1) 
    {
        lpid_t* pids;
        {
            // Shared with other threads: bf_cleaner_thread_t and
            // other page_writer_threads,so we have to lock it.
            CRITICAL_SECTION(cs, _pwc->_pwc_lock); 

            /* Report our previous run back to the master */
            _pwc->pages_written += count;

            bool wake_master = false;
            if(_pwc->pages_written == _pwc->pages_submitted)
                wake_master = true;

            if(rc.is_error()) {
                _pwc->thread_rc = rc;
                wake_master = true;
            }

            if(wake_master) {
                DO_PTHREAD(pthread_cond_signal(&_pwc->_wake_master));
            }

            // now start the next loop
            // Exit this little loop when pages submitted exceeds
            // pages_claimed by other threads, which is to say,
            // when there's more work to do that another thread hasn't
            // taken responsibility for.
            while(_pwc->vthis()->pages_claimed >= 
                    _pwc->vthis()->pages_submitted) 
            {
               bool* retire = _pwc->vthis()->retire;
               if(retire && *retire) 
                   goto done;

               // Give up the lock and re-acquire it when awakened
               DO_PTHREAD(pthread_cond_wait(&_pwc->_wake_slaves, 
                           &_pwc->_pwc_lock));

            }
            
            // index <-  starting point of my chunk
            int index = _pwc->pages_claimed; 
            int end = std::min(_pwc->pages_submitted, index + largest_claim);
            // pages_claimed <- end of my chunk
            _pwc->pages_claimed = end;      
            
            count = end - index;
            // We should never get here with no pids ptr.
            // The pointer goes to 0 when everybody is done.
            w_assert1(_pwc->pids != NULL);
            pids = _pwc->pids+index;
        }

        // now do the actual page writes
        rc = smlevel_0::bf->_clean_segment(count, pids, pbuf, 
                WAIT_IMMEDIATE, &_pwc->cancelslaves );

        // we don't care if cleaning failed for normal reasons
        if(rc.is_error()
           && rc.err_num() == smlevel_0::eBPFORCEFAILED) {
            rc = RCOK; // avoid an error-not-checked when this
                       // is an eBPFORCEFAILED (GNATS 133)
                       // Is enough to fix the bug?
        } else {
            // choke here
            w_assert0(!rc.is_error()
              || rc.err_num() == smlevel_0::eBPFORCEFAILED);
        }

    }
 done:
    return;
}

/*********************************************************************
 *
 *  bf_m::_clean_buf(pwc, mincontig, count, pids, timeout, retire_flag)
 *
 *  Sort pids array (of "count" elements) and write out those pages
 *  in the buffer pool. Retire_flag points to a bool flag that any
 *  one can set to cause _clean_buf() to return prematurely.
 *
 *  Callers: _scan, bf_cleaner_thread_t::run()
 *
 *********************************************************************/

rc_t
bf_m::_clean_buf(
    bf_page_writer_control_t * pwc, // if null, do serial,synchronous cleaning
                                      // else do parallel async cleaning w/
                                      // slave threads
    const std::vector<uint32_t>& bufidxes, // indexes of chosen buftab[] entries
    timeout_in_ms              timeout, // WAIT_FOREVER or WAIT_IMMEDIATE
    bool*                      retire_flag)
{
    if (bufidxes.empty()) {
        return RCOK;
    }
    // separate pages that have write-order requirement
    // do this repeatedly
    std::vector<uint32_t> remaining (bufidxes);
    while (true) { // until we write out all pages
        size_t next_flushed_count = 0;
        lpid_t *next_flushed_pids = new lpid_t[remaining.size()];
        if (next_flushed_pids == NULL) {
            return RC(eOUTOFMEMORY);
        }
        w_auto_delete_array_t<lpid_t> next_flushed_pids_del (next_flushed_pids);

        std::vector<uint32_t> next_remaining;
        for (std::vector<uint32_t>::const_iterator it = remaining.begin(); it != remaining.end(); ++it) {
            uint32_t idx = *it;
            bfcb_t &cb = _core->_buftab[idx];
            if (!cb.write_order_dependencies().empty()) {
            // if any active dependency exists, skip this page
#if W_DEBUG_LEVEL>3
                cout << "skipped dependent page to assure careful-write-order (still young):"
                    << " successor=" << cb.pid() << endl;
#endif //W_DEBUG_LEVEL
                next_remaining.push_back(idx);
            } else {
                next_flushed_pids[next_flushed_count++] = cb.pid();
            }
        }
        w_assert1(next_flushed_count + next_remaining.size() == remaining.size());
        if (next_flushed_count == 0) {
            if (next_remaining.size() > 0) {
#if W_DEBUG_LEVEL>0
                cout << "Couldn't write out " << next_remaining.size()
                    << " pages because of dependency. Will retry later." << endl;
#endif //W_DEBUG_LEVEL>0
            }
            break; // nothing could be flushed
        }

        remaining = next_remaining; // for next iteration

        // sort the pids so we can write them out in sorted order
        ::qsort(next_flushed_pids, next_flushed_count, sizeof(lpid_t), cmp_lpid);

        if(!pwc) {
            // Direct/serial cleaning -- as done by any caller of _scan
            page_s* pbuf = new page_s[max_many_pages]; 
            w_auto_delete_array_t<page_s> pbuf_del (pbuf);
            W_DO(_clean_segment(next_flushed_count, next_flushed_pids, pbuf, timeout, retire_flag));
            continue;
        }
        
        CRITICAL_SECTION(cs, pwc->_pwc_lock); 
        w_assert1(pwc->pages_submitted == 0);
        pwc->pids = next_flushed_pids; // already sorted
        pwc->pages_submitted = next_flushed_count;
        pwc->pages_written = pwc->pages_claimed = 0;
        pwc->cancelslaves = false; 
        pwc->thread_rc = RCOK;

        DO_PTHREAD(pthread_cond_broadcast(&pwc->_wake_slaves));

        while(pwc->pages_written < pwc->pages_submitted) 
        {
            if(pwc->thread_rc.is_error()) 
                    pwc->cancelslaves = 1;
            if(retire_flag && *retire_flag) {
                    DO_PTHREAD(pthread_cond_broadcast(&pwc->_wake_slaves));
                    break;
            }
            DO_PTHREAD(pthread_cond_wait(&pwc->_wake_master, &pwc->_pwc_lock));
        }

        pwc->pages_submitted = pwc->pages_claimed = pwc->pages_written = 0;
        pwc->pids = 0;

        if (pwc->thread_rc.is_error()) {
            return pwc->thread_rc;
        }
    }
    return RCOK;
}

void  bfcb_t::set_rec_lsn(const lsn_t &what) { 
    // This assert is clearly not always true but we need to 
    // identify the cases.
    w_assert0(latch.is_mine());
    w_assert0(!what.valid() || !smlevel_0::log
          || what >= smlevel_0::log->global_min_lsn());
    _rec_lsn = what;
}

void  bfcb_t::mark_clean() {
    // assertions check _dirty first, then _rec_lsn, so update them in
    // that order! 
    // Using 'volatile' forces the compiler not to reorder these.
    // On machines with weak consistency we still
    // need a write barrier as well to force the buffers.
    _dirty =  false;
    _rec_lsn = lsn_t::null;
    lintel::atomic_thread_fence(lintel::memory_order_release);
    mark_clean_dependencies();
}
void  bfcb_t::mark_clean_dependencies() {
    // this is not needed except emergent eviction.
    // back pointer should have deleted it when the pointee is evicted (see below).
    // however, in emergent eviction (rounds>=4), this page might be deleted before it
    _write_order_dependencies->clear();

    // as this page is now clean, resolve dependency on this page
    if (!_wod_back_pointers->empty()) {
        int32_t this_idx = this - bf_core_m::_buftab;
        for (std::list<int32_t>::const_iterator it = _wod_back_pointers->begin(); it != _wod_back_pointers->end(); ++it) {
            int32_t dependent_idx = *it;
            bfcb_t *dependent = bf_core_m::_buftab + dependent_idx;
            dependent->write_order_dependencies().remove(this_idx);
        }
        _wod_back_pointers->clear();
    }
}

/*
 * bf_m::_clean_segment : the workhorse of the page_writer_threads
 *
 * Here are some notes from FRJ:
 * The page cleaners have no business looking at in-transit-* pages: 
 * in-pages are not dirty by definition, and out-pages are currently being 
 * flushed.
 *
 * High-level goings-on: In order to make page cleaning as unobtrusive
 * as possible, the page cleaners latch dirty pages in SH mode (and
 * one at a time) just long enough to copy them, set the old_rec_lsn,
 * and mark them as clean.  They then write out groups of copied pages
 * without holding any latches.  Once the I/O completes, they go back
 * and clear the page's old_rec_lsn without latching.
 *
 * Yes, this breaks all kinds of rules, with the resulting (potential)
 * mess handled as follows:
 *
 *  - Entities such as checkpoint threads, which need to know what
 *    rec_lsn really made it to disk last, should call safe_rec_lsn
 *    rather than curr_rec_lsn to filter out in-progress cleanings.
 *
 *  - Threads which modify the page after cleaning has started
 *    continue updating the rec_lsn as before
 *
 *  - Other threads which might write out the page will not do so if
 *    the old_rec_lsn is set, instead grabbing the corresponding page
 *    mutex to ensure the in-progress write completes before
 *    continuing.
 *
 * Callers: page_writer_thread_t::run(), _clean_buf()
 */
rc_t
bf_m::_clean_segment(
       int count, // 1 to npages
       lpid_t* pids, //  populated list of 'count' pids, npages() is max size
       page_s* pbuf,  // pre-allocated array for page copies; not populated
                      // and only max_many_pages in size
       timeout_in_ms timeout, // WAIT_IMMEDIATE or WAIT_FOREVER
       // WAIT_IMMEDIATE is used by the _scan methods to avoid
       // latch-latch deadlocks when they are trying to force_until_lsn.
       bool* cancel_flag // might be null
       ) 
{
    lpid_t           first_pid;
    bfcb_t*          bparray[max_many_pages];
    // We grab at most 2 page mutexes; any run of pages is at most
    // max_many_pages long and can therefore require span 2 mutexes at most.
    pthread_mutex_t* page_locks[2] = {NULL,NULL};

    lsn_t   curr_lsn = lsn_t::null;

    bool force_failed = false;
    w_rc_t ffrc;
    // dummy system transaction to call free_page().
    sys_xct_section_t sxs (false, false);
    W_DO(sxs.check_error_on_start());

    {
        int consecutive = 0;
        w_assert1(page_locks[0] == NULL);
        w_assert1(page_locks[1] == NULL);

        // This is for sanity checks:
        W_IFDEBUG3(int page_locks_owned = 0;)

        for(int i=0; i < count; i++) 
        {
            bool skipped = true; 
            lpid_t &p = pids[i];
            bfcb_t* bp;
#if defined(EXPENSIVE_LATCH_COUNTS) && EXPENSIVE_LATCH_COUNTS>0
            rc_t rc = _core->find(bp, p, LATCH_SH, timeout, 0/*ref_bit*/,
                    & GET_TSTAT(bf_hit_wait_scan));
#else
            rc_t rc = _core->find(bp, p, LATCH_SH, timeout);
#endif
            // latches the page unless it returns with error

            if(!rc.is_error()) 
            {
                // store might be wrong
                w_assert2(p.page == bp->frame()->pid.page);
                if( bp->dirty() ) 
                {
                    if ((bp->frame()->page_flags & page_p::t_tobedeleted) != 0) {
#if W_DEBUG_DEVEL>2
                        cout << "delete page by bf_m:" << bp->pid() << endl;
#endif // W_DEBUG_DEVEL>2
                        rc_t rc_del = io->dealloc_a_page(p);
                        if (rc_del.is_error()) {
                            cerr << "couldn't delete page " << bp->pid() << " by bf_m:" << rc_del << endl;
                            w_assert1(false);
                            // shouldn't happen. but continue
                        }
                        bp->mark_clean();
                        _core->unpin(bp);
                        continue;
                    }
#if W_DEBUG_DEVEL>2
                    cout << "_clean_segment dirty page:" << bp->pid() << endl;
#endif // W_DEBUG_DEVEL>2
                    skipped = false;
                    pthread_mutex_t** lock_acquired = NULL;

                    if(consecutive == 0) {
                        // First page seen since last run was written. 
                        // Grab the first page mutex.
                        // to prevent other threads from trying to write 
                        // out this run of pages.
                        first_pid = p;
                        w_assert2(page_locks[0] == NULL); 
                        w_assert3(page_locks_owned == 0) ;
                        DO_PTHREAD(pthread_mutex_lock(page_locks[0] = 
                                page_write_mutex_t::locate(p))); 
                        W_IFDEBUG3(page_locks_owned ++;)
                        lock_acquired = &page_locks[0];
            
                        // First page - not yet spanning the range of 2
                        // page mutexes.
                        w_assert2(page_locks[1] == NULL); 

                        // unnecessary if above assertion holds...
                        page_locks[1] = NULL; 
                        w_assert3(page_locks_owned == 1); 
                    } else {
                        // non-consecutive would have caused a 
                        // flush after the previous page
                        w_assert2(page_locks[0] != NULL); 
                        w_assert3(page_locks_owned >= 1); 

                        w_assert1(p.page == 
                        first_pid.page+consecutive && p.vol() == 
                                first_pid.vol());

                        // Is this page mutex the same as the one 
                        // we already have? 
                        // If not, grab the next one in line.
                        pthread_mutex_t* m2 = page_write_mutex_t::locate(p);

                        // This assert says that either we already
                        // have the mutex for this page or this
                        // is the first time we grab it 
                        w_assert2((page_locks[1] == NULL)
                                || (page_locks[1] == m2) );

                        if(page_locks[0] != m2 && page_locks[1]==NULL) {
                            w_assert3(page_locks_owned == 1); 
                            pthread_mutex_lock(page_locks[1] = m2);
                            W_IFDEBUG3(page_locks_owned ++;)
                            lock_acquired = &page_locks[1];
                        }
                        // one or the other should match this page's
                        // page_write_mutex
                        w_assert2((page_locks[1] == m2)
                                || (page_locks[0] == m2) );
                    }
                    w_assert3((page_locks_owned == 1)
                            ||(page_locks_owned == 2));
            
                    // check again for dirty now that we hold the page write mutex
                    // // Note: we could hold the latch more than once if we came
                    // through force_all.
                    w_assert2(bp->latch.held_by_me()); 

                    if(_core->_in_htab(bp) && bp->dirty()) {
                        // ... copy the whole page
                        w_assert0(bp->curr_rec_lsn().valid()
                                || !smlevel_0::logging_enabled); // else why are we cleaning?
                        w_assert0(!bp->old_rec_lsn().valid()); // never set when mutex is free!
                        w_assert1(consecutive < smlevel_0::max_many_pages);
                        pbuf[consecutive] = *bp->frame();
                        bparray[consecutive] = bp;
                        
                        // save the rec_lsn and mark the page clean
                        bp->save_rec_lsn();
                        bp->mark_clean();

                        // These are the same asserts that we have
                        // below where you see GNATS 161 note.
                        w_assert1(pbuf[consecutive].pid.page 
                                == bp->pid().page);
                        w_assert1(bp->old_rec_lsn().valid()
                                || !smlevel_0::logging_enabled);
                        
                        w_assert2 (
                            // st_regular:
                            (bp->curr_rec_lsn() <= bp->frame()->lsn) ||
                            // st_tmp
                            ((bp->get_storeflags() & smlevel_0::st_tmp) 
                                        == smlevel_0::st_tmp) ||
                            // in redo, we're not logging, so rec_lsn is
                            // meaningless.
                            smlevel_0::in_recovery_redo()
                            );
                        consecutive++;
                    }
                    else {
                        // oops... became clean during the gap
                        // or is subject to page replacement and is being
                        // written out by another thread
                        skipped = true;
                        w_assert0(lock_acquired);
                        pthread_mutex_unlock(*lock_acquired);
                        W_IFDEBUG3(page_locks_owned --;)
                        INC_TSTAT(bf_dirty_page_cleaned);
                        *lock_acquired = NULL;
                    }            
                } // o.w. skipped = true
                _core->unpin(bp); // does latch.release 
            } else {
                // latch failed.
                w_assert1(bp->latch.held_by_me() == false); 
                if(rc.err_num() == eFRAMENOTFOUND) {
                    // ok. somebody just evicted it and used _replace_out
                    // skip it and don't worry about considering it
                    // unprocessed either.  skipped is true
                    INC_TSTAT(bf_already_evicted);
                }
                else {
                    
                    ffrc = rc;
                    // can be fcOS when timeout != WAIT_IMMEDIATE
                    // but keep this assert to catch scenario and debug it
                    if(ffrc.err_num() == fcOS) {
                        cerr << "clean_segment PRINT MY LATCHES" << endl;
                        print_my_latches();
                        cerr << "clean_segment PRINT ALL LATCHES" << endl;
                        print_all_latches();
                    }
                    w_assert0(ffrc.err_num() == sthread_t::stTIMEOUT);
                    force_failed = true;
                }
                w_assert2(skipped);
            }

            // Determine if we need to flush the buffer.
            // We might have consecutive > 0 and still not yet need
            // to flush, so it is possible that we get here with
            // skipped=true (indicating the last pid met condition 4
            // in the comment below) and consecutive > 0.
            if(consecutive > 0) 
            {
                /* Several reasons why we might need to flush:

                   1. We just examined the last page on our list
                   2. The next pid is non-consecutive
                   3. We have max_many_pages consecutive pids
                   4. A consecutive pid was unpinnable or not dirty 
                      after all
                   5. The next pid would cause page_locks[1] <
                      page_locks[0] (due to modulo wraparound),
                      admitting the risk of deadlock
                   6. timeout=WAIT_FOREVER (risk of deadlock with an
                      EX-latch holder who decides to clean the page)

                   Note that the first three cases can be checked
                   before pinning the page (in the preceding loop
                   iteration). The fourth case requires pinning the
                   page but -- conveniently -- never buffers a dirty
                   page; we can safely wait until it's unpinned again
                   before flushing the other pages.
                 */
                bool should_flush =  
                        skipped  // case 4
                        || (i+1 == count)  // case 1 last page in bp
                        || (consecutive == max_many_pages) // case 3 above
                        || (timeout != WAIT_IMMEDIATE) // case 6 above
                        ;
                // Now check case 2
                if(!should_flush) 
                {
                    lpid_t const &next_pid = pids[i+1];
                    if(first_pid.page+consecutive != next_pid.page 
                            || first_pid.vol() != next_pid.vol()) 
                    {
                        // next pid is non-consecutive (case 2 above)
                        should_flush = true;
                    }
                    else 
                    if(page_write_mutex_t::locate(next_pid)
                            < page_write_mutex_t::locate(first_pid))
                    {
                        // would break the page_write_mutex protocol (case 5)
                        should_flush = true;
                    }
                }

                if(should_flush) 
                {
#if W_DEBUG_LEVEL > 1
                    // sanity checks on page locks: we hold #0 first,
                    // and #1 only if #0 is also held.
                    w_assert3((page_locks_owned == 1)
                              ||(page_locks_owned == 2));
                    w_assert2(page_locks[0] != NULL);
                    w_assert3( (page_locks_owned == 1) == (page_locks[1] == NULL) );
#endif
                    // write out the copies while we hold the page locks.
                    W_COERCE( _write_out(pbuf, consecutive) );

                    // after writing out the copies, we need to
                    // mark the pages clean.
                    // Free the page locks first, so we don't get 
                    // deadlock.
                    // This allows someone else to race in here
                    // and do nasty things with these pages before we get
                    // the latches, but that's ok -- see comments below.

                    while(consecutive > 0) 
                    {
                        // p points to a copy of the page
                        // Note that we are cleaning in reverse order. That
                        // probably doesn't matter.
                        //
                        consecutive--;
                        page_s* ps = &pbuf[consecutive];
                        bp = bparray[consecutive];

#if W_DEBUG_LEVEL > 0
                        pthread_mutex_t* thispagelock = 
                            page_write_mutex_t::locate(ps->pid);
                        if(thispagelock != page_locks[1]) {  
                            w_assert1(thispagelock == page_locks[0]) ;
                        }
#endif
                        // nobody should have been 
                        // able to evict the page...
                        // but its store can change in the meantime.
                        // w_assert0(ps->pid == bp->pid());
                        //
                        // BUG GNATS 161: an eviction *COULD HAVE*
                        // happened.
                        bfpid_t p = ps->pid;
                        bfcb_t* bfcbp;
                        if(_core->get_cb(p, bfcbp, true)) {
                            w_assert0(bfcbp == bp);
                            w_assert0(ps->pid.page == bp->pid().page);
                            w_assert0(bp->old_rec_lsn().valid()
                                || !smlevel_0::logging_enabled);

                            // mark the page as no longer being written out
                            bp->clr_old_rec_lsn();

                            bp->unpin_frame();
                        } else {
                            INC_TSTAT(bf_evicted_while_cleaning);
                        }
                        /* else not in the htab anymore, can't clear */ 

                    } // while consecutive>0

                    // Free the page locks  
                    w_assert1(page_locks[0] != NULL);
                    if(page_locks[1])  
                    {
                        DO_PTHREAD(pthread_mutex_unlock(page_locks[1]));
                        page_locks[1] = 0;
                        W_IFDEBUG3(page_locks_owned--;)
                    }
                    DO_PTHREAD(pthread_mutex_unlock(page_locks[0]));
                    page_locks[0] = 0;
                    W_IFDEBUG3(page_locks_owned--;)

                    
                    // FRJ: this is a benign race. Ignore any whining 
                    // from race detectors
                    // only safe to cancel if we know there are no 
                    // clean-but-not-clean pages around
                    // Note: cancel is not the same as retire;
                    // cancel leaves unwritten pages; retire
                    // means stop when you are done with your runs.
                    //
                    if (cancel_flag && *cancel_flag)   return RCOK;
                    w_assert1(page_locks[0] == NULL);
                    w_assert1(page_locks[1] == NULL);
                } // should_flush
            } // if consecutive > 0
        } 

        if( (timeout == WAIT_IMMEDIATE || !force_failed) == false) {
            w_ostrstream o;
            o << "force failed with rc =" << ffrc 
                << " timeout = " << timeout;
            fprintf(stderr, "%s\n", o.c_str());
        }
        w_assert0(timeout == WAIT_IMMEDIATE || !force_failed);
    }
    W_DO (sxs.end_sys_xct (ffrc));
    w_assert0(ffrc.is_error() == false
                || ffrc.err_num() == sthread_t::stTIMEOUT
                || ffrc.err_num() == fcOS
                );
    // alas, could be fcOS (see find() in bf_core.cpp)
    return force_failed?  RC(eBPFORCEFAILED) : RCOK;
}


/*********************************************************************
 * 
 *  bf_m::activate_background_flushing(vid_t *v=0,bool aggressive)
 *
 *********************************************************************/
void
bf_m::activate_background_flushing(vid_t *,bool aggressive)
{
    if(!smlevel_0::shutting_down) 
    {
        if (_cleaner_threads)  {
            CRITICAL_SECTION(cs, _cleaner_threads_list_mutex);
            bf_cleaner_thread_t *t;
            w_list_i<bf_cleaner_thread_t, queue_based_block_lock_t> 
                                                 i(*_cleaner_threads);
            while((t = i.next())) {
                t->activate(aggressive);
            }
        }
        // else  if(!smlevel_0::shutting_down) {
            //
            //This doesn't work when called from
            //log flush daemon! Then it waits on the condition for which
            //the daemon is the only signaller!
            //W_IGNORE(force_all());  // simulate cleaner thread in flushing
            // BUG_BF_SHUTDOWN_FIX
        // }
        // else, we are shutting down but the bf cleaners are gone already,
        // and if we had shutdown cleanly, flush_all would already have 
        // happened.
    }
}


/*********************************************************************
 *
 *  bf_m::_write_out(ba, cnt)
 *
 *  Write out cnt COPIES OF frames.
 *  Note: all pages in the ba array belong to the same volume.
 *
 *  NOTE: the CALLER MUST CLEAN THE FRAMES!
 *
 *********************************************************************/
rc_t
bf_m::_write_out(const page_s* pbuf, uint32_t cnt)
{
    uint32_t  i;

    /* 
     * we'll compute the highest lsn of the bunch
     * of pages, and do one log flush to that lsn
     */
    lsn_t  highest = lsn_t::null;
    for (i = 0; i < cnt; i++)  
    {
        /*
         *  if recovery option enabled, dirty frame must have valid
         *  lsn unless it corresponds to a temporary page
         *
         */
        if (log)  {
            lsn_t lsn = pbuf[i].lsn;
            if (lsn.valid()) { // should include st_tmp pages
                    if(lsn > highest) {
                        highest = lsn;
                }
            }
            /* else XXXYYY (look for other comments with XXXYYY)
             * had better be a tmp page -- except:
             * Page is first a temp page; it's converted to regular,
             * forced to disk. Then it's read, marked regular, and an
             * update is attempted.  Inorder to do the update, the page
             * is latched EX, marked dirty (in the bfcb).
             * If the update fails. the page is unpinned.
             * The cleaner finds the page marked dirty (but isn't 
             * "really" dirty), and here would find a regular file
             * with an invalid lsn
             */
        } 
        pbuf[i].update_checksum(); // store checksum based on latest page content
    }

    // WAL: ensure that the log is durable to this lsn
    if(highest > lsn_t::null && log) {
        INC_TSTAT(bf_log_flush_lsn);
        W_COERCE( log->flush(highest) );
    }

    io->write_many_pages(pbuf, cnt);
    _incr_page_write(cnt, true); // in background

    return RCOK;
}


/*********************************************************************
 *
 *  bf_m::_replace_out(b)
 *
 * Called from bf_m::fix to write out a dirty victim page.
 * The bf_m::_write_out method is not suitable for this purpose because
 * it uses the "pid" field of the frame control block (bfcb_t) whereas
 * during replacement we want to write out the page specified by the
 * "old_pid" field.
 * Furthermore, _write_out writes from copies of the page, not from the
 * bfcb_t.  This writes a page from the buffer pool, so it can
 * get into the frame to clean it.
 *
 * Assumptions: b is not in the hash table, as it's a replacement frame.
 * The replacement frame is not latched by anyone, is not being
 * cleaned by a page cleaner (these things are enforced in replacement
 * by its can_replace() call while holding the page_write_mutex).
 *********************************************************************/
rc_t
bf_m::_replace_out(bfcb_t* b)
{
    INC_TSTAT(bf_replaced_dirty);

    w_assert3(b);
    w_assert1(is_cached(b) == false); // is not in htab
    w_assert1(b->latch.is_mine() == true); 
    w_assert1(b->latch.held_by_me() == true); 

    if (log)  {
        lsn_t lsn = b->frame()->lsn;
        if (lsn.valid()) {
            INC_TSTAT(bf_log_flush_lsn);
            // WAL: Make sure the log records are flushed. 
            W_COERCE(log->flush(lsn));
        } else {
            // Should not happen: st_tmp should have valid lsn
            // w_assert1(b->get_storeflags() & st_tmp);
            w_assert1(!b->dirty());
        }
    }

#if W_DEBUG_LEVEL > 5
    // if (b->get_storeflags() & st_tmp) 
    {
        w_ostrstream s;
        s << "_replace_out tmp pid " << b->pid() 
                << " page lsn " << b->frame()->lsn
                << " store_flags & st_tmp " 
                << int(b->get_storeflags() & st_tmp) ;
        fprintf(stderr, "%s\n", s.c_str());
    }
#endif
    w_assert1(b->latch.is_mine() == true); 
    w_assert1(b->latch.held_by_me() == true); 
    b->frame()->update_checksum(); // store checksum based on latest page content
    io->write_many_pages(b->frame(), 1);
    _incr_page_write(1, false); // for replacement

    // Caller grabbed the page mutex but has no lock on the 
    // control block yet; the frame is a replacement
    // and cannot be found in the hash table yet but we
    // latched it to use the same protocol as
    // callers of _write_out 
    w_assert1(b->latch.is_mine() == true); 
    w_assert1(b->latch.held_by_me() == true); 

    // Clear dirty flag and rec_lsn for the new page
    // that we're about to read in to this frame...
    b->mark_clean();

    return RCOK;
}


/*********************************************************************
 *
 *  bf_m::get_page(pid, b, no_read, ignore_store_id, is_new)
 *
 *  Initialize the frame pointed by "b' with the page 
 *  identified by "pid". If "no_read" is true, do not read 
 *  page from disk; just initialize its header. 
 *
 *********************************************************************/
rc_t
bf_m::get_page(
    const lpid_t&        pid,
    bfcb_t*              b, 
    uint16_t              W_IFDEBUG2(ptag), 
    bool                 no_read, 
    bool                 W_IFDEBUG2(ignore_store_id))
{

    if (! no_read)  {
        DBGTHRD(<<"get_page : read");

        // At the time of this writing, the only error we can
        // get back from sm_io::read_page is eBADVOL (from the
        // pid).
        W_DO( io->read_page(pid, *b->frame_nonconst()) );

        DBGTHRD(<<"get_page "
            << "found frame " << ::hex << (unsigned long) (b->frame()) << ::dec
            << " frame's pid " << b->frame()->pid);
        // for each page retrieved from disk, compare its checksum
        if (b->frame()->lsn != lsn_t::null
            && (b->frame()->page_flags & page_p::t_virgin) == 0) {
            uint32_t checksum = b->frame()->calculate_checksum();
            if (checksum != b->frame()->checksum) {
#if W_DEBUG_LEVEL>0
                cerr << "bad page checksum in page " << pid << endl;
#endif // W_DEBUG_LEVEL>0
                return RC (eBADCHECKSUM);
            }
        }
    }

    // TODO: if  nothing is to be done for the no_read case,
    // then avoid calling this altogether
    //
    if (! no_read)  {
        DBGTHRD(<<"get_page : set flags");
        // clear virgin flag, and set written flag
        b->frame_nonconst()->page_flags &= ~page_p::t_virgin;
        b->frame_nonconst()->page_flags |= page_p::t_written;

#if W_DEBUG_LEVEL>1
        /*
         * NOTE: the store ID may not be correct during
         * redo-recovery in the case where a page has been
         * deallocated and reused.  
         * This can arise because the page
         * will have a new store ID.  
         *
         * Also, if the page LSN is 0 then the page is
         * new and should have a page ID of 0.
         * (NB: I think the frame's lsn is never lsn_t::null,
         * if a page has been formatted or read into it. Only an
         * uninitialized frame would look like that.)
         */
        if (b->frame()->lsn == lsn_t::null)  {
            w_assert3(b->frame()->pid.page == 0
                    || !smlevel_0::logging_enabled);
        } else {
            w_assert3(pid.page == b->frame()->pid.page &&
                      pid.vol() == b->frame()->pid.vol());
            if( pid.store() != b->frame()->pid.store() ) {
                if(! ignore_store_id) {
                    w_ostrstream s;
                    s << "bad store number " << b->frame()->pid.store()
                    << " expected " << pid.store()
                    << "; page tag "  << b->frame()->tag
                    << " expected tag "  << ptag
                    << "; page store flags " << b->frame()->get_page_storeflags()
                    << "; lsn " << b->frame()->lsn;
                    fprintf(stderr, "On page fix: %s\n", s.c_str());
                }
            }
            w_assert3(ignore_store_id ||
                      pid.store() == b->frame()->pid.store());
        }
#endif /* W_DEBUG_LEVEL>2 */
    }

    return RCOK;
}




/*********************************************************************
 * 
 *  bf_m::enable_background_flushing(vid_t v)
 *
 *  Spawn cleaner thread.
 *
 *********************************************************************/
rc_t
bf_m::enable_background_flushing(vid_t v)
{
    if(!_cleaner_threads) return RCOK;

    {
        bool error;
        if (!option_t::str_to_bool(_backgroundflush->value(), error)) {
                // background flushing is turned off
            return RCOK;
        }
    }
    {
        CRITICAL_SECTION(cs, _cleaner_threads_list_mutex);

        bf_cleaner_thread_t *t;
        {
            w_list_i<bf_cleaner_thread_t, queue_based_block_lock_t> i(*_cleaner_threads);
            while((t = i.next())) {
                if(t->vol() == v) {
                    // found
                    return RCOK;
                }
            }
        }

        /* Create thread (1) for buffer pool cleaning -- this will fork page
         * writer threads
         */
        t = new bf_cleaner_thread_t(v);
        if (! t)
            return RC(eOUTOFMEMORY);
        _cleaner_threads->push(t);
        rc_t e = t->fork();
        if (e.is_error()) {
                //t->retire();
                t->_link.detach();
                delete t;
                return e;
        }
    }
    return RCOK;
}



/*********************************************************************
 *
 *  bf_m::disable_background_flushing()
 *  bf_m::disable_background_flushing(vid_t v)
 *
 *  Kill cleaner thread.
 *
 *********************************************************************/
rc_t
bf_m::disable_background_flushing()
{
    if(!_cleaner_threads) return RCOK;

    bf_cleaner_thread_t *t=NULL;
    do {
        CRITICAL_SECTION(cs, _cleaner_threads_list_mutex);
        w_list_i<bf_cleaner_thread_t, queue_based_block_lock_t> 
                                                i(*_cleaner_threads);

        while((t = i.next())) {
            // free up the mutex so we don't deadlock with others,
            // should someone else simultaneously be trying to activate
            // background flushing.
            cs.pause();
            t->retire();
            cs.resume();
            t->_link.detach();
            delete t;
            break;
        }
    } while(t != NULL);
    return RCOK;
}

rc_t
bf_m::disable_background_flushing(vid_t v)
{
    if(!_cleaner_threads) return RCOK;
    {
        CRITICAL_SECTION(cs, _cleaner_threads_list_mutex);
        bf_cleaner_thread_t *t;

        w_list_i<bf_cleaner_thread_t, queue_based_block_lock_t> 
                                            i(*_cleaner_threads);
        // This is one of the few list modifications that is safe in
        // scope of an iterator:
        while((t = i.next())) {
            if(t->vol() == v) {
                // found
                t->retire();
                W_COERCE(t->join());
                t->_link.detach();
                delete t;
            }
        } 
    }
    return RCOK;
}

/*********************************************************************
 *
 *  bf_m::get_rec_lsn(start, count, pid, rec_lsn, ret)
 *
 *  Get recovery lsn of "count" frames in the buffer pool starting at
 *  index "start". The pids and rec_lsns are returned in "pid" and
 *  "rec_lsn" arrays, respectively. The values of "start" and "count"
 *  are updated to reflect where the search ended and how many dirty
 *  pages it found, respectively.
 *
 *********************************************************************/
rc_t
bf_m::get_rec_lsn(int &start, int &count, lpid_t pid[], lsn_t rec_lsn[],
    lsn_t &min_rec_lsn)
{
    w_assert9(start >= 0 && count > 0);
    w_assert9(start + count <= npages());

    int i;
    for (i = 0; i < count && start < npages(); start++)  {
        if (_core->_buftab[start].dirty() && 
                _core->_buftab[start].pid().page 
                ) {
            /*
             * w_assert9(_core->_buftab[start].rec_lsn != lsn_t::null);
             * See comments at XXXYYY  for reason we took out this
             * assertion.
             *
             * Avoid checkpointing temp pages.
             *
             * NOTE: page cleaners break several rules as they write
             * out dirty pages (see comments for
             * bf_m::_clean_segment), and using safe_rec_lsn() deals with
             * the problem.
             */
            lsn_t rlsn = _core->_buftab[start].safe_rec_lsn();
            if(rlsn != lsn_t::null) {
                pid[i] = _core->_buftab[start].pid();
                rec_lsn[i] = rlsn;
                if(min_rec_lsn > rec_lsn[i]) min_rec_lsn = rec_lsn[i];
                i++;
            } else {
                w_assert9(_core->_buftab[start].get_storeflags() & st_tmp);
            }
        }
    }
    count = i;

    return RCOK;
}


/*********************************************************************
 *
 *  bf_m::min_rec_lsn()
 *
 *  Return the minimum recovery lsn of all pages in the buffer pool.
 *
 *********************************************************************/
lsn_t
bf_m::min_rec_lsn()
{
    lsn_t lsn = lsn_t::max;
    for (int i = 0; i < npages(); i++)  {
    lsn_t rec_lsn = _core->_buftab[i].safe_rec_lsn();
    if (_core->_buftab[i].dirty() && 
            _core->_buftab[i].pid().page &&
            rec_lsn < lsn)
        lsn = rec_lsn;
    }
    return lsn;
}


/*********************************************************************
 *
 *  bf_m::dump()
 *
 *  Print to stdout content of buffer pool (for debugging)
 *
 *********************************************************************/
void 
bf_m::dump(ostream &o)
{
    _core->dump(o);
    if(bf_cleaner_thread_t::_histogram) {
        int j;
        for(int i=0; i< npages()+1; i++) {
             j = bf_cleaner_thread_t::_histogram[i];
             if(j!= 0) {
                o << i << " pages: " << j << " sweeps" <<endl;
             }
        }
    }
}


/*********************************************************************
 *
 *  bf_m::_discard_all()
 *  (private)
 *
 *  Discard all pages in the buffer pool.
 *
 *********************************************************************/
rc_t
bf_m::_discard_all()
{
    return  _scan(bf_filter_none_t(), false /*write_dirty*/, true/*discard*/);
}


/*********************************************************************
 *
 *  bf_m::discard_store(stid)
 *
 *  Discard all pages belonging to stid.
 *
 *********************************************************************/
rc_t
bf_m::discard_store(stid_t stid)
{
    return  
        _scan(bf_filter_store_t(stid), false /*write_dirty*/, true/*discard*/);
}


/*********************************************************************
 *
 *  bf_m::discard_volume(vid)
 *
 *  Discard all pages originating from volume vid.
 *
 *********************************************************************/
rc_t
bf_m::discard_volume(vid_t vid)
{
    return _scan(bf_filter_vol_t(vid), false /*write_dirty*/, true/*discard*/);
}


/*********************************************************************
 *
 *  bf_m::force_all(bool flush)
 *
 *  Force (write out) all pages in the buffer pool. If "flush"
 *  is true, then invalidate the pages as well.
 *
 *********************************************************************/
rc_t
bf_m::force_all(bool flush)
{
    return _scan(bf_filter_none_t(), true /*write_dirty*/, flush/*discard*/);
}



/*********************************************************************
 *
 *  bf_m::force_store(stid, is temp, invalidate)
 *
 *  Force (write out) all pages belonging to stid. If "invalidate"
 *  is true, then invalidate the pages as well.
 *
 *********************************************************************/
rc_t
bf_m::force_store(stid_t stid, bool invalidate)
{
    return _scan(bf_filter_store_t(stid), true /*write_dirty*/, invalidate);
}



/*********************************************************************
 *
 *  bf_m::force_volume(vid, flush)
 *
 *  Force (write out) all pages originating from volume vid.
 *  If "flush" is true, then invalidate the pages as well.
 *
 *********************************************************************/
rc_t
bf_m::force_volume(vid_t vid, bool flush)
{
    DBGTHRD(<<"force_volume " << vid << " flush " << flush );
    
    // let all _scans do direct, serial
    // scans (don't use page writer threads)
    return _scan(bf_filter_vol_t(vid), true /*write_dirty*/, flush/*discard*/);
}


/*********************************************************************
 *
 *  bf_m::force_until_lsn(lsn, flush)
 *
 *  Flush (write out) all pages whose rec_lsn is less than 
 *  or equal to "lsn". If "flush" is true, then invalidate the
 *  pages as well.
 *
 *  Do this directly (serial writes)
 *  We are called from ssm at shutdown (background flushing is already
 *  disabled), right after recovery at startup (background flushing is
 *  enabled), and in emergency-log-flush from xct_impl.cpp when it
 *  thinks we're about to run out of log space
 * 
 *********************************************************************/
rc_t
bf_m::force_until_lsn(const lsn_t& lsn, bool flush)
{
    // let all _scans do direct, serial
    // scans (don't use page writer threads)
    return _scan(bf_filter_lsn_t(lsn), true /*write_dirty*/, flush/*discard*/);
}



/*********************************************************************
 *
 *  bf_m::_scan(filter, write_dirty, discard)
 *
 *  Scan and filter all pages in the buffer pool. For successful
 *  candidates (those that passed filter test):
 *     1. if frame is dirty and "write_dirty" is true, then write it out
 *     2. if "discard" is true then invalidate the frame
 *
 *  NB:
 *     Scans of the entire buffer pool that are done by scanning the
 *     buftab[] array are "safe" if the filters always check the pid
 *     or something that will necessarily fail for invalid (free) entries.
 *
 *     The filters could pick up pinned or in-transit frames, 
 *     since the filters use no synchronization.
 *     Hence, the _scan has to pin the pages and re-check the filter.
 *
 *     NB: callers of _scan must not care if, after a frame is checked,
 *     another page slips in that would pass the filter.  In other words,
 *     the scan is definitely not atomic.  For example, for force_until_lsn()
 *     no page that slips in after the frame is checked could possibly get
 *     an older lsn...  and in the case of discard_*(), the caller
 *     always has a EX lock on the store/volume (it's destroying 
 *     the store/volue)  (or it's the bf_m shutting down)
 *     so no other tx could read pages for the given store.  (Another thread
 *     in the same tx could read them, but that would be a bug in the 
 *     (value-added) server).
 *     In the case for force_volume and force_all, the above comment applies.
 *     In the case for force_store, it's used for debugging & smsh,
 *     and for changing the store flags of a store (the store's pages have to
 *     be made durable and discarded
 *     before the xct can commit, making the pages' new
 *     status (logged or not) visible to other xcts.)
 *
 *     Callers:                    write_dirty    discard      thread/lock ctxt
 *      _discard_all                 false           true 
 *          private: called @shutdown                          exclusive
 *      discard_volume               false           true
 *          volume dismount                                    exclusive
 *      discard_store                false           true
 *          destroying temps on mount/dismount                 exclusive
 *          destroy file                                       exclusive
 *          destroy n swap file (sort)                         exclusive
 *      force_until_lsn              true            f/t
 *    *     xct emerg log flush                      false     fg
 *          start up                                 false     exclusive
 *          shut down                                false     exclusive
 *      force_all                    true            f/t
 *          called @shutdown                         true      exclusive
 *    *     activate background flushing             false     fg
 *    *     force_buffers                            t/f       fg
 *      force_volume                 true            f/t
 *          volume dismount                          true      exclusive
 *      force_store                  true            f/t
 *          set_store_flags                          true      exclusive
 *    *     force_vol_hdr_buffers                    true      fg
 *    *     force_store_buffers                      t/f       fg
 *
 * Now all these callers do serial writes.   This is problematic in
 * that they can cause latch-latch deadlocks.
 *
 * All the ones with the asterisk in the first column above can be called by
 * "foreground" threads, that is, client threads, which can have some page(s)
 * fixed, and without exclusive access to all the pages in the volume/store
 * (via lock or by being the only SM thread to run).
 *
 * Let's look at each of them:
 * - The force_vol_hdr_buffers, force_store_buffers, force_all calls can 
 *   be documented:  do not do this with anything fixed.
 *
 * - Activating bg flushing can happen anytime, but it doesn't discard, and
 *   so it will skip out after the call to _clean_buf (assuming it were
 *   able to get enough memory for the list of pids). It doesn't
 *   care if _clean_buf returns an error.
 *
 * - The emergency log flush code in xct_impl.cpp also doesn't discard,
 *   so that should also skip out after the call to _clean_buf (assuming
 *   it were able to get enough memory for the list of pids).
 * Conclusion: we should be ok here.
 *
 *********************************************************************/
rc_t
bf_m::_scan(const bf_filter_t& filter, bool write_dirty, bool discard)
{
    INC_TSTAT(bf_fg_scan_cnt);
    if(write_dirty) {
        /*
         * Sync the log. This is now necessary because
         * the log is buffered.
         * Plus this is more efficient than small syncs when
         * pages are flushed below.
         */
        if (smlevel_0::log) { // log manager is running
            DBGTHRD(<< "flushing log");
            INC_TSTAT(bf_log_flush_all);
            W_IGNORE(smlevel_0::log->flush_all());
        }
    }
    if (write_dirty)  
    {
        /* One scan of entire buffer pool */
        std::vector<uint32_t> bufidxes;
        /*
         *  Write_dirty, and there is enough memory for pid array...
         *  Fill pid array with qualifying candidates, and call _clean_buf().
         */
        for (int i = 0; i < npages(); i++)  {
            w_assert1(bf_core_m::_buftab);
            if (filter.is_good(bf_core_m::_buftab[i]) &&
                _core->_buftab[i].dirty()) {
                bufidxes.push_back(i);
            }
        }

        w_assert1(bufidxes.size() <= (size_t) npages());
        DBGTHRD(<<" _scan calling clean_buf with " << bufidxes.size() << " pages ");
        // NOTE: this could return in error if it couldn't
        // clean all the buffers
        W_DO(_clean_buf(NULL,
                    bufidxes, WAIT_FOREVER, 0) );
        lintel::unsafe::atomic_fetch_sub(&bf_cleaner_thread_t::_ndirty, bufidxes.size());
        if (! discard)   {
            return RCOK;        // done 
        }
        /*
         *  else, fall thru 
         */
    } 
    /*
     * One of: 
     * - !write_dirty or 
     * - we didn't have enough memory or
     * - write_dirty and discard.
     *  We need EX latch to discard, SH latch to write.
     *
     *  NOTE: this could cause latch-latch deadlocks, just as 
     *  the call to _clean_buf above could, since we are using
     *  WAIT_FOREVER.  
     *  (Example: 2 user threads hold EX latches on pages, try to log 
     *  updates, run out of log space, one tries emergency log flush
     *  (other awaits the mutex for same),  cannot EX latch 
     *  a page to flush so cannot scavenge parition, hangs waiting for 
     *  EX latch.  Essentially, noone should call _scan while
     *  holding EX latches on pages because _scan will try to acquire
     *  latches in arbitrary order (no, the pids are not sorted).
     *
     *  HOWEVER: 
     *  a) it's highly unlikely that we don't have enough memory, 
     *  and 
     *  b) in each of the caller cases (see comments above the start
     *  of this method), we either have exclusive access to the pages
     *  by virtue of an EX lock on the volume/store/file, or we are
     *  the only "client" thread to be running b/c we are starting the sm up
     *  or shutting it down.
     *  So we should be safe.  
     *  ANY NEW USES OF _scan HAD BETTER CONSIDER ALL THIS! 
     */

    latch_mode_t mode = discard ? LATCH_EX : LATCH_SH;
    
    /*
     *  Go over buffer pool. For each good candidate, fix in 
     *  mode, and force and/or flush the page.
     *  Because we're looking at the contents of
     *  the frames before latching them, the contents could
     *  change during or after the filter checking.
     */
    w_rc_t rc;
    for (int i = 0; i < npages(); i++)  {
        if (filter.is_good(_core->_buftab[i])) {
            bfcb_t* b = &_core->_buftab[i];
            w_assert2(! b->latch.held_by_me());
            w_assert2(! b->latch.is_mine());

#if defined(EXPENSIVE_LATCH_COUNTS) && EXPENSIVE_LATCH_COUNTS>0
            rc = _core->find(b, _core->_buftab[i].pid(), mode, WAIT_FOREVER,
                    0/*ref_bit*/, 
                    & GET_TSTAT(bf_hit_wait_scan));
#else
            rc = _core->find(b, _core->_buftab[i].pid(), mode, WAIT_FOREVER);
#endif
            // if find failed, it sets b to NULL
            
            if (rc.is_error()) {
                // find failed: reset b
                b = &_core->_buftab[i];
                
                /*
                 * Not found or timed out: could be that the
                 * page was replaced while we waited for the
                 * latch or core mutex; in that case this page
                 * might no longer be around.  We should re-check
                 * the frame.
                 */
                w_assert2(!b->latch.is_mine());
                rc = _core->pin(b, mode);

                // Might be invalid frame now - in that case, skip it
                if(rc.is_error()) {
                    // not in hash table: didn't get pinned
                    // NB: could be in transit
                    w_assert2(!b->latch.is_mine());
                    continue;
                } else {
                    // in hash table: did get pinned
                    w_assert2(b->latch.is_mine());

                    // re-check the frame
                    if(! filter.is_good(*b)) {
                        _core->unpin(b);
                        w_assert2(!b->latch.is_mine());
                        continue;
                    }
                    // else drop down
                }
                w_assert2(b->latch.is_mine());
            }
            w_assert2(b == &_core->_buftab[i]);
            w_assert2(b->latch.is_mine());

#if W_DEBUG_LEVEL > 1
            // Since the protocol is pin, then latch,
            // the pin count can be greater than the latch count,
            // and if it is, it just means someone else is waiting
            // to latch this page.
            // He who awaits the latch has to check, after acquiring the
            // latch, that the page is still there, i.e., that the
            // frame is still what it was when he pinned it.
            if(discard) {
                w_assert2(b->pin_cnt() >= 1);
                w_assert2(b->latch.is_mine());
            }
#endif
            w_assert9(filter.is_good(*b));

            if (write_dirty && b->dirty())  {
#if W_DEBUG_LEVEL > 1
                if (b->get_storeflags() & st_tmp) {
                    cerr << "calling _write_out tmp pid " << b->pid() << endl;
                }
#endif
                CRITICAL_SECTION(cs, page_write_mutex_t::locate(b->pid())); 
                w_assert0(!b->old_rec_lsn().valid()); // never set when the mutex is free!
                rc = _write_out(b->frame(), 1);
                if(rc.is_error()) {
                    // we should not get here, because
                    // _write_out only returns RCOK;
                    w_assert9(0);
                    //                    b->latch.release();
                    return rc;
                }
                
                w_assert0(b->latch.is_mine());
                b->mark_clean();
            }

            w_assert2(filter.is_good(*b));
            w_assert2(b->latch.held_by_me());

            if (discard)  {
              // FRJ: this is identical to discard() except for the filter check
                bfcb_t* tmp = b;
                w_assert2(b->pin_cnt() >= 1);
                rc = _core->remove(tmp); 
                if (rc.is_error())  { // released the latch already
                    /* ignore */ ;
                    w_assert2(!b->latch.is_mine());
                    w_assert2(rc.err_num() == eHOTPAGE);
                    // see comments in bf_core.cpp 
                    
                } // there really is no else for this
            } else {
                _core->unpin(b);
            }
        }
    }
    return RCOK;
}


/*********************************************************************
 *
 *  bf_m::_set_dirty(bfcb_t)
 *       
 *  Mark the bfcb_t dirty. 
 *  Called by bf_m::set_dirty() and bf_m::unfix(...dirty==true...)
 *
 *  Return true if we kicked the cleaner; false o.w.
 *  There are 2 different callers; one needs to set a Boolean kick_cleaner
 *  if we did so, which is why we return the Boolean value.
 *
 *********************************************************************/
bool
bf_m::_set_dirty(bfcb_t* b)
{
    if( !b->dirty() ) {
        b->set_dirty_bit();
        w_assert2( _core->latch_mode(b) == LATCH_EX );
        /*
         * The following assert should hold because:
         * prior to set_dirty, the page should have
         * been fixed in EX mode, which causes the frame
         * control block's rec_lsn to be set (that is,
         * if the rec_lsn is lsn_t::null it will be made non-null but
         * it isn't updated otherwise).
         * If this assert fails, the reason is that the
         * page wasn't latched properly before doing an update,
         * in which case, the above assertion should have failed.
         * NB: this is NOT the lsn on the page, by the way.
         */
#if W_DEBUG_LEVEL > 2
        if (log && 
                !smlevel_0::in_recovery_redo() 
                ) 
        {
            DBGTHRD(<< "pid " << b->pid() <<" mode=" 
            <<  int(_core->latch_mode(b)) << " rec_lsn=" << b->curr_rec_lsn());
            w_assert3(b->latch.mode() == LATCH_EX);
            w_assert3(b->curr_rec_lsn() != lsn_t::null); 
            w_assert3(b->dirty()); // we just set it so it's now dirty
        }
#endif 
        int ndirty = lintel::unsafe::atomic_fetch_add(&bf_cleaner_thread_t::_ndirty,1)+1;
        if(ndirty > bf_cleaner_thread_t::_dirty_threshold) {
            if(ndirty % (bf_cleaner_thread_t::_dirty_threshold/4) == 0) {
                INC_TSTAT(bf_kick_threshold);
                return true;
            }
        }
    }
    return false;
}


/*********************************************************************
 *
 *  bf_m::set_dirty(page_s *buf)
 *
 *  Mark the buffer page "buf" dirty. Called by page_p::set_dirty().
 *  page_p::set_dirty() is called: on redo (logrec.cpp)
 *  and,  in forward processing,
 *  when a log record isn't written to the log because logging
 *  is disabled, the page is st_tmp, etc.
 *
 *  Calls helper _set_dirty(bfcb_t *)
 *
 *********************************************************************/
rc_t
bf_m::set_dirty(const page_s* buf)
{
    bfcb_t* b = get_cb(buf);
    if (!b)  {
        // buf is probably something on the stack
        return RCOK;
    }
    w_assert1(b->frame() == buf);
    if( _set_dirty(b) ) {
        vid_t v = b->pid().vol();
        activate_background_flushing(&v);
    }

    return RCOK;
}

rc_t bf_m::_check_write_order_cycle(
    int32_t suc_idx,
    bfcb_t &pre,
    bool &has_cycle) {
    std::list<int32_t>& dependencies = pre.write_order_dependencies();
    for (std::list<int32_t>::const_iterator it = dependencies.begin(); it != dependencies.end(); ++it) {
        //might need SH latch here
        int32_t pointee = *it;
        if (pointee == suc_idx) {
            has_cycle = true;
            break;
        }
        // might need SH latch here
        bfcb_t* bp = bf_core_m::_buftab + pointee;
        rc_t rc = _check_write_order_cycle(suc_idx, *bp, has_cycle);
        if (rc.is_error()) {
            return rc;
        }
        if (has_cycle) {
            break;
        }
    }
    return RCOK;
}
rc_t bf_m::register_write_order_dependency(
    const page_s* successor,
    const page_s* predecessor)
{
    // the caller must make sure these exist in bufferpool as dirty pages
    int suc_idx = successor - bf_core_m::_bufpool;
    int pre_idx = predecessor - bf_core_m::_bufpool;
    w_assert1(suc_idx>=0 && suc_idx<bf_core_m::_num_bufs);
    w_assert1(pre_idx>=0 && pre_idx<bf_core_m::_num_bufs);

    bfcb_t* suc = bf_core_m::_buftab + suc_idx;
    bfcb_t* pre = bf_core_m::_buftab + pre_idx;
    w_assert1(suc);
    w_assert1(pre);
    w_assert1(suc->frame() == successor);
    w_assert1(pre->frame() == predecessor);
    w_assert1(suc->dirty());
    w_assert1(pre->dirty());

    // check cycles
    bool has_cycle = false;
    W_DO(_check_write_order_cycle(suc_idx, *pre, has_cycle));
    if (has_cycle) {
        return RC(eWRITEORDERLOOP);
    }
    
    suc->write_order_dependencies().push_back(pre_idx);
    pre->wod_back_pointers().push_back(suc_idx);
    return RCOK;
}


/*********************************************************************
 *
 *  bf_m::is_dirty(buf)
 *
 *  For debugging
 *
 *********************************************************************/
bool
bf_m::is_dirty(const page_s* buf)
{
    bfcb_t* b = get_cb(buf);
    w_assert3(b); 
    w_assert3(b->frame() == buf);
    return b->dirty();
}

/*********************************************************************
 *
 *  bf_m::snapshot(ndirty, nclean, nfree, nfixed)
 *
 *  Return statistics of buffer usage. 
 *
 *********************************************************************/
void
bf_m::snapshot(
    u_int& ndirty, 
    u_int& nclean,
    u_int& nfree, 
    u_int& nfixed)
{
    nfixed = nfree = ndirty = nclean = 0;
    for (int i = 0; i < npages(); i++) { 
        if (_core->_buftab[i].pid().page)  {
            _core->_buftab[i].dirty() ? ++ndirty : ++nclean;
        }
    }
    _core->snapshot(nfixed, nfree);

    /* 
     *  assertion cannot be maintained ... need to lock up
     *  the whole rsrc_m/bf_m for consistent results.
     *
     *  w_assert9(nfree == _num_bufs - ndirty - nclean);
     */
}

void                 
bf_m::snapshot_me(
    u_int&                             nsh, 
    u_int&                             nex,
    u_int&                             ndiff
)
{
    _core->snapshot_me(nsh, nex, ndiff);
}



/*********************************************************************
 *  
 *  Filters
 *
 *********************************************************************/
bf_filter_store_t::bf_filter_store_t(const stid_t& stid)
    : _stid(stid)
{
}

inline bool
bf_filter_store_t::is_good(const bfcb_t& p) const
{
    return p.pid().page && (p.pid().stid() == _stid);
}

NORET
bf_filter_vol_t::bf_filter_vol_t(const vid_t& vid)
    : _vid(vid)
{
}

bool
bf_filter_vol_t::is_good(const bfcb_t& p) const
{
    return p.pid().page && (p.pid().vol() == _vid);
}

NORET
bf_filter_none_t::bf_filter_none_t()
{
}

bool
bf_filter_none_t::is_good(const bfcb_t& p) const
{
    return p.pid() != lpid_t::null;
}

NORET
bf_filter_sweep_t::bf_filter_sweep_t(bool urgent, vid_t v)
  : _urgent(urgent), _vol(v)
{
}

bool
bf_filter_sweep_t::is_good(const bfcb_t& p) const
{
    if( p.pid()._stid.vol != _vol)  return false;
    if( ! p.pid().page)             return false;

    if( p.hotbit() > 0 ) {
        // skip hot pages even if they are dirty.
        // however, if the pool is too full
        // we'll include the dirty hot pages
        //
        if (!_urgent) {
            INC_TSTAT(bf_sweep_page_hot_skipped);
            return false; 
        }
    }
    return p.dirty();
}

NORET
bf_filter_sweep_old_t::bf_filter_sweep_old_t(int log_segment, 
        bool urgent, vid_t v )
    : _segment(log_segment), _vol(v), _urgent(urgent)
{
}

bool bf_filter_sweep_old_t::is_good(const bfcb_t& p) const
{
    if( p.pid()._stid.vol != _vol)       return false;
    if( ! p.pid().page)                  return false;
    if( ! p.curr_rec_lsn().hi() == _segment ) return false;

    if( p.hotbit() > 0 ) {
        // skip hot pages even if they are dirty.
        // however, if the pool is too full
        // we'll include the dirty hot pages
        //
        if (!_urgent) {
            INC_TSTAT(bf_sweep_page_hot_skipped);
            return false; 
        }
    }
    
    // can't afford to skip hot pages any more
    return p.dirty();
}

NORET
bf_filter_lsn_t::bf_filter_lsn_t(const lsn_t& lsn)  
    : _lsn(lsn)
{
}


bool
bf_filter_lsn_t::is_good(const bfcb_t& p) const
{
    // replaced the following with 
    // a simpler computation of return value
    // return p.pid.page && p.rec_lsn()  && (p.rec_lsn() <= _lsn);
    
#if W_DEBUG_LEVEL > 2
    if( ! p.pid().page ) {
        w_assert3(! p.curr_rec_lsn().valid() );
    }
#endif 

    // This includes determination of st_tmp pages:
    return 
        p.curr_rec_lsn().valid()  
        && 
        (p.curr_rec_lsn() <= _lsn);
}



int
bf_m::collect(vtable_t &res, bool names_too)
{
    return _core->collect(res, names_too);
}

// These are collected in smstats.cpp
void                        
bf_m::htab_stats(bf_htab_stats_t &out) const
{
    if(_core) _core->htab_stats(out);
}

// Called in w_assertN for
// some N>0 so it's not used in an optimized build:
bool
bf_m::check_lsn_invariant(const page_s *page)
{
    bfcb_t *b = bf_m::get_cb(page);
    return check_lsn_invariant(b);
}
bool
bf_m::check_lsn_invariant(const bfcb_t *b)
{
    // Test 1: is page in buffer pool?
    // If not, return true, although we should
    // not call this in such a case.
    w_assert1(_core->_in_htab(b));

    bool dirty = b->dirty();
    bool is_tmp = (b->get_storeflags() & st_tmp) == st_tmp;
    lsn_t rec_lsn = b->curr_rec_lsn();
    lsn_t page_lsn = b->frame()->lsn;

    if(rec_lsn.valid()) {
        // rec_lsn valid : intent to modify
        if(dirty) {
            // dirty bit true : modify logged or
            // is temp page and modify is done but not logged.
            
            w_assert0(is_tmp || page_lsn >= rec_lsn);
        }
        else
        {
            // dirty bit false
            // modify not yet logged or page is temp
            // and it won't get logged.
            // Must be fixed in EX mode.
            // This only makes sense if the holder is me,
            // otherwise it's racy.
            w_assert0(b->latch.is_mine());
            w_assert0(is_tmp || page_lsn <= rec_lsn);
        }
    } else {
        // rec_lsn not valid : page should be clean.
        if(dirty) {
            // rec_lsn, tail-of-log lsn aren't meaningful here.
            w_assert0(smlevel_0::in_recovery_redo());
            w_assert0(b->latch.is_mine());
        } else {
            // clean, durable. May be fixed SH, unfixed.
            w_assert0(b->latch.is_mine()==false);
            w_assert0(is_tmp || page_lsn >= rec_lsn);
        }
    }
    // If we get this far, we're ok
    return true;
}
