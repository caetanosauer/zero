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

 $Id: restart.cpp,v 1.145 2010/12/08 17:37:43 nhall Exp $

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
#define RESTART_C

#ifdef __GNUG__
#pragma implementation "restart.h"
#pragma implementation "restart_s.h"
#endif

#include <sm_int_1.h>
#include "restart.h"
#include "restart_s.h"
#include "w_heap.h"
// include crash.h for definition of LOGTRACE1
#include "crash.h"


#ifdef EXPLICIT_TEMPLATE
template class w_hash_t<dp_entry_t, unsafe_list_dummy_lock_t, bfpid_t>;
template class w_hash_i<dp_entry_t, unsafe_list_dummy_lock_t, bfpid_t>;
template class w_list_t<dp_entry_t, unsafe_list_dummy_lock_t>;
template class w_list_i<dp_entry_t, unsafe_list_dummy_lock_t>;
template class Heap<xct_t*, CmpXctUndoLsns>;
#endif

typedef class Heap<xct_t*, CmpXctUndoLsns> XctPtrHeap;

tid_t                restart_m::_redo_tid;

/*********************************************************************
 *
 *  class dirty_pages_tab_t
 *
 *  In-memory dirty pages table -- a dictionary of of pid and 
 *  its recovery lsn.  Used only in recovery, which is to say,
 *  only 1 thread is active here, so the hash table isn't 
 *  protected.
 *
 *********************************************************************/
class dirty_pages_tab_t {
public:
    NORET                        dirty_pages_tab_t(int sz);
    NORET                        ~dirty_pages_tab_t();
    
    dirty_pages_tab_t&           insert(const lpid_t&  pid, const lsn_t& lsn);
    dirty_pages_tab_t&           remove(const lpid_t& pid);

    bool                         look_up(const lpid_t& pid, lsn_t** lsn = 0); 

    lsn_t                        min_rec_lsn();

    int                          size() const { return tab.num_members(); }
    void                         check(ostream &) const;
    
    friend ostream& operator<<(ostream&, const dirty_pages_tab_t& s);
    
private:
    w_hash_t<dp_entry_t, unsafe_list_dummy_lock_t, bfpid_t> tab; // hash table for dictionary

    // disabled
    NORET                        dirty_pages_tab_t(const dirty_pages_tab_t&);
    dirty_pages_tab_t&           operator=(const dirty_pages_tab_t&);

    lsn_t                        cachedMinRecLSN;
    bool                         validCachedMinRecLSN;
};


/*********************************************************************
 *  
 *  restart_m::recover(master)
 *
 *  Start the recovery process. Master is the master lsn (lsn of
 *  the last successful checkpoint record).
 *
 *********************************************************************/
void 
restart_m::recover(lsn_t master)
{
    FUNC(restart_m::recover);
    dirty_pages_tab_t dptab(2 * bf->npages());
    lsn_t redo_lsn;

    // set so mount and dismount redo can tell that they should log stuff.

    smlevel_0::errlog->clog << info_prio << "Restart recovery:" << flushl;
#if W_DEBUG_LEVEL > 2
    {
        DBGOUT5(<<"TX TABLE before analysis:");
        xct_i iter(true); // lock list
        xct_t* xd;
        while ((xd = iter.next()))  {
            w_assert2(  xd->state() == xct_t::xct_active ||
                    xd->state() == xct_t::xct_prepared ||
                    xd->state() == xct_t::xct_freeing_space );
            DBGOUT5(<< "transaction " << xd->tid() << " has state " << xd->state());
        }
        DBGOUT5(<<"END TX TABLE before analysis:");
    }
#endif 

    /*
     *  Phase 1: ANALYSIS.
     *  Output : dirty page table and redo lsn
     */
    smlevel_0::errlog->clog << info_prio << "Analysis ..." << flushl;


    DBGOUT3(<<"starting analysis at " << master << " redo_lsn = " << redo_lsn);
    if(logtrace) {
        // Print some info about the log tracing that will follow.
        // It's so hard to deciper if you're not always looking at this, so
        // we print a little legend.
        fprintf(stderr, "\nLEGEND:\n");
        w_ostrstream s; 
        s <<" th.#" 
        << " STT" 
        << " lsn"
        << " A/R/I/U"
        << "LOGREC(TID, TYPE, FLAGS:F/U PAGE <INFO> (prev)|[prev]";
        fprintf(stderr, "%s\n", s.c_str()); 
        fprintf(stderr, " #: thread id\n");
        fprintf(stderr, " STT: xct state or ??? if unknown\n");
        fprintf(stderr, " A: read for analysis\n");
        fprintf(stderr, " R: read for redo\n");
        fprintf(stderr, " U: read for rollback\n");
        fprintf(stderr, " I: inserted (undo pass or after recovery)\n");
        fprintf(stderr, " F: inserted by xct in forward processing\n");
        fprintf(stderr, " U: inserted by xct while rolling back\n");
        fprintf(stderr, " [prev-lsn] for non-compensation records\n");
        fprintf(stderr, " (undo-lsn) for compensation records\n");
        fprintf(stderr, "\n\n");
    }
    analysis_pass(master, dptab, redo_lsn);

    if(dptab.size() || xct_t::num_active_xcts()) {
        smlevel_0::errlog->clog << info_prio 
            << "Log contains " << dptab.size()
            << " dirty pages and " << xct_t::num_active_xcts()
            << " active transactions" << flushl;
    }
    else {
        smlevel_0::errlog->clog << info_prio  
            << "Database is clean" << flushl;
    }

    // For recovery debugging: dptab.check(cerr);
    
    /*
     *  Phase 2: REDO -- use dirty page table and redo lsn of phase 1
     *                  We save curr_lsn before redo_pass() and assert after
     *                 redo_pass that no log record has been generated.
     *  pass in highest_lsn for debugging
     */
    smlevel_0::errlog->clog << info_prio << "Redo ..." << flushl;
    lsn_t curr_lsn = log->curr_lsn(); 

#if W_DEBUG_LEVEL > 2
    {
        DBGOUT5(<<"TX TABLE at end of analysis:");
        xct_i iter(true); // lock list
        xct_t* xd;
        while ((xd = iter.next()))  {
            w_assert1(  xd->state() == xct_t::xct_active ||
                        xd->state() == xct_t::xct_prepared);
            DBGOUT5(<< "Transaction " << xd->tid() << " has state " << xd->state());
        }
        DBGOUT5(<<"END TX TABLE at end of analysis:");
    }
#endif 

    DBGOUT3(<<"starting redo at " << redo_lsn << " highest_lsn " << curr_lsn);
    redo_pass(redo_lsn, curr_lsn, dptab);


    /* no logging during redo */
    w_assert1(curr_lsn == log->curr_lsn()); 

    /* In order to preserve the invariant that the rec_lsn <= page's lsn1,
     * we need to make sure that all dirty pages get flushed to disk,
     * since the redo phase does NOT log these page updates, it causes
     * rec_lsns to be at the tail of the log while the page lsns are
     * in the middle of the log somewhere.  It seems worthwhile to
     * do this flush, slow though it might be, because if we have a crash
     * and have to re-recover, we would have less to do at that time.
     */
    W_COERCE(bf->force_all(true));

    /*
     *  Phase 3: UNDO -- abort all active transactions
     */
    smlevel_0::errlog->clog  << info_prio<< "Undo ..." 
        << " curr_lsn = " << curr_lsn
        << flushl;

    undo_pass();

    smlevel_0::errlog->clog << info_prio << "Oldest active transaction is " 
        << xct_t::oldest_tid() << flushl;
    smlevel_0::errlog->clog << info_prio 
        << "First new transaction will be greater than "
        << xct_t::youngest_tid() << flushl;

#if W_DEBUG_LEVEL >= 0
    /* Print the prepared xcts even if not in debug mode 
     * because the locks held by prepared transactions
     * can prevent any other work from being done
     */
    {
        int number=0;

        smlevel_0::errlog->clog << info_prio 
        << "Prepared transactions:" << endl;
        DBGOUT5(<<"TX TABLE at end of recovery:");
        xct_i iter(true); // lock list
        xct_t* xd;
        while ((xd = iter.next()))  {
            w_assert0(xd->state()==xct_t::xct_prepared);
            server_handle_t ch = xd->get_coordinator();
            const gtid_t *gtid = xd->gtid();
            smlevel_0::errlog->clog << info_prio 
                << "Tid: " <<xd->tid() << endl;
                if(gtid) {
                    smlevel_0::errlog->clog << info_prio 
                    << "\t Global tid: " << *gtid << endl;
                } else {
                    smlevel_0::errlog->clog << info_prio 
                    << "\t No global tid. " << endl;
                }
                smlevel_0::errlog->clog << info_prio 
                << "\t Coordinator: " << ch
                << flushl;
            number++;
        }
        if(number == 0) {
            smlevel_0::errlog->clog << info_prio  << " none." << endl;
        } else {
            smlevel_0::errlog->clog << info_prio 
            << "*************************  WARNING ****************************"
            << endl
            << endl
            << "WARNING: There are prepared transactions to be resolved!" 
            << endl
            << endl
            << "***************************************************************"
            << endl;
        }
        DBG(<<"END TX TABLE at end of recovery:");
    }
#endif 

    smlevel_0::errlog->clog << info_prio << "Restart successful." << flushl;
}


/*********************************************************************
 *
 *  restart_m::analysis_pass(master, dptab, redo_lsn)
 *
 *  Scan log forward from master_lsn. Insert and update dptab.
 *  Compute redo_lsn.
 *
 *********************************************************************/
void 
restart_m::analysis_pass(
    lsn_t                 master,
    dirty_pages_tab_t&    dptab,
    lsn_t&                redo_lsn
)
{
    FUNC(restart_m::analysis_pass);

    AutoTurnOffLogging turnedOnWhenDestroyed;

    redo_lsn = lsn_t::null;
    if (master == lsn_t::null) return;

    smlevel_0::operating_mode = smlevel_0::t_in_analysis;

    /*
     *  Open a forward scan
     */
    log_i         scan(*log, master);
    logrec_t*     log_rec_buf;
    lsn_t         lsn;

    lsn_t         theLastMountLSNBeforeChkpt;

    /*
     *  Assert first record is Checkpoint Begin Log
     *  and get last mount/dismount lsn from it
     */
    {
        if (! scan.next(lsn, log_rec_buf)) {
            W_COERCE(scan.get_last_rc());
        }
        logrec_t&        r = *log_rec_buf;
        w_assert1(r.type() == logrec_t::t_chkpt_begin);
        theLastMountLSNBeforeChkpt = *(lsn_t *)r.data();
        DBGOUT3( << "last mount LSN from chkpt_begin=" << theLastMountLSNBeforeChkpt);
    }

    unsigned int cur_segment = 0;
    
    /*
     *  Number of complete chkpts handled.  Only the first
     *  chkpt is actually handled.  There may be a second
     *  complete chkpt due to a race condition between writing
     *  a chkpt_end record, updating the master lsn and crashing.
     *  Used to avoid processing an incomplete checkpoint.
     */
    int num_chkpt_end_handled = 0;

    while (scan.next(lsn, log_rec_buf)) {
        logrec_t&        r = *log_rec_buf;

        /*
         *  Scan next record
         */
        DBGOUT5( << setiosflags(ios::right) << lsn 
                  << resetiosflags(ios::right) << " A: " << r );
        w_assert1(lsn == r.lsn_ck());

        if(lsn.hi() != cur_segment) {
            cur_segment = lsn.hi();
            smlevel_0::errlog->clog << info_prio  
               << "Analyzing log segment " << cur_segment << flushl;
        }
        
        //if the log was a system transaction fused to a single log entry,
        // we should do the equivalent to xct_end here
        if (r.is_single_sys_xct()) {
            xct_t* xd = xct_t::new_xct(xct_t::_nxt_tid.atomic_incr(), // let's use a new transaction id
                xct_t::xct_active, lsn,
                lsn_t::null); // no next_undo
            w_assert1(xd);
            xd->set_last_lsn(lsn);

            lpid_t page_of_interest = r.construct_pid();
            DBGOUT5(<<"analysis (single_log system xct): default " <<  r.type()
                    << " page of interest " << page_of_interest);
            w_assert1(!r.is_undo()); // no UNDO for ssx
            w_assert0(r.is_redo());
            if (!(dptab.look_up(page_of_interest))) {
                dptab.insert( page_of_interest, lsn );
            }
            
            xd->change_state(xct_t::xct_ended);
            xct_t::destroy_xct(xd);
            continue;
        }

        xct_t* xd = 0;

        /*
         *  If log is transaction related, insert the transaction
         *  into transaction table if it is not already there.
         */
        if ((r.tid() != tid_t::null) && ! (xd = xct_t::look_up(r.tid()))
                // comments can be after xct has ended
                    && r.type()!=logrec_t::t_comment ) {
            DBGOUT5(<<"analysis: inserting tx " << r.tid() << " active ");
            xd = xct_t::new_xct(r.tid(), xct_t::xct_active, lsn, r.prev());
            w_assert1(xd);
            xct_t::update_youngest_tid(r.tid());
        }

        /*
         *  Update last lsn of transaction
         */
        if (xd) {
            xd->set_last_lsn(lsn);
            w_assert1( xd->tid() == r.tid() );
        }

        switch (r.type()) {
        case logrec_t::t_xct_prepare_st:
        case logrec_t::t_xct_prepare_lk:
        case logrec_t::t_xct_prepare_alk:
        case logrec_t::t_xct_prepare_stores:
        case logrec_t::t_xct_prepare_fi:
            if (num_chkpt_end_handled == 0)  {
                // - redo now, because our redo phase can start after
                //   the master checkpoint.
                // - records after chkpt will be handled in redo and only
                //   if the xct is not in the prepared state to prevent
                // - redoing these records.
                //   records before/during chkpt will be ignored in redo
                r.redo(0);
            }
            break;

        case logrec_t::t_chkpt_begin:
            /*
             *  Found an incomplete checkpoint --- ignore 
             */
            break;

        case logrec_t::t_chkpt_bf_tab:
            if (num_chkpt_end_handled == 0)  {
                /*
                 *  Still processing the master checkpoint record.
                 *  For each entry in log,
                 *        if it is not in dptab, insert it.
                 *          If it is already in the dptab, update the rec_lsn.
                 */
                const chkpt_bf_tab_t* dp = (chkpt_bf_tab_t*) r.data();
                for (uint i = 0; i < dp->count; i++)  {
                    lsn_t* rec_lsn;
                    if (! dptab.look_up(dp->brec[i].pid, &rec_lsn))  {
                        DBGOUT5(<<"dptab.insert dirty pg " 
                        << dp->brec[i].pid << " " << dp->brec[i].rec_lsn);
                        dptab.insert(dp->brec[i].pid, dp->brec[i].rec_lsn);
                    } else {
                        DBGOUT5(<<"dptab.update dirty pg " 
                        << dp->brec[i].pid << " " << dp->brec[i].rec_lsn);
                        *rec_lsn = dp->brec[i].rec_lsn;
                    }
                }
            }
            break;
                
        case logrec_t::t_chkpt_xct_tab:
            if (num_chkpt_end_handled == 0)  {
                /*
                 *  Still processing the master checkpoint record.
                 *  For each entry in the log,
                 *         If the xct is not in xct tab, insert it.
                 */
                const chkpt_xct_tab_t* dp = (chkpt_xct_tab_t*) r.data();
                xct_t::update_youngest_tid(dp->youngest);
                for (uint i = 0; i < dp->count; i++)  {
                    xd = xct_t::look_up(dp->xrec[i].tid);
                    if (!xd) {
                        if (dp->xrec[i].state != xct_t::xct_ended)  {
                            xd = xct_t::new_xct(dp->xrec[i].tid,
                                           dp->xrec[i].state,
                                           dp->xrec[i].last_lsn,
                                           dp->xrec[i].undo_nxt);
                            DBGOUT5(<<"add xct " << dp->xrec[i].tid
                                    << " state " << dp->xrec[i].state
                                    << " last lsn " << dp->xrec[i].last_lsn
                                    << " undo " << dp->xrec[i].undo_nxt
                                );
                            w_assert1(xd);
                        }
                        // skip finished ones
                        // (they can get in there!)
                    } else {
                       // Could be active or aborting
                       w_assert9(dp->xrec[i].state != xct_t::xct_ended);
                    }
                }
            }
            break;
            
        case logrec_t::t_chkpt_dev_tab:
            if (num_chkpt_end_handled == 0)  {
                /*
                 *  Still processing the master checkpoint record.
                 *  For each entry in the log, mount the device.
                 */
                const chkpt_dev_tab_t* dv = (chkpt_dev_tab_t*) r.data();
                for (uint i = 0; i < dv->count; i++)  {
                    smlevel_0::errlog->clog << info_prio 
                        << "Device " << dv->devrec[i].dev_name 
                         << " will be recovered as vid " << dv->devrec[i].vid
                         << flushl;
                    W_COERCE(io_m::mount(dv->devrec[i].dev_name, 
                                       dv->devrec[i].vid));

                    w_assert9(io_m::is_mounted(dv->devrec[i].vid));
                }
            }
            break;
        
        case logrec_t::t_dismount_vol:
        case logrec_t::t_mount_vol:
            /* JK: perform all mounts and dismounts up to the minimum redo lsn,
            * so that the system has the right volumes mounted during 
            * the redo phase.  the only time the this should  be redone is 
            * when no dirty pages were in the checkpoint and a 
            * mount/dismount occurs  before the first page is dirtied after 
            * the checkpoint.  the case of the first dirty  page occuring 
            * before the checkpoint is handled by undoing mounts/dismounts 
            * back to the min dirty page lsn in the analysis_pass 
            * after the log has been scanned.
            */

            w_assert9(num_chkpt_end_handled > 0);  
            // mount & dismount shouldn't happen during a check point
            
            if (lsn < dptab.min_rec_lsn())  {
                r.redo(0);
            }
            break;
                
        case logrec_t::t_chkpt_end:
            /*
             *  Done with the master checkpoint record. Flag true 
             *  to avoid processing an incomplete checkpoint.
             */
#if W_DEBUG_LEVEL > 4
            {
                lsn_t l, l2;
                volatile unsigned long i = sizeof(lsn_t); 
                        // GROT: stop gcc from 
                        // optimizing memcpy into something that 
                        // chokes on sparc due to misalignment

                memcpy(&l, (lsn_t*) r.data(), i);
                memcpy(&l2, ((lsn_t*) r.data())+1, i);

                DBGOUT5(<<"checkpt end: master=" << l 
                    << " min_rec_lsn= " << l2);

                if(lsn == l) {
                    w_assert9(l2 == dptab.min_rec_lsn());
                }
            }

            if (num_chkpt_end_handled > 2) {
                /*
                 * We hope we do not encounter more than one complete chkpt.
                 * Unfortunately, we *can* crash between the flushing
                 * of a checkpoint-end record and the time we
                 * update the master record (move the pointer to the last
                 * checkpoint)
                 */
                smlevel_0::errlog->clog  << error_prio
                << "Warning: more than 2 complete checkpoints found! " 
                <<flushl;
                /* 
                 * comment out the following if you are testing
                 * a situation that involves a crash at the
                 * critical point
                 */
                // w_assert9(0);
            }

#endif 

            num_chkpt_end_handled++;
            break;


        case logrec_t::t_xct_freeing_space:
                xd->change_state(xct_t::xct_freeing_space);
                break;

        case logrec_t::t_xct_end_group: 
                {
                // Do what we do for t_xct_end for each of the
                // transactions in the list, then drop through
                // and do it for the xct named in "xd" (the attached one)
            
                const xct_list_t* list = (xct_list_t*) r.data();
                int listlen = list->count;
                for(int i=0; i < listlen; i++)
                {
                    xd = xct_t::look_up(list->xrec[i].tid);
                    // If it's not there, it could have been a read-only xct?
                    w_assert0(xd);
                    /* Now do exactly what's done below */
                    /*
                     *  Remove xct from xct tab
                     */
                    if (xd->state() == xct_t::xct_prepared || 
                            xd->state() == xct_t::xct_freeing_space) 
                    {
                        /*
                         * was prepared in the master
                         * checkpoint, so the locks
                         * were acquired.  have to free them
                         */
                        me()->attach_xct(xd);        
                        W_COERCE( lm->unlock_duration() );
                        me()->detach_xct(xd);        
                    }
                    xd->change_state(xct_t::xct_ended);
                    xct_t::destroy_xct(xd);
                }
                }
                break;

        case logrec_t::t_xct_abort:
        case logrec_t::t_xct_end:
            /*
             *  Remove xct from xct tab
             */
            if (xd->state() == xct_t::xct_prepared || 
                    xd->state() == xct_t::xct_freeing_space) 
            {
                /*
                 * was prepared in the master
                 * checkpoint, so the locks
                 * were acquired.  have to free them
                 */
                me()->attach_xct(xd);        
                W_COERCE( lm->unlock_duration() );
                me()->detach_xct(xd);        
            }
            xd->change_state(xct_t::xct_ended);
            xct_t::destroy_xct(xd);
            break;

        default: {
            lpid_t page_of_interest = r.construct_pid();
            DBGOUT5(<<"analysis: default " << 
                    r.type() << " tid " << r.tid()
                    << " page of interest " << page_of_interest);
            if (r.is_page_update()) {
                DBGOUT5(<<"is page update " );
                DBGOUT5( << setiosflags(ios::right) << lsn 
                    << resetiosflags(ios::right) << " A: " 
                    << "is page update " << page_of_interest );
                // redoable, has a pid, and is not compensated.
                // Why the compensated predicate?
                if (r.is_undo()) {
                    /*
                     *  r is undoable. Update next undo lsn of xct
                     */
                    xd->set_undo_nxt(lsn);
                }
                w_assert0(r.is_redo());
                // TODO: remove the predicate is_redo below. It's
                // superfluous
                if (r.is_redo() && !(dptab.look_up(page_of_interest))) {
                    /*
                     *  r is redoable and not in dptab ...
                     *  Register a new dirty page.
                     */
                    DBGOUT5(<<"dptab.insert dirty pg " << page_of_interest 
                        << " " << lsn);
                    DBGOUT5( << setiosflags(ios::right) << lsn
                            << resetiosflags(ios::right) << " A: " 
                            << "insert in dirty page table " << page_of_interest );
                    dptab.insert( page_of_interest, lsn );
                }

            } else if (r.is_cpsn()) {
                /* 
                 *  Update undo_nxt lsn of xct
                 */
                if(r.is_undo()) {
                    DBGOUT5(<<"is cpsn, undo " << " undo_nxt<--lsn " << lsn );
                    /*
                     *  r is undoable. There is one possible case of
                     *  this (undoable compensation record)
                     */
                    xd->set_undo_nxt(lsn);
                } else {
                    DBGOUT5(<<"is cpsn, not undo " << " undo_next<--lsn " << r.undo_nxt() );
                    xd->set_undo_nxt(r.undo_nxt());
                }
                if (r.is_redo() && !(dptab.look_up(page_of_interest))) {
                    /*
                     *  r is redoable and not in dptab ...
                     *  Register a new dirty page.
                     */
                    DBGOUT5(<<"dptab.insert dirty pg " << page_of_interest 
                        << " " << lsn);
                    DBGOUT5( << setiosflags(ios::right) << lsn
                          << resetiosflags(ios::right) << " A: " 
                          << "insert in dirty page table " << page_of_interest );
                    dptab.insert( page_of_interest, lsn );
                }
            } else if (
                    r.type()!=logrec_t::t_comment
                    && r.type()!=logrec_t::t_btree_noop
                    ) {
                W_FATAL(eINTERNAL);
            }
        }// case default
        }// switch
    }

    /*
     *  Start of redo is the minimum of recovery lsn of all entries
     *  in the dirty page table.
     */
    redo_lsn = dptab.min_rec_lsn();

    /*
     * undo any mounts/dismounts that occured between chkpt and min_rec_lsn
     */
    DBGOUT3( << ((theLastMountLSNBeforeChkpt != lsn_t::null && 
                    theLastMountLSNBeforeChkpt > redo_lsn) \
            ? "redoing mounts/dismounts before chkpt but after redo_lsn"  \
            : "no mounts/dismounts need to be redone"));

    { // Contain the scope of the following __copy__buf:

    logrec_t* __copy__buf = new logrec_t;
    if(! __copy__buf) { W_FATAL(eOUTOFMEMORY); }
    w_auto_delete_t<logrec_t> auto_del(__copy__buf);
    logrec_t&         copy = *__copy__buf;

    while (theLastMountLSNBeforeChkpt != lsn_t::null 
        && theLastMountLSNBeforeChkpt > redo_lsn)  {

        W_COERCE(log->fetch(theLastMountLSNBeforeChkpt, log_rec_buf, 0));  

        // HAVE THE LOG_M MUTEX
        // We have to release it in order to do the mount/dismounts
        // so we make a copy of the log record (log_rec_buf points
        // into the log_m's copy, and thus we have the mutex.`

        logrec_t& r = *log_rec_buf;

        /* Only copy the valid portion of the log record. */
        memcpy(__copy__buf, &r, r.length());
        log->release();

        DBGOUT3( << theLastMountLSNBeforeChkpt << ": " << copy );

        w_assert9(copy.type() == logrec_t::t_dismount_vol || 
                    copy.type() == logrec_t::t_mount_vol);

        chkpt_dev_tab_t *dp = (chkpt_dev_tab_t*)copy.data();
        w_assert9(dp->count == 1);

        // it is ok if the mount/dismount fails, since this 
        // may be caused by the destruction
        // of the volume.  if that was the case then there 
        // won't be updates that need to be
        // done/undone to this volume so it doesn't matter.
        if (copy.type() == logrec_t::t_dismount_vol)  {
            W_IGNORE(io_m::mount(dp->devrec[0].dev_name, dp->devrec[0].vid));
        }  else  {
            W_IGNORE(io_m::dismount(dp->devrec[0].vid));
        }

        theLastMountLSNBeforeChkpt = copy.prev();
    }
    // close scope so the
    // auto-release will free the log rec copy buffer, __copy__buf
    } 

    io_m::SetLastMountLSN(theLastMountLSNBeforeChkpt);

    w_base_t::base_stat_t f = GET_TSTAT(log_fetches);
    w_base_t::base_stat_t i = GET_TSTAT(log_inserts);
    smlevel_0::errlog->clog << info_prio 
        << "After analysis_pass: " 
        << f << " log_fetches, " 
        << i << " log_inserts " 
        << " redo_lsn is "  << redo_lsn
        << flushl;
}



/*********************************************************************
 * 
 *  restart_m::redo_pass(redo_lsn, highest_lsn, dptab)
 *
 *  Scan log forward from redo_lsn. Base on entries in dptab, 
 *  apply redo if durable page is old.
 *
 *********************************************************************/
void 
restart_m::redo_pass(
    lsn_t redo_lsn, 
    const lsn_t & W_IFDEBUG3(highest_lsn),
    dirty_pages_tab_t& dptab
)
{
    FUNC(restart_m::redo_pass);
    smlevel_0::operating_mode = smlevel_0::t_in_redo;

    AutoTurnOffLogging turnedOnWhenDestroyed;

    /*
     *  Open a scan
     */
    DBGOUT3(<<"Start redo scanning at redo_lsn = " << redo_lsn);
    log_i scan(*log, redo_lsn);
    lsn_t cur_lsn = log->curr_lsn();
    if(redo_lsn < cur_lsn) {
        DBGOUT3(<< "Redoing log from " << redo_lsn
                << " to " << cur_lsn);
        smlevel_0::errlog->clog << info_prio  
            << "Redoing log from " << redo_lsn 
            << " to " << cur_lsn << flushl;
    }
    DBGOUT3( << "LSN " << " A/R/I(pass): " << "LOGREC(TID, TYPE, FLAGS:F/U(fwd/rolling-back) PAGE <INFO>");

    /*
     *  Allocate a (temporary) log record buffer for reading 
     */
    logrec_t* log_rec_buf=0;

    lsn_t lsn;
    lsn_t expected_lsn = redo_lsn;
    while (scan.next(lsn, log_rec_buf))  {
        DBGOUT5(<<"redo scan returned lsn " << lsn
                << " expected " << expected_lsn);

        logrec_t& r = *log_rec_buf;
        /*
         *  For each log record ...
         */
        lsn_t* rec_lsn = 0;                // points to rec_lsn in dptab entry

        if (!r.valid_header(lsn)) {
            smlevel_0::errlog->clog << error_prio 
            << "Internal error during redo recovery." << flushl;
            smlevel_0::errlog->clog << error_prio 
            << "    log record at position: " << lsn 
            << " appears invalid." << endl << flushl;
            abort();
        }

        bool redone = false;
        DBGOUT5( << setiosflags(ios::right) << lsn
                      << resetiosflags(ios::right) << " R: " << r);
        w_assert1(lsn == r.lsn_ck());
        w_assert1(lsn == expected_lsn || lsn.hi() == expected_lsn.hi()+1);
        expected_lsn.advance(r.length());

        if ( r.is_redo() ) {
            if (r.null_pid()) {
                /*
                 * If the transaction is still in the table after analysis, 
                 * it didn't get committed or aborted yet,
                 * so go ahead and process it.  
                 * If it isn't in the table, it was  already 
                 * committed or aborted.
                 * If it's in the table, its state is prepared or active.
                 * Nothing in the table should now be in aborting state.
                 */
                if (r.tid() != tid_t::null)  {
                    xct_t *xd = xct_t::look_up(r.tid());
                    if (xd) {
                        if (xd->state() == xct_t::xct_active)  {
                            DBGOUT5(<<"redo - no page, xct is " << r.tid());
                            r.redo(0);
                            redone = true;
                        }  else  {
                            DBGOUT5(<<"no page, prepared xct " << r.tid());
                            w_assert1(xd->state() == xct_t::xct_prepared);
                            w_assert2(r.type() == logrec_t::t_xct_prepare_st
                                ||    r.type() == logrec_t::t_xct_prepare_lk
                                ||    r.type() == logrec_t::t_xct_prepare_alk
                                ||    r.type() == logrec_t::t_xct_prepare_stores
                                ||    r.type() == logrec_t::t_xct_prepare_fi);
                        }
                    }
                }  else  {
                    // JK: redo mounts and dismounts, at the start of redo, 
                    // all the volumes which
                    // were mounted at the redo lsn should be mounted.  
                    // need to do this to take
                    // care of the case of creating a volume which mounts the 
                        // volume under a temporary
                    // volume id inorder to create stores and initialize the 
                        // volume.  this temporary
                    // volume id can be reused, which is why this must be done.

                    w_assert9(r.type() == logrec_t::t_dismount_vol || 
                                r.type() == logrec_t::t_mount_vol);
                    DBGOUT5(<<"redo - no page, no xct ");
                    r.redo(0);
                    io_m::SetLastMountLSN(lsn);
                    redone = true;
                }

            } else {
                lpid_t        page_updated = r.construct_pid();
                bool found = dptab.look_up(page_updated, &rec_lsn);
                if(found && (lsn >= *rec_lsn))  {
                    /*
                     *  We are only concerned about log records that involve
                     *  page updates.
                     */
                    DBGOUT5(<<"redo page update, pid " 
                            << r.shpid() 
                            << "(" << page_updated << ")"
                            << " rec_lsn: "  << *rec_lsn
                            << " log record: "  << lsn
                            );
                    w_assert1(r.shpid()); 

                    /*
                     *  Fix the page.
                     */ 
                    page_p page;

                    /* 
                     * The following code determines whether to perform
                     * redo on the page.  If the log record is for a page
                     * format (page_init) then there are two possible
                     * implementations.
                     * 
                     * 1) Trusted LSN on New Pages
                     *   If we assume that the LSNs on new pages can always be
                     *   trusted then the code reads in the page and 
                     *   checks the page lsn to see if the log record
                     *   needs to be redone.  Note that this requires that
                     *   pages on volumes stored on a raw device must be
                     *   zero'd when the volume is created.
                     * 
                     * 2) No Trusted LSN on New Pages
                     *   If new pages are not in a known (ie. lsn of 0) state
                     *   then when a page_init record is encountered, it
                     *   must always be redone and therefore all records after
                     *   it must be redone.
                     *
                     * ATTENTION!!!!!! case 2 causes problems with
                     *   tmp file pages that can get reformatted as tmp files,
                     *   then converted to regular followed by a restart with
                     *   no chkpt after the conversion and flushing of pages
                     *   to disk, and so it has been disabled. That is to
                     *   say:
                     *
                     *   DO NOT BUILD WITH
                     *   DONT_TRUST_PAGE_LSN defined . In any case, I
                     *   removed the code for its defined case.
                     */
                    store_flag_t store_flags = st_bad;
                    DBGOUT5(<< "TRUST_PAGE_LSN");
                    // TODO TODO ! this should use the special bufferpool for alloc_p and stnode_p
                    //if (r.tag() == page_p::t_alloc_p || r.tag() == page_p::t_stnode_p) {
                    //} else {
                        W_COERCE( page.fix(page_updated,
                                    page_p::t_any_p, 
                                    LATCH_EX, 
                                    0,  // page_flags
                                    store_flags,
                                    true // ignore store_id
                                    ) );
                    //}

#if W_DEBUG_LEVEL > 2
                    if(page_updated != page.pid()) {
                        DBGOUT5(<<"Pids don't match: expected " << page_updated
                            << " got " << page.pid());
                    }
#endif 

                    lsn_t page_lsn = page.lsn();
                    DBGOUT5( << setiosflags(ios::right) << lsn
                              << resetiosflags(ios::right) << " R: " 
                                << " page_lsn " << page_lsn
                                << " will redo if 1: " << int(page_lsn < lsn));
                    if (page_lsn < lsn) 
                    {
                        /*
                         *  Redo must be performed if page has lower lsn 
                         *  than record.
                         *
                         * NB: this business of attaching the xct isn't
                         * all that reliable.  If the xct was found during
                         * analysis to have committed, the xct won't be found
                         * in the table, yet we might have to redo the records
                         * anyway.  For that reason, not only do we attach it,
                         * but we also stuff it into a global variable, redo_tid.
                         * This is redundant, and we should fix this.  The 
                         * RIGHT thing to do is probably to leave the xct in the table
                         * after analysis, and make xct_end redo-able -- at that
                         * point, we should remove the xct from the table.
                         * However, since we don't have any code that really needs this
                         * to happen (recovery all happens w/o grabbing locks; there
                         * is no need for xct in redo as of this writing, and for
                         * undo we will not have found the xct to have ended), we
                         * choose to leave well enough alone.
                         */
                        if (!r.is_single_sys_xct()) {
                            xct_t* xd = 0; 
                            if (r.tid() != tid_t::null)  { 
                                if ((xd = xct_t::look_up(r.tid())))  {
                                    /*
                                    * xd will be aborted following redo
                                    * thread attached to xd to make sure that
                                    * redo is correct for the transaction
                                    */
                                    me()->attach_xct(xd);
                                }
                            }

                            /*
                            *  Perform the redo. Do not generate log.
                            */
                            {
                                bool was_dirty = page.is_dirty();
                                // remember the tid for space resv hack.
                                _redo_tid = r.tid();
                                r.redo(page.is_fixed() ? &page : 0);
                                redone = true;
                                _redo_tid = tid_t::null;
                                page.set_lsns(lsn);        /* page is updated */

                                /* If we crash during recovery the _default_
                                value_of_rec_lsn_ is too high and we risk
                                losing data if a checkpoint sees it.
                                By _default_value_of_rec_lsn_ what is meant
                                is that which is set by update_rec_lsn on
                                the page fix.  That is, it is set to the
                                tail of the log,  which is correct for
                                forward processing, but not for recovery
                                processing.
                                The problem is that because the log allows a
                                scan to be ongoing while other threads
                                are appending to the tail, there is no one
                                "current" log pointer. So we can't easily
                                ask the log for the correct lsn - it's
                                context-dependent.  The fix code is too far
                                in the call stack from that context, so it's
                                hard for bf's update_rec_lsn to get the right
                                lsn. Therefore, it's optimized
                                for the most common case in forward processing, 
                                and recovery/redo, tmp-page and other unlogged-
                                update cases have to expend a little more
                                effort to keep the rec_lsn accurate.
                    
                                FRJ: in our case the correct rec_lsn is
                                anything not later than the new
                                page_lsn (as if it had just been logged
                                the first time, back in the past)
                                */
                                page.repair_rec_lsn(was_dirty, lsn);
                            }
                                
                            if (xd) me()->detach_xct(xd);
                        } else {
                            // single-log-sys-xct doesn't have tid (because it's not needed!).
                            // we simply creates a new ssx and runs it.
                            sys_xct_section_t sxs (true); // single log!
                            w_assert1(!sxs.check_error_on_start().is_error());
                            bool was_dirty = page.is_dirty();
                            r.redo(page.is_fixed() ? &page : 0);
                            redone = true;
                            page.set_lsns(lsn);
                            page.repair_rec_lsn(was_dirty, lsn);
                            W_IFDEBUG1(rc_t sxs_rc =) sxs.end_sys_xct (RCOK);
                            w_assert1(!sxs_rc.is_error());
                        }
                    } else 
#if W_DEBUG_LEVEL>2
                    if(page_lsn >= highest_lsn) {
                        DBGOUT1( << "WAL violation! page " 
                        << page.pid()
                        << " has lsn " << page_lsn
                        << " end of log is record prior to " << highest_lsn);

                        W_FATAL(eINTERNAL);
                    } else
#endif 
                    {
                        DBGOUT5( << setiosflags(ios::right) << lsn
                                  << resetiosflags(ios::right) << " R: " 
                                  << " page_lsn " << page_lsn
                                  << " will skip & increment rec_lsn ");
                        /*
                         *  Bump the recovery lsn for the page to indicate that 
                         *  the page is younger than this record; the earliest
                         *  record we have to apply is that after the page lsn.
                         *  NOTE: rec_lsn points INTO the dirty page table so
                         *  this changes the rec_lsn in the dptab.
                         */
                        *rec_lsn = page_lsn.advance(1); // non-const method
                    }

                    // page.destructor is supposed to do this:
                    // page.unfix();
                } else {
                    if(found) {
                        DBGOUT5( << setiosflags(ios::right) << lsn
                          << resetiosflags(ios::right) << " R: page " 
                          << page_updated << " found in dptab but lsn " << lsn
                           << " < page rec_lsn=" << 
                        (lsn_t)(rec_lsn?(*rec_lsn):(lsn_t::null))
                            << " will skip ");
                    } else {
                        DBGOUT5( << setiosflags(ios::right) << lsn
                          << resetiosflags(ios::right) << " R: page " 
                          << page_updated << " not found in dptab; will skip " );
                    }
                    DBGOUT5(<<"not found in dptab: log record/lsn= " << lsn 
                        << " page_updated=" << page_updated
                        << " page=" << r.shpid()
                        << " page rec_lsn=" << 
                        (lsn_t)(rec_lsn?(*rec_lsn):(lsn_t::null))
                        );
                }
            }
        }
        DBGOUT5( << setiosflags(ios::right) << lsn
                      << resetiosflags(ios::right) << " R: " 
                      << (redone ? " redone" : " skipped") );
    }
    {
        w_base_t::base_stat_t f = GET_TSTAT(log_fetches);
        w_base_t::base_stat_t i = GET_TSTAT(log_inserts);
        smlevel_0::errlog->clog << info_prio 
            << "Redo_pass: "
            << f << " log_fetches, " 
            << i << " log_inserts " << flushl;
    }
}

/*********************************************************************
 *
 *  restart_m::undo_pass()
 *
 *  abort all the active transactions, doing so in a strictly reverse
 *  chronological order.  This is done to get around a boundary condition
 *  in which an xct is aborted (for any reason) when the data volume is
 *  very close to full. Because undoing a btree remove can cause a page 
 *  split, we could be unable to allocate a new page for the split, and
 *  this leaves us with a completely unrecoverable volume.  Until we
 *  ran into this case, we were using a pool of threads to do parallel
 *  rollbacks.  If we find an alternative way to deal with the corner case,
 *  such as not allowing volumes to get more than some threshold full,
 *  or having utilties that allow migration from one volume to a larger
 *  volume, we will leave this in place.  *Real* storage managers might
 *  have a variety of ways to cope with this.
 *
 *  But then there will also be the problem of page allocations, which
 *  I think is another reason for undoing in reverse chronological order.
 *
 *********************************************************************/
void 
restart_m::undo_pass()
{
    FUNC(restart_m::undo_pass);

    smlevel_0::operating_mode = smlevel_0::t_in_undo;

    CmpXctUndoLsns        cmp;
    XctPtrHeap            heap(cmp);
    xct_t*                xd;
    {
        w_ostrstream s;
        s << "restart undo_pass";
        (void) log_comment(s.c_str());
    }

    {
        xct_i iter(true); // lock list
        while ((xd = iter.next()))  {
            DBGOUT3( << "Transaction " << xd->tid() 
                    << " has state " << xd->state() );

            if (xd->state() == xct_t::xct_active)  {
                heap.AddElementDontHeapify(xd);
            }
        }

        heap.Heapify();
    }  // destroy iter

    if(heap.NumElements() > 0) {
        DBGOUT3(<<"Undoing  " << heap.NumElements() << " active transactions ");
        smlevel_0::errlog->clog << info_prio  
            << "Undoing " << heap.NumElements() << " active transactions "
            << flushl;
    }
    
    // rollback the xct with the largest lsn until the 2nd largest lsn,
    // and repeat until all xct's are rolled back completely

    if (heap.NumElements() > 1)  { 
        while (heap.First()->undo_nxt() != lsn_t::null)  
        {
            xd = heap.First();

            DBGOUT3( << "Transaction " << xd->tid() 
                << " with undo_nxt lsn " << xd->undo_nxt()
                << " rolling back to " << heap.Second()->undo_nxt() 
                );

            // Note that this rollback happens while the transaction
            // is still in active state.  It behaves as if it were
            // a rollback to a save_point.
            // If there's only one transaction on the heap, it
            // rolls back via abort() (below), and then is rolled back
            // in aborting state.
            me()->attach_xct(xd);

#if 0 && W_DEBUG_LEVEL > 4
            {
                lsn_t tmp = heap.Second()->undo_nxt();
                if(tmp == lsn_t::null) {
                    fprintf(stderr, 
                            "WARNING: Rolling back to null lsn_t\n");
                    // Is this a degenerate xct that's still active?
                    // TODO WRITE A RESTART SCRIPT FOR THAT CASE
                }
            }
#endif
            // Undo until the next-highest undo_nxt for an active
            // xct. If that xct's last inserted log record is a compensation,
            // the compensated-to lsn will be the lsn we find -- just
            // noted that for the purpose of deciphering the log...
            W_COERCE( xd->rollback(heap.Second()->undo_nxt()) );
            me()->detach_xct(xd);

            w_assert9(xd->undo_nxt() < heap.Second()->undo_nxt() 
                    || xd->undo_nxt() == lsn_t::null);

            heap.ReplacedFirst();
        }
    }
    // all xct are completely rolled back, now abort them all

    while (heap.NumElements() > 0)  
    {
        xd = heap.RemoveFirst();

        w_assert9(xd->undo_nxt() == lsn_t::null || heap.NumElements() == 0);

        DBGOUT3( << "Transaction " << xd->tid() 
                << " is rolled back: aborting it now " );

        me()->attach_xct(xd);
        W_COERCE( xd->abort() );
        xct_t::destroy_xct(xd);
    }
    {
        w_base_t::base_stat_t f = GET_TSTAT(log_fetches);
        w_base_t::base_stat_t i = GET_TSTAT(log_inserts);
        smlevel_0::errlog->clog << info_prio 
            << "Undo_pass: "
            << f << " log_fetches, " 
            << i << " log_inserts " << flushl;
    }
}



/*********************************************************************
 *
 *  dirty_pages_tab_t::dirty_pages_tab_t(sz)
 *
 *  Construct a dirty page table with hash table of sz slots.
 *
 *********************************************************************/
NORET 
dirty_pages_tab_t::dirty_pages_tab_t(int sz) 
: tab(sz, W_HASH_ARG(dp_entry_t, pid, link), unsafe_nolock),
  cachedMinRecLSN(lsn_t::null),
  validCachedMinRecLSN(false)
{ 
}

/*********************************************************************
 *
 *  dirty_pages_tab_t::~dirty_pages_tab_t(sz)
 *
 *  Destroy the dirty page table.
 *
 *********************************************************************/
NORET
dirty_pages_tab_t::~dirty_pages_tab_t()
{
    /*
     *  Pop all remaining entries and delete them.
     */
    while(1) {
        w_hash_i<dp_entry_t, unsafe_list_dummy_lock_t, bfpid_t> iter(tab);
        dp_entry_t* p = iter.next();
        if( !p) break;
        tab.remove(p);
        delete p;
    }
}

void
dirty_pages_tab_t::check(ostream &o) const
{
    w_hash_i<dp_entry_t, unsafe_list_dummy_lock_t, bfpid_t> iter(tab);
    const dp_entry_t* p;
    lpid_t            pid;
    o << " Dirty page table: " <<endl;
    while ((p = iter.next()))  {
        pid = p->pid;
        dp_entry_t* p2 = tab.lookup(pid);
        w_assert3(p2);
        w_assert3(p2==p);
        o << " Page " << p->pid
        << " lsn " << p->rec_lsn
        << " ok " << int((p2 == p)?1:0)
        << endl;
    }
}

/*********************************************************************
 *
 *  friend operator<< for dirty page table
 *
 *********************************************************************/
NORET
ostream& operator<<(ostream& o, const dirty_pages_tab_t& s)
{
    o << " Dirty page table: " <<endl;

    w_hash_i<dp_entry_t, unsafe_list_dummy_lock_t, bfpid_t> iter(s.tab);
    const dp_entry_t* p;
    while ((p = iter.next()))  {
        o << " Page " << p->pid
        << " lsn " << p->rec_lsn
        << endl;
    }
    return o;
}

/*********************************************************************
 *
 *  dirty_pages_tab_t::min_rec_lsn()
 *
 *  Compute and return the minimum of the recovery lsn of all
 *  entries in the table.
 *
 *********************************************************************/
lsn_t
dirty_pages_tab_t::min_rec_lsn()
{
    lsn_t l = lsn_t::max;
    if (validCachedMinRecLSN)  {
        l = cachedMinRecLSN;
    }  else  {
        w_hash_i<dp_entry_t, unsafe_list_dummy_lock_t, bfpid_t> iter(tab);
        dp_entry_t* p;
        while ((p = iter.next())) {
            if (l > p->rec_lsn && p->rec_lsn != lsn_t::null) {
                l = p->rec_lsn;
            }
        }
        cachedMinRecLSN = l;
        validCachedMinRecLSN = true;
    }

    return l;
}


/*********************************************************************
 *
 *  dirty_pages_tab_t::insert(pid, lsn)
 *
 *  Insert an association (pid, lsn) into the table.
 *
 *********************************************************************/
dirty_pages_tab_t&
dirty_pages_tab_t::insert( 
    const lpid_t&       pid,
    const lsn_t&        lsn)
{
    if (validCachedMinRecLSN && lsn < cachedMinRecLSN)  {
        cachedMinRecLSN = lsn;
    }
    w_assert1(! tab.lookup(pid) );
    dp_entry_t* p = new dp_entry_t(pid, lsn);
    w_assert1(p);
    tab.push(p);
    w_assert3(tab.lookup(pid));
    return *this;
}



/*********************************************************************
 *
 *  dirty_pages_tab_t::look_up(pid, lsn)
 *
 *  Look up pid in the table and return a pointer to its lsn
 *  (so caller may update it in place) in "lsn".
 *
 *********************************************************************/

bool
dirty_pages_tab_t::look_up(const lpid_t& pid, lsn_t** lsn)
{
    if (lsn) *lsn = 0;

    dp_entry_t* p = tab.lookup(pid);
    if (p && lsn) *lsn = &p->rec_lsn;
    return (p != 0);
}


/*********************************************************************
 *
 *  dirty_pages_tab_t::remove(pid)
 *
 *  Remove entry of pid from the table.
 *
 *********************************************************************/
dirty_pages_tab_t& 
dirty_pages_tab_t::remove(const lpid_t& pid)
{
    validCachedMinRecLSN = false;
    dp_entry_t* p = tab.remove(pid);
    w_assert1(p);
    delete p;
    return *this;
}
