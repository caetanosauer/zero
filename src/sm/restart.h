/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */
 
     /*<std-header orig-src='shore' incl-file-exclusion='RESTART_H'>
        
        $Id: restart.h,v 1.27 2010/07/01 00:08:22 nhall Exp $
        
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

#ifndef RESTART_H
#define RESTART_H

#include "w_defines.h"

class dirty_pages_tab_t;

#ifndef BF_S_H
#include <bf_s.h>
#endif

class restart_m : public smlevel_1 {
public:
    NORET                        restart_m()        {};
    NORET                        ~restart_m()        {};

    static void                 recover(lsn_t master);

private:

    static void                 analysis_pass(
        lsn_t                             master,
        dirty_pages_tab_t&                ptab, 
        lsn_t&                            redo_lsn
        );

    static void                 redo_pass(
        lsn_t                             redo_lsn, 
        const lsn_t                     &highest,  /* for debugging */
        dirty_pages_tab_t&             ptab);

    static void                 undo_pass();

private:
    // keep track of tid from log record that we're redoing
    // for a horrid space-recovery handling hack
    static tid_t                _redo_tid;

    /**
     * \brief sub-routine of redo_pass() for logs that have pid.
     */
    static void                 _redo_log_with_pid(
        logrec_t& r, lsn_t &lsn, const lsn_t &highest_lsn,
        lpid_t page_updated, dirty_pages_tab_t& dptab, bool &redone);
public:
    tid_t                        *redo_tid() { return &_redo_tid; }

};


class AutoTurnOffLogging {
         bool _original_value;
    public:
        AutoTurnOffLogging()
        {
            w_assert1(smlevel_0::logging_enabled);
            _original_value = smlevel_0::logging_enabled;
            smlevel_0::logging_enabled = false;
        };

        ~AutoTurnOffLogging()
        {
            w_assert1(!smlevel_0::logging_enabled);
            // restore original value
            smlevel_0::logging_enabled = _original_value;
        };
    private:
        AutoTurnOffLogging& operator=(const AutoTurnOffLogging&);
        AutoTurnOffLogging(const AutoTurnOffLogging&);
};


class CmpXctUndoLsns
{
    public:
        bool                        gt(const xct_t* x, const xct_t* y) const;
};


inline bool
CmpXctUndoLsns::gt(const xct_t* x, const xct_t* y) const
{
    return x->undo_nxt() > y->undo_nxt();
}

#endif
