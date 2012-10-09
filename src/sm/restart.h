#ifndef RESTART_H
#define RESTART_H

#include "w_defines.h"

class dirty_pages_tab_t;

#ifdef __GNUG__
#pragma interface
#endif

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
