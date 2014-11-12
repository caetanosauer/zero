#ifndef PLOG_XCT_H
#define PLOG_XCT_H

#include "sm_int_1.h"
#include "plog_ext.h"
#include "xct.h"

class plog_xct_t : public xct_t
{
public:
    static const std::string IMPL_NAME;

    virtual rc_t get_logbuf(logrec_t*& lr, int nbytes);
    virtual rc_t give_logbuf(logrec_t* lr, const fixable_page_h* p,
                    const fixable_page_h* p2);

    static
    xct_t*                        new_xct(
        sm_stats_info_t*             stats = 0,  // allocated by caller
        timeout_in_ms                timeout = WAIT_SPECIFIED_BY_THREAD,
        bool                         sys_xct = false,
        bool                         single_log_sys_xct = false,
        bool                         loser_xct = false );

    plog_xct_t(
        xct_core*                     core,
        sm_stats_info_t*             stats,  // allocated by caller
        const lsn_t&                 last_lsn,
        const lsn_t&                 undo_nxt,
        bool                         sys_xct = false,
        bool                         single_log_sys_xct = false,
        bool                         loser_xct = false
    );
    virtual ~plog_xct_t() {};
    
    // initialized by constructor in ss_m
    static plog_ext_m* ext_mgr;

protected:
    virtual rc_t _abort();
    virtual rc_t _commit(uint32_t flags, lsn_t* plastlsn=NULL);

    enum { NEW_EXT_THRESHOLD = sizeof(logrec_t) };

    plog_ext_m::extent_t* curr_ext;
    plog_ext_m::extent_t* first_ext;

    void link_new_ext();
};

#endif
