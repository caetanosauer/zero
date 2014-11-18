#ifndef PLOG_XCT_H
#define PLOG_XCT_H

#include "sm_int_1.h"
#include "logrec.h"
#include "xct.h"
#include "plog.h"

class plog_xct_t : public xct_t
{
public:
    static const std::string IMPL_NAME;

    virtual rc_t get_logbuf(logrec_t*& lr, int nbytes);
    virtual rc_t give_logbuf(logrec_t* lr, const fixable_page_h* p,
                    const fixable_page_h* p2);

    plog_xct_t(
        sm_stats_info_t*             stats = NULL,
        timeout_in_ms                timeout = WAIT_SPECIFIED_BY_THREAD,
        bool                         sys_xct = false,
        bool                         single_log_sys_xct = false,
        const lsn_t&                 last_lsn = lsn_t::null,
        const lsn_t&                 undo_nxt = lsn_t::null,
        bool                         loser_xct = false
    );
    
    virtual ~plog_xct_t() {
        //w_assert1(!curr_ext);
        //w_assert1(!first_ext);
    };

    void* operator new(size_t s);
    void operator delete(void* p, size_t s);

protected:
    plog_t plog;

    virtual rc_t _abort();
    virtual rc_t _commit(uint32_t flags, lsn_t* plastlsn=NULL);

    enum { NEW_EXT_THRESHOLD = sizeof(logrec_t) };
};

#endif
