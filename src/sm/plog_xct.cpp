#include "w_defines.h"

#define SM_SOURCE
#define PLOG_XCT_C

#include "plog_xct.h"
#include "allocator.h"

const std::string plog_xct_t::IMPL_NAME = "plog";

DEFINE_SM_ALLOC(plog_xct_t);

plog_xct_t::plog_xct_t(
    sm_stats_info_t*             stats,  // allocated by caller
    timeout_in_ms                timeout,
    bool                         sys_xct,
    bool                         single_log_sys_xct,
    const lsn_t&                 last_lsn,
    const lsn_t&                 undo_nxt,
    bool                         loser_xct
)
    : xct_t(stats, timeout, sys_xct, single_log_sys_xct, tid_t::null,
            last_lsn, undo_nxt, loser_xct)
{
    // original _log_buf should not be used
    delete _log_buf;
    _log_buf = NULL;
}

plog_xct_t::~plog_xct_t()
{
}

rc_t plog_xct_t::get_logbuf(logrec_t*& lr, int nbytes)
{
    char* data = plog.get();

    // In the current milestone (M1), log records are replicated into
    // both the private log and the traditional ARIES log. To achieve that,
    // we simply use the logrec pointer in the current extent as the xct
    // logbuf in the traditional implementation.
    _log_buf = (logrec_t*) data;

    // The replication also means we need to invoke log reservations,
    // which is done in the traditional get_logbuf
    xct_t::get_logbuf(lr, nbytes);

    return RCOK;
}

rc_t plog_xct_t::give_logbuf(logrec_t* lr, const fixable_page_h* p,
                    const fixable_page_h* p2)
{
    // replicate logic on traditional log, i.e., call log->insert and set LSN
    xct_t::give_logbuf(lr, p, p2);
    plog.give(lr);

    // avoid xct_t destructor trying to deallocate plog's memory
    _log_buf = NULL;

    return RCOK;
}

rc_t plog_xct_t::_abort()
{
    xct_t::_abort();
    plog.set_state(plog_t::ABORTED);
    return RCOK;
}

rc_t plog_xct_t::_commit(uint32_t flags, lsn_t* plastlsn)
{
    xct_t::_commit(flags, plastlsn);
    plog.set_state(plog_t::COMMITTED);
    return RCOK;
}
