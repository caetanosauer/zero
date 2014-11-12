
#include "w_defines.h"

#define SM_SOURCE
#define PLOG_XCT_C

#include "plog_xct.h"

const std::string plog_xct_t::IMPL_NAME = "plog";
plog_ext_m* plog_xct_t::ext_mgr = NULL;

// copied from xct.cpp
#if defined(USE_BLOCK_ALLOC_FOR_XCT_IMPL) && (USE_BLOCK_ALLOC_FOR_XCT_IMPL==1)
DECLARE_TLS(block_alloc<xct_t>, xct_pool);
DECLARE_TLS(block_alloc<xct_t::xct_core>, core_pool);
#define NEW_XCT new (*xct_pool)
#define DELETE_XCT(xd) xct_pool->destroy_object(xd)
#define NEW_CORE new (*core_pool)
#define DELETE_CORE(c) core_pool->destroy_object(c)
#else
#define NEW_XCT new
#define DELETE_XCT(xd) delete xd
#define NEW_CORE new
#define DELETE_CORE(c) delete c
#endif

xct_t*
plog_xct_t::new_xct(
        sm_stats_info_t* stats,
        timeout_in_ms timeout,
        bool sys_xct,
        bool single_log_sys_xct,
        bool loser_xct)
{
    // For normal user transaction

    xct_core* core = NEW_CORE xct_core(_nxt_tid.atomic_incr(),
                       xct_active, timeout);
    xct_t* xd = NEW_XCT plog_xct_t(core, stats, lsn_t(), lsn_t(),
                              sys_xct, single_log_sys_xct, loser_xct);
    me()->attach_xct(xd);
    return xd;
}

plog_xct_t::plog_xct_t(
    xct_core*                     core,
    sm_stats_info_t*             stats,  // allocated by caller
    const lsn_t&                 last_lsn,
    const lsn_t&                 undo_nxt,
    bool                         sys_xct,
    bool                         single_log_sys_xct,
    bool                         loser_xct
)
    : xct_t(core, stats, last_lsn, undo_nxt, sys_xct, single_log_sys_xct, loser_xct)
{
}

rc_t plog_xct_t::get_logbuf(logrec_t*& lr, int nbytes)
{
    if (curr_ext->size >= NEW_EXT_THRESHOLD) {
        link_new_ext();
    }
    w_assert3(curr_ext->size >= NEW_EXT_THRESHOLD);
    w_assert1(!curr_ext->committed);

    lr = (logrec_t*) (curr_ext->data + curr_ext->size);

    // In the current milestone (M1), log records are replicated into
    // both the private log and the traditional ARIES log. To achieve that,
    // we simply use the logrec pointer in the current extent as the xct
    // logbuf in the traditional implementation.
    _log_buf = lr;

    // The replication also means we need to invoke log reservations,
    // which is done in the traditional get_logbuf
    xct_t::get_logbuf(lr, nbytes);

    return RCOK;
}

rc_t plog_xct_t::give_logbuf(logrec_t* lr, const fixable_page_h* p,
                    const fixable_page_h* p2)
{
    // If this ever happens, then memory exception should have been thrown
    // already anyway. This is the drawback of not knowing the logrec size
    // before insertion. The only solution is to use an extent size that
    // can accommodate the largest possible logrec.
    w_assert1(curr_ext->space_available() < lr->length());

    // replicate logic on traditional log, i.e., call log->insert and set LSN
    xct_t::give_logbuf(lr, p, p2);

    // set LSN as offset within extent
    lr->fill_xct_attr(tid(), lsn_t(0, curr_ext->size));

    curr_ext->size += lr->length();
    return RCOK;
}

rc_t plog_xct_t::_abort()
{
    return RCOK;
}

rc_t plog_xct_t::_commit(uint32_t flags, lsn_t* plastlsn)
{
    return RCOK;
}

void plog_xct_t::link_new_ext()
{
    // TODO lock?
    plog_ext_m::extent_t* n = ext_mgr->alloc_extent();
    w_assert1(curr_ext->next == NULL);
    curr_ext->next = n;
    curr_ext = n;
}
