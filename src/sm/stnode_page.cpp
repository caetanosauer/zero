/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#define SM_SOURCE
#include "sm_base.h"

#include "stnode_page.h"
#include "xct_logger.h"

constexpr PageID stnode_page::stpid = 1;

stnode_cache_t::stnode_cache_t(bool create)
    : prev_page_lsn(lsn_t::null)
{
    int res= posix_memalign((void**) &_stnode_page, sizeof(generic_page),
            sizeof(generic_page));
    w_assert0(res == 0);

    if (create) {
        memset(&_stnode_page, 0, sizeof(generic_page));
        _stnode_page.pid = stnode_page::stpid;
        _stnode_page.lsn = lsn_t::null;
        Logger::log_page_chain<stnode_format_log>(prev_page_lsn);
    }
    else {
        fixable_page_h p;
        W_COERCE(p.fix_direct(stnode_page::stpid, LATCH_EX,
                    false, create));
        w_assert1(p.pid() == stnode_page::stpid);
        memcpy(&_stnode_page, p.get_generic_page(), sizeof(stnode_page));
        prev_page_lsn = p.lsn();
        p.unfix(true /* evict */);
    }
}

PageID stnode_cache_t::get_root_pid(StoreID store) const
{
    w_assert1(store < stnode_page::max);

    // CRITICAL_SECTION (cs, _spin_lock);
    // Commented out to improve scalability, as this is called for
    // EVERY operation.  NOTE this protection is not needed because
    // this is unsafe only when there is a concurrent DROP INDEX (or
    // DROP TABLE).  It should be protected by intent locks (if it's
    // no-lock mode... it's user's responsibility).
    //
    // JIRA: ZERO-168 notes that DROP INDEX/TABLE currently are not
    // implemented and to fix this routine once they are.
    return _stnode_page.get(store).root;
}

stnode_t stnode_cache_t::get_stnode(StoreID store) const
{
    return _stnode_page.get(store);
}

bool stnode_cache_t::is_allocated(StoreID store) const
{
    CRITICAL_SECTION (cs, _latch);
    return get_stnode(store).is_used();
}

StoreID stnode_cache_t::get_min_unused_stid() const
{
    // Caller should hold the latch or guarantee mutual exclusion externally

    // Let's start from 1, not 0.  All user store ID's will begin with 1.
    // Store-ID 0 will be a special store-ID for stnode_page/alloc_page's
    for (size_t i = 1; i < stnode_page::max; ++i) {
        if (!_stnode_page.get(i).is_used()) {
            return i;
        }
    }
    return stnode_page::max;
}

void stnode_cache_t::get_used_stores(std::vector<StoreID>& ret) const
{
    ret.clear();

    CRITICAL_SECTION (cs, _latch);
    for (size_t i = 1; i < stnode_page::max; ++i) {
        if (_stnode_page.get(i).is_used()) {
            ret.push_back((StoreID) i);
        }
    }
}

rc_t stnode_cache_t::sx_create_store(PageID root_pid, StoreID& snum, bool redo)
{
    CRITICAL_SECTION (cs, _latch);

    snum = get_min_unused_stid();
    if (snum == stnode_page::max) {
        return RC(eSTCACHEFULL);
    }

    _stnode_page.set_root(snum, root_pid);

    if (!redo) {
        Logger::log_page_chain<create_store_log>(prev_page_lsn, root_pid, snum);
    }

    return RCOK;
}

rc_t stnode_cache_t::sx_append_extent(extent_id_t ext, bool redo)
{
    CRITICAL_SECTION (cs, _latch);

    _stnode_page.set_last_extent(ext);

    if (!redo) {
        Logger::log_page_chain<append_extent_log>(prev_page_lsn, ext);
    }

    return RCOK;
}

void stnode_cache_t::dump(ostream& out)
{
    CRITICAL_SECTION (cs, _latch);
    out << "STNODE CACHE:" << endl;
    for (size_t i = 1; i < stnode_page::max; ++i) {
        stnode_t s = _stnode_page.get(i);
        if (s.is_used()) {
            out << "stid: " << i
                << " root: " << s.root
                << endl;
        }
    }
    cout << "last_extent: " << _stnode_page.get_last_extent() << endl;
}

lsn_t stnode_cache_t::get_page_lsn() {
    CRITICAL_SECTION (cs, _latch);
    return prev_page_lsn;
}

rc_t stnode_cache_t::write_page(lsn_t rec_lsn)
{
    generic_page* buf;
    int res = posix_memalign((void**) &buf, SM_PAGESIZE, SM_PAGESIZE);
    w_assert0(res == 0);

    lsn_t emlsn = get_page_lsn();
    W_DO(smlevel_0::vol->read_page_verify(stnode_page::stpid, buf, emlsn));
    W_DO(smlevel_0::vol->write_page(stnode_page::stpid, buf));
    Logger::log_sys<page_write_log>(stnode_page::stpid, rec_lsn, 1);

    delete[] buf;
    return RCOK;
}
