/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#define SM_SOURCE
#include "sm_base.h"

#include "stnode_page.h"

#define IO_ALIGN 512

stnode_cache_t::stnode_cache_t(stnode_page& stpage)
{
    int r = posix_memalign((void**) &_stnode_page, IO_ALIGN, sizeof(stnode_page));
    memcpy(_stnode_page, &stpage, sizeof(stnode_page));
    w_assert0(r == 0);
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
    return _stnode_page->get(store).root;
}

stnode_t stnode_cache_t::get_stnode(StoreID store) const
{
    return _stnode_page->get(store);
}

bool stnode_cache_t::is_allocated(StoreID store) const
{
    CRITICAL_SECTION (cs, _latch);
    return get_stnode(store).is_used();
}

StoreID stnode_cache_t::get_min_unused_store_ID() const
{
    // Caller should hold the latch

    // Let's start from 1, not 0.  All user store ID's will begin with 1.
    // Store-ID 0 will be a special store-ID for stnode_page/alloc_page's
    for (size_t i = 1; i < stnode_page::max; ++i) {
        if (!_stnode_page->get(i).is_used()) {
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
        if (_stnode_page->get(i).is_used()) {
            ret.push_back((StoreID) i);
        }
    }
}

rc_t stnode_cache_t::sx_create_store(PageID root_pid, StoreID& snum, bool redo)
{
    CRITICAL_SECTION (cs, _latch);

    snum = get_min_unused_store_ID();
    if (snum == stnode_page::max) {
        return RC(eSTCACHEFULL);
    }

    _stnode_page->set_root(snum, root_pid);
    _stnode_page->update_last_extent(snum, 0);

    sys_xct_section_t ssx(true);
    if (!redo) {
        log_create_store(root_pid, snum);
    }
    // CS TODO: use decoupled propagation
    // W_DO(write_stnode_page());
    W_DO(ssx.end_sys_xct(RCOK));

    return RCOK;
}

rc_t stnode_cache_t::sx_append_extent(StoreID snum, extent_id_t ext, bool redo)
{
    CRITICAL_SECTION (cs, _latch);

    if (snum >= stnode_page::max) {
        return RC(eSTCACHEFULL);
    }

    sys_xct_section_t ssx(true);
    _stnode_page->update_last_extent(snum, ext);
    if (!redo) {
        W_DO(log_append_extent(snum, ext));
    }
    W_DO(write_stnode_page());
    W_DO(ssx.end_sys_xct(RCOK));

    return RCOK;
}

/*
 * CS TODO
 * Problem: stnode page is the root catalog page of the DB, so it must be
 * handled properly so that the system is initialized correctly after restart.
 * We have the following options:
 * 1) On every SX that modifies the stnode page, force it before commit.
 * 2) Fix stnode page on buffer pool, so that checkpoints recognize its dirty
 *    status and REDO repairs it at restart.
 * 3) During startup, always invoke single-page recovery on stnode page,
 *    without managing it in the buffer pool.
 *
 * Option 1 adds unecessary overhead to normal processing.
 * Option 2 is the best, most general one, but it requires the buffer pool to
 * break the assumption that it only manages btree pages. For instance, there
 * is currently no fix primitive that can be used.
 * Option 3 is probably the easiest to implement, but it entails special
 * handling of metadata pages, which is not as elegant as option 1 and not
 * really what we wanted.
 *
 * Going with option 1 for now. Once I update the buffer pool code with proper
 * fix primitives and correct (perhaps mandatory) swizzling, come back and
 * implement option 2.
 * BUT WAIT: If we have decoupled checkpoints, we don't need to worry about
 * having the page in the buffer pool to detect it as dirty!
 * The same problem and solution applies to alloc pages
 */

rc_t stnode_cache_t::write_stnode_page()
{
    // Caller must hold latch

    return smlevel_0::vol->write_page(stnode_page::stpid,
            (generic_page*) _stnode_page);
}
