/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#include "sm_base.h"
#include "vol.h"
#include "alloc_cache.h"
#include "smthread.h"
#include "xct_logger.h"

const size_t alloc_cache_t::extent_size = alloc_page::bits_held;

alloc_cache_t::alloc_cache_t(stnode_cache_t& stcache, bool virgin)
    : stcache(stcache)
{
    vector<StoreID> stores;
    stcache.get_used_stores(stores);

    if (virgin) {
        // first extent (which has stnode page) is assigned to store 0,
        // which baiscally means the extent does not belong to any particular
        // store
        last_alloc_page.push_back(stnode_page::stpid);
    }
    else {
        // Load last allocated PID of each store using the last extent
        // recorded in the stnode page
        last_alloc_page.resize(stores.size() + 1, 0);
        for (auto s : stores) {
            extent_id_t ext = stcache.get_last_extent(s);
            W_COERCE(load_alloc_page(s, ext));
        }
    }
}

rc_t alloc_cache_t::load_alloc_page(StoreID stid, extent_id_t ext)
{
    spinlock_write_critical_section cs(&_latch);

    PageID alloc_pid = ext * extent_size;
    fixable_page_h p;
    W_DO(p.fix_direct(alloc_pid, LATCH_SH, false, false));
    alloc_page* page = (alloc_page*) p.get_generic_page();

    auto last_set = page->get_last_set_bit();

    last_alloc_page[stid] = alloc_pid + last_set;

    return RCOK;
}

PageID alloc_cache_t::get_last_allocated_pid(StoreID s) const
{
    spinlock_read_critical_section cs(&_latch);
    return last_alloc_page[s];
}

PageID alloc_cache_t::get_last_allocated_pid() const
{
    spinlock_read_critical_section cs(&_latch);
    return _get_last_allocated_pid_internal();
}

PageID alloc_cache_t::_get_last_allocated_pid_internal() const
{
    PageID max = 0;
    for (auto p : last_alloc_page) {
        if (p > max) { max = p; }
    }
    return max;
}

bool alloc_cache_t::is_allocated(PageID pid)
{
    extent_id_t ext = pid / extent_size;

    PageID alloc_pid = ext * extent_size;
    fixable_page_h p;
    W_COERCE(p.fix_direct(alloc_pid, LATCH_SH, false, false));
    alloc_page* page = (alloc_page*) p.get_generic_page();

    return page->get_bit(pid - alloc_pid);
}

rc_t alloc_cache_t::sx_allocate_page(PageID& pid, StoreID stid)
{
    // get pid and update last_alloc_page in critical section
    {
        spinlock_write_critical_section cs(&_latch);

        if (last_alloc_page.size() <= stid) {
            last_alloc_page.resize(stid + 1, 0);
        }

        pid = last_alloc_page[stid] + 1;
        w_assert1(stid != 0 || pid != stnode_page::stpid);

        if (pid == 1 || pid % extent_size == 0) {
            extent_id_t ext = _get_last_allocated_pid_internal() / extent_size + 1;
            pid = ext * extent_size + 1;
            W_DO(stcache.sx_append_extent(stid, ext));
        }

        last_alloc_page[stid] = pid;

        // CS TODO: page allocation should transfer ownership instead of just
        // marking the page as allocated; otherwise, zombie pages may appear
        // due to system failures after allocation but before setting the
        // pointer on the new owner/parent page. To fix this, an SSX to
        // allocate an emptry b-tree child would be the best option.
    }
    w_assert1(pid % extent_size > 0);


    // Now set the corresponding bit in the alloc page
    fixable_page_h p;
    PageID alloc_pid = pid - (pid % extent_size);
    W_DO(p.fix_direct(alloc_pid, LATCH_EX, false, false));
    alloc_page* page = (alloc_page*) p.get_generic_page();
    w_assert1(!page->get_bit(pid - alloc_pid));
    page->set_bit(pid - alloc_pid);
    Logger::log_p<alloc_page_log>(&p, pid);

    return RCOK;
}

rc_t alloc_cache_t::sx_deallocate_page(PageID pid)
{
    w_assert1(pid % extent_size > 0);

    // Just unset the corresponding bit in the alloc page
    fixable_page_h p;
    PageID alloc_pid = pid - (pid % extent_size);
    W_DO(p.fix_direct(alloc_pid, LATCH_EX, false, false));
    alloc_page* page = (alloc_page*) p.get_generic_page();
    w_assert1(page->get_bit(pid - alloc_pid));
    page->unset_bit(pid - alloc_pid);
    Logger::log_p<dealloc_page_log>(&p, pid);

    return RCOK;
}
