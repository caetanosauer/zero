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

alloc_cache_t::alloc_cache_t(stnode_cache_t& stcache, bool virgin, bool clustered)
    : stcache(stcache)
{
    if (virgin) {
        // Allocate first extent and stnode pid in it. First extent (which has
        // stnode page) is assigned to store 0, which baiscally means the
        // extent does not belong to any particular store.
        PageID pid;
        constexpr StoreID stid = 0;
        W_COERCE(sx_allocate_page(pid, stid));
        w_assert0(pid == stnode_page::stpid);
    }
}

rc_t alloc_cache_t::load_alloc_page(StoreID stid, extent_id_t ext)
{
    // caller must hold latch

    PageID alloc_pid = ext * extent_size;
    fixable_page_h p;
    W_DO(p.fix_direct(alloc_pid, LATCH_SH));
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
    size_t used_stores = stcache.get_store_count();
    for (size_t i = 0; i <= used_stores; i++) {
        if (!initialized[i]) {
            auto ext = stcache.get_last_extent(i);
            W_COERCE(load_alloc_page(i, ext));
        }
        auto p = last_alloc_page[i];
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
    sys_xct_section_t ssx(true);

    // get pid and update last_alloc_page in critical section
    {
        spinlock_write_critical_section cs(&_latch);

        if (last_alloc_page.size() <= stid) {
            last_alloc_page.resize(stid + 1, 0);
            initialized.resize(stid + 1, false);
        }

        if (!initialized[stid]) {
            if (!stcache.is_used(stid)) {
                // Allocate first extent of the given store on demand
                auto new_ext = stcache.get_last_extent() + 1;
                W_COERCE(stcache.sx_append_extent(stid, new_ext));
                W_COERCE(sx_format_alloc_page(new_ext * extent_size));
            }
            else {
                auto ext = stcache.get_last_extent(stid);
                W_DO(load_alloc_page(stid, ext));
            }
            initialized[stid] = true;
        }

        pid = last_alloc_page[stid] + 1;

        // If last_alloc_page[stid] was 0 or the last pid in an extent,
        // it's time to allocate a new extend for that store
        if (pid == 1 || pid % extent_size == 0) {
            // Find next extent ID to allocate based on maximum pid allocated
            // so far. If no pages were allocated yet (max_pid == 0), this
            // means we are initializing the alloc_cache, and the extent 0
            // should be allocated.
            PageID max_pid = _get_last_allocated_pid_internal();
            extent_id_t ext = max_pid == 0 ? 0 : max_pid / extent_size + 1;
            W_DO(stcache.sx_append_extent(stid, ext));

            // Format alloc page for the new extent
            PageID alloc_pid = ext * extent_size;
            W_DO(sx_format_alloc_page(alloc_pid));

            // Allocated pid will be the first on that extent
            pid = ext * extent_size + 1;
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
    constexpr bool conditional = false, virgin = false;
    W_DO(p.fix_direct(alloc_pid, LATCH_EX, conditional, virgin));
    auto page = reinterpret_cast<alloc_page*>(p.get_generic_page());
    w_assert1(!page->get_bit(pid - alloc_pid));
    page->set_bit(pid - alloc_pid);
    Logger::log_p<alloc_page_log>(&p, pid);

    W_DO(ssx.end_sys_xct(RCOK));

    return RCOK;
}

rc_t alloc_cache_t::sx_format_alloc_page(PageID alloc_pid)
{
    w_assert1(alloc_pid % extent_size == 0);

    sys_xct_section_t ssx(true);

    fixable_page_h p;
    constexpr bool conditional = false, virgin = true;
    W_DO(p.fix_direct(alloc_pid, LATCH_EX, conditional, virgin));

    auto apage = reinterpret_cast<alloc_page*>(p.get_generic_page());
    apage->format_empty();
    Logger::log_p<alloc_format_log>(&p);

    W_DO(ssx.end_sys_xct(RCOK));
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
