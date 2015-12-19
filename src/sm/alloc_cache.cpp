/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"
#define SM_SOURCE

#include "sm_base.h"

#include "alloc_cache.h"
#include "smthread.h"
#include "eventlog.h"

const size_t alloc_cache_t::extent_size = alloc_page::bits_held;

alloc_cache_t::alloc_cache_t(stnode_cache_t& stcache, bool virgin)
    : stcache(stcache)
{
    vector<StoreID> stores;
    stcache.get_used_stores(stores);

    last_alloc_page.resize(stores.size() + 1, 0);
    freed_pages.resize(stores.size() + 1);

    if (virgin) {
        // Extend 0 and stnode pid are always allocated
        loaded_extents.push_back(true);
        last_alloc_page[0] = stnode_page::stpid;
    }
    else {
        extent_id_t max_ext = 0;
        for (size_t i = 0; i < stores.size(); i++) {
            stnode_t s = stcache.get_stnode(stores[i]);

            if (s.last_extent >= max_ext) {
                max_ext = s.last_extent;
                loaded_extents.resize(max_ext + 1, false);
            }
            loaded_extents[s.last_extent] = true;

            W_COERCE(load_alloc_page(s.last_extent, true));


            // CS TODO: read alloc page of last extent of each store to
            // determine last_alloc_pid and add any non-cotiguous free
            // pages found in that last extent. All other extents should
            // be retrieved on demand when calling is_allocated. In any
            // case, there should be a method to eagerly load the alloc
            // pages of all extents.
        }
    }
}

rc_t alloc_cache_t::load_alloc_page(extent_id_t ext, bool is_last_ext)
{
    PageID alloc_pid = ext * extent_size;
    fixable_page_h p;
    W_DO(p.fix_direct(alloc_pid, LATCH_SH, false, false));

    spinlock_write_critical_section cs(&_latch);

    // protect against race on concurrent loads
    if (loaded_extents[ext]) {
        p.unfix();
        return RCOK;
    }

    alloc_page* page = (alloc_page*) p.get_generic_page();
    StoreID store = page->store;

    size_t last_alloc = 0;
    size_t j = alloc_page::bits_held;
    while (j > 0) {
        if (page->get_bit(j)) {
            if (last_alloc == 0) {
                last_alloc = j;
                if (is_last_ext) {
                    last_alloc_page[store] = alloc_pid + j;
                }
            }
        }
        else if (last_alloc != 0) {
            freed_pages[store].push_back(alloc_pid + j);
        }

        j--;
    }

    page_lsns[p.pid()] = p.lsn();
    loaded_extents[ext] = true;
    p.unfix();

    return RCOK;
}

bool alloc_cache_t::is_allocated(PageID pid)
{
    // No latching required to check if loaded. Any races will be
    // resolved inside load_alloc_page
    extent_id_t ext = pid / extent_size;
    bool found = loaded_extents[ext];

    if (!found) {
        W_COERCE(load_alloc_page(ext, false));
    }

    spinlock_read_critical_section cs(&_latch);

    // loaded cannot go from true to false, so this is safe
    w_assert0(loaded_extents[ext]);

    PageID max_pid = 0;
    for (size_t i = 0; i < last_alloc_page.size(); i++) {
        if (last_alloc_page[i] > max_pid) {
            max_pid = last_alloc_page[i];
        }
    }

    if (pid > max_pid) { return false; }

    list<PageID>::iterator iter;
    for (size_t i = 0; i < freed_pages.size(); i++) {
        list<PageID>& freed = freed_pages[i];
        for (iter = freed.begin(); iter != freed.end(); iter++) {
            if (*iter == pid) {
                return false;
            }
        }
    }

    return true;
}

PageID alloc_cache_t::get_last_allocated_pid() const
{
    spinlock_read_critical_section cs(&_latch);

    PageID max = 0;
    for (size_t i = 0; i < last_alloc_page.size(); i++) {
        if (last_alloc_page[i] > max) {
            max = last_alloc_page[i];
        }
    }

    return max;
}

rc_t alloc_cache_t::sx_allocate_page(PageID& pid, StoreID store, bool redo)
{
    spinlock_write_critical_section cs(&_latch);

    if (redo) {
        // all space before this pid must not be contiguous free space
        if (last_alloc_page[store] < pid) {
            last_alloc_page[store] = pid;
        }
        // if pid is on freed list, remove
        list<PageID>& freed = freed_pages[store];
        list<PageID>::iterator iter = freed.begin();
        while (iter != freed.end()) {
            if (*iter == pid) {
                freed.erase(iter);
            }
            iter++;
        }
    }
    else {
        pid = last_alloc_page[store] + 1;
        if (pid % extent_size == 0) {
            PageID max = 0;
            for (size_t i = 0; i < last_alloc_page.size(); i++) {
                if (last_alloc_page[i] > max) {
                    max = last_alloc_page[i];
                }
            }
            extent_id_t ext = max / extent_size + 1;
            W_DO(stcache.sx_append_extent(store, ext));

            pid = ext * extent_size + 1;
        }
        last_alloc_page[store] = pid;

        // CS TODO: page allocation should transfer ownership instead of just
        // marking the page as allocated; otherwise, zombie pages may appear
        // due to system failures after allocation but before setting the
        // pointer on the new owner/parent page. To fix this, an SSX to
        // allocate an emptry b-tree child would be the best option.
        // sys_xct_section_t ssx(true);
        // W_DO(log_alloc_page(pid));
        sysevent::log_alloc_page(pid, page_lsns[pid / extent_size]);
        // ssx.end_sys_xct(RCOK);
    }

    return RCOK;
}

rc_t alloc_cache_t::sx_deallocate_page(PageID pid, StoreID store, bool redo)
{
    spinlock_write_critical_section cs(&_latch);

    // CS TODO: if we don't want to pass the store around, we could
    // read the alloc page to see to which store it belongs

    // Just add to list of freed pages
    freed_pages[store].push_back(pid);

    if (!redo) {
        // sys_xct_section_t ssx(true);
        // W_DO(log_dealloc_page(pid));
        sysevent::log_dealloc_page(pid, page_lsns[pid / extent_size]);
        // ssx.end_sys_xct(RCOK);
    }

    return RCOK;
}

rc_t alloc_cache_t::force_pages()
{
    spinlock_read_critical_section cs(&_latch);

    // CS TODO: the easiest thing here would be to use log replay to bring the
    // allocation pages up to date (i.e., decoupled cleaner)
    // CS TODO this should also wait for ongoing restore (if we use vol_t::write_page)
    // See comment on the end of stnode_page.cpp

    return RCOK;
}
