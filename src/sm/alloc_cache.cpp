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
    : stcache(stcache), last_alloc_page(0)
{
    vector<StoreID> stores;
    stcache.get_used_stores(stores);

    if (virgin) {
        // Extend 0 and stnode pid are always allocated
        loaded_extents.push_back(true);
        last_alloc_page = stnode_page::stpid;
    }
    else {
        // Load last extent eagerly and the rest of them on demand
        extent_id_t ext = stcache.get_last_extent();
        loaded_extents.resize(ext + 1, false);
        W_COERCE(load_alloc_page(ext, true));
    }
}

rc_t alloc_cache_t::load_alloc_page(extent_id_t ext, bool is_last_ext)
{
    // Perform I/O without holding latch
    PageID alloc_pid = ext * extent_size;
    fixable_page_h p;
    W_DO(p.fix_direct(alloc_pid, LATCH_SH, false, false));

    spinlock_write_critical_section cs(&_latch);

    // protect against race on concurrent loads
    if (loaded_extents[ext]) {
        p.unfix();
        return RCOK;
    }

    if (is_last_ext) {
        // we know that at least all pids in lower extents were once allocated
        last_alloc_page = alloc_pid;
    }

    alloc_page* page = (alloc_page*) p.get_generic_page();

    size_t last_alloc = 0;
    size_t j = alloc_page::bits_held;
    while (j > 0) {
        if (page->get_bit(j)) {
            if (last_alloc == 0) {
                last_alloc = j;
                if (is_last_ext) {
                    last_alloc_page = alloc_pid + j;
                }
            }
        }
        else if (last_alloc != 0) {
            freed_pages.insert(alloc_pid + j);
        }

        j--;
    }

    page_lsns[p.pid()] = p.lsn();
    loaded_extents[ext] = true;
    p.unfix();

    return RCOK;
}

PageID alloc_cache_t::get_last_allocated_pid() const
{
    spinlock_read_critical_section cs(&_latch);
    return last_alloc_page;
}

lsn_t alloc_cache_t::get_page_lsn(PageID pid)
{
    spinlock_read_critical_section cs(&_latch);
    map<PageID, lsn_t>::const_iterator it = page_lsns.find(pid);
    if (it == page_lsns.end()) { return lsn_t::null; }
    return it->second;
}

bool alloc_cache_t::is_allocated(PageID pid)
{
    // No latching required to check if loaded. Any races will be
    // resolved inside load_alloc_page
    extent_id_t ext = pid / extent_size;
    if (!loaded_extents[ext]) {
        W_COERCE(load_alloc_page(ext, false));
    }

    spinlock_read_critical_section cs(&_latch);

    // loaded cannot go from true to false, so this is safe
    w_assert0(loaded_extents[ext]);

    if (pid > last_alloc_page) { return false; }

    pid_set::const_iterator iter = freed_pages.find(pid);
    return (iter == freed_pages.end());
}

rc_t alloc_cache_t::sx_allocate_page(PageID& pid, bool redo)
{
    spinlock_write_critical_section cs(&_latch);

    if (redo) {
        // all space before this pid must not be contiguous free space
        if (last_alloc_page < pid) {
            last_alloc_page = pid;
        }
        // if pid is on freed list, remove
        pid_set::iterator iter = freed_pages.find(pid);
        if (iter != freed_pages.end()) {
            freed_pages.erase(iter);
        }
    }
    else {
        pid = last_alloc_page + 1;
        w_assert1(pid != stnode_page::stpid);

        if (pid % extent_size == 0) {
            extent_id_t ext = last_alloc_page / extent_size + 1;
            pid = ext * extent_size + 1;
            W_DO(stcache.sx_append_extent(ext));
        }

        last_alloc_page = pid;

        // CS TODO: page allocation should transfer ownership instead of just
        // marking the page as allocated; otherwise, zombie pages may appear
        // due to system failures after allocation but before setting the
        // pointer on the new owner/parent page. To fix this, an SSX to
        // allocate an emptry b-tree child would be the best option.

        // Entry in page_lsns array is updated by the log insertion
        extent_id_t ext = pid / extent_size;
        sysevent::log_alloc_page(pid, page_lsns[ext * extent_size]);
    }

    return RCOK;
}

rc_t alloc_cache_t::sx_deallocate_page(PageID pid, bool redo)
{
    spinlock_write_critical_section cs(&_latch);

    // Just add to list of freed pages
    freed_pages.insert(pid);

    if (!redo) {
        // Entry in page_lsns array is updated by the log insertion
        extent_id_t ext = pid / extent_size;
        sysevent::log_dealloc_page(pid, page_lsns[ext * extent_size]);
    }

    return RCOK;
}

rc_t alloc_cache_t::write_dirty_pages(lsn_t rec_lsn)
{
    generic_page* buf = NULL;
    lsn_t page_lsn = lsn_t::null;
    extent_id_t last_extent = 0;

    // We just have to iterate over the extents in the page_lsns table, since
    // those are the only ones which were modified since the system started.
    {
        spinlock_read_critical_section cs (&_latch);
        last_extent = last_alloc_page / extent_size;
    }

    map<PageID, lsn_t>::const_iterator iter;
    for (extent_id_t ext = 0; ext <= last_extent; ext++) {
        PageID alloc_pid = ext * extent_size;
        // While in the critical section, just verify if the extent alloc page
        // needs to be written, to avoid blocking threads trying to allocate
        // pages for too long.
        {
            spinlock_read_critical_section cs(&_latch);
            iter = page_lsns.find(alloc_pid);
            if (iter == page_lsns.end()) { continue; }
            if (iter->second < rec_lsn) { continue; }
            page_lsn = iter->second;
        }

        if (!buf) {
            int res = posix_memalign((void**) &buf, SM_PAGESIZE, SM_PAGESIZE);
            w_assert0(res == 0);
        }

        // Read old page image into buffer, replay updates with SPR, and write
        // it back
        W_DO(smlevel_0::vol->read_page_verify(alloc_pid, buf, page_lsn));
        W_DO(smlevel_0::vol->write_page(alloc_pid, buf));
    }

    if (buf) { delete[] buf; }

    return RCOK;
}
