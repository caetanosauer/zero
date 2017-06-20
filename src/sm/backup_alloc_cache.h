#ifndef BACKUP_ALLOC_CACHE_H
#define BACKUP_ALLOC_CACHE_H

#include "w_defines.h"
#include "alloc_cache.h"
#include <vector>
#include <unordered_set>

class backup_alloc_cache_t
{
public:
    static constexpr size_t extent_size = alloc_cache_t::extent_size;

    backup_alloc_cache_t(PageID end_pid)
        : end_pid(end_pid)
    {
        auto extents = end_pid / extent_size;
        if (end_pid % extent_size != 0) { extents++; }

        alloc_pages.resize(extents);
        loaded_ext.resize(extents, false);
    }

    bool is_allocated(PageID pid)
    {
        if (pid >= end_pid) { return false; }
        auto ext = pid / extent_size;
        PageID alloc_pid = ext * extent_size;
        if (pid == alloc_pid) { return true; }

        ensure_loaded(ext);

        auto alloc_p = reinterpret_cast<alloc_page*>(&alloc_pages[ext]);
        w_assert0(alloc_pid == alloc_p->pid);
        return alloc_p->get_bit(pid - alloc_pid);
    }

    PageID get_end_pid() { return end_pid; }

private:
    // Required if vol_t uses O_DIRECT (true by default)
    using Alloc = memalign_allocator<generic_page>;

    const PageID end_pid;
    std::vector<generic_page, Alloc> alloc_pages;
    std::vector<bool> loaded_ext;

    void ensure_loaded(size_t ext)
    {
        w_assert0(ext < alloc_pages.size());
        if (!loaded_ext[ext]) {
            smlevel_0::vol->read_backup(ext * extent_size, 1, &alloc_pages[ext]);
            loaded_ext[ext] = true;
        }
    }
};

#endif
