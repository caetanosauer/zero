/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "sm_base.h"
#include "vol.h"
#include "stnode_page.h"
#include "alloc_cache.h"
#include "xct_logger.h"

constexpr PageID stnode_page::stpid = 1;

stnode_cache_t::stnode_cache_t(bool create)
{
    fixable_page_h p;
    W_COERCE(p.fix_direct(stnode_page::stpid, LATCH_EX, false, create));

    if (create) {
        sys_xct_section_t ssx(true);
        auto spage = reinterpret_cast<stnode_page*>(p.get_generic_page());
        spage->format_empty();
        Logger::log_p<stnode_format_log>(&p);
        W_COERCE(ssx.end_sys_xct(RCOK));
    }
}

PageID stnode_cache_t::get_root_pid(StoreID store) const
{
    w_assert1(store < stnode_page::max);
    fixable_page_h p;
    W_COERCE(p.fix_direct(stnode_page::stpid, LATCH_SH));
    auto spage = reinterpret_cast<stnode_page*>(p.get_generic_page());
    return spage->get(store).root;
}

stnode_t stnode_cache_t::get_stnode(StoreID store) const
{
    fixable_page_h p;
    W_COERCE(p.fix_direct(stnode_page::stpid, LATCH_SH));
    auto spage = reinterpret_cast<stnode_page*>(p.get_generic_page());
    return spage->get(store);
}

bool stnode_cache_t::is_allocated(StoreID store) const
{
    return get_stnode(store).is_used();
}

StoreID stnode_cache_t::get_min_unused_stid(stnode_page* spage) const
{
    // Caller should hold the latch or guarantee mutual exclusion externally

    // Let's start from 1, not 0.  All user store ID's will begin with 1.
    // Store-ID 0 will be a special store-ID for stnode_page/alloc_page's
    for (size_t i = 1; i < stnode_page::max; ++i) {
        if (!spage->get(i).is_used()) {
            return i;
        }
    }
    return stnode_page::max;
}

void stnode_cache_t::get_used_stores(std::vector<StoreID>& ret) const
{
    ret.clear();

    fixable_page_h p;
    W_COERCE(p.fix_direct(stnode_page::stpid, LATCH_SH));
    auto spage = reinterpret_cast<stnode_page*>(p.get_generic_page());

    for (size_t i = 1; i < stnode_page::max; ++i) {
        if (spage->get(i).is_used()) {
            ret.push_back((StoreID) i);
        }
    }
}

rc_t stnode_cache_t::sx_create_store(PageID root_pid, StoreID& snum) const
{
    w_assert1(root_pid > 0);

    fixable_page_h p;
    W_COERCE(p.fix_direct(stnode_page::stpid, LATCH_EX));
    auto spage = reinterpret_cast<stnode_page*>(p.get_generic_page());

    snum = get_min_unused_stid(spage);
    if (snum == stnode_page::max) {
        return RC(eSTCACHEFULL);
    }

    spage->set_root(snum, root_pid);
    spage->set_last_extent(snum, 0);

    Logger::log_p<create_store_log>(&p, snum, root_pid);
    return RCOK;
}

rc_t stnode_cache_t::sx_append_extent(StoreID snum, extent_id_t ext) const
{
    sys_xct_section_t ssx(true);

    fixable_page_h p;
    W_COERCE(p.fix_direct(stnode_page::stpid, LATCH_EX));
    auto spage = reinterpret_cast<stnode_page*>(p.get_generic_page());
    spage->set_last_extent(snum, ext);
    Logger::log_p<append_extent_log>(&p, snum, ext);

    return ssx.end_sys_xct(RCOK);
}

void stnode_cache_t::dump(std::ostream& out) const
{
    fixable_page_h p;
    W_COERCE(p.fix_direct(stnode_page::stpid, LATCH_SH));
    auto spage = reinterpret_cast<stnode_page*>(p.get_generic_page());

    out << "STNODE CACHE:" << endl;
    for (size_t i = 0; i < stnode_page::max; ++i) {
        stnode_t s = spage->get(i);
        if (i == 0 || s.is_used()) {
            out << "stid: " << i
                << " root: " << s.root
                << " last_extent: " << s.last_extent
                << endl;
        }
    }
}

extent_id_t stnode_cache_t::get_last_extent(StoreID snum) const
{
    fixable_page_h p;
    W_COERCE(p.fix_direct(stnode_page::stpid, LATCH_SH));
    auto spage = reinterpret_cast<stnode_page*>(p.get_generic_page());

    return spage->get_last_extent(snum);
}
