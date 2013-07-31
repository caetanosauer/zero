/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#define SM_SOURCE

#ifdef __GNUG__
#   pragma implementation "stnode_p.h"
#endif

#include "sm_int_2.h"

#include "page_s.h"
#include "stnode_p.h"
#include "crash.h"
#include "sm_s.h"
#include "bf_fixed.h"

rc_t
stnode_p::format(const lpid_t& pid) {
    // no records/slots. just array of stnode_t.
    ::memset(_pp, 0, sizeof(page_s));
    _pp->pid = pid;
    _pp->tag = t_stnode_p;
    return RCOK;
}    

stnode_t& stnode_p::get(size_t idx)
{
    w_assert1(idx < max);
    return reinterpret_cast<stnode_t*>(_pp->data)[idx];
}

stnode_cache_t::stnode_cache_t (vid_t vid, bf_fixed_m* fixed_pages): _vid(vid), _fixed_pages(fixed_pages) {
    page_s* page = _fixed_pages->get_pages() + _fixed_pages->get_page_cnt() - 1;
    w_assert1(page->pid.vol() == _vid);
    _stnodes = (stnode_t*) page->data;
}

shpid_t stnode_cache_t::get_root_pid (snum_t store) const
{
    if (store >= stnode_p::max) {
        w_assert1(false);
        return 0;
    }

    // CRITICAL_SECTION (cs, _spin_lock);
    // commented out to improve scalability, as this is called for EVERY operation.
    // NOTE this protection is not needed because this is unsafe
    // only when there is a concurrent DROP INDEX (or DROP TABLE).
    // It should be protected by intent locks
    // (if it's no-lock mode... it's user's responsibility)
    return _stnodes[store].root;
}
void stnode_cache_t::get_stnode (snum_t store, stnode_t &stnode) const
{
    if (store >= stnode_p::max) {
        w_assert1(false);
        stnode = stnode_t();
        return;
    }
    CRITICAL_SECTION (cs, _spin_lock);
    stnode = _stnodes[store];
}

snum_t stnode_cache_t::get_min_unused_store_id () const
{
    // this method is not so efficient, but this is rarely called.
    CRITICAL_SECTION (cs, _spin_lock);
    // let's start from 1, not 0. All user store ID will begin with 1.
    // store-id 0 will be a special store-id for stnode_p/alloc_p
    for (int i = 1; i < stnode_p::max; ++i) {
        if (_stnodes[i].root == 0) {
            return i;
        }
    }
    return stnode_p::max;
}

std::vector<snum_t> stnode_cache_t::get_all_used_store_id() const
{
    std::vector<snum_t> ret;
    CRITICAL_SECTION (cs, _spin_lock);
    for (int i = 1; i < stnode_p::max; ++i) {
        if (_stnodes[i].root != 0) {
            ret.push_back((snum_t) i);
        }
    }
    return ret;
}


rc_t
stnode_cache_t::store_operation(const store_operation_param& param)
{
    w_assert1(param.snum() < stnode_p::max);

    store_operation_param new_param(param);
    stnode_t stnode;
    get_stnode (param.snum(), stnode); // copy out current value.

    switch (param.op())  {
        case smlevel_0::t_delete_store:
            {
                stnode.root        = 0;
                stnode.flags       = smlevel_0::st_bad;
                stnode.deleting    = smlevel_0::t_not_deleting_store;
            }
            break;
        case smlevel_0::t_create_store:
            {
                w_assert1(stnode.root == 0);

                stnode.root        = 0;
                stnode.flags       = param.new_store_flags();
                stnode.deleting    = smlevel_0::t_not_deleting_store;
            }
            DBGOUT3 ( << "t_create_store:" << param.snum());
            break;
        case smlevel_0::t_set_deleting:
            {
                // bogus assertion:
                // If we crash/restart between the time the
                // xct gets into xct_freeing_space and
                // the time xct_end is logged, the
                // this store operation might already have been done, 
                // w_assert3(stnode.deleting != param.new_deleting_value());
                w_assert3(param.old_deleting_value() == smlevel_0::t_unknown_deleting
                        || stnode.deleting == param.old_deleting_value());

                new_param.set_old_deleting_value(
                    (store_operation_param::store_deleting_t)stnode.deleting);

                stnode.deleting    = param.new_deleting_value();
            }
            break;
        case smlevel_0::t_set_store_flags:
            {
                if (stnode.flags == param.new_store_flags())  {
                    // xct may have converted file type to regular and 
                    // then the automatic
                    // conversion at commit from insert_file 
                    // to regular needs to be ignored
                    DBG(<<"store flags already set");
                    return RCOK;
                } else  {
                    w_assert3(param.old_store_flags() == smlevel_0::st_bad
                            || stnode.flags == param.old_store_flags());

                    new_param.set_old_store_flags(
                            (store_operation_param::store_flag_t)stnode.flags);

                    stnode.flags        = param.new_store_flags();
                }
                w_assert3(stnode.flags != smlevel_0::st_bad);
            }
            break;
        case smlevel_0::t_set_root:
            {
                w_assert3(stnode.root == 0);
                w_assert3(param.root());

                stnode.root        = param.root();
            }
            DBGOUT3 ( << "t_set_root:" << param.snum() << ". root=" << param.root());
            break;
        default:
            w_assert0(false);
    }

    // log it and apply the change to the stnode_p
    CRITICAL_SECTION (cs, _spin_lock);
    spinlock_read_critical_section cs2(&_fixed_pages->get_checkpoint_lock()); // protect against checkpoint. see bf_fixed_m comment.
    W_DO( log_store_operation(new_param) );
    _stnodes[param.snum()] = stnode;
    _fixed_pages->get_dirty_flags()[_fixed_pages->get_page_cnt() - 1] = true;
    return RCOK;
}
