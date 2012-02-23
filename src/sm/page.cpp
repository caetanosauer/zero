#include "w_defines.h"

#define SM_SOURCE
#define PAGE_C
#ifdef __GNUG__
#   pragma implementation "page.h"
#   pragma implementation "page_s.h"
#endif
#include "sm_int_1.h"
#include "page.h"
#include "btree_p.h"
#include "w_key.h"

uint32_t
page_p::get_store_flags() const
{
    return _pp->get_page_storeflags();
}

void
page_p::set_store_flags(uint32_t f)
{
    // If fixed in ex mode, set through the buffer control block
    // Do not set if not fixed EX.
    bfcb_t *b = bf_m::get_cb(_pp);
    if(b && is_mine()) {
        b->set_storeflags(f);
    } else {
        // If this isn't a buffer-pool page, we don't care about
        // keeping any bfcb_t up-to-date. 
        // This is used when we are constructing a page_p with
        // a buffer on the stack. This happens in formatting
        // a volume, for example. 
        _pp->set_page_storeflags(f);
    }
}

void page_p::repair_rec_lsn(bool was_dirty, lsn_t const &new_rlsn) {
    if( !smlevel_0::logging_enabled) return;
    bfcb_t* bp = bf_m::get_cb(_pp);
    const lsn_t &rec_lsn = bp->curr_rec_lsn();
    w_assert2(is_latched_by_me());
    w_assert2(is_mine());
    if(was_dirty) {
        // never mind!
        w_assert0(rec_lsn <= lsn());
    }
    else {
        w_assert0(rec_lsn > lsn() );
        if(new_rlsn.valid()) {
            w_assert0(new_rlsn <= lsn());
            w_assert2(bp->dirty());
            bp->set_rec_lsn(new_rlsn);
            INC_TSTAT(restart_repair_rec_lsn);
        }
        else {
            bp->mark_clean();
        }
    }
}

const char*  page_p::tag_name(tag_t t)
{
    switch (t) {
    case t_alloc_p: 
        return "t_alloc_p";
    case t_stnode_p:
        return "t_stnode_p";
    case t_btree_p:
        return "t_btree_p";
    default:
        W_FATAL(eINTERNAL);
    }

    W_FATAL(eINTERNAL);
    return 0;
}



/*********************************************************************
 *
 *  page_p::_format(pid, tag, page_flags, store_flags)
 *
 *  Called from page-type-specific 4-argument method:
 *    xxx::format(pid, tag, page_flags, store_flags)
 *
 *  Format the page with "pid", "tag", and "page_flags" 
 *        and store_flags (goes into the log record, not into the page)
 *        If log_it is true, it issues a page_init log record
 *
 *********************************************************************/
rc_t
page_p::_format(lpid_t pid, tag_t tag, 
               uint32_t             page_flags, 
               store_flag_t /*store_flags*/
               ) 
{
    uint32_t             sf;

    w_assert3((page_flags & ~t_virgin) == 0); // expect only virgin 
    /*
     *  Check alignments
     */
    w_assert3(is_aligned(data_sz));
    w_assert3(is_aligned(_pp->data - (char*) _pp));
    w_assert3(sizeof(page_s) == page_sz);
    w_assert3(is_aligned(_pp->data));

    /*
     *  Do the formatting...
     *  ORIGINALLY:
     *  Note: store_flags must be valid before page is formatted
     *  unless we're in redo and DONT_TRUST_PAGE_LSN is turned on.
     *  NOW:
     *  store_flags are passed in. The fix() that preceded this
     *  will have stuffed some store_flags into the page(as before)
     *  but they could be wrong. Now that we are logging the store
     *  flags with the page_format log record, we can force the
     *  page to have the correct flags due to redo of the format.
     *  What this does NOT do is fix the store flags in the st_node.
     * See notes in bf_m::_fix
     *
     *  The following code writes all 1's into the page (except
     *  for store-flags) in the hope of helping debug problems
     *  involving updates to pages and/or re-use of deallocated
     *  pages.
     */
    sf = _pp->get_page_storeflags(); // save flags
#ifdef ZERO_INIT
    /* NB -- NOTE -------- NOTE BENE
    *  Note this is not exactly zero-init, but it doesn't matter
    * WHAT we use to init each byte for the purpose of purify or valgrind
    */
    // because we do this, note that we shouldn't receive any arguments
    // as reference or pointer. It might be also nuked!
    memset(_pp, '\017', sizeof(*_pp)); // trash the whole page
#endif //ZERO_INIT

    if (tag != t_alloc_p && tag != t_stnode_p) {
        // _pp->set_page_storeflags(sf); // restore flag
        this->set_store_flags(sf); // changed to do it through the page_p, bfcb_t
        // TODO: any assertions on store_flags?

#if W_DEBUG_LEVEL > 2
        if(
        (smlevel_0::operating_mode == smlevel_0::t_in_undo)
        ||
        (smlevel_0::operating_mode == smlevel_0::t_forward_processing)
        )  // do the assert below
        w_assert3(sf != st_bad);
#endif 
    }

    _pp->lsn= lsn_t(0, 1);
    _pp->pid = pid;
    _pp->page_flags = page_flags;
     w_assert3(tag != t_bad_p);
    _pp->tag = tag;  // must be set before rsvd_mode() is called
    _pp->record_head8 = to_offset8(data_sz);
    _pp->nslots = _pp->nghosts = _pp->btree_consecutive_skewed_insertions = 0;

    return RCOK;
}


/*********************************************************************
 *
 *  page_p::_fix(bool,
 *    pid, ptag, mode, page_flags, store_flags, ignore_store_id, refbit)
 *
 *
 *  Fix a frame for "pid" in buffer pool in latch "mode". 
 *
 *  "Ignore_store_id" indicates whether the store ID
 *  on the page can be trusted to match pid.store; usually it can, 
 *  but if not, then passing in true avoids an extra assert check.
 *  "Refbit" is a page replacement hint to bf when the page is 
 *  unfixed.
 *
 *  NB: this does not set the tag() to ptag -- format does that
 *
 *********************************************************************/
rc_t
page_p::_fix_core(
    bool                 condl,
    const lpid_t&        pid,
    tag_t                ptag,
    latch_mode_t         m, 
    uint32_t              page_flags,
    store_flag_t&        store_flags,//used only if page_flags & t_virgin
    bool                 ignore_store_id, 
    int                  refbit)
{
    w_assert1(pid.vol() != vid_t::null);
    w_assert3(!_pp || bf->is_bf_page(_pp, false));
    store_flag_t        ret_store_flags = store_flags;

    // store flags will be st_bad in the t_virgin/no_read forward-processing
    // case because we are fixing the page before a format.

    if (store_flags & st_insert_file)  {
        store_flags = (store_flag_t) (store_flags|st_tmp); 
        // is st_tmp and st_insert_file
    }
    /* allow these only */
    w_assert1((page_flags & ~t_virgin) == 0);

    W_IFTRACE(const char * bf_fix="";)
    if (_pp && _pp->pid == pid) 
    {
        if(_mode >= m)  {
            /*
             *  We have already fixed the page... do nothing.
             */
            W_IFTRACE(bf_fix="no-op";)
        } else if(condl) {
            W_IFTRACE(bf_fix="latch-upgrade";)
              bool would_block = false;
              bf->upgrade_latch_if_not_block(_pp, would_block);
              if(would_block)
                       return RC(sthread_t::stINUSE);
              w_assert2(_pp && bf->is_bf_page(_pp, true));
              _mode = bf->latch_mode(_pp);
        } else {
            W_IFTRACE(bf_fix="latch-upgrade";)
            /*
             *  We have already fixed the page, but we need
             *  to upgrade the latch mode.
             */
            bf->upgrade_latch(_pp, m); // might block
            w_assert2(_pp && bf->is_bf_page(_pp, true));
            _mode = bf->latch_mode(_pp);
            w_assert3(_mode >= m);
        }
    } else {
        /*
         * wrong page or no page at all
         */

        if (_pp)  {
            bf->unfix(_pp, false, _refbit);
            _pp = 0;
        } else {
            W_IFTRACE(bf_fix="bf-fix **********************************";)
        }


        if(condl) {
            W_DO( bf->conditional_fix(_pp, pid, ptag, m, 
                      (page_flags & t_virgin) != 0,  // no_read
                      ret_store_flags,
                      ignore_store_id, store_flags) );
                        w_assert2(_pp && bf->is_bf_page(_pp, true));
        } else {
            W_DO( bf->fix(_pp, pid, ptag, m, 
                      (page_flags & t_virgin) != 0,  // no_read
                      ret_store_flags,
                      ignore_store_id, store_flags) );
                        w_assert2(_pp && bf->is_bf_page(_pp, true));
        }

#if W_DEBUG_LEVEL > 2
        if( (page_flags & t_virgin) != 0  )  {
            if(
             (smlevel_0::operating_mode == smlevel_0::t_in_undo)
             ||
             (smlevel_0::operating_mode == smlevel_0::t_forward_processing)
            )  // do the assert below
            w_assert3(ret_store_flags != st_bad);
        }
#endif 
        _mode = bf->latch_mode(_pp);
        w_assert3(_mode >= m);
    }

    _refbit = refbit;
    
    w_assert3(_mode >= m);
    store_flags = ret_store_flags;

    w_assert2(is_fixed());
    w_assert2(_pp && bf->is_bf_page(_pp, true));
    INC_TSTAT(page_fix_cnt);  
    DBGTHRD(<<"page fix: tag " << ptag << " pid " << pid);
    
    return RCOK;
}

bool                         
page_p::is_latched_by_me() const
{
    return _pp ? bf->fixed_by_me(_pp) : false;
}

const latch_t *                         
page_p::my_latch() const
{
    return _pp ? bf->my_latch(_pp) : NULL;
}

bool                         
page_p::is_mine() const
{
    return _pp ? bf->is_mine(_pp) : false;
}

bool page_p::check_space_for_insert(size_t rec_size) {
    size_t contiguous_free_space = usable_space();
    return contiguous_free_space >= align(rec_size) + slot_sz;
}

bool page_p::pinned_by_me() const
{
    return bf->fixed_by_me(_pp);
}

w_rc_t
page_p::_copy(const page_p& p) 
{
    _refbit = p._refbit;
    _mode = p._mode;
    _pp = p._pp;
    if (_pp) {
        if( bf->is_bf_page(_pp)) {
            W_DO(bf->refix(_pp, _mode));
        }
    }
    return RCOK;
}

page_p& 
page_p::operator=(const page_p& p)
{
    if (this != &p)  {
        if(_pp) {
            if (bf->is_bf_page(_pp))   {
                bf->unfix(_pp, false, _refbit);
                _pp = 0;
            }
        }

        W_COERCE(_copy(p));
    }
    return *this;
}

void
page_p::upgrade_latch(latch_mode_t m)
{
    w_assert3(bf->is_bf_page(_pp));
    bf->upgrade_latch(_pp, m);
    _mode = bf->latch_mode(_pp);
}
void page_p::downgrade_latch()
{
    w_assert1(_mode == LATCH_EX);
    w_assert3(bf->is_bf_page(_pp));
    bf->downgrade_latch(_pp);
    _mode = LATCH_SH;
}

rc_t
page_p::upgrade_latch_if_not_block(bool& would_block)
{
    w_assert3(bf->is_bf_page(_pp));
    bf->upgrade_latch_if_not_block(_pp, would_block);
    if (!would_block) _mode = LATCH_EX;
    return RCOK;
}

rc_t page_p::set_tobedeleted (bool log_it) {
    if ((_pp->page_flags & t_tobedeleted) == 0) {
        if (log_it) {
            W_DO(log_page_set_tobedeleted (*this));
        }
        _pp->page_flags ^= t_tobedeleted;
        set_dirty();
    }
    return RCOK;
}

void page_p::unset_tobedeleted() {
    if ((_pp->page_flags & t_tobedeleted) != 0) {
        _pp->page_flags ^= t_tobedeleted;
        // we don't need set_dirty() as it's always dirty if this is ever called
        // (UNDOing this means the page wasn't deleted yet by bufferpool, so it's dirty)
    }
}
