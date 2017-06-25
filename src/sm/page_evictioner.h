#ifndef PAGE_EVICTIONER_H
#define PAGE_EVICTIONER_H

#include "smthread.h"
#include "sm_options.h"
#include "lsn.h"
#include "bf_hashtable.h"
#include "allocator.h"
#include "generic_page.h"
#include "bf_tree_cb.h"

#include "worker_thread.h"

#include <random>

class bf_tree_m;
class generic_page;
struct bf_tree_cb_t;

class page_evictioner_base : public worker_thread_t {
public:

    page_evictioner_base(bf_tree_m* bufferpool, const sm_options& options);
    virtual ~page_evictioner_base();

    /**
     * Every time a page is fixed, this method is called. The policy then should
     * do whatever it wants.
     */
    void            ref(bf_idx idx);


    /**
     * Pick victim must return the bf_idx. The corresponding CB must be latched
     * in EX mode. If for any reason it must exit without a victim, this method
     * must return bf_idx 0.
     */
    bf_idx          pick_victim();

    bool evict_one(bf_idx);

protected:
    /** the buffer pool this cleaner deals with. */
    bf_tree_m*                  _bufferpool;
    bool                        _swizzling_enabled;
    bool                        _maintain_emlsn;
    bool                        _log_evictions;
    bool                        _random_pick;
    bool                        _use_clock;

    std::default_random_engine _rnd_gen;
    std::uniform_int_distribution<bf_idx> _rnd_distr;
    bf_idx get_random_idx() { return _rnd_distr(_rnd_gen); }

    // Maximum number of pick_victim attempts before throwing "eviction stuck" error
    unsigned _max_attempts;

    // Cleaner is waken up every this many eviction attempts
    unsigned _wakeup_cleaner_attempts;

    // Dirty pages are flushed after this many eviction attempts
    unsigned _clean_only_attempts;

private:
    /**
     * When eviction is triggered, _about_ this number of cb will be evicted at
     * once. If this amount of cb is already free, the eviction does nothing and
     * goes back to sleep. Given as a ratio of the buffer size (currently 1%).
     */
    const float EVICT_BATCH_RATIO = 0.01;

    /**
     * Last control block examined.
     */
    std::atomic<bf_idx>                      _current_frame;

    // Used by simple CLOCK policy
    std::vector<bool> _clock_ref_bits;

    /**
     * In case swizziling is enabled, it will unswizzle the parent point.
     * Additionally, it will update the parent emlsn.
     * This two operations are kept in a single method because both require
     * looking up the parent, latching, etc, so we save some work.
     */
    bool unswizzle_and_update_emlsn(bf_idx idx);

    void flush_dirty_page(const bf_tree_cb_t& cb);

    virtual void do_work ();
};

#endif
