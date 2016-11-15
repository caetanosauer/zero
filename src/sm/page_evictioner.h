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
    virtual void            ref(bf_idx idx);


protected:
    /** the buffer pool this cleaner deals with. */
    bf_tree_m*                  _bufferpool;
    bool                        _swizziling_enabled;

    /**
     * Pick victim must return the bf_idx. The corresponding CB must be latched
     * in EX mode. If for any reason it must exit without a victim, this method
     * must return bf_idx 0.
     */
    virtual bf_idx          pick_victim();


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
	bf_idx                      _current_frame;

	/**
	 * In case swizziling is enabled, it will unswizzle the parent point.
	 * Additionally, it will update the parent emlsn.
	 * This two operations are kept in a single method because both require
	 * looking up the parent, latching, etc, so we save some work.
	 */
	bool unswizzle_and_update_emlsn(bf_idx idx);

	virtual void do_work ();
};

class page_evictioner_gclock : public page_evictioner_base {
public:
    page_evictioner_gclock(bf_tree_m* bufferpool, const sm_options& options);
    virtual ~page_evictioner_gclock();

    virtual void            ref(bf_idx idx);

protected:
    virtual bf_idx          pick_victim();

private:
    uint16_t            _k;
    uint16_t*           _counts;
    bf_idx              _current_frame;
};

#endif