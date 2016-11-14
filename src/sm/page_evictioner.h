#ifndef PAGE_EVICTIONER_H
#define PAGE_EVICTIONER_H

#include "smthread.h"
#include "sm_options.h"
#include "lsn.h"
#include "bf_hashtable.h"
#include "allocator.h"
#include "generic_page.h"

#include "worker_thread.h"

class bf_tree_m;
class generic_page;

class page_evictioner_base : public worker_thread_t {
public:
	page_evictioner_base(bf_tree_m* bufferpool, const sm_options& options);
    virtual ~page_evictioner_base();

    /**
     * When eviction is triggered, _about_ this number of cb will be evicted at
     * once. If this amount of cb is already free, the eviction does nothing and
     * goes back to sleep. Given as a ratio of the buffer size (currently 1%).
     */
    const float EVICT_BATCH_RATIO = 0.01;

protected:
    /** the buffer pool this cleaner deals with. */
    bf_tree_m*                  _bufferpool;
    bool                        _swizziling_enabled;    
    bf_idx                      _current_frame;

    virtual void do_work ();
};

#endif 