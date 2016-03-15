#ifndef PAGE_CLEANER_H
#define PAGE_CLEANER_H

#include "smthread.h"
#include "sm_options.h"
#include "lsn.h"
#include "bf_hashtable.h"
#include "allocator.h"
#include "generic_page.h"

#include <algorithm>
#include <chrono>
#include <mutex>
#include <condition_variable>

class bf_tree_m;
class generic_page;

class page_cleaner_base : public smthread_t {
public:
    page_cleaner_base(bf_tree_m* bufferpool, const sm_options& _options);
    virtual ~page_cleaner_base();

    void run();

    /**
     * Wakes up the cleaner thread assigned to the given volume.
     */
    void wakeup(bool wait = false);

    /**
     * Gracefully request all cleaners to stop.
     */
    void shutdown ();

protected:

    virtual void do_work() = 0;

    void flush_workspace(size_t from, size_t to);


    /** the buffer pool this cleaner deals with. */
    bf_tree_m*                  _bufferpool;

    /** in-transit buffer for written pages */
    vector<generic_page, memalign_allocator<generic_page>> _workspace;
    size_t _workspace_size;

   vector<bf_idx> _workspace_cb_indexes;

    long _interval_msec;

    lsn_t _clean_lsn;

private:

    std::mutex _wakeup_mutex;
    std::condition_variable _wakeup_condvar;

    std::mutex _done_mutex;
    std::condition_variable _done_condvar;

    /** whether this thread has been requested to stop. */
    bool _stop_requested;
    /** whether this thread has been requested to wakeup. */
    bool _wakeup_requested;
    /** number of do_work() rounds already completed by the claner */
    unsigned long _rounds_completed;
};

#endif

