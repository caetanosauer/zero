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
#include <atomic>

class bf_tree_m;
class generic_page;

class page_cleaner_base : public smthread_t {
public:
    page_cleaner_base(bf_tree_m* bufferpool, const sm_options& _options);
    virtual ~page_cleaner_base();

    void run();

    /**
     * Wakes up the cleaner thread.
     * If wait = true, the call will block until the cleaner has performed
     * at least a full cleaning round. This means that if the cleaner is
     * currently busy, we will wait for the current round to finish, start
     * a new round, and wait for that one too.
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

    bool should_exit() const { return _stop_requested; }
    unsigned long get_rounds_completed() const { return _rounds_completed; };

private:

    std::mutex _cond_mutex;
    std::condition_variable _wakeup_condvar;
    std::condition_variable _done_condvar;

    /** whether this thread has been requested to stop. */
    std::atomic<bool> _stop_requested;
    /** whether this thread has been requested to wakeup. */
    bool _wakeup_requested;
    /** whether this thread is currently busy (and not waiting for wakeup */
    bool _cleaner_busy;
    /** number of do_work() rounds already completed by the claner */
    unsigned long _rounds_completed;
};

#endif

