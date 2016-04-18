#ifndef PAGE_CLEANER_H
#define PAGE_CLEANER_H

#include "smthread.h"
#include "sm_options.h"
#include "lsn.h"
#include "bf_hashtable.h"
#include "allocator.h"
#include "generic_page.h"

#include "worker_thread.h"

class bf_tree_m;
class generic_page;

class page_cleaner_base : public worker_thread_t {
public:
    page_cleaner_base(bf_tree_m* bufferpool, const sm_options& _options);
    virtual ~page_cleaner_base();

protected:
    void flush_workspace(size_t from, size_t to);

    /** the buffer pool this cleaner deals with. */
    bf_tree_m*                  _bufferpool;

    /** in-transit buffer for written pages */
    vector<generic_page, memalign_allocator<generic_page>> _workspace;
    size_t _workspace_size;

    vector<bf_idx> _workspace_cb_indexes;

    lsn_t _clean_lsn;
};

#endif

