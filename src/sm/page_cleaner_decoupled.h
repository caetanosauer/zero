#ifndef PAGE_CLEANER_DECOUPLED_H
#define PAGE_CLEANER_DECOUPLED_H

#include "bf_tree.h"
#include "logarchiver.h"
#include "vol.h"
#include "generic_page.h"
#include "lsn.h"
#include "smthread.h"
#include "page_cleaner.h"

class page_cleaner_decoupled : public page_cleaner_base{
public:
    page_cleaner_decoupled(const sm_options& _options);
    virtual ~page_cleaner_decoupled();

    virtual void notify_archived_lsn(lsn_t);

protected:
    virtual void do_work();

private:

    void update_cb_clean(size_t from, size_t to);
    void flush_segments();

    std::vector<PageID> segments;
    bool _write_elision;
    bool _use_page_img_logrec;
    size_t _segment_size;
    lsn_t _last_lsn;
};

#endif // PAGE_CLEANER_H
