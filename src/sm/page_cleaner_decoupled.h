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
    page_cleaner_decoupled(bf_tree_m* _bufferpool, const sm_options& _options);
    virtual ~page_cleaner_decoupled();

protected:
    virtual void do_work();

private:
    void fill_cb_indexes();
};

#endif // PAGE_CLEANER_H
