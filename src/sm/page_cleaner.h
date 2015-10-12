#ifndef PAGE_CLEANER_H
#define PAGE_CLEANER_H

#include "w_defines.h"
#include "sm_base.h"
#include "smthread.h"
#include "vid_t.h"
#include "vol.h"
#include "generic_page.h"
#include "logarchiver.h"
#include "bf_tree.h"

#include <vector>

struct CleanerControl {
    pthread_mutex_t mutex;
    pthread_cond_t activateCond;
    lsn_t endLSN;
    bool activated;
    bool listening;
    bool* shutdownFlag;

    CleanerControl(bool* shutdown);
    ~CleanerControl();
    bool activate(bool wait, lsn_t lsn = lsn_t::null);
    bool waitForActivation();
};

class page_cleaner : public smthread_t {
public:
    const static int SEQ_PAGES = 16; // read/write 16 pages at a time

    page_cleaner (vol_t* _volume, LogArchiver::ArchiveDirectory* _archive, bf_tree_m* _buffer);
    ~page_cleaner ();
    void run();
    void activate(lsn_t endLSN);
    void shutdown();
    bool isActive() { return control.activated; }

private:
    vol_t* volume;
    LogArchiver::ArchiveDirectory* archive;
    bf_tree_m* buffer_manager;

    vector<generic_page> workspace;

    bool shutdownFlag;
    CleanerControl control;

    void flush_workspace();
};


#endif // PAGE_CLEANER_H