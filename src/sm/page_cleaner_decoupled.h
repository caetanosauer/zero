#ifndef PAGE_CLEANER_DECOUPLED_H
#define PAGE_CLEANER_DECOUPLED_H

#include "bf_tree.h"        // For page_cleaner_mgr::bufferpool
#include "logarchiver.h"    // For page_cleaner_mgr::archive (nested class)
#include "vol.h"            // For page_cleaner_mgr::cleaners and page_cleaner_slave::volume
#include "generic_page.h"   // For page_cleaner_slave::workspace (?)
#include "lsn.h"            // For page_cleaner_slave::completed_lsn
#include "w_base.h"         // For w_rc_t
#include "smthread.h"       // For smthread_t
#include "page_cleaner_base.h"

#include <vector>
#include <pthread.h>

struct CleanerControl {
    bool* shutdownFlag;
    int sleep_time;

    pthread_mutex_t mutex;
    pthread_cond_t activateCond;

    bool activated;
    bool listening;

    CleanerControl(bool* _shutdown, uint _sleep_time);
    ~CleanerControl();

    bool activate(bool wait);
    bool waitForActivation();
    bool waitForReturn();
};

class page_cleaner_decoupled : public page_cleaner_base{
public:
    page_cleaner_decoupled( bf_tree_m*                     _bufferpool,
                            const sm_options&              _options);
    ~page_cleaner_decoupled();

    void run();
    w_rc_t shutdown();

    w_rc_t wakeup_cleaner();      // async clean
    w_rc_t force_volume();        // sync clean

    bool isActive() { return control->activated; }

private:
    bf_tree_m* bufferpool;

    generic_page* workspace;
    uint workspace_size;
    bool workspace_empty;

    bool shutdownFlag;
    lsn_t completed_lsn;
    CleanerControl* control;

    w_rc_t flush_workspace();
};

#endif // PAGE_CLEANER_H
