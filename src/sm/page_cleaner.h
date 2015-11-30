#ifndef PAGE_CLEANER_H
#define PAGE_CLEANER_H

#include "bf_tree.h"        // For page_cleaner_mgr::bufferpool
#include "logarchiver.h"    // For page_cleaner_mgr::archive (nested class)
#include "vol.h"            // For page_cleaner_mgr::cleaners and page_cleaner_slave::volume
#include "generic_page.h"   // For page_cleaner_slave::workspace (?)
#include "lsn.h"            // For page_cleaner_slave::completed_lsn
#include "w_base.h"         // For w_rc_t
#include "smthread.h"       // For smthread_t

#include <vector>
#include <pthread.h>

class page_cleaner_slave;

enum cleaner_mode_t {
    NORMAL,
    EAGER
};

class page_cleaner_mgr {
    friend class page_cleaner_slave;
public:
    page_cleaner_mgr(bf_tree_m* _bufferpool, LogArchiver::ArchiveDirectory* _archive, const sm_options& options);
    ~page_cleaner_mgr();

    w_rc_t install_cleaner();
    w_rc_t uninstall_cleaner();

    bool wakeup_cleaner(); // async clean
    w_rc_t force_all();        // sync clean

private:
    bf_tree_m* bufferpool;
    LogArchiver::ArchiveDirectory* archive;

    page_cleaner_slave* cleaner;

    cleaner_mode_t mode;
    uint sleep_time;
    uint buffer_size;

    const static int DFT_BUFFER_SIZE = 16;      // in pages
    const static bool DFT_EAGER = false;
    const static int DFT_SLEEP_TIME = 10000;    //
};

struct CleanerControl {
    bool* shutdownFlag;
    cleaner_mode_t mode;
    uint sleep_time;

    pthread_mutex_t mutex;
    pthread_cond_t activateCond;

    bool activated;
    bool listening;

    CleanerControl(bool* _shutdown, cleaner_mode_t _mode, uint _sleep_time);
    ~CleanerControl();

    bool activate(bool wait);
    bool waitForActivation();
    bool waitForReturn();
};

class page_cleaner_slave : public smthread_t {
public:

    page_cleaner_slave (page_cleaner_mgr* _master,
                    vol_t* _volume,
                    uint _bufsize,
                    cleaner_mode_t _mode,
                    uint _sleep_time);
    ~page_cleaner_slave ();
    void run();

    /* Returns true only if the cleaner was activated as a result of this call.
     * The cleaner might have been activated by other thread. In case the caller
     * wants to make a sync call to cleaner, he can embed it in a while loop:
     *     while(!cleaner->activate()) {
     *         sleep();
     *     }
     */
    bool activate();
    void shutdown();
    bool isActive() { return control.activated; }

private:
    page_cleaner_mgr* master;
    vol_t* volume;

    generic_page* workspace;
    uint workspace_size;
    bool workspace_empty;

    bool shutdownFlag;
    lsn_t completed_lsn;
    CleanerControl control;

    w_rc_t flush_workspace();
};


#endif // PAGE_CLEANER_H
