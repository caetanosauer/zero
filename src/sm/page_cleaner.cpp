#include "page_cleaner.h"

#include "sm.h" //for ss_m::shutting_down and ss_m::shutdown_clean
#include "logrec.h"
#include "fixable_page_h.h"
#include "bf_tree_cb.h"

bool _dirty_shutdown_happening_now() {
    return (ss_m::shutting_down && !ss_m::shutdown_clean);
}

CleanerControl::CleanerControl(bool* _shutdownFlag, cleaner_mode_t _mode, uint _sleep_time)
    : shutdownFlag(_shutdownFlag), mode(_mode), sleep_time(_sleep_time),
    activated(false), listening(false)
{
    DO_PTHREAD(pthread_mutex_init(&mutex, NULL));
    DO_PTHREAD(pthread_cond_init(&activateCond, NULL));
}

CleanerControl::~CleanerControl()
{
    DO_PTHREAD(pthread_mutex_destroy(&mutex));
    DO_PTHREAD(pthread_cond_destroy(&activateCond));
}

bool CleanerControl::activate(bool wait)
{
    if (wait) {
        DO_PTHREAD(pthread_mutex_lock(&mutex));
    }
    else {
        if (pthread_mutex_trylock(&mutex) != 0) {
            return false;
        }
    }
    // now we hold the mutex -- signal archiver thread and set endLSN

    /* run() is in the same critical section of mutex, meaning that if we got here
     * is because run() released the mutex with cond_wait. */

    if(activated == true) {
        DO_PTHREAD(pthread_mutex_unlock(&mutex));
        return false;
    }
    else {
        DBGTHRD(<< "Activating cleaner thread");
        activated = true;
        DO_PTHREAD(pthread_cond_signal(&activateCond));
        DO_PTHREAD(pthread_mutex_unlock(&mutex));
    }

    /*
     * Returning true only indicates that signal was sent, and not that the
     * archiver thread is running with the given endLSN. Another thread
     * calling activate may get the mutex before the log archiver and set
     * another endLSN. In fact, it does not even mean that the signal was
     * received, since the thread may not be listening yet.
     */
    return activated;
}

bool CleanerControl::waitForActivation()
{
    // WARNING: mutex must be held by caller!
    listening = true;
    while(!activated) {
        struct timespec timeout;
        sthread_t::timeout_to_timespec(sleep_time, timeout); // 100ms
        int code = pthread_cond_timedwait(&activateCond, &mutex, &timeout);
        if (code == ETIMEDOUT) {
            if (*shutdownFlag) {
                DBGTHRD(<< "Activation failed due to shutdown. Exiting");
                return false;
            }
            if(mode == EAGER) {
                DBGTHRD(<< "Cleaner activating proactively");
                activated = true;
            }
        }
        DO_PTHREAD_TIMED(code);
    }
    listening = false;
    return true;
}

page_cleaner_slave::page_cleaner_slave(page_cleaner_mgr* _master,
                                       vol_t*            _volume,
                                       uint              _bufsize,
                                       cleaner_mode_t    _mode,
                                       uint              _sleep_time)
: master(_master),
  volume(_volume),
  workspace_size(_bufsize),
  workspace_empty(true),
  shutdownFlag(false),
  control(&shutdownFlag, _mode, _sleep_time)
{
    completed_lsn = lsn_t(1,0);
    posix_memalign((void**) &workspace, sizeof(generic_page), workspace_size * sizeof(generic_page));
    memset(workspace, '\0', workspace_size * sizeof(generic_page));
}

page_cleaner_slave::~page_cleaner_slave()
{
    delete[] workspace;
}

void page_cleaner_slave::run() {
    while(true) {
        CRITICAL_SECTION(cs, control.mutex);

        bool activated = control.waitForActivation();
        if (!activated) {
            break;
        }

        lintel::atomic_thread_fence(lintel::memory_order_release);
        if (shutdownFlag) {
            control.activated = false;
            break;
        }

        lsn_t last_lsn = master->archive->getLastLSN();
        if(last_lsn <= completed_lsn) {
            DBGTHRD(<< "Nothing archived to clean.");

            bf_idx block_cnt = master->bufferpool->_block_cnt;
            bool in_real_hurry = (unsigned)master->bufferpool->_dirty_page_count_approximate > (block_cnt / 4 * 3);
            if(in_real_hurry) {
                DBGTHRD(<< "We are in a hurry. Flushing log and archive for cleaner.");
                ss_m::log->flush_all();
                ss_m::logArchiver->requestFlushSync(ss_m::log->durable_lsn());
            }
            else {
                control.activated = false;
                continue;
            }
        }

        DBGTHRD(<< "Cleaner thread activated from " << completed_lsn);

        PageID first_page = volume->first_data_pageid();
        LogArchiver::ArchiveScanner logScan(master->archive);
        // CS TODO block size
        LogArchiver::ArchiveScanner::RunMerger* merger = logScan.open(first_page, 0,
                completed_lsn, 1048576);

        generic_page* page = NULL;
        logrec_t* lr;
        while (merger != NULL && merger->next(lr)) {

            PageID lrpid = lr->pid();

            if(page != NULL && page->pid != lrpid) {
                page->checksum = page->calculate_checksum();
            }

            if(!workspace_empty
                && workspace[workspace_size-1].pid != 0
                && workspace[workspace_size-1].pid < lrpid) {
                w_assert0(workspace[workspace_size-1].pid >= page->pid);
                flush_workspace();

                memset(workspace, '\0', workspace_size * sizeof(generic_page));
                workspace_empty = true;
            }

            if(workspace_empty) {
                /* true for ignoreRestore */
                w_rc_t err = volume->read_many_pages(lrpid, workspace, workspace_size, true);
                if(err.err_num() == eVOLFAILED) {
                    DBGOUT(<<"Trying to clean pages, but device is failed. Cleaner deactivating.");
                    control.activated = false;
                    break;
                }
                else if(err.err_num() != stSHORTIO) {
                    W_COERCE(err);
                }
                workspace_empty = false;

                page = &workspace[0];
            }
            else {
                PageID base_pid = workspace[0].pid;
                page = &workspace[lrpid - base_pid];
            }

            if(page->lsn >= lr->lsn_ck()) {
                DBGOUT(<<"Not replaying log record " << lr->lsn_ck() << ". Page " << page->pid << " is up-to-date.");
                continue;
            }

            page->pid = lrpid;

            fixable_page_h fixable;
            fixable.setup_for_restore(page);
            lr->redo(&fixable);
            fixable.update_initial_and_last_lsn(lr->lsn_ck());
            fixable.update_clsn(lr->lsn_ck());

            DBGOUT(<<"Replayed log record " << lr->lsn_ck() << " for page " << page->pid);
        }

        if(!workspace_empty) {
            page->checksum = page->calculate_checksum();
            flush_workspace();

            memset(workspace, '\0', workspace_size * sizeof(generic_page));
            workspace_empty = true;
        }
        completed_lsn = last_lsn;
        DBGTHRD(<< "Cleaner thread deactivating. Cleaned until " << completed_lsn);
        control.activated = false;
    }
}

bool page_cleaner_slave::activate() {
    //DBGTHRD(<< "Requesting activation of cleaner thread");
    return control.activate(false);
}

void page_cleaner_slave::shutdown() {
    shutdownFlag = true;
    // make other threads see new shutdown value
    lintel::atomic_thread_fence(lintel::memory_order_release);
}

w_rc_t page_cleaner_slave::flush_workspace() {
    if (_dirty_shutdown_happening_now()) {
        return RCOK;
    }

    PageID first_pid = workspace[0].pid;
    DBGOUT1(<<"Flushing write buffer from page "<<first_pid << " to page " << first_pid + workspace_size-1);
    W_COERCE(volume->write_many_pages(first_pid, workspace, workspace_size));

    for(uint i=0; i<workspace_size; ++i) {
        generic_page& flushed = workspace[i];
        bf_idx idx = master->bufferpool->lookup(flushed.pid);
        if(idx != 0) {
            //page is in the buffer
            bf_tree_cb_t& cb = master->bufferpool->get_cb(idx);
            w_rc_t latch_rc = cb.latch().latch_acquire(LATCH_SH, sthread_t::WAIT_IMMEDIATE);

            if (latch_rc.is_error()) {
                continue;
            }

            generic_page& buffered = *smlevel_0::bf->get_page(idx);

            if (buffered.pid == flushed.pid) {
                w_assert0(buffered.lsn >= flushed.lsn);

                if (buffered.lsn == flushed.lsn && cb._dirty) {
                    cb._dirty = false;
                    master->bufferpool->_dirty_page_count_approximate--;
                    DBGOUT1(<<"Setting page " << flushed.pid << " clean.");
                }

                // cb._rec_lsn = _write_buffer[i].lsn.data();
                cb._rec_lsn = lsn_t::null.data();
                cb._dependency_idx = 0;
                cb._dependency_lsn = 0;
                cb._dependency_shpid = 0;
            }
            cb.latch().latch_release();
        }
    }
    return RCOK;
}

page_cleaner_mgr::page_cleaner_mgr( bf_tree_m* _bufferpool, LogArchiver::ArchiveDirectory* _archive, const sm_options& options)
    : bufferpool(_bufferpool), archive(_archive)
{
    bool eager = options.get_bool_option("sm_decoupled_cleaner_mode", DFT_EAGER);
    if(eager) {
        mode = EAGER;
    }
    else {
        mode = NORMAL;
    }
    sleep_time = options.get_int_option("sm_decoupled_cleaner_interval", DFT_SLEEP_TIME);
    buffer_size = options.get_int_option("sm_decoupled_cleaner_bufsize", DFT_BUFFER_SIZE);
    cleaner = NULL;
}

page_cleaner_mgr::~page_cleaner_mgr() {
}

w_rc_t page_cleaner_mgr::install_cleaner() {
    cleaner = new page_cleaner_slave(this, smlevel_0::vol, buffer_size, mode, sleep_time);
    cleaner->fork();
    return RCOK;
}

w_rc_t page_cleaner_mgr::uninstall_cleaner() {
    if (cleaner) {
        cleaner->shutdown();
        cleaner->join();
        delete cleaner;
    }
    return RCOK;
}

w_rc_t page_cleaner_mgr::force_all() {
    while(true) {
        /* We have to flush log and archive to guarantee that we are going to
         * replay the most recent log records and clean the buffer */
        W_DO(ss_m::log->flush_all());
        ss_m::logArchiver->requestFlushSync(ss_m::log->durable_lsn());

        // We do this for reasons
        // W_DO (smlevel_0::vol->force_fixed_buffers());

        wakeup_cleaner();

        bool all_clean = true;
        bf_idx block_cnt = bufferpool->_block_cnt;
        for (bf_idx idx = 1; idx < block_cnt; ++idx) {
            // no latching is needed -- fuzzy check
            bf_tree_cb_t &cb = bufferpool->get_cb(idx);
            if (cb._dirty) {
                all_clean = false;
                break;
            }
        }
        if (all_clean) {
            break;
        }
    }
    return RCOK;
}

bool page_cleaner_mgr::wakeup_cleaner() {
    return cleaner->activate();
}
