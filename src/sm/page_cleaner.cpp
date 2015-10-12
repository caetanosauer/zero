#include "page_cleaner.h"

CleanerControl::CleanerControl(bool* shutdownFlag)
    : endLSN(lsn_t::null), activated(false), listening(false), shutdownFlag(shutdownFlag)
{
    DO_PTHREAD(pthread_mutex_init(&mutex, NULL));
    DO_PTHREAD(pthread_cond_init(&activateCond, NULL));
}

CleanerControl::~CleanerControl()
{
    DO_PTHREAD(pthread_mutex_destroy(&mutex));
    DO_PTHREAD(pthread_cond_destroy(&activateCond));
}

bool CleanerControl::activate(bool wait, lsn_t lsn)
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

    /* Make sure signal is sent only if thread is listening.
     * TODO: BUG? The mutex alone cannot guarantee that the signal is not lost,
     * since the activate call may happen before the thread ever starts
     * listening. If we ever get problems with archiver getting stuck, this
     * would be one of the first things to try. We could, e.g., replace
     * the listening flag with something like "gotSignal" and loop this
     * method until it's true.
     */
    // activation may not decrease the endLSN
    w_assert0(lsn >= endLSN);
    endLSN = lsn;
    activated = true;
    DO_PTHREAD(pthread_cond_signal(&activateCond));
    DO_PTHREAD(pthread_mutex_unlock(&mutex));

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
        sthread_t::timeout_to_timespec(100, timeout); // 100ms
        int code = pthread_cond_timedwait(&activateCond, &mutex, &timeout);
        if (code == ETIMEDOUT) {
            if (*shutdownFlag) {
                DBGTHRD(<< "Activation failed due to shutdown. Exiting");
                return false;
            }
        }
        DO_PTHREAD_TIMED(code);
    }
    listening = false;
    return true;
}


page_cleaner::page_cleaner(vol_t* _volume, LogArchiver::ArchiveDirectory* _archive, bf_tree_m* _buffer_manager)
: volume(_volume), archive(_archive), buffer_manager(_buffer_manager), shutdownFlag(false), control(&shutdownFlag) {
}

page_cleaner::~page_cleaner() {

}

void page_cleaner::run() {
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

        DBGTHRD(<< "Cleaner thread activated until " << control.endLSN);

        lpid_t first_page = lpid_t(volume->vid(), volume->first_data_pageid());
        LogArchiver::ArchiveScanner logScan(archive);
        LogArchiver::ArchiveScanner::RunMerger* merger = logScan.open(first_page, lpid_t::null, control.endLSN);

        generic_page* page = NULL;
        logrec_t* lr;
        while (merger->next(lr)) {

            lpid_t lrpid = lr->construct_pid();

            if(page != NULL && page->pid != lrpid) {
                page->checksum = page->calculate_checksum();
            }

            if(workspace.size() > 0 
                && workspace.back().pid != lpid_t::null
                && workspace.back().pid < lrpid) {
                w_assert0(workspace.size() == SEQ_PAGES);
                w_assert0(workspace.back().pid == page->pid);
                flush_workspace();
                workspace.clear();
            }

            if(workspace.size() == 0) {
                workspace.resize(SEQ_PAGES);
                volume->read_many_pages(lrpid.page, &workspace[0], SEQ_PAGES);
                page = &workspace[0];
            }
            else {
                shpid_t base_pid = workspace[0].pid.page;
                page = &workspace[lrpid.page - base_pid];
            }

            page->pid = lrpid;

            fixable_page_h fixable;
            fixable.setup_for_restore(page);
            lr->redo(&fixable);
            fixable.update_initial_and_last_lsn(lr->lsn_ck());
            fixable.update_clsn(lr->lsn_ck());

            DBGOUT(<<"Replayed log record " << lr->lsn_ck() << " for page " << page->pid);
        }

        flush_workspace();
        control.activated = false;
    }   
}

void page_cleaner::activate(lsn_t endLSN) {
    DBGTHRD(<< "Activating cleaner thread until " << endLSN);
    control.activate(true, endLSN);
}

void page_cleaner::shutdown() {
    shutdownFlag = true;
    // make other threads see new shutdown value
    lintel::atomic_thread_fence(lintel::memory_order_release);
}

void page_cleaner::flush_workspace() {
    /*
    if (_dirty_shutdown_happening()) {
        return RCOK;
    }*/

    shpid_t first_pid = workspace.front().pid.page;
    DBGOUT1(<<"Flushing write buffer from page "<<first_pid << " to page " << first_pid + workspace.size()-1);
    W_COERCE(volume->write_many_pages(first_pid, &workspace[0], workspace.size()-1));

    for(uint i=0; i<workspace.size(); ++i) {
        generic_page& flushed = workspace[i];
        uint64_t key = bf_key(flushed.pid);
        bf_idx idx = buffer_manager->lookup_in_doubt(key);
        if(idx != 0) {
            //page is in the buffer
            bf_tree_cb_t& cb = buffer_manager->get_cb(idx);
            cb.latch().latch_acquire(LATCH_SH, sthread_t::WAIT_FOREVER);
            generic_page& buffered = *smlevel_0::bf->get_page(idx);

            if (buffered.pid == flushed.pid) {
                w_assert0(buffered.lsn >= flushed.lsn);

                if (buffered.lsn == flushed.lsn) {
                    cb._dirty = false;
                    DBGOUT1(<<"Setting page " << flushed.pid.page << " clean.");
                }
                // CS TODO: why are in_doubt and recovery_access set here???
                cb._in_doubt = false;
                cb._recovery_access = false;
                --buffer_manager->_dirty_page_count_approximate;

                // cb._rec_lsn = _write_buffer[i].lsn.data();
                cb._rec_lsn = lsn_t::null.data();
                cb._dependency_idx = 0;
                cb._dependency_lsn = 0;
                cb._dependency_shpid = 0;
            }
            cb.latch().latch_release();
        }
    }
}