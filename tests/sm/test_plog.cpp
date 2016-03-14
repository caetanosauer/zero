#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"

#include "sm_base.h"
#include "sm_external.h"

#include "logbuf_common.h"
#include "logbuf_core.h"
#include "logbuf_seg.h"
#include "log_core.h"
#include "log_storage.h"

#include "logrec.h"
#include "lsn.h"

#include "w_debug.h"

#include <pthread.h>
#include <memory.h>
#include <AtomicCounter.hpp>

#include <vector>
#include <algorithm>

#define protected public // to access protected fields of plog_t
#include "xct.h"
#include "plog_xct.h"

btree_test_env *test_env = NULL;

void fill_with_comments()
{
    W_COERCE(log_comment("test1"));
    W_COERCE(log_comment("test2"));
    W_COERCE(log_comment("test3"));
}

struct test_scan : public restart_test_base
{
    rc_t pre_shutdown(ss_m*) {
        plog_xct_t* xct = new plog_xct_t();
        fill_with_comments();

        { // forward scan
            plog_t::iter_t* iter = xct->plog.iterate_forwards();
            logrec_t* lr = NULL;
            EXPECT_EQ(iter->next(lr), true);
            EXPECT_EQ(lr->type(), logrec_t::t_comment);
            EXPECT_EQ(strncmp(lr->data(), "test1", 5), 0);
            EXPECT_EQ(iter->next(lr), true);
            EXPECT_EQ(lr->type(), logrec_t::t_comment);
            EXPECT_EQ(strncmp(lr->data(), "test2", 5), 0);
            EXPECT_EQ(iter->next(lr), true);
            EXPECT_EQ(lr->type(), logrec_t::t_comment);
            EXPECT_EQ(strncmp(lr->data(), "test3", 5), 0);
            EXPECT_EQ(iter->next(lr), false);
            delete iter;
        }

        { // backward scan
            plog_t::iter_t* iter = xct->plog.iterate_backwards();
            logrec_t* lr = NULL;
            EXPECT_EQ(iter->next(lr), true);
            EXPECT_EQ(lr->type(), logrec_t::t_comment);
            EXPECT_EQ(strncmp(lr->data(), "test3", 5), 0);
            EXPECT_EQ(iter->next(lr), true);
            EXPECT_EQ(lr->type(), logrec_t::t_comment);
            EXPECT_EQ(strncmp(lr->data(), "test2", 5), 0);
            EXPECT_EQ(iter->next(lr), true);
            EXPECT_EQ(lr->type(), logrec_t::t_comment);
            EXPECT_EQ(strncmp(lr->data(), "test1", 5), 0);
            EXPECT_EQ(iter->next(lr), false);
            delete iter;
        }

        xct->commit();
        delete xct;

        return RCOK;
    }

    rc_t post_shutdown(ss_m*) {
        return RCOK;
    }
};

struct test_sys_xct_single : public restart_test_base
{
    rc_t pre_shutdown(ss_m*) {
        // 1. Create an empty B-tree
        {
            sys_xct_section_t sx(false);
            _stid_list = new stid_t[1];
            W_DO(smlevel_0::io->create_store(_volume._vid,
                        smlevel_0::st_regular,
                        _stid_list[0]));
            lpid_t root_pid;
            W_DO(smlevel_2::bt->create(_stid_list[0], root_pid));
            sx.end_sys_xct(RCOK);
        }

        // 2. Allocate a new page -> SSX we are testing for
        {
            lpid_t allocd_pid;
            W_DO(smlevel_0::io->sx_alloc_a_page(_stid_list[0], allocd_pid));
        }

        {
            log_i iter(*smlevel_0::clog, lsn_t(1,0), true /* forward */);
            lsn_t lsn = lsn_t::null;
            logrec_t lr;
            int count = 0;
            while (iter.xct_next(lsn, lr)) {
                if (lr->type() == logrec_t::t_alloc_a_page) {
                    count++;
                }
            }
            // 2 allocations: root page and the one allocated manually
            EXPECT_EQ(count, 2);
        }

        return RCOK;
    }

    rc_t post_shutdown(ss_m*) {
        return RCOK;
    }
};

struct test_sys_xct_multi : public restart_test_base
{
    rc_t pre_shutdown(ss_m*) {
        sys_xct_section_t sx(false);

        _stid_list = new stid_t[1];
        W_DO(smlevel_0::io->create_store(_volume._vid,
                    smlevel_0::st_regular,
                    _stid_list[0]));
        lpid_t root_pid;
        W_DO(smlevel_2::bt->create(_stid_list[0], root_pid));

        sx.end_sys_xct(RCOK);

        {
            log_i iter(*smlevel_0::clog, lsn_t(1,0), true /* forward */);
            lsn_t lsn = lsn_t::null;
            logrec_t lr;
            int count = 0;
            int total = 0;
            while (iter.xct_next(lsn, lr)) {
                DBGOUT3(<< "Scanned: " << *lr << endl);
                if (lr->type() == logrec_t::t_store_operation) {
                    count++;
                }
                total++;
            }
            // there should be two store ops: creation and root assignment
            EXPECT_EQ(2, count);
            DBGOUT3(<< "Scanned " << total << " logrecs" << endl);
        }

        return RCOK;
    }

    rc_t post_shutdown(ss_m*) {
        return RCOK;
    }
};

struct test_sys_xct_nested_abort : public restart_test_base
{
    rc_t pre_shutdown(ss_m* ssm) {
        ssm->begin_xct();

        // STEP 1: create index in a nested sys xct
        {
            lpid_t root_pid;
            sys_xct_section_t ssx(false);
            _stid_list = new stid_t[1];
            W_DO(smlevel_0::io->create_store(_volume._vid,
                        smlevel_0::st_regular,
                        _stid_list[0]));
            W_DO(smlevel_2::bt->create(_stid_list[0], root_pid));
            ssx.end_sys_xct(RCOK);
        }

        // STEP 2: add some records and abort
        W_DO(x_btree_insert(ssm, _stid_list[0], "key0", "value0"));
        W_DO(x_btree_insert(ssm, _stid_list[0], "key1", "value1"));

        ssm->abort_xct();

        // STEP 3: look for index creation log records
        {
            log_i iter(*smlevel_0::clog, lsn_t(1,0), true /* forward */);
            lsn_t lsn = lsn_t::null;
            logrec_t lr;
            int count = 0;
            while (iter.xct_next(lsn, lr)) {
                DBGOUT3(<< "Scanned: " << *lr << endl);
                if (lr->type() == logrec_t::t_store_operation) {
                    count++;
                }
                // insertion log records must not be in clog
                EXPECT_TRUE(lr->type() != logrec_t::t_btree_insert);
                EXPECT_TRUE(lr->type() != logrec_t::t_btree_insert_nonghost);
            }
            EXPECT_EQ(count, 2);
        }

        return RCOK;
    }

    rc_t post_shutdown(ss_m*) {
        return RCOK;
    }
};

struct test_sys_xct_nested_abort_implicit : public restart_test_base
{
    rc_t pre_shutdown(ss_m* ssm) {
        ssm->begin_xct();

        // STEP 1: create index -- implicitly a nested sys xct
        lpid_t root_pid;
        _stid_list = new stid_t[1];
        ssm->create_index(_volume._vid, _stid_list[0]);

        // STEP 2: add some records and abort
        W_DO(x_btree_insert(ssm, _stid_list[0], "key0", "value0"));
        W_DO(x_btree_insert(ssm, _stid_list[0], "key1", "value1"));

        ssm->abort_xct();

        // STEP 3: look for index creation log records
        {
            log_i iter(*smlevel_0::clog, lsn_t(1,0), true /* forward */);
            lsn_t lsn = lsn_t::null;
            logrec_t lr;
            int count = 0;
            while (iter.xct_next(lsn, lr)) {
                DBGOUT3(<< "Scanned: " << *lr << endl);
                // index creation allocates a page in a nested SSX,
                // so the allocation must be in the log even though the
                // user transaction aborted
                if (lr->type() == logrec_t::t_alloc_a_page) {
                    count++;
                }
                // insertion log records must not be in clog
                EXPECT_TRUE(lr->type() != logrec_t::t_btree_insert);
                EXPECT_TRUE(lr->type() != logrec_t::t_btree_insert_nonghost);
            }
            EXPECT_EQ(count, 1);
        }

        return RCOK;
    }

    rc_t post_shutdown(ss_m*) {
        return RCOK;
    }
};

struct test_clog_abort : public restart_test_base
{
    rc_t pre_shutdown(ss_m* ssm) {
        // populate a B-tree but do not commit
        _stid_list = new stid_t[1];
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));

        // populating creates a second tx, which we want to abort
        W_DO(test_env->btree_populate_records(_stid_list[0], false,
                    t_test_txn_abort));

        // since xct did not commit, its logrecs should not be in the clog
        log_i iter(*smlevel_0::clog, lsn_t(1,0), true /* forward */);
        lsn_t lsn = lsn_t::null;
        logrec_t lr;
        while (iter.xct_next(lsn, lr)) {
            // no btree inserts should be found since population aborted
            EXPECT_TRUE(lr->type() != logrec_t::t_btree_insert);
            EXPECT_TRUE(lr->type() != logrec_t::t_btree_insert_nonghost);
        }
        return RCOK;
    }

    rc_t post_shutdown(ss_m*) {
        return RCOK;
    }
};

struct test_clog_commit : public restart_test_base
{
    rc_t pre_shutdown(ss_m* ssm) {
        // create a B-tree and commit
        _stid_list = new stid_t[1];
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));

        // populating creates a second transaction
        W_DO(test_env->btree_populate_records(_stid_list[0], false,
                    t_test_txn_commit));

        // xct committed so its logecs should be found in clog
        log_i iter(*smlevel_0::clog, lsn_t(1,0), true /* forward */);
        lsn_t lsn = lsn_t::null;
        logrec_t lr;
        tid_t found_tid = tid_t::null;
        int count = 0;
        int insert_count = 0;

        while (iter.xct_next(lsn, lr)) {
            // SSX overwrites the TID field with other data
            if (!lr->is_single_sys_xct() && lr->tid() != tid_t::null) {
                if (found_tid == tid_t::null) {
                    found_tid = lr->tid();
                }
                count++;
            }
            if (lr->type() == logrec_t::t_btree_insert
                    || lr->type() == logrec_t::t_btree_insert_nonghost)
            {
                insert_count++;
            }
        }

        // some logrecs must have been found
        EXPECT_TRUE(count > 0);
        EXPECT_TRUE(insert_count > 0);
        EXPECT_TRUE(found_tid != tid_t::null);

        return RCOK;
    }

    rc_t post_shutdown(ss_m*) {
        return RCOK;
    }
};

rc_t find_pages_in_log(std::vector<lpid_t>& pages, log_m* log)
{
    log_i iter(*log, lsn_t(1,0), true /* forward */);
    logrec_t lr;
    lsn_t lsn = lsn_t::null;
    while (iter.xct_next(lsn, lr)) {
        if (!lr->null_pid()) {
            lpid_t pid = lr->construct_pid();
            vector<lpid_t>::iterator found =
                std::find(pages.begin(), pages.end(), pid);
            if (found == pages.end()) { // page not on list -> add it
                pages.push_back(pid);
            }
        }
    }
    return RCOK;
}

rc_t find_last_update(log_m* log, const lpid_t& pid, lsn_t& ret)
{
    log_i iter(*log, log->curr_lsn(), false /* forward */);
    logrec_t lr;
    lsn_t lsn = lsn_t::null;
    while (iter.xct_next(lsn, lr)) {
        if (lr->construct_pid() == pid) {
            ret = lr->lsn_ck();
            return RCOK;
        }
    }
    ret = lsn_t::null;
    return RCOK;
}

// COPIED FROM plog_xct.cpp
rc_t my_fix(const lpid_t& pid, latch_mode_t mode, bf_tree_cb_t*& cb,
        generic_page*& page)
{
    uint64_t key = bf_key(pid.vol(), pid.page);
    latch_t* latch;

    while (true) {
        bf_idx idx = smlevel_0::bf->lookup_in_doubt(key);
        w_assert0(idx != 0);

        cb = &smlevel_0::bf->get_cb(idx);
        page = smlevel_0::bf->get_page(cb);

        // Latch the page to prevent it being evicted.
        // Shared mode is fine because only this method increases
        // the PageLSN value, which is done with compare-and-swap
        latch = cb->latchp();
        W_DO(latch->latch_acquire(mode));

        // After latching, we have to re-check if the page was not replaced
        // while we performed the lookup and waited for the latch.
        if (cb->_pid_shpid == pid.page) {
            break;
        }
    }

    return RCOK;
}

rc_t check_page_lsn(const lpid_t& pid, const lsn_t& lsn)
{
    // get page from buffer pool -- no concurrency at this point
    bf_tree_cb_t* cb = NULL;
    generic_page* page = NULL;
    my_fix(pid, LATCH_EX, cb, page);
    EXPECT_TRUE(page != NULL);
    EXPECT_TRUE(cb != NULL);

    // CS TODO -- clsn removed
    // check clsn value of fixed page
    // EXPECT_EQ(lsn, page->clsn);

    cb->latch().latch_release();

    return RCOK;
}

rc_t check_last_update(const lpid_t& pid, const lsn_t& lsn)
{
    lsn_t last_lsn = lsn_t::null;
    W_DO(find_last_update(smlevel_0::clog, pid, last_lsn));
    EXPECT_TRUE(last_lsn != lsn_t::null);
    EXPECT_TRUE(last_lsn == lsn);

    return RCOK;
}

struct test_page_clsn : public restart_test_base
{
    rc_t pre_shutdown(ss_m* ssm) {
        // create a B-tree and commit
        _stid_list = new stid_t[1];
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));

        // insert one record and commit
        W_DO(ssm->begin_xct());
        W_DO(x_btree_insert(ssm, _stid_list[0], "key0", "value0"));
        W_DO(ssm->commit_xct());

        // get page ids from log -- there should be only the root
        std::vector<lpid_t> pages;
        W_DO(find_pages_in_log(pages, smlevel_0::clog));
        EXPECT_EQ(pages.size(), 1);
        lpid_t pid = pages[0];

        // get lsn of last update on the page and check that it matches clsn
        lsn_t last_lsn = lsn_t::null;
        W_DO(find_last_update(smlevel_0::clog, pid, last_lsn));
        EXPECT_TRUE(last_lsn != lsn_t::null);
        W_DO(check_page_lsn(pid, last_lsn));

        // another transaction inserts a record but does not yet commit
        W_DO(ssm->begin_xct());
        W_DO(x_btree_insert(ssm, _stid_list[0], "key1", "value1"));

        // clsn must be the same set earlier by the first TA
        W_DO(check_page_lsn(pid, last_lsn));
        // and the last update in clog is also still the same
        {
            lsn_t check_lsn;
            W_DO(find_last_update(smlevel_0::clog, pid, check_lsn));
            EXPECT_EQ(last_lsn, check_lsn);
        }

        // now the TA commits and the LSN must have been updated
        W_DO(ssm->commit_xct());
        lsn_t last_lsn2 = lsn_t::null;
        W_DO(find_last_update(smlevel_0::clog, pid, last_lsn2));
        EXPECT_TRUE(last_lsn2 != lsn_t::null);
        EXPECT_TRUE(last_lsn2 != last_lsn);
        W_DO(check_page_lsn(pid, last_lsn2));

        // new transaction inserts a record but aborts
        W_DO(ssm->begin_xct());
        W_DO(x_btree_insert(ssm, _stid_list[0], "key3", "value3"));
        W_DO(ssm->abort_xct());

        // LSN must remain the same as before the TA started
        W_DO(check_last_update(pid, last_lsn2));
        W_DO(check_page_lsn(pid, last_lsn2));

        return RCOK;
    }

    rc_t post_shutdown(ss_m*) {
        return RCOK;
    }
};

#define MY_PLOG_TEST(name, test_class) \
    TEST(PLogTest, name) { \
        sm_options sm_options; \
        sm_options.set_string_option("sm_clogdir", test_env->clog_dir); \
        sm_options.set_string_option("sm_log_impl", plog_xct_t::IMPL_NAME); \
        test_env->empty_logdata_dir(); \
        test_class the_test; \
        restart_test_options options; \
        options.shutdown_mode = normal_shutdown; \
        options.restart_mode = smlevel_0::t_restart_disable; \
        EXPECT_EQ(test_env->runRestartTest(&the_test, \
                    &options, \
                    true, \
                    default_quota_in_pages, \
                    sm_options), 0); \
    }

MY_PLOG_TEST(Scan, test_scan);
MY_PLOG_TEST(SysXctMulti, test_sys_xct_multi);
MY_PLOG_TEST(SysXctSingle, test_sys_xct_single);
MY_PLOG_TEST(SysXctNestedAbort, test_sys_xct_nested_abort);
MY_PLOG_TEST(SysXctNestedAbortImplicit, test_sys_xct_nested_abort_implicit);
MY_PLOG_TEST(ClogCommit, test_clog_commit);
MY_PLOG_TEST(ClogAbort, test_clog_abort);
MY_PLOG_TEST(PageCLSN, test_page_clsn);

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
