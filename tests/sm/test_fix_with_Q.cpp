/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */


/**
 * Tests for fixing pages in Q mode
 */


#include "btree_test_env.h"
#include "gtest/gtest.h"

#include "btree_page_h.h"
#include "smthread.h"

#include <AtomicCounter.hpp>

btree_test_env *test_env;


/***************************************************************************/
/*                                                                         */
/* Helper functions                                                        */
/*                                                                         */
/***************************************************************************/

/**
 * Abstract class for performing operations in the background using another thread.
 * Subclass this to instantiate internal_act with the actions you want to use.
 * 
 * Sample usage:
 * 
 *   FooBackgroundOperator op(...);
 *   W_DO(op.action(1));   // synchronously runs first action of FooBackgroundOperator 
 *                         // in background thread, returning its error
 *   W_DO(op.action(2));   // ditto for second action
 *   W_DO(op.stop());
 */
class BackgroundOperator : public smthread_t {
protected:
    virtual w_rc_t internal_act(int argument) = 0;

public:
    BackgroundOperator() : 
        smthread_t(t_regular, "PageHoldingThread"), _started(false), _command(0) {}
    virtual ~BackgroundOperator() {}

    w_rc_t action(int argument) {
        w_assert1(argument > 0);
        W_DO(start());
        _command = argument;
        while (_command != 0) {}
        if (_result == w_error_ok) {
            return RCOK;
        } else {
            return RC((w_error_codes)_result.load());
        }
    }

    w_rc_t stop() {
        W_DO(start());
        _command = -1;
        while (_command != 0) {}
        W_DO(join(1000));
        _started = false;
        return RCOK;
    }

private:
    bool                _started; // accessed only from foreground thread
    lintel::Atomic<int> _command;
    lintel::Atomic<int> _result;  // really w_error_codes

    w_rc_t start() {
        if (!_started) {
            _started = true;
            W_DO(fork());
        }
        return RCOK;
    }

    void run() {
        for (;;) {
            if (_command > 0) {
                w_rc_t result;
                result   = internal_act(_command);
                _result  = result.err_num();
                _command = 0;
            } else if (_command == -1) {
                _command = 0;
                return;
            }
        }
    }
};


class FixRootPageOperator : public BackgroundOperator {
    volid_t      _vol;
    snum_t       _store;
    latch_mode_t _mode;
    btree_page_h _page;
    
public:
    FixRootPageOperator(volid_t vol, snum_t store, latch_mode_t mode) :
        _vol(vol), _store(store), _mode(mode) {}

    virtual w_rc_t internal_act(int argument) {
        switch (argument) {
        case 1:
            return _page.fix_root(_vol, _store, _mode);
        case 2:
            _page.unfix();
        }
        return RCOK;
    }
};


/***************************************************************************/
/*                                                                         */
/* Actual tests                                                            */
/*                                                                         */
/***************************************************************************/

w_rc_t test_root_page_fixing(ss_m* ssm, test_volume_t *test_volume) {
    // create a root page:
    stid_t stid;
    lpid_t root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    // Can we fix it in Q mode?  Do inspectors behave properly?
    btree_page_h root_page;
    W_DO(root_page.fix_root(root_pid.vol().vol, root_pid.store(), LATCH_Q));
    EXPECT_EQ(root_page.is_fixed(),                  true);
    EXPECT_EQ(root_page.is_latched(),                false);
    EXPECT_EQ(root_page.latch_mode(),                LATCH_Q);
    EXPECT_EQ(root_page.change_possible_after_fix(), false);
    root_page.unfix();
    EXPECT_EQ(root_page.is_fixed(), false);

    return RCOK;
}

TEST (FixWithQTest, RootPageFixing) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(test_root_page_fixing, true, default_locktable_size, 4096, 1024), 0);
}


w_rc_t test_root_page_fixing_with_interference(ss_m* ssm, test_volume_t *test_volume) {
    // create a root page:
    stid_t stid;
    lpid_t root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    {   // Q vs. already Q:
        FixRootPageOperator op(root_pid.vol().vol, root_pid.store(), LATCH_Q);
        W_DO(op.action(1));   // fix root page in background with Q mode
        btree_page_h root_page;
        W_DO(root_page.fix_root(root_pid.vol().vol, root_pid.store(), LATCH_Q));
        EXPECT_EQ(root_page.change_possible_after_fix(), false);
        root_page.unfix();
        W_DO(op.action(2));  // unfix root page in background
        W_DO(op.stop());
    }

    {   // Q vs. already SH:
        FixRootPageOperator op(root_pid.vol().vol, root_pid.store(), LATCH_SH);
        W_DO(op.action(1));
        btree_page_h root_page;
        W_DO(root_page.fix_root(root_pid.vol().vol, root_pid.store(), LATCH_Q));
        EXPECT_EQ(root_page.change_possible_after_fix(), false);
        root_page.unfix();
        W_DO(op.action(2));
        W_DO(op.stop());
    }

    {   // Q vs. already EX:
        FixRootPageOperator op(root_pid.vol().vol, root_pid.store(), LATCH_EX);
        W_DO(op.action(1));
        btree_page_h root_page;
        w_rc_t r = root_page.fix_root(root_pid.vol().vol, root_pid.store(), LATCH_Q);
        EXPECT_EQ(r.err_num(),          eLATCHQFAIL);
        EXPECT_EQ(root_page.is_fixed(), false);
        W_DO(op.action(2));
        W_DO(op.stop());
    }

    return RCOK;
}

TEST (FixWithQTest, RootPageFixingWithInterference) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(test_root_page_fixing_with_interference, true, default_locktable_size, 4096, 1024), 0);
}


w_rc_t test_root_page_change_possible(ss_m* ssm, test_volume_t *test_volume) {
    // create a root page:
    stid_t stid;
    lpid_t root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    /*
     * Already tested vs. nothing, vs. already {Q, SH, EX} in previous tests...
     * 
     * Here we test versus {Q, SH, EX} held during or from middle onwards.
     */

    {   // vs. Q:
        FixRootPageOperator op(root_pid.vol().vol, root_pid.store(), LATCH_Q);
        btree_page_h root_page;
        W_DO(root_page.fix_root(root_pid.vol().vol, root_pid.store(), LATCH_Q));
        EXPECT_EQ(root_page.change_possible_after_fix(), false);
        W_DO(op.action(1));  // fix   root page in background with Q mode
        EXPECT_EQ(root_page.change_possible_after_fix(), false);
        W_DO(op.action(2));  // unfix root page in background
        EXPECT_EQ(root_page.change_possible_after_fix(), false);
        W_DO(op.stop());
        root_page.unfix();
    }

    {   // vs. SH:
        FixRootPageOperator op(root_pid.vol().vol, root_pid.store(), LATCH_SH);
        btree_page_h root_page;
        W_DO(root_page.fix_root(root_pid.vol().vol, root_pid.store(), LATCH_Q));
        EXPECT_EQ(root_page.change_possible_after_fix(), false);
        W_DO(op.action(1));
        EXPECT_EQ(root_page.change_possible_after_fix(), false);
        W_DO(op.action(2));
        EXPECT_EQ(root_page.change_possible_after_fix(), false);
        W_DO(op.stop());
        root_page.unfix();
    }

    {   // vs. EX:
        FixRootPageOperator op(root_pid.vol().vol, root_pid.store(), LATCH_EX);
        btree_page_h root_page;
        W_DO(root_page.fix_root(root_pid.vol().vol, root_pid.store(), LATCH_Q));
        EXPECT_EQ(root_page.change_possible_after_fix(), false);
        W_DO(op.action(1));
        EXPECT_EQ(root_page.change_possible_after_fix(), true);
        W_DO(op.action(2));
        EXPECT_EQ(root_page.change_possible_after_fix(), true);
        W_DO(op.stop());
        root_page.unfix();
    }

    return RCOK;
}

TEST (FixWithQTest, RootPageChangePossible) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(test_root_page_change_possible, true, default_locktable_size, 4096, 1024), 0);
}



w_rc_t test_non_root_page_fixing(ss_m* ssm, test_volume_t *test_volume) {
    /*
     * Create a B-tree with at least two levels:
     */

    stid_t stid;
    lpid_t root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    const int recsize = SM_PAGESIZE / 20;
    char datastr[recsize];
    memset (datastr, 'a', recsize);
    vec_t data;
    data.set(datastr, recsize);

    // insert key000, key002 ... key198 (will be at least 5 pages)
    W_DO(ssm->begin_xct());
    test_env->set_xct_query_lock();
    w_keystr_t key;
    char keystr[6] = "";
    memset(keystr, '\0', 6);
    keystr[0] = 'k';
    keystr[1] = 'e';
    keystr[2] = 'y';
    for (int i = 0; i < 200; i += 2) {
        keystr[3] = ('0' + ((i / 100) % 10));
        keystr[4] = ('0' + ((i / 10) % 10));
        keystr[5] = ('0' + ((i / 1) % 10));
        key.construct_regularkey(keystr, 6);
        W_DO(ssm->create_assoc(stid, key, data));
    }
    W_DO(ssm->commit_xct());
    W_DO(x_btree_verify(ssm, stid));


    /*
     * Get the shpid of a child of the root page:
     */
    btree_page_h root_page;
    W_DO(root_page.fix_root(root_pid.vol().vol, root_pid.store(), LATCH_SH));
    shpid_t child_pid = *root_page.child_slot_address(0);


    // Can we fix it in Q mode?  Do inspectors behave properly?
    btree_page_h child_page;
    w_rc_t r = child_page.fix_nonroot(root_page, root_pid.vol().vol, child_pid, 
                                      LATCH_Q, false, false);
    if (!is_swizzled_pointer(child_pid)) {
        EXPECT_EQ(r.err_num(), eLATCHQFAIL);
        return RCOK;
    }
    EXPECT_EQ(child_page.is_fixed(),                  true);
    EXPECT_EQ(child_page.is_latched(),                false);
    EXPECT_EQ(child_page.latch_mode(),                LATCH_Q);
    EXPECT_EQ(child_page.change_possible_after_fix(), false);
    child_page.unfix();
    EXPECT_EQ(child_page.is_fixed(),                  false);


    // test Q->Q crabbing:
    root_page.unfix();
    W_DO(root_page.fix_root(root_pid.vol().vol, root_pid.store(), LATCH_Q));
    W_DO(child_page.fix_nonroot(root_page, root_pid.vol().vol, child_pid, 
                                LATCH_Q, false, false));
    FixRootPageOperator op(root_pid.vol().vol, root_pid.store(), LATCH_EX);
    W_DO(op.action(1));
    W_DO(op.action(2));
    EXPECT_EQ(root_page.change_possible_after_fix(), true);
    W_DO(op.stop());
    EXPECT_EQ(child_page.change_possible_after_fix(), false);
    child_page.unfix();
    r = child_page.fix_nonroot(root_page, root_pid.vol().vol, child_pid, 
                               LATCH_Q, false, false);
    EXPECT_EQ(r.err_num(), ePARENTLATCHQFAIL);

    // test Q->{SH,EX} crabbing:
    r = child_page.fix_nonroot(root_page, root_pid.vol().vol, child_pid, 
                               LATCH_SH, false, false);
    EXPECT_EQ(r.err_num(), ePARENTLATCHQFAIL);
    r = child_page.fix_nonroot(root_page, root_pid.vol().vol, child_pid, 
                               LATCH_EX, false, false);
    EXPECT_EQ(r.err_num(), ePARENTLATCHQFAIL);

    root_page.unfix();
    W_DO(root_page.fix_root(root_pid.vol().vol, root_pid.store(), LATCH_Q));
    W_DO(child_page.fix_nonroot(root_page, root_pid.vol().vol, child_pid, 
                                LATCH_SH, false, false));
    child_page.unfix();
    W_DO(child_page.fix_nonroot(root_page, root_pid.vol().vol, child_pid,
                                LATCH_EX, false, false));
    child_page.unfix();

    // test eNEEDREALLATCH error cases:
    r = child_page.fix_nonroot(root_page, root_pid.vol().vol, child_pid, 
                               LATCH_Q, false, true); // ask for virgin page
    EXPECT_EQ(r.err_num(), eNEEDREALLATCH);
    r = child_page.fix_nonroot(root_page, root_pid.vol().vol, 
                               // unswizzled pointer:
                               smlevel_0::bf->normalize_shpid(child_pid), 
                               LATCH_EX, false, false);
    EXPECT_EQ(r.err_num(), eNEEDREALLATCH);
    root_page.unfix();
    
    return RCOK;
}

TEST (FixWithQTest, NonRootPageFixing) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(test_non_root_page_fixing, true, default_locktable_size, 4096, 1024), 0);
}



int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
