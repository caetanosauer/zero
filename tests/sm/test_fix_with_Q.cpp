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

#include <Lintel/AtomicCounter.hpp>

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
    EXPECT_EQ(root_page.is_fixed(),   true);
    EXPECT_EQ(root_page.is_latched(), false);
    EXPECT_EQ(root_page.latch_mode(), LATCH_Q);
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
        root_page.unfix();
        W_DO(op.action(2));  // unfix root page in background
        W_DO(op.stop());
    }

    {   // Q vs. already SH:
        FixRootPageOperator op(root_pid.vol().vol, root_pid.store(), LATCH_SH);
        W_DO(op.action(1));
        btree_page_h root_page;
        W_DO(root_page.fix_root(root_pid.vol().vol, root_pid.store(), LATCH_Q));
        root_page.unfix();
        W_DO(op.action(2));
        W_DO(op.stop());
    }

    {   // Q vs. already EX:
        FixRootPageOperator op(root_pid.vol().vol, root_pid.store(), LATCH_EX);
        W_DO(op.action(1));
        btree_page_h root_page;
        w_rc_t r = root_page.fix_root(root_pid.vol().vol, root_pid.store(), LATCH_Q);
        EXPECT_EQ(r.err_num(), eLATCHQFAIL);
        W_DO(op.action(2));
        W_DO(op.stop());
    }

    return RCOK;
}

TEST (FixWithQTest, RootPageFixingWithInterference) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(test_root_page_fixing_with_interference, true, default_locktable_size, 4096, 1024), 0);
}



int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
