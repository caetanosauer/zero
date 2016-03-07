#define SM_SOURCE
#define BTREE_C

#include "sm_base.h"
#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btree_page_h.h"
#include "btree_impl.h"
#include "w_key.h"
#include "xct.h"

#include "smthread.h"

btree_test_env *test_env;

/**
 * Unit test for BTree Merge/Rebalance/De-Adopt.
 */

/** Used to hold a latch on a root page by another thread. */
class page_holding_thread_t : public smthread_t {
public:
    page_holding_thread_t(StoreID stid)
        : smthread_t(t_regular, "page_holding_thread_t"),
          page_held_flag(false),
          release_request_flag(false),
          released_flag(false),
          _stid(stid), _retval(0) {}
    ~page_holding_thread_t()  {_page.unfix();}
    void run();
    int  return_value() const { return _retval; }

    /** true when this thread got latch on the page. ONLY THIS THREAD UPDATES IT. */
    bool volatile page_held_flag;

    /** true when caller thread requests to release the latch. ONLY CALLER THREAD UPDATES IT. */
    bool volatile release_request_flag;

    /** true when this released the latch. ONLY THIS THREAD UPDATES IT. */
    bool volatile released_flag;

    StoreID _stid;
    int _retval;
    btree_page_h _page;
};

void page_holding_thread_t::run()
{
    {
        rc_t rc = _page.fix_root(_stid, LATCH_SH);
        if (rc.is_error()) {
            cerr << "Could not latch page: " << rc << endl;
            _retval = 1;
            page_held_flag = true;
            released_flag = true;
            return;
        }
        page_held_flag = true;
    }

    // wait for signal from the caller
    while (!release_request_flag) {
        ::usleep (5000);
    }

    _page.unfix();
    released_flag = true;
}

// make a initial tree used by following tests
w_rc_t prepare_test(ss_m* ssm, test_volume_t *test_volume, StoreID &stid, PageID &root_pid, bool second_insert = true) {
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
    // now it should be two-level

    // let's cause split
    if (second_insert) {
        // let's keep SH latch on root (parent) to prevent automatic adoption.
        // in these test cases, we want foster relationships to start from
        page_holding_thread_t holder (stid);
        W_DO(holder.fork());
        while (!holder.page_held_flag) {
            ::usleep (5000);
        }

        EXPECT_EQ (holder._page.level(), 2);
        int real_children_before = holder._page.nrecs() + 1;

        W_DO(ssm->begin_xct());
        test_env->set_xct_query_lock();
        for (int i = 1; i < 201; i += 6) {
            keystr[3] = ('0' + ((i / 100) % 10));
            keystr[4] = ('0' + ((i / 10) % 10));
            keystr[5] = ('0' + ((i / 1) % 10));
            key.construct_regularkey(keystr, 6);
            W_DO(ssm->create_assoc(stid, key, data));
        }
        W_DO(ssm->commit_xct());

        EXPECT_EQ (real_children_before, holder._page.nrecs() + 1); // shouldn't have increased it
        W_DO(x_btree_verify(ssm, stid));

        // release it
        holder.release_request_flag = true;
        W_DO(holder.join(1000));
        EXPECT_TRUE (holder.released_flag);
        EXPECT_EQ(0, holder.return_value());
    }
    W_DO(smlevel_0::bf->get_cleaner()->force_volume()); // clean them up
    return RCOK;
}

w_rc_t merge_simple(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO (prepare_test(ssm, test_volume, stid, root_pid));

    W_DO(ssm->begin_xct());
    test_env->set_xct_query_lock();
    btree_page_h root_p;
    W_DO(root_p.fix_root(stid, LATCH_EX));
    EXPECT_TRUE (root_p.is_node());

    PageID pid0 = root_p.pid0();
    PageID foster;
    {
        btree_page_h child_p;
        W_DO(child_p.fix_nonroot(root_p, pid0, LATCH_EX));
        EXPECT_TRUE (child_p.is_leaf());

        foster = child_p.get_foster();
        cout << "the left-most child is " << (child_p.used_space() * 100 / SM_PAGESIZE)
            << "% full. foster=" << child_p.get_foster() << endl;
        EXPECT_NE (child_p.get_foster(), (uint) 0);

        // let's make this page almost empty
        // (directly uses mark_ghost to not trigger automatic merge/rebalance)
        vector<slotid_t> slots;
        for (slotid_t i = 1; i < child_p.nrecs(); ++i) {
            slots.push_back(i);
            child_p.mark_ghost (i);
        }
        W_DO(log_btree_ghost_mark (child_p, slots, false /*is_sys_txn*/));
        cout << "now it is " << (child_p.used_space() * 100 / SM_PAGESIZE)
            << "% full. foster=" << child_p.get_foster() << endl;
    }
    W_DO(ssm->commit_xct());// commit the deletions

    {
        btree_page_h child_p;
        W_DO(child_p.fix_nonroot(root_p, pid0, LATCH_EX));
        W_DO(btree_impl::_sx_defrag_page(child_p));
    }
    W_DO(x_btree_verify(ssm, stid));
    W_DO(ssm->begin_xct());
    test_env->set_xct_query_lock();
    {
        btree_page_h child_p;
        W_DO(child_p.fix_nonroot(root_p, pid0, LATCH_EX));
        EXPECT_TRUE (child_p.is_leaf());

        cout << "now it is " << (child_p.used_space() * 100 / SM_PAGESIZE)
            << "% full. foster=" << child_p.get_foster() << endl;
        EXPECT_LT (child_p.used_space() * 100 / SM_PAGESIZE, (uint) 10);

        // okay, let's fire merging
        W_DO(btree_impl::_sx_merge_foster(child_p));

        cout << "after merging, it is " << (child_p.used_space() * 100 / SM_PAGESIZE)
            << "% full. foster=" << child_p.get_foster() << endl;
        EXPECT_GT (child_p.used_space() * 100 / SM_PAGESIZE, (uint) 50);
        EXPECT_NE (child_p.get_foster(), foster);
    }
    W_DO(ssm->commit_xct());
    W_DO(x_btree_verify(ssm, stid));
    return RCOK;
}

TEST (BtreeMergeTest, MergeSimple) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(merge_simple), 0);
}
TEST (BtreeMergeTest, MergeSimpleLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(merge_simple, true), 0);
}

w_rc_t merge_cycle_fail(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO (prepare_test(ssm, test_volume, stid, root_pid));

    // let's test the write-order cycle
    W_DO(ssm->begin_xct());
    test_env->set_xct_query_lock();

    btree_page_h root_p;
    W_DO(root_p.fix_root(stid, LATCH_EX));
    EXPECT_TRUE (root_p.is_node());

    PageID pid0 = root_p.pid0();
    PageID foster;
    {
        btree_page_h child_p;
        W_DO(child_p.fix_nonroot(root_p, pid0, LATCH_EX));
        EXPECT_TRUE (child_p.is_leaf());
        PageID new_page_id;
        w_keystr_t dummy_key;
        dummy_key.construct_regularkey("key003A", 7); // something in between

        W_DO(btree_impl::_sx_split_foster(child_p, new_page_id, dummy_key));
        // this should have caused a usual split (not no-record-split)
        // which causes a write-order dependency right->left
        EXPECT_TRUE (child_p.is_dirty());

        foster = child_p.get_foster();
        cout << "After split, the left-most child is " << (child_p.used_space() * 100 / SM_PAGESIZE)
            << "% full. foster=" << child_p.get_foster() << endl;
        size_t nrecs_before = child_p.nrecs();
        EXPECT_EQ (foster, new_page_id);

        // let's merge it! this will cause a cycle, so it shouldn't do anything
        W_DO(btree_impl::_sx_merge_foster(child_p));

        size_t nrecs_after = child_p.nrecs();
        EXPECT_EQ (nrecs_after, nrecs_before);
        EXPECT_EQ (foster, child_p.get_foster()); // and the foster-child page shouldn't be deleted
        cout << "now it is " << (child_p.used_space() * 100 / SM_PAGESIZE)
            << "% full. foster=" << child_p.get_foster() << endl;

        // check that it's still there
        btree_page_h new_p;
        W_DO(new_p.fix_nonroot(child_p, new_page_id, LATCH_EX));
        EXPECT_TRUE (child_p.is_dirty());
        EXPECT_TRUE (new_p.is_dirty());
    }
    W_DO(ssm->commit_xct());// commit the deletions

    W_DO(x_btree_verify(ssm, stid));
    return RCOK;
}

TEST (BtreeMergeTest, MergeCycleFail) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(merge_cycle_fail), 0);
}
TEST (BtreeMergeTest, MergeCycleFailLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(merge_cycle_fail, true), 0);
}
w_rc_t rebalance_simple(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO (prepare_test(ssm, test_volume, stid, root_pid));

    W_DO(ssm->begin_xct());
    test_env->set_xct_query_lock();
    btree_page_h root_p;
    W_DO(root_p.fix_root(stid, LATCH_EX));
    EXPECT_TRUE (root_p.is_node());
    {
        PageID child_pid = root_pid;
        child_pid = root_p.pid0();

        btree_page_h child_p;
        W_DO(child_p.fix_nonroot(root_p, child_pid, LATCH_EX));
        EXPECT_TRUE (child_p.is_leaf());
        EXPECT_NE (child_p.get_foster(), (uint) 0);
        int original_recs = child_p.nrecs();
        cout << "originally " << child_p.nrecs() << " in child" << endl;

        cout << "child is " << (child_p.used_space() * 100 / SM_PAGESIZE)
            << "% full. foster=" << child_p.get_foster() << endl;

        // right page should still have enough entries, so this does nothing
        W_DO(btree_impl::_sx_rebalance_foster(child_p));

        EXPECT_EQ (child_p.nrecs(), original_recs);
        cout << "after first try " << child_p.nrecs() << " in child" << endl;

        // so, let's remove almost all entries from right page
        // (directly uses mark_ghost to not trigger automatic merge/rebalance)
        PageID foster_child_pid = child_p.get_foster();
        btree_page_h foster_child_p;
        W_DO(foster_child_p.fix_nonroot(child_p, foster_child_pid, LATCH_EX));

        cout << "foster-child is " << (foster_child_p.used_space() * 100 / SM_PAGESIZE)
            << "% full. foster=" << foster_child_p.get_foster() << endl;

        vector<slotid_t> slots;
        for (slotid_t i = 1; i < foster_child_p.nrecs(); ++i) {
            slots.push_back(i);
            foster_child_p.mark_ghost (i);
        }
        W_DO(log_btree_ghost_mark (foster_child_p, slots, false /*is_sys_txn*/));
        W_DO(ssm->commit_xct());// commit the deletions
        W_DO(ssm->begin_xct());
        test_env->set_xct_query_lock();
        W_DO(btree_impl::_sx_defrag_page(foster_child_p)); // reclaim ghosts

        cout << "after deletion, foster-child is " << (foster_child_p.used_space() * 100 / SM_PAGESIZE)
            << "% full. foster=" << foster_child_p.get_foster() << endl;
        cout << "after deletion " << foster_child_p.nrecs() << " in foster-child" << endl;
        int original_child_recs = foster_child_p.nrecs();
        foster_child_p.unfix();

        // okay, now let's rebalance
        W_DO(btree_impl::_sx_rebalance_foster(child_p));

        cout << "after rebalance " << child_p.nrecs() << " in child" << endl;
        cout << "after rebalance, child is " << (child_p.used_space() * 100 / SM_PAGESIZE)
            << "% full. foster=" << child_p.get_foster() << endl;
        EXPECT_LT (child_p.nrecs(), original_recs);

        W_DO(foster_child_p.fix_nonroot(child_p, foster_child_pid, LATCH_EX));
        cout << "after rebalance " << foster_child_p.nrecs() << " in foster-child" << endl;
        cout << "after rebalance, foster-child is " << (foster_child_p.used_space() * 100 / SM_PAGESIZE)
            << "% full. foster=" << foster_child_p.get_foster() << endl;
        EXPECT_GT (foster_child_p.nrecs(), original_child_recs);

        // is it well balanced?
        EXPECT_LT (child_p.nrecs(), foster_child_p.nrecs() * 2);
        EXPECT_GT (child_p.nrecs() * 2, foster_child_p.nrecs());
    }
    W_DO(ssm->commit_xct());// commit the deletions
    W_DO(x_btree_verify(ssm, stid));
    return RCOK;
}

TEST (BtreeMergeTest, RebalanceSimple) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(rebalance_simple), 0);
}
TEST (BtreeMergeTest, RebalanceSimpleLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(rebalance_simple, true), 0);
}

w_rc_t deadopt_simple(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO (prepare_test(ssm, test_volume, stid, root_pid, false)); // don't do insert after adopt

    // so, the tree doesn't have foster
    W_DO(ssm->begin_xct());
    test_env->set_xct_query_lock();
    {
        btree_page_h root_p;
        W_DO(root_p.fix_root(stid, LATCH_EX));
        EXPECT_TRUE (root_p.is_node());
        int original_nrecs = root_p.nrecs();
        cout << "originally " << original_nrecs  << " pages under root" << endl;
        {
            EXPECT_GE (root_p.nrecs(), 3);
            PageID child_pid = root_p.child(0);
            PageID right_pid = root_p.child(1);
            btree_page_h child_p;
            W_DO(child_p.fix_nonroot(root_p, child_pid, LATCH_EX));
            EXPECT_TRUE (child_p.is_leaf());
            EXPECT_EQ (child_p.get_foster(), (uint) 0);
            child_p.unfix();

            // make him deadopt right sibling
            W_DO(btree_impl::_sx_deadopt_foster(root_p, 0));

            W_DO(child_p.fix_nonroot(root_p, child_pid, LATCH_EX));
            EXPECT_EQ (child_p.get_foster(), right_pid);
        }
        {
            // then pid0 de-adopts him
            PageID child_pid = root_p.pid0();
            PageID right_pid = root_p.child(0);
            btree_page_h child_p;
            W_DO(child_p.fix_nonroot(root_p, child_pid, LATCH_EX));
            EXPECT_TRUE (child_p.is_leaf());
            EXPECT_EQ (child_p.get_foster(), (uint) 0);

            child_p.unfix();

            // make him de-adopt right sibling
            W_DO(btree_impl::_sx_deadopt_foster(root_p, -1));

            W_DO(child_p.fix_nonroot(root_p, child_pid, LATCH_EX));
            EXPECT_EQ (child_p.get_foster(), right_pid);
        }
        EXPECT_EQ (root_p.nrecs(), original_nrecs - 2);
        cout << "now " << root_p.nrecs() << " pages under root" << endl;
    }
    W_DO(ssm->commit_xct());// commit the deletions
    W_DO(x_btree_verify(ssm, stid));
    return RCOK;
}

TEST (BtreeMergeTest, DeAdoptSimple) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(deadopt_simple), 0);
}
TEST (BtreeMergeTest, DeAdoptSimpleLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(deadopt_simple, true), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
