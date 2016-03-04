#include "btree_test_env.h"
#include "generic_page.h"
#include "bf_core.h"
#include "btree.h"
#include "btree_page_h.h"
#include "btree_impl.h"
#include "log_code.h"
#include "w_error.h"

#include <vector>

btree_test_env *test_env;
/**
 * Unit test for bufferpool (bf_m).
 * NOTE the old bufferpool has been eliminated. these testcases will be
 * also removed, or integrated into the new testcases (test_bf_tree.cpp).
 */
/*

class bf_m_test { // friend of bf_m
public:
    static bf_core_m* get_core() {
        return bf_m::_core;
    }
};

w_rc_t bf_get_empty(ss_m* ssm, test_volume_t *test_volume) {
    stid_t stid;
    lpid_t root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    // write out all pages and discard from bufferpool
    W_DO(bf_m::force_all(true));

    generic_page *root_s = NULL;
    smlevel_0::store_flag_t stflags;
    W_DO (bf_m::fix(root_s, root_pid, 0, LATCH_SH, false, stflags, true, smlevel_0::st_unallocated));
    EXPECT_TRUE (root_s != NULL);
    if (root_s == NULL) return RCOK;
    EXPECT_EQ (root_s->nslots, 1); // first slot is header
    EXPECT_EQ (root_pid.page, root_s->pid.page);
    EXPECT_EQ (root_pid.store(), root_s->pid.store());
    EXPECT_EQ (root_pid.vol(), root_s->pid.vol());
    bf_m::unfix(root_s, false, 1);

    return RCOK;
}


TEST (BufferpoolTest, GetEmpty) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(bf_get_empty), 0);
}

// this test case assumes 32 pages as bufferpool size
const size_t BF_GET_MANY_BF_SIZE = 32;
w_rc_t bf_get_many(ss_m* ssm, test_volume_t *test_volume) {
    stid_t stid;
    lpid_t root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(x_btree_verify(ssm, stid));

    // this should create 100s of pages (>BF_GET_MANY_BF_SIZE)
    W_DO(test_env->begin_xct());
    {
        const size_t datasize = SM_PAGESIZE / 5;
        char keystr[4], datastr[datasize + 1];
        keystr[3] = '\0';
        datastr[datasize] = '\0';
        memset (datastr, 'a', datasize);
        for (int i = 0; i < 200; ++i) {
            keystr[0] = datastr[0] = '0' + (i / 100);
            keystr[1] = datastr[1] = '0' + ((i / 10) % 10);
            keystr[2] = datastr[2] = '0' + (i % 10);
            W_DO(x_btree_insert(ssm, stid, keystr, datastr));
            if (i % 20 == 0) {
                // to avoid "cannot find free resource"
                W_DO(test_env->commit_xct());
                W_DO(bf_m::force_all());
                ::usleep (2000);
                W_DO(test_env->begin_xct());
            }
        }
    }
    W_DO(test_env->commit_xct());
    W_DO(x_btree_verify(ssm, stid));

    // write out all pages and discard from bufferpool
    W_DO(bf_m::force_all(true));

    std::vector<shpid_t> pids;
    {
        btree_page_h root_p;
        W_DO (root_p.fix (root_pid, LATCH_SH));
        pids.push_back (root_p.pid0());
        for (int i = 0; i < root_p.nrecs(); ++i) {
            pids.push_back (root_p.child(i));
        }
    }

    // grab as many as possible without unfix
    cout << "reading " << pids.size() << " page from buffer pool" << endl
        << "intentionally making buffer pool full. the following error message is NOT an error!" << endl;
    std::vector<generic_page*> child_pages;
    for (size_t i = 0; i < pids.size(); ++i) {
        lpid_t child_pid = root_pid;
        child_pid.page = pids[i];
        generic_page *child_s = NULL;

        smlevel_0::store_flag_t stflags;
        w_rc_t rc = bf_m::fix(child_s, child_pid, 0, LATCH_SH, false, stflags, true, smlevel_0::st_unallocated);
        if (rc.is_error()) {
            // buffer pool is now full!
            EXPECT_EQ (rc.err_num(), (w_error_codes) fcFULL);
            EXPECT_TRUE (child_s == NULL);
            break;
        } else {
            EXPECT_FALSE (child_s == NULL);
        }
        if (child_s != NULL) {
            child_pages.push_back(child_s);
        }
    }

    cout << "maximum: got " << child_pages.size() << " pages pinned at same time!" << endl;

    // note, it might not be exactly BF_GET_MANY_BF_SIZE as there are
    // some other essential pages (metadata) kept in bufferpool
    EXPECT_LE (child_pages.size(), BF_GET_MANY_BF_SIZE);
    EXPECT_GE (child_pages.size(), BF_GET_MANY_BF_SIZE / 2);// but must be more than half of it (most likely BF_GET_MANY_BF_SIZE-1)
    for (size_t i = 0; i < child_pages.size(); ++i) {
        bf_m::unfix(child_pages[i], false, 1);
    }

    return RCOK;
}

TEST (BufferpoolTest, GetMany) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(bf_get_many, false, default_locktable_size, 512, BF_GET_MANY_BF_SIZE), 0);
}

w_rc_t create_records(ss_m* ssm, const stid_t &stid, size_t records) {
    W_DO(ssm->begin_xct());
    {
        const size_t datasize = SM_PAGESIZE / 5;
        char keystr[4], datastr[datasize + 1];
        keystr[3] = '\0';
        datastr[datasize] = '\0';
        memset (datastr, 'a', datasize);
        for (size_t i = 0; i < records; ++i) {
            keystr[0] = datastr[0] = '0' + (i / 100);
            keystr[1] = datastr[1] = '0' + ((i / 10) % 10);
            keystr[2] = datastr[2] = '0' + (i % 10);
            W_DO(x_btree_insert(ssm, stid, keystr, datastr));
            if (i % 20 == 0) {
                // occasionally commit/begin to make sure bufferpool is not full
                W_DO(ssm->commit_xct());
                W_DO(ssm->begin_xct());
            }
        }
    }
    W_DO(ssm->commit_xct());
    W_DO(x_btree_verify(ssm, stid));
    return RCOK;
}

w_rc_t collect_pids(const lpid_t &root_pid, std::vector<lpid_t> &pids) {
    btree_page_h root_p;
    W_DO (root_p.fix (root_pid, LATCH_SH));
    pids.push_back (root_p.pid());
    for (int i = -1; i < root_p.nrecs(); ++i) {
        lpid_t next_pid = root_p.pid();
        next_pid.page = i == -1 ? root_p.pid0() : root_p.child(i);
        pids.push_back (next_pid);
    }
    return RCOK;
}

w_rc_t bf_careful_write_order(ss_m* ssm, test_volume_t *test_volume) {
    stid_t stid;
    lpid_t root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(x_btree_verify(ssm, stid));

    // create a few pages  (at least 4) by inserting large records
    W_DO(create_records(ssm, stid, 20) );
    std::vector<lpid_t> pids;
    W_DO(collect_pids (root_pid, pids));
    EXPECT_GE (pids.size(), (size_t) 4);

    // write out and evict
    W_DO(bf_m::force_all(true));
    btree_page_h p[4];
    {
        for (size_t i = 0; i < 4; ++i) {
            W_DO (p[i].fix (pids[i], LATCH_EX));
            cout << "p[" << i << "]="<< p[i].pid() << endl;
        }
    }
    lsn_t currlsn = smlevel_0::log->curr_lsn();
    EXPECT_TRUE (currlsn.valid());
    for (size_t i = 0; i < 4; ++i) {
        p[i].set_lsns(currlsn);
        p[i].set_dirty();
    }

    bf_core_m *core = bf_m_test::get_core();

    // 1->2, 1->0, 0->2, 2->3
    // so, if 2 is kept fixed, only 3 can be written out
    W_DO(bf_m::register_write_order_dependency(&(p[1].persistent_part()), &(p[2].persistent_part())));
    W_DO(bf_m::register_write_order_dependency(&(p[1].persistent_part()), &(p[0].persistent_part())));
    W_DO(bf_m::register_write_order_dependency(&(p[0].persistent_part()), &(p[2].persistent_part())));
    W_DO(bf_m::register_write_order_dependency(&(p[2].persistent_part()), &(p[3].persistent_part())));
    p[0].unfix_dirty();
    p[1].unfix_dirty();
    p[3].unfix_dirty();

    cout << "p[2] is still fixed. wait for cleaner..." << endl;
    bf_m::activate_background_flushing(NULL, true); // aggressive=true to kick cleaner in urgent mode
    ::usleep(200*1000);
    W_DO (p[0].fix (pids[0], LATCH_SH));
    W_DO (p[1].fix (pids[1], LATCH_SH));
    W_DO (p[3].fix (pids[3], LATCH_SH));
    EXPECT_TRUE (p[0].is_dirty());
    EXPECT_TRUE (p[1].is_dirty());
    EXPECT_FALSE (p[3].is_dirty());
    p[0].unfix();
    p[1].unfix();
    p[3].unfix();
    EXPECT_TRUE (p[2].is_dirty());
    {//also check bufferpool
        bfcb_t* bp;
        rc_t rc = core->find(bp, pids[2], LATCH_SH, WAIT_IMMEDIATE);
        EXPECT_FALSE (rc.is_error());
        EXPECT_TRUE (bp->dirty());
        core->unpin(bp);
    }

    cout << "do it again to make sure still other pages are dirty..." << endl;
    bf_m::activate_background_flushing(NULL, true);
    ::usleep(200*1000);
    W_DO (p[0].fix (pids[0], LATCH_SH));
    W_DO (p[1].fix (pids[1], LATCH_SH));
    W_DO (p[3].fix (pids[3], LATCH_SH));
    EXPECT_TRUE (p[0].is_dirty());
    EXPECT_TRUE (p[1].is_dirty());
    EXPECT_FALSE (p[3].is_dirty());
    p[0].unfix();
    p[1].unfix();
    p[3].unfix();
    EXPECT_TRUE (p[2].is_dirty());
    {//also check bufferpool
        bfcb_t* bp;
        rc_t rc = core->find(bp, pids[2], LATCH_SH, WAIT_IMMEDIATE);
        EXPECT_FALSE (rc.is_error());
        EXPECT_TRUE (bp->dirty());
        core->unpin(bp);
    }

    cout << "finally, call cleaner with all pages unfixed to check if all pages will be written out..." << endl;
    p[2].unfix_dirty();
    for (int i = 0; i < 3; ++i) {
        bf_m::activate_background_flushing(NULL, true);
        ::usleep(200*1000);
    }
    for (size_t i = 0; i < 4; ++i) {
        W_DO (p[i].fix (pids[i], LATCH_SH));
    }
    EXPECT_FALSE (p[0].is_dirty());
    EXPECT_FALSE (p[1].is_dirty());
    EXPECT_FALSE (p[2].is_dirty());
    EXPECT_FALSE (p[3].is_dirty());
    {//also check bufferpool
        bfcb_t* bp;
        rc_t rc = core->find(bp, pids[2], LATCH_SH, WAIT_IMMEDIATE);
        EXPECT_FALSE (rc.is_error());
        EXPECT_FALSE (bp->dirty());
        core->unpin(bp);
    }

    cout << "done." << endl;
    return RCOK;
}
TEST (BufferpoolTest, CarefulWriteOrder) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(bf_careful_write_order), 0);
}

w_rc_t bf_careful_write_order_cycle(ss_m* ssm, test_volume_t *test_volume) {
    stid_t stid;
    lpid_t root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(x_btree_verify(ssm, stid));

    // create a few pages  (at least 3) by inserting large records
    W_DO(create_records(ssm, stid, 15) );
    std::vector<lpid_t> pids;
    W_DO(collect_pids (root_pid, pids));
    EXPECT_GE (pids.size(), (size_t) 3);

    // write out and evict
    W_DO(bf_m::force_all(true));
    btree_page_h p[3];
    for (size_t i = 0; i < 3; ++i) {
        W_DO (p[i].fix (pids[i], LATCH_EX));
    }
    lsn_t currlsn = smlevel_0::log->curr_lsn();
    EXPECT_TRUE (currlsn.valid());
    for (size_t i = 0; i < 3; ++i) {
        p[i].set_lsns(currlsn);
        p[i].set_dirty();
    }

    // 1->2, 0->1. these are fine.
    W_DO(bf_m::register_write_order_dependency(&(p[1].persistent_part()), &(p[2].persistent_part())));
    W_DO(bf_m::register_write_order_dependency(&(p[0].persistent_part()), &(p[1].persistent_part())));
    // 2->1. this makes a cycle, so it should fail
    rc_t rc = bf_m::register_write_order_dependency(&(p[2].persistent_part()), &(p[1].persistent_part()));
    EXPECT_EQ ((w_error_codes) bf_m::eWRITEORDERLOOP, rc.err_num());
    return RCOK;
}
TEST (BufferpoolTest, CarefulWriteOrderCycle) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(bf_careful_write_order_cycle), 0);
}
*/
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
