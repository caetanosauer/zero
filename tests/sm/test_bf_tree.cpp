#include "btree_test_env.h"
#include "generic_page.h"
#include "bf.h"
#include "btree.h"
#include "btree_page_h.h"
#include "btree_impl.h"
#include "log.h"
#include "w_error.h"

#include "bf_fixed.h"
#include "bf_tree_cb.h"
#include "bf_tree.h"
#include "sm_io.h"
#include "sm_int_0.h"

#include <vector>
#include <set>
#include <boost/concept_check.hpp>

btree_test_env *test_env;
/**
 * Unit test for new bufferpool for B-tree pages (bf_tree_m).
 * This class (test_bf_tree) is a friend of bf_tree_m so that
 * we can test private methods in these test cases.
 */
class test_bf_tree {
public:
    static bf_idx get_bf_idx (bf_tree_m *bf, generic_page* page) {
        return (page - bf->_buffer);
    }
    static bf_tree_cb_t* get_bf_control_block (bf_tree_m *bf, generic_page* page) {
        bf_idx idx = get_bf_idx(bf, page);
        return bf->get_cbp(idx);
    }


    /** manually emulate the btree page layout */
    static void _add_child_pointer (btree_page *page, shpid_t child) {
        btree_page_h p;
        p.fix_nonbufferpool_page(reinterpret_cast<generic_page*>(page)); // <<<>>>
        if (!page->insert_item(p.nrecs()+1, false, 0, child, 0)) {
            w_assert1(false);
        }
        if (p.nrecs() == 0) {
            *p.page_pointer_address(-1) = child;
        }
    }
};


enum test_size_t {
    SMALL, NORMAL, LARGE
};

void run_bf_test(w_rc_t (*func)(ss_m*, test_volume_t*),
    test_size_t size, bool initially_enable_cleaners, bool enable_swizzling) {
    // (some of) tests in this file needs REALLY big log.
    test_env->empty_logdata_dir();
    sm_options options;
    options.set_int_option("sm_logbufsize", 512 << 10);
    options.set_int_option("sm_logsize", 8192 << 10);
    options.set_int_option("sm_locktablesize", default_locktable_size);
    options.set_int_option("sm_bufpoolsize", SM_PAGESIZE / 1024 *
        (size == LARGE ? 10000 : (size == NORMAL ? 256 : 50)));
    options.set_int_option("sm_num_page_writers", 1);
    options.set_int_option("sm_cleaner_interval_millisec_min",
        (size == LARGE ? 10000 : (size == NORMAL ? 1000 : 20)));
    options.set_int_option("sm_cleaner_interval_millisec_max", 10000);
    options.set_int_option("sm_cleaner_write_buffer_pages", 64);
    options.set_bool_option("sm_backgroundflush", initially_enable_cleaners);
    options.set_bool_option("sm_bufferpool_swizzle", enable_swizzling);

    options.set_int_option("sm_rawlock_lockpool_initseg",
        (size == LARGE ? 100 : (size == NORMAL ? 50 : 20)));
    options.set_int_option("sm_rawlock_lockpool_segsize",
        (size == LARGE ? 1 << 14 : (size == NORMAL ? 1 << 12 : 1 << 10)));
    options.set_int_option("sm_rawlock_gc_generation_count",
        (size == LARGE ? 30 : (size == NORMAL ? 20 : 10)));
    options.set_int_option("sm_rawlock_gc_free_segment_count",
        (size == LARGE ? 50 : (size == NORMAL ? 20 : 10)));
    options.set_int_option("sm_rawlock_gc_max_segment_count",
        (size == LARGE ? 200 : (size == NORMAL ? 100 : 50)));

    int quota = (size == LARGE ? 4096 : (size == NORMAL ? 1024 : 256));
    EXPECT_EQ(test_env->runBtreeTest(func, false, quota, options), 0);
}

TEST (TreeBufferpoolTest, AlignmentCheck) {
    cout << "sizeof(bf_tree_cb_t)=" << sizeof(bf_tree_cb_t) << endl;
    EXPECT_EQ ((uint)0, sizeof(bf_tree_cb_t) % 8);
}

w_rc_t test_bf_init(ss_m* /*ssm*/, test_volume_t */*test_volume*/) {
    g_me()->sleep(200);
    bf_tree_m &pool(*smlevel_0::bf);
    pool.debug_dump(std::cout);
    return RCOK;
}
TEST (TreeBufferpoolTest, Init) {
    run_bf_test(test_bf_init, SMALL, true, true);
}
w_rc_t test_bf_fix_virgin_root(ss_m* /*ssm*/, test_volume_t *test_volume) {
    lsn_t thelsn = smlevel_0::log->curr_lsn();
    bf_tree_m &pool(*smlevel_0::bf);
    for (size_t i = 1; i < 4; ++i) {
        generic_page *page = NULL;
        lpid_t pid (test_volume->_vid, i, i + 10);
        W_DO(pool.fix_virgin_root(page, test_volume->_vid, i, i + 10));
        EXPECT_TRUE (page != NULL);
        if (page != NULL) {
            ::memset(page, 0, sizeof(generic_page));
            btree_page *bp = reinterpret_cast<btree_page*>(page);
            page->pid      = pid;
            bp->lsn        = thelsn;
            page->tag      = t_btree_p;
            bp->btree_level = 1;
            bp->init_items();                
            pool.set_dirty(page);
            pool.unfix(page);
        }

        // fix again
        page = NULL;
        W_DO(pool.fix_root(page, test_volume->_vid, i, LATCH_SH, false));
        EXPECT_TRUE (page != NULL);
        if (page != NULL) {
            btree_page *bp = reinterpret_cast<btree_page*>(page);
            EXPECT_EQ(i + 10, page->pid.page);
            EXPECT_EQ(i, page->pid.store());
            EXPECT_EQ(test_volume->_vid.vol, page->pid.vol().vol);
            EXPECT_EQ(thelsn, bp->lsn);
            EXPECT_EQ(1, bp->btree_level);
            pool.unfix(page);
        }
    }
    pool.debug_dump(std::cout);
    return RCOK;
}
TEST (TreeBufferpoolTest, FixVirginRoot) {
    run_bf_test(test_bf_fix_virgin_root, SMALL, true, true);
}

w_rc_t test_bf_fix_virgin_child(ss_m* /*ssm*/, test_volume_t *test_volume) {
    bf_tree_m &pool(*smlevel_0::bf);
    lsn_t thelsn = smlevel_0::log->curr_lsn();
    lpid_t root_pid(test_volume->_vid, 1, 11);

    generic_page *root_page = NULL;
    W_DO(pool.fix_virgin_root(root_page, test_volume->_vid, 1, 11));
    EXPECT_TRUE (root_page != NULL);
    ::memset(root_page, 0, sizeof(generic_page));
    btree_page *rbp = reinterpret_cast<btree_page*>(root_page);
    root_page->pid    = root_pid;
    rbp->lsn          = thelsn;
    root_page->tag    = t_btree_p;
    rbp->btree_level  = 2;
    rbp->btree_foster = 0;
    rbp->init_items();
    for (size_t i = 0; i < 3; ++i) {
        generic_page *page = NULL;
        lpid_t pid (test_volume->_vid, 1, root_pid.page + 1 + i);
        test_bf_tree::_add_child_pointer (rbp, pid.page);

        W_DO(pool.fix_nonroot(page, root_page, pid.vol().vol, pid.page, LATCH_EX, false, true));
        EXPECT_TRUE (page != NULL);
        if (page != NULL) {
            ::memset(page, 0, sizeof(generic_page));
            btree_page *bp = reinterpret_cast<btree_page*>(page);
            page->pid = pid;
            bp->lsn = thelsn;
            page->tag = t_btree_p;
            bp->btree_level = 1;
            bp->init_items();
            pool.set_dirty(page);
            pool.unfix(page);
        }

        // fix again
        page = NULL;
        W_DO(pool.fix_nonroot(page, root_page, pid.vol().vol, pid.page, LATCH_SH, false, false));
        EXPECT_TRUE (page != NULL);
        if (page != NULL) {
            btree_page *bp = reinterpret_cast<btree_page*>(page);
            EXPECT_EQ(pid.page, page->pid.page);
            EXPECT_EQ(pid.store(), page->pid.store());
            EXPECT_EQ(test_volume->_vid.vol, page->pid.vol().vol);
            EXPECT_EQ(thelsn, bp->lsn);
            EXPECT_EQ(t_btree_p, page->tag);
            EXPECT_EQ(1, bp->btree_level);
            pool.unfix(page);
        }
    }

    pool.set_dirty(root_page);
    pool.unfix(root_page);
    pool.debug_dump(std::cout);
    return RCOK;
}
TEST (TreeBufferpoolTest, FixVirginChild) {
    run_bf_test(test_bf_fix_virgin_child, SMALL, true, true);
}

// make big enough database for tests
w_rc_t prepare_test(ss_m* ssm, test_volume_t *test_volume, stid_t &stid, lpid_t &root_pid) {
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    const int recsize = SM_PAGESIZE / 6;
    char datastr[recsize];
    ::memset (datastr, 'a', recsize);
    vec_t data;
    data.set(datastr, recsize);

    // create at least 30 pages.
    // commit each time so that pages can be evicted
    w_keystr_t key;
    char keystr[6] = "";
    ::memset(keystr, '\0', 6);
    keystr[0] = 'k';
    keystr[1] = 'e';
    keystr[2] = 'y';
    for (int i = 0; i < 180; ++i) {
        keystr[3] = ('0' + ((i / 100) % 10));
        keystr[4] = ('0' + ((i / 10) % 10));
        keystr[5] = ('0' + ((i / 1) % 10));
        key.construct_regularkey(keystr, 6);
        W_DO(ssm->begin_xct());
        test_env->set_xct_query_lock();
        W_DO(ssm->create_assoc(stid, key, data));
        W_DO(ssm->commit_xct());
    }
    W_DO(x_btree_verify(ssm, stid));
    W_DO(ssm->force_buffers()); // clean them up
    return RCOK;
}

w_rc_t test_bf_evict(ss_m* ssm, test_volume_t *test_volume) {
    stid_t stid;
    lpid_t root_pid;
    W_DO (prepare_test(ssm, test_volume, stid, root_pid));

    bf_tree_m &pool(*smlevel_0::bf);
    lsn_t thelsn = smlevel_0::log->curr_lsn();

    btree_page_h root_p;
    W_DO(root_p.fix_root(root_pid.vol().vol, root_pid.store(), LATCH_SH));
    EXPECT_TRUE (root_p.is_node());
    EXPECT_TRUE (root_p.nrecs() > 30);

    const size_t keep_latch_i = 23; // this page will be kept latched
    bf_idx keep_latch_idx = 0;
    generic_page* keep_latch_page = NULL;
    std::set<bf_idx> dirty_idx;
    for (size_t i = 0; i < 30; ++i) {
        generic_page *page = NULL;
        lpid_t pid (root_pid.vol().vol, root_pid.store(), root_p.child(i));

        W_DO(pool.fix_nonroot(page, root_p.get_generic_page(), pid.vol().vol, pid.page, i % 5 == 0 ? LATCH_EX : LATCH_SH, false, false));
        EXPECT_TRUE (page != NULL);
        if (page != NULL) {
            bf_idx idx = test_bf_tree::get_bf_idx(&pool, page);
            EXPECT_TRUE (dirty_idx.find (idx) == dirty_idx.end());
            btree_page *bp = reinterpret_cast<btree_page*>(page);
            bp->lsn         = thelsn;
            if (i % 5 == 0) {
                dirty_idx.insert (idx);
                DBGOUT2(<<"dirty_idx i=" << i << " idx=" << idx);
            }
            if (i == keep_latch_i) {
                keep_latch_idx = idx;
                keep_latch_page = page;
            } else {
                EXPECT_NE (keep_latch_idx, idx); // although not dirty, no one will evict the latched page
                if (i % 5 == 0) {
                    pool.set_dirty(page);
                }
                pool.unfix(page);
            }
        }
    }

    // fix again
    for (size_t i = 0; i < 30; ++i) {
        if (i == keep_latch_i) {
            bf_tree_cb_t &cb (*test_bf_tree::get_bf_control_block(&pool, keep_latch_page));
            EXPECT_FALSE(cb._dirty);
        } else {
            generic_page *page = NULL;
            lpid_t pid (root_pid.vol().vol, root_pid.store(), root_p.child(i));
            W_DO(pool.fix_nonroot(page, root_p.get_generic_page(), pid.vol().vol, pid.page, LATCH_SH, false, false));
            EXPECT_TRUE (page != NULL);
            if (page != NULL) {
                btree_page *bp = reinterpret_cast<btree_page*>(page);
                bf_idx idx = test_bf_tree::get_bf_idx(&pool, page);
                EXPECT_NE (keep_latch_idx, idx);
                bf_tree_cb_t &cb (*test_bf_tree::get_bf_control_block(&pool, page));
                EXPECT_EQ(pid.page, page->pid.page) << "i" << i << ".idx" << idx;
                EXPECT_EQ(pid.store(), page->pid.store());
                EXPECT_EQ(test_volume->_vid.vol, page->pid.vol().vol);
                EXPECT_EQ(1, bp->btree_level);
                if (i % 5 == 0) {
                    EXPECT_EQ(thelsn, bp->lsn);
                    EXPECT_TRUE (dirty_idx.find (idx) != dirty_idx.end()) << "i" << i << ".idx" << idx;
                    EXPECT_TRUE(cb._dirty) << "i" << i << ".idx" << idx;
                } else {
                    EXPECT_TRUE (dirty_idx.find (idx) == dirty_idx.end()) << "i" << i << ".idx" << idx;
                    EXPECT_FALSE(cb._dirty) << "i" << i << ".idx" << idx;
                }
                pool.unfix(page);
            }
        }
    }
    pool.debug_dump(std::cout);
    pool.unfix(keep_latch_page);
    root_p.unfix();

    return RCOK;
}
TEST (TreeBufferpoolTest, EvictNoSwizzle) {
    run_bf_test(test_bf_evict, NORMAL, false, false);
}
TEST (TreeBufferpoolTest, EvictSwizzle) {
    run_bf_test(test_bf_evict, NORMAL, false, true);
}

w_rc_t _test_bf_swizzle(ss_m* /*ssm*/, test_volume_t *test_volume, bool enable_swizzle) {
    bf_tree_m &pool(*smlevel_0::bf);
    lpid_t root_pid(test_volume->_vid, 1, 3);

    generic_page *root_page = NULL;
    W_DO(pool.fix_virgin_root(root_page, test_volume->_vid, root_pid.store(), root_pid.page));
    EXPECT_TRUE (root_page != NULL);
    ::memset(root_page, 0, sizeof(generic_page));
    btree_page *rbp = reinterpret_cast<btree_page*>(root_page);
    root_page->pid = root_pid;
    root_page->tag = t_btree_p;
    rbp->btree_level = 2;
    rbp->btree_foster = 0;
    rbp->init_items();
    bf_tree_cb_t &root_cb (*test_bf_tree::get_bf_control_block(&pool, root_page));
    EXPECT_EQ(1, root_cb._pin_cnt); // root page is always swizzled by volume descriptor, so pin_cnt is 1.
    if (enable_swizzle) {
        EXPECT_TRUE (pool.is_swizzled(root_page));
    }

    pool.debug_dump_page_pointers(std::cout, root_page);
    for (size_t i = 0; i < 20; ++i) {
        generic_page *page = NULL;
        lpid_t pid (test_volume->_vid, 1, root_pid.page + 1 + i);
        test_bf_tree::_add_child_pointer (rbp, pid.page);

        if (enable_swizzle) {
#ifdef BP_MAINTAIN_PARENT_PTR
            EXPECT_EQ ((int) (1 + i), root_cb._pin_cnt);
#else // BP_MAINTAIN_PARENT_PTR
            EXPECT_EQ ((int) (1), root_cb._pin_cnt);
#endif // BP_MAINTAIN_PARENT_PTR
        } else {
            EXPECT_EQ ((int) (1), root_cb._pin_cnt);
        }
        W_DO(pool.fix_nonroot(page, root_page, pid.vol().vol, pid.page, LATCH_EX, false, true));
        EXPECT_TRUE (page != NULL);
        if (page != NULL) {
            bf_tree_cb_t &cb (*test_bf_tree::get_bf_control_block(&pool, page));
            if (enable_swizzle) {
                EXPECT_EQ (1, cb._pin_cnt); // because it's swizzled, pin_cnt is 1
                EXPECT_TRUE (pool.is_swizzled(page));
#ifdef BP_MAINTAIN_PARENT_PTR
                EXPECT_EQ ((int) (2 + i), root_cb._pin_cnt); // parent's pin_cnt is added 
#else // BP_MAINTAIN_PARENT_PTR
                EXPECT_EQ ((int) 1, root_cb._pin_cnt);
#endif // BP_MAINTAIN_PARENT_PTR
            } else {
                EXPECT_EQ (0, cb._pin_cnt); // otherwise, it's 0 after fix()
                EXPECT_EQ ((int) (1), root_cb._pin_cnt);
#ifdef BP_MAINTAIN_PARENT_PTR
                EXPECT_EQ ((uint) 0, cb._parent);
#endif // BP_MAINTAIN_PARENT_PTR
            }
            ::memset(page, 0, sizeof(generic_page));
            btree_page *bp = reinterpret_cast<btree_page*>(page);
            page->pid = pid;
            page->tag = t_btree_p;
            bp->btree_level = 1;
            bp->init_items();
            pool.set_dirty(page);
            pool.unfix(page);
            //  same after unfix too.
            if (enable_swizzle) {
                EXPECT_EQ (1, cb._pin_cnt);
                EXPECT_TRUE (pool.is_swizzled(page));
            } else {
                EXPECT_EQ (0, cb._pin_cnt);
            }
        }
    }
    pool.debug_dump_page_pointers(std::cout, root_page);
    if (enable_swizzle) {
#ifdef BP_MAINTAIN_PARENT_PTR
        EXPECT_EQ (1 + 20, root_cb._pin_cnt);
#else // BP_MAINTAIN_PARENT_PTR
        EXPECT_EQ (1, root_cb._pin_cnt);
#endif // BP_MAINTAIN_PARENT_PTR
    } else {
        EXPECT_EQ (1, root_cb._pin_cnt);
    }
    // fix again
    for (size_t i = 0; i < 20; ++i) {
        generic_page *page = NULL;
        lpid_t pid (test_volume->_vid, 1, root_pid.page + 1 + i);
        W_DO(pool.fix_nonroot(page, root_page, pid.vol().vol, pid.page, LATCH_SH, false, false));
        EXPECT_TRUE (page != NULL);
        if (page != NULL) {
            btree_page *bp = reinterpret_cast<btree_page*>(page);
            bf_tree_cb_t &cb (*test_bf_tree::get_bf_control_block(&pool, page));
            EXPECT_EQ(pid.page, page->pid.page);
            EXPECT_EQ(pid.store(), page->pid.store());
            EXPECT_EQ(test_volume->_vid.vol, page->pid.vol().vol);
            EXPECT_EQ(1, bp->btree_level);
            EXPECT_TRUE(cb._dirty);
            if (enable_swizzle) {
                EXPECT_EQ (1, cb._pin_cnt);
                EXPECT_TRUE (pool.is_swizzled(page));
            } else {
                EXPECT_EQ (0, cb._pin_cnt);
            }
            pool.unfix(page);
            if (enable_swizzle) {
                EXPECT_EQ (1, cb._pin_cnt);
                EXPECT_TRUE (pool.is_swizzled(page));
            } else {
                EXPECT_EQ (0, cb._pin_cnt);
            }
        }
    }
    if (enable_swizzle) {
#ifdef BP_MAINTAIN_PARENT_PTR
        EXPECT_EQ (1 + 20, root_cb._pin_cnt);
#else // BP_MAINTAIN_PARENT_PTR
        EXPECT_EQ (1, root_cb._pin_cnt);
#endif // BP_MAINTAIN_PARENT_PTR
    } else {
        EXPECT_EQ (1, root_cb._pin_cnt);
    }
    pool.set_dirty(root_page);
    pool.unfix(root_page);
    if (enable_swizzle) {
#ifdef BP_MAINTAIN_PARENT_PTR
        EXPECT_EQ (1 + 20, root_cb._pin_cnt);
#else // BP_MAINTAIN_PARENT_PTR
        EXPECT_EQ (1, root_cb._pin_cnt);
#endif // BP_MAINTAIN_PARENT_PTR
    } else {
        EXPECT_EQ (1, root_cb._pin_cnt);
    }

    pool.debug_dump_page_pointers(std::cout, root_page);
    pool.debug_dump(std::cout);
    return RCOK;
}
w_rc_t test_bf_swizzle(ss_m* ssm, test_volume_t *test_volume) {
    return _test_bf_swizzle(ssm, test_volume, true);
}
w_rc_t test_bf_noswizzle(ss_m* ssm, test_volume_t *test_volume) {
    return _test_bf_swizzle(ssm, test_volume, false);
}
TEST (TreeBufferpoolTest, Swizzle) {
    // disable background cleaner because we test pin_cnt
    run_bf_test(test_bf_swizzle, LARGE, false, true);
}
TEST (TreeBufferpoolTest, NoSwizzle) {
    run_bf_test(test_bf_noswizzle, LARGE, false, false);
}

#ifdef BP_MAINTAIN_PARENT_PTR
w_rc_t test_bf_switch_parent(ss_m* /*ssm*/, test_volume_t *test_volume) {
    
    bf_tree_m &pool(*smlevel_0::bf);
    lpid_t root_pid(test_volume->_vid, 1, 3);

    generic_page *root_page = NULL;
    W_DO(pool.fix_virgin_root(root_page, test_volume->_vid, root_pid.store(), root_pid.page));
    EXPECT_TRUE (root_page != NULL);
    ::memset(root_page, 0, sizeof(generic_page));
    root_page->pid = root_pid;
    root_page->tag = t_btree_p;
    root_page->btree_level = 2;
    root_page->btree_foster = 0;
    root_page->init_items();
    bf_tree_cb_t &root_cb (*test_bf_tree::get_bf_control_block(&pool, root_page));
    EXPECT_EQ(1, root_cb._pin_cnt); // root page is always swizzled, so pin_cnt is 1.
    EXPECT_TRUE (pool.is_swizzled(root_page));

    lpid_t child_pid(test_volume->_vid, 1, root_pid.page + 1);
    test_bf_tree::_add_child_pointer (root_page, child_pid.page);
    generic_page *child_page = NULL;
    W_DO(pool.fix_nonroot(child_page, root_page, child_pid.vol().vol, child_pid.page, LATCH_EX, true, true));
    EXPECT_TRUE (child_page != NULL);
    ::memset(child_page, 0, sizeof(generic_page));
    child_page->pid = child_pid;
    child_page->tag = t_btree_p;
    child_page->btree_level = 1;
    child_page->btree_foster = 0;
    child_page->init_items();
    bf_tree_cb_t &child_cb (*test_bf_tree::get_bf_control_block(&pool, child_page));
    EXPECT_EQ(1, child_cb._pin_cnt);
    EXPECT_EQ(2, root_cb._pin_cnt); // added as child
    EXPECT_EQ(test_bf_tree::get_bf_idx(&pool, root_page), child_cb._parent);
    EXPECT_TRUE (pool.is_swizzled(child_page));

    lpid_t sibling_pid(test_volume->_vid, 1, root_pid.page + 2);
    child_page->btree_foster = sibling_pid.page; // add as a foster child
    generic_page *sibling_page = NULL;
    W_DO(pool.fix_nonroot(sibling_page, child_page, sibling_pid.vol().vol, sibling_pid.page, LATCH_EX, true, true));
    EXPECT_TRUE (sibling_page != NULL);
    ::memset(sibling_page, 0, sizeof(generic_page));
    sibling_page->pid = sibling_pid;
    sibling_page->tag = t_btree_p;
    sibling_page->btree_level = 1;
    sibling_page->btree_foster = 0;
    sibling_page->init_items();
    bf_tree_cb_t &sibling_cb (*test_bf_tree::get_bf_control_block(&pool, sibling_page));
    EXPECT_EQ(1, sibling_cb._pin_cnt);
    EXPECT_EQ(2, child_cb._pin_cnt); // added as child
    EXPECT_EQ(2, root_cb._pin_cnt);
    EXPECT_EQ(test_bf_tree::get_bf_idx(&pool, child_page), sibling_cb._parent);
    EXPECT_TRUE (pool.is_swizzled(sibling_page));

    pool.debug_dump(std::cout);

    // then, adopt the sibling to real parent
    test_bf_tree::_add_child_pointer (root_page, sibling_pid.page);
    child_page->btree_foster = 0;
    pool.switch_parent(sibling_page, root_page);

    // moved the pin count to real parent
    EXPECT_EQ(1, sibling_cb._pin_cnt);
    EXPECT_EQ(1, child_cb._pin_cnt);
    EXPECT_EQ(3, root_cb._pin_cnt);
    EXPECT_EQ(test_bf_tree::get_bf_idx(&pool, root_page), sibling_cb._parent);

    pool.debug_dump(std::cout);

    pool.unfix(sibling_page);
    pool.unfix(child_page);
    pool.unfix(root_page);

    pool.debug_dump(std::cout);
    return RCOK;
}
TEST (TreeBufferpoolTest, SwitchParent) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(test_bf_switch_parent,
        false, default_locktable_size, default_quota_in_pages, 10,
        1, 10000, 10000, 64, false, true
    ), 0);
}
#endif // BP_MAINTAIN_PARENT_PTR

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
