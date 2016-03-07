#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"

btree_test_env *test_env;

/**
 * Unit test for cursor.
 */

w_rc_t prep_test(ss_m* ssm, test_volume_t *test_volume, StoreID &stid, PageID &root_pid) {
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));
    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_insert(stid, "10", "dat1"));
    W_DO(test_env->btree_insert(stid, "20", "dat2"));
    W_DO(test_env->btree_insert(stid, "30", "dat3"));
    W_DO(test_env->btree_insert(stid, "40", "dat4"));
    W_DO(test_env->btree_insert(stid, "50", "dat5"));
    W_DO(test_env->commit_xct());
    return RCOK;
}

w_keystr_t neginf_key () {
    w_keystr_t key;
    key.construct_neginfkey();
    return key;
}
w_keystr_t posinf_key () {
    w_keystr_t key;
    key.construct_posinfkey();
    return key;
}
w_keystr_t reg_key (const char *s) {
    w_keystr_t key;
    key.construct_regularkey(s, ::strlen(s));
    return key;
}
std::string get_dat (bt_cursor_t &cursor) {
    return std::string(cursor.elem(), cursor.elen());
}

rc_t check_result (bt_cursor_t &cursor, int from, int to, bool forward) {
    for (int i = forward ? from : to ; forward ? i <= to : i >= from; forward ? ++i : --i) {
        char keybuf[3] = "10";
        char datbuf[5] = "dat1";
        keybuf[0] = datbuf[3] = '0' + i;
        W_DO(cursor.next());
        EXPECT_FALSE (cursor.eof());
        EXPECT_EQ(cursor.key(), reg_key(keybuf));
        EXPECT_EQ(get_dat(cursor), datbuf);
    }
    W_DO(cursor.next());
    EXPECT_TRUE (cursor.eof());
    return RCOK;
}

w_rc_t full_scan(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(prep_test(ssm, test_volume, stid, root_pid));

    W_DO(test_env->begin_xct());
    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, true);
        W_DO(check_result(cursor, 1, 5, true));
        bt_cursor_t cursor_back (stid, false);
        W_DO(check_result(cursor_back, 1, 5, false));
    }

    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, neginf_key(), true, posinf_key(), true, true);
        W_DO(check_result(cursor, 1, 5, true));
        bt_cursor_t cursor_back (stid, neginf_key(), true, posinf_key(), true, false);
        W_DO(check_result(cursor_back, 1, 5, false));
    }

    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, neginf_key(), false, posinf_key(), false, true);
        W_DO(check_result(cursor, 1, 5, true));
        bt_cursor_t cursor_back (stid, neginf_key(), false, posinf_key(), false, false);
        W_DO(check_result(cursor_back, 1, 5, false));
    }
    W_DO(test_env->commit_xct());
    return RCOK;
}

TEST (BtreeCursorTest, FullScan) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(full_scan), 0);
}
TEST (BtreeCursorTest, FullScanLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(full_scan, true), 0);
}

w_rc_t low_cond(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(prep_test(ssm, test_volume, stid, root_pid));

    W_DO(test_env->begin_xct());
    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, reg_key("30"), true, posinf_key(), true, true);
        W_DO(check_result(cursor, 3, 5, true));
        bt_cursor_t cursor_back (stid, reg_key("30"), true, posinf_key(), true, false);
        W_DO(check_result(cursor_back, 3, 5, false));
    }

    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, reg_key("30"), false, posinf_key(), true, true);
        W_DO(check_result(cursor, 4, 5, true));
        bt_cursor_t cursor_back (stid, reg_key("30"), false, posinf_key(), true, false);
        W_DO(check_result(cursor_back, 4, 5, false));
    }

    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, reg_key("25"), true, posinf_key(), true, true);
        W_DO(check_result(cursor, 3, 5, true));
        bt_cursor_t cursor_back (stid, reg_key("25"), true, posinf_key(), true, false);
        W_DO(check_result(cursor_back, 3, 5, false));
    }

    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, reg_key("25"), false, posinf_key(), true, true);
        W_DO(check_result(cursor, 3, 5, true));
        bt_cursor_t cursor_back (stid, reg_key("25"), false, posinf_key(), true, false);
        W_DO(check_result(cursor_back, 3, 5, false));
    }


    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, reg_key("00"), false, posinf_key(), true, true);
        W_DO(check_result(cursor, 1, 5, true));
        bt_cursor_t cursor_back (stid, reg_key("00"), false, posinf_key(), true, false);
        W_DO(check_result(cursor_back, 1, 5, false));
    }
    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, reg_key("00"), true, posinf_key(), true, true);
        W_DO(check_result(cursor, 1, 5, true));
        bt_cursor_t cursor_back (stid, reg_key("00"), true, posinf_key(), true, false);
        W_DO(check_result(cursor_back, 1, 5, false));
    }
    W_DO(test_env->commit_xct());
    return RCOK;
}

TEST (BtreeCursorTest, LowCond) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(low_cond), 0);
}
TEST (BtreeCursorTest, LowCondLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(low_cond, true), 0);
}

w_rc_t upp_cond(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(prep_test(ssm, test_volume, stid, root_pid));
    W_DO(test_env->begin_xct());

    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, neginf_key(), true, reg_key("40"), true, true);
        W_DO(check_result(cursor, 1, 4, true));
        bt_cursor_t cursor_back (stid, neginf_key(), true, reg_key("40"), true, false);
        W_DO(check_result(cursor_back, 1, 4, false));
    }

    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, neginf_key(), true, reg_key("40"), false, true);
        W_DO(check_result(cursor, 1, 3, true));
        bt_cursor_t cursor_back (stid, neginf_key(), true, reg_key("40"), false, false);
        W_DO(check_result(cursor_back, 1, 3, false));
    }

    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, neginf_key(), true, reg_key("45"), true, true);
        W_DO(check_result(cursor, 1, 4, true));
        bt_cursor_t cursor_back (stid, neginf_key(), true, reg_key("45"), true, false);
        W_DO(check_result(cursor_back, 1, 4, false));
    }

    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, neginf_key(), true, reg_key("45"), false, true);
        W_DO(check_result(cursor, 1, 4, true));
        bt_cursor_t cursor_back (stid, neginf_key(), true, reg_key("45"), false, false);
        W_DO(check_result(cursor_back, 1, 4, false));
    }

    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, neginf_key(), true, reg_key("65"), false, true);
        W_DO(check_result(cursor, 1, 5, true));
        bt_cursor_t cursor_back (stid, neginf_key(), true, reg_key("65"), false, false);
        W_DO(check_result(cursor_back, 1, 5, false));
    }

    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, neginf_key(), true, reg_key("65"), true, true);
        W_DO(check_result(cursor, 1, 5, true));
        bt_cursor_t cursor_back (stid, neginf_key(), true, reg_key("65"), true, false);
        W_DO(check_result(cursor_back, 1, 5, false));
    }
    W_DO(test_env->commit_xct());
    return RCOK;
}

TEST (BtreeCursorTest, UppCond) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(upp_cond), 0);
}
TEST (BtreeCursorTest, UppCondLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(upp_cond, true), 0);
}

w_rc_t both_cond(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(prep_test(ssm, test_volume, stid, root_pid));
    W_DO(test_env->begin_xct());

    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, reg_key("15"), true, reg_key("40"), true, true);
        W_DO(check_result(cursor, 2, 4, true));
        bt_cursor_t cursor_back (stid, reg_key("15"), true, reg_key("40"), true, false);
        W_DO(check_result(cursor_back, 2, 4, false));
    }
    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, reg_key("15"), false, reg_key("40"), true, true);
        W_DO(check_result(cursor, 2, 4, true));
        bt_cursor_t cursor_back (stid, reg_key("15"), false, reg_key("40"), true, false);
        W_DO(check_result(cursor_back, 2, 4, false));
    }

    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, reg_key("15"), true, reg_key("40"), false, true);
        W_DO(check_result(cursor, 2, 3, true));
        bt_cursor_t cursor_back (stid, reg_key("15"), true, reg_key("40"), false, false);
        W_DO(check_result(cursor_back, 2, 3, false));
    }
    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, reg_key("15"), false, reg_key("40"), false, true);
        W_DO(check_result(cursor, 2, 3, true));
        bt_cursor_t cursor_back (stid, reg_key("15"), false, reg_key("40"), false, false);
        W_DO(check_result(cursor_back, 2, 3, false));
    }

    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, reg_key("30"), true, reg_key("30"), true, true);
        W_DO(check_result(cursor, 3, 3, true));
        bt_cursor_t cursor_back (stid, reg_key("30"), true, reg_key("30"), true, false);
        W_DO(check_result(cursor_back, 3, 3, false));
    }

    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, reg_key("30"), false, reg_key("30"), true, true);
        W_DO(check_result(cursor, 4, 3, true));
        bt_cursor_t cursor_back (stid, reg_key("30"), false, reg_key("30"), true, false);
        W_DO(check_result(cursor_back, 4, 3, false));
    }

    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, reg_key("35"), true, reg_key("30"), true, true);
        W_DO(check_result(cursor, 4, 3, true));
        bt_cursor_t cursor_back (stid, reg_key("35"), true, reg_key("30"), true, false);
        W_DO(check_result(cursor_back, 4, 3, false));
    }
    W_DO(test_env->commit_xct());
    return RCOK;
}

TEST (BtreeCursorTest, BothCond) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(both_cond), 0);
}
TEST (BtreeCursorTest, BothCondLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(both_cond, true), 0);
}

rc_t check_result2 (bt_cursor_t &cursor, int from, int to, bool forward) {
    for (int i = forward ? from : to ; forward ? i <= to : i >= from; forward ? ++i : --i) {
        char keybuf[3];
        keybuf[0] = '0' + (i / 10);
        keybuf[1] = '0' + (i % 10);
        keybuf[2] = '\0';
        W_DO(cursor.next());
        EXPECT_FALSE (cursor.eof());
        EXPECT_EQ(cursor.key(), reg_key(keybuf));
    }
    W_DO(cursor.next());
    EXPECT_TRUE (cursor.eof());
    return RCOK;
}

w_rc_t span_pages(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    char keystr[3] = "";

    const size_t datsize = (SM_PAGESIZE / 6);
    //const size_t datsize = btree_m::max_entry_size() - sizeof(keystr);
    w_assert1(datsize <= btree_m::max_entry_size() - sizeof(keystr));
    char datastr[datsize + 1];
    keystr[2] = '\0';
    datastr[datsize] = '\0';
    ::memset (datastr, 'a', datsize);

    W_DO(test_env->begin_xct());
    for (int i = 10; i < 90; ++i) {
        keystr[0] = '0' + (i / 10);
        keystr[1] = '0' + (i % 10);
        W_DO(test_env->btree_insert(stid, keystr, datastr));
    }
    W_DO(test_env->commit_xct());
    W_DO(test_env->begin_xct());
    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, true);
        W_DO(check_result2(cursor, 10, 89, true));
        bt_cursor_t cursor_back (stid, false);
        W_DO(check_result2(cursor_back, 10, 89, false));
    }

    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, reg_key("343"), true, reg_key("80"), true, true);
        W_DO(check_result2(cursor, 35, 80, true));
        bt_cursor_t cursor_back (stid, reg_key("343"), true, reg_key("80"), true, false);
        W_DO(check_result2(cursor_back, 35, 80, false));
    }

    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, reg_key("36"), false, reg_key("798"), true, true);
        W_DO(check_result2(cursor, 37, 79, true));
        bt_cursor_t cursor_back (stid, reg_key("36"), false, reg_key("798"), true, false);
        W_DO(check_result2(cursor_back, 37, 79, false));
    }

    {
        SCOPED_TRACE("from here!");
        bt_cursor_t cursor (stid, reg_key("378"), false, reg_key("812"), true, true);
        W_DO(check_result2(cursor, 38, 81, true));
        bt_cursor_t cursor_back (stid, reg_key("378"), false, reg_key("812"), true, false);
        W_DO(check_result2(cursor_back, 38, 81, false));
    }

    W_DO(test_env->commit_xct());
    return RCOK;
}

TEST (BtreeCursorTest, SpanPages) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(span_pages), 0);
}
TEST (BtreeCursorTest, SpanPagesLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(span_pages, true), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
