#include "btree_test_env.h"
#include "sm_base.h"
#include "log_core.h"
#include "vol.h"
#include "chkpt.h"
#include "btree_logrec.h"
#include "eventlog.h"

#include <vector>

btree_test_env *test_env;
logrec_t logrec;
generic_page page;
btree_page_h page_h;
vector<lsn_t> undoNextMap;
const char someString[12] = "Checkpoint!";
cvec_t elem;
w_keystr_t key;

void init()
{
    const size_t maxTid = 255;
    undoNextMap.resize(maxTid, lsn_t::null);
    page_h.setup_for_restore(&page);
    elem.put(someString, 12);
}

void flushLog()
{
    W_COERCE(smlevel_0::log->flush_all(true));
}

lsn_t makeUpdate(unsigned tid, PageID pid, string kstr)
{
    lsn_t lsn;
    page.pid = pid;
    page.store = 1;
    key.construct_regularkey(kstr.c_str(), kstr.size());
    new (&logrec) btree_insert_log(page_h, key, elem, false);
    logrec.set_tid(tid_t(tid, 0));
    logrec.set_undo_nxt(undoNextMap[tid]);
    W_COERCE(smlevel_0::log->insert(logrec, &lsn));
    undoNextMap[tid] = lsn;
    flushLog();

    return lsn;
}

lsn_t logDummy()
{
    lsn_t lsn;
    string empty("");
    logrec_t* lr = new (&logrec) comment_log(empty.c_str());
    W_COERCE(smlevel_0::log->insert(*lr, &lsn));
    flushLog();

    return lsn;
}

lsn_t commitXct(unsigned tid)
{
    lsn_t lsn;
    new (&logrec) xct_end_log();
    logrec.set_tid(tid_t(tid, 0));
    logrec.set_undo_nxt(undoNextMap[tid]);
    W_COERCE(smlevel_0::log->insert(logrec, &lsn));
    undoNextMap[tid] = lsn_t::null;
    flushLog();

    return lsn;
}

lsn_t abortXct(unsigned tid)
{
    lsn_t lsn;
    new (&logrec) xct_abort_log();
    logrec.set_tid(tid_t(tid, 0));
    logrec.set_undo_nxt(undoNextMap[tid]);
    W_COERCE(smlevel_0::log->insert(logrec, &lsn));
    undoNextMap[tid] = lsn_t::null;
    flushLog();

    return lsn;
}

lsn_t generateCLR(unsigned tid, lsn_t lsn)
{
    lsn_t ret;
    new (&logrec) compensate_log(lsn);
    logrec.set_tid(tid_t(tid, 0));
    W_COERCE(smlevel_0::log->insert(logrec, &ret));
    flushLog();

    return ret;
}

rc_t emptyChkpt(ss_m*, test_volume_t*)
{
    chkpt_t chkpt;
    chkpt.scan_log();

    EXPECT_EQ(0, chkpt.buf_tab.size());
    EXPECT_EQ(0, chkpt.xct_tab.size());
    EXPECT_TRUE(chkpt.bkp_path.empty());
    EXPECT_TRUE(chkpt.get_highest_tid().is_null());
    EXPECT_TRUE(chkpt.get_min_rec_lsn().is_null());
    EXPECT_TRUE(chkpt.get_min_xct_lsn().is_null());

    return RCOK;
}

rc_t oneUpdateDirtyUncommitted(ss_m*, test_volume_t*)
{
    lsn_t lsn = makeUpdate(1, 1, "key1");

    chkpt_t chkpt;
    chkpt.scan_log();

    EXPECT_EQ(1, chkpt.buf_tab.size());
    EXPECT_EQ(1, chkpt.xct_tab.size());
    EXPECT_TRUE(chkpt.bkp_path.empty());
    EXPECT_EQ(tid_t(1,0), chkpt.get_highest_tid());
    EXPECT_EQ(lsn, chkpt.get_min_rec_lsn());
    EXPECT_EQ(lsn, chkpt.get_min_xct_lsn());

    return RCOK;
}

rc_t twoUpdatesDirtyUncommitted(ss_m*, test_volume_t*)
{
    lsn_t lsn1 = makeUpdate(1, 1, "key1");
    lsn_t lsn2 = makeUpdate(1, 1, "key2");

    chkpt_t chkpt;
    chkpt.scan_log();

    EXPECT_EQ(1, chkpt.buf_tab.size());
    EXPECT_EQ(1, chkpt.xct_tab.size());
    EXPECT_TRUE(chkpt.bkp_path.empty());
    EXPECT_EQ(tid_t(1,0), chkpt.get_highest_tid());
    EXPECT_EQ(lsn1, chkpt.get_min_rec_lsn());
    EXPECT_EQ(lsn1, chkpt.get_min_xct_lsn());
    EXPECT_EQ(lsn2, chkpt.buf_tab[1].page_lsn);
    EXPECT_EQ(lsn2, chkpt.xct_tab[tid_t(1,0)].last_lsn);

    return RCOK;
}

rc_t twoUpdatesDirtyCommitted(ss_m*, test_volume_t*)
{
    lsn_t lsn1 = makeUpdate(1, 1, "key1");
    lsn_t lsn2 = makeUpdate(1, 1, "key2");
    commitXct(1);

    chkpt_t chkpt;
    chkpt.scan_log();

    EXPECT_EQ(1, chkpt.buf_tab.size());
    EXPECT_EQ(0, chkpt.xct_tab.size());
    EXPECT_TRUE(chkpt.bkp_path.empty());
    EXPECT_EQ(tid_t(1,0), chkpt.get_highest_tid());
    EXPECT_EQ(lsn1, chkpt.get_min_rec_lsn());
    EXPECT_EQ(lsn_t::null, chkpt.get_min_xct_lsn());
    EXPECT_EQ(lsn2, chkpt.buf_tab[1].page_lsn);

    return RCOK;
}

rc_t twoUpdatesDirtyAborting(ss_m*, test_volume_t*)
{
    lsn_t lsn1 = makeUpdate(1, 1, "key1");
    lsn_t lsn2 = makeUpdate(1, 1, "key2");
    lsn_t clr1 = generateCLR(1, lsn2);
    lsn_t clr2 = generateCLR(1, lsn1);

    chkpt_t chkpt;
    chkpt.scan_log();

    EXPECT_EQ(1, chkpt.buf_tab.size());
    EXPECT_EQ(1, chkpt.xct_tab.size());
    EXPECT_TRUE(chkpt.bkp_path.empty());
    EXPECT_EQ(tid_t(1,0), chkpt.get_highest_tid());
    EXPECT_EQ(lsn1, chkpt.get_min_rec_lsn());
    EXPECT_EQ(lsn1, chkpt.get_min_xct_lsn());
    EXPECT_EQ(lsn2, chkpt.buf_tab[1].page_lsn);
    EXPECT_EQ(clr2, chkpt.xct_tab[tid_t(1,0)].last_lsn);
    (void) clr1;

    return RCOK;
}

rc_t twoUpdatesDirtyAborted(ss_m*, test_volume_t*)
{
    lsn_t lsn1 = makeUpdate(1, 1, "key1");
    lsn_t lsn2 = makeUpdate(1, 1, "key2");
    generateCLR(1, lsn2);
    generateCLR(1, lsn1);
    abortXct(1);

    chkpt_t chkpt;
    chkpt.scan_log();

    EXPECT_EQ(1, chkpt.buf_tab.size());
    EXPECT_EQ(0, chkpt.xct_tab.size());
    EXPECT_TRUE(chkpt.bkp_path.empty());
    EXPECT_EQ(tid_t(1,0), chkpt.get_highest_tid());
    EXPECT_EQ(lsn1, chkpt.get_min_rec_lsn());
    EXPECT_EQ(lsn_t::null, chkpt.get_min_xct_lsn());
    EXPECT_EQ(lsn2, chkpt.buf_tab[1].page_lsn);

    return RCOK;
}

rc_t twoXcts(ss_m*, test_volume_t*)
{
    lsn_t lsn1 = makeUpdate(1, 1, "key1");
    lsn_t lsn2 = makeUpdate(1, 2, "key2");
    lsn_t lsn3 = makeUpdate(2, 3, "key1");
    lsn_t lsn4 = makeUpdate(2, 4, "key2");
    generateCLR(1, lsn2);
    lsn_t clr1 = generateCLR(1, lsn1);

    chkpt_t chkpt;
    chkpt.scan_log();

    EXPECT_EQ(4, chkpt.buf_tab.size());
    EXPECT_EQ(2, chkpt.xct_tab.size());
    EXPECT_TRUE(chkpt.bkp_path.empty());
    EXPECT_EQ(tid_t(2,0), chkpt.get_highest_tid());
    EXPECT_EQ(lsn1, chkpt.get_min_rec_lsn());
    EXPECT_EQ(lsn1, chkpt.get_min_xct_lsn());
    EXPECT_EQ(lsn1, chkpt.buf_tab[1].page_lsn);
    EXPECT_EQ(lsn2, chkpt.buf_tab[2].page_lsn);
    EXPECT_EQ(lsn3, chkpt.buf_tab[3].page_lsn);
    EXPECT_EQ(lsn4, chkpt.buf_tab[4].page_lsn);
    EXPECT_EQ(clr1, chkpt.xct_tab[tid_t(1,0)].last_lsn);
    EXPECT_EQ(lsn4, chkpt.xct_tab[tid_t(2,0)].last_lsn);

    return RCOK;
}

rc_t twoXctsOneCommitted(ss_m*, test_volume_t*)
{
    lsn_t lsn1 = makeUpdate(1, 1, "key1");
    lsn_t lsn2 = makeUpdate(1, 2, "key2");
    lsn_t lsn3 = makeUpdate(2, 3, "key1");
    lsn_t lsn4 = makeUpdate(2, 4, "key2");
    commitXct(2);

    chkpt_t chkpt;
    chkpt.scan_log();

    EXPECT_EQ(4, chkpt.buf_tab.size());
    EXPECT_EQ(1, chkpt.xct_tab.size());
    EXPECT_TRUE(chkpt.bkp_path.empty());
    EXPECT_EQ(tid_t(2,0), chkpt.get_highest_tid());
    EXPECT_EQ(lsn1, chkpt.get_min_rec_lsn());
    EXPECT_EQ(lsn1, chkpt.get_min_xct_lsn());
    EXPECT_EQ(lsn1, chkpt.buf_tab[1].page_lsn);
    EXPECT_EQ(lsn2, chkpt.buf_tab[2].page_lsn);
    EXPECT_EQ(lsn3, chkpt.buf_tab[3].page_lsn);
    EXPECT_EQ(lsn4, chkpt.buf_tab[4].page_lsn);
    EXPECT_EQ(lsn2, chkpt.xct_tab[tid_t(1,0)].last_lsn);

    return RCOK;
}

rc_t onePageClean(ss_m*, test_volume_t*)
{
    lsn_t lsn1 = makeUpdate(1, 1, "key1");
    lsn_t lsn2 = makeUpdate(1, 2, "key2");
    sysevent::log_page_write(1, lsn2);
    flushLog();

    chkpt_t chkpt;
    chkpt.scan_log();

    EXPECT_EQ(1, chkpt.buf_tab.size());
    EXPECT_EQ(1, chkpt.xct_tab.size());
    EXPECT_TRUE(chkpt.bkp_path.empty());
    EXPECT_EQ(tid_t(1,0), chkpt.get_highest_tid());
    EXPECT_EQ(lsn2, chkpt.get_min_rec_lsn());
    EXPECT_EQ(lsn1, chkpt.get_min_xct_lsn());
    EXPECT_EQ(lsn2, chkpt.buf_tab[2].page_lsn);
    EXPECT_EQ(lsn2, chkpt.xct_tab[tid_t(1,0)].last_lsn);

    return RCOK;
}

rc_t twoPagesCleanTwoDirty(ss_m*, test_volume_t*)
{
    lsn_t lsn1 = makeUpdate(1, 1, "key1");
    lsn_t lsn2 = makeUpdate(1, 2, "key1");
    lsn_t lsn3 = makeUpdate(1, 2, "key2");
    lsn_t lsn4 = makeUpdate(1, 3, "key1");
    lsn_t lsn5 = makeUpdate(1, 4, "key1");
    lsn_t lsn6 = makeUpdate(1, 4, "key2");
    sysevent::log_page_write(1, lsn6);
    sysevent::log_page_write(3, lsn6);
    flushLog();

    chkpt_t chkpt;
    chkpt.scan_log();

    EXPECT_EQ(2, chkpt.buf_tab.size());
    EXPECT_EQ(1, chkpt.xct_tab.size());
    EXPECT_TRUE(chkpt.bkp_path.empty());
    EXPECT_EQ(tid_t(1,0), chkpt.get_highest_tid());
    EXPECT_EQ(lsn2, chkpt.get_min_rec_lsn());
    EXPECT_EQ(lsn1, chkpt.get_min_xct_lsn());
    EXPECT_EQ(lsn3, chkpt.buf_tab[2].page_lsn);
    EXPECT_EQ(lsn2, chkpt.buf_tab[2].rec_lsn);
    EXPECT_EQ(lsn6, chkpt.buf_tab[4].page_lsn);
    EXPECT_EQ(lsn5, chkpt.buf_tab[4].rec_lsn);
    EXPECT_EQ(lsn6, chkpt.xct_tab[tid_t(1,0)].last_lsn);
    (void) lsn4;

    return RCOK;
}

rc_t pagesDirtiedTwice(ss_m*, test_volume_t*)
{
    lsn_t lsn1 = makeUpdate(1, 1, "key1");
    lsn_t lsn2 = makeUpdate(1, 2, "key1");
    lsn_t lsn3 = makeUpdate(1, 2, "key2");
    lsn_t lsn4 = makeUpdate(1, 3, "key1");
    lsn_t lsn5 = makeUpdate(1, 4, "key1");
    lsn_t lsn6 = makeUpdate(1, 4, "key2");
    sysevent::log_page_write(1, lsn6);
    sysevent::log_page_write(3, lsn6);

    lsn_t lsn7 = makeUpdate(1, 1, "key2");

    chkpt_t chkpt;
    chkpt.scan_log();

    EXPECT_EQ(3, chkpt.buf_tab.size());
    EXPECT_EQ(1, chkpt.xct_tab.size());
    EXPECT_TRUE(chkpt.bkp_path.empty());
    EXPECT_EQ(tid_t(1,0), chkpt.get_highest_tid());
    EXPECT_EQ(lsn2, chkpt.get_min_rec_lsn());
    EXPECT_EQ(lsn1, chkpt.get_min_xct_lsn());
    EXPECT_EQ(lsn7, chkpt.buf_tab[1].page_lsn);
    EXPECT_EQ(lsn7, chkpt.buf_tab[1].rec_lsn);
    EXPECT_EQ(lsn3, chkpt.buf_tab[2].page_lsn);
    EXPECT_EQ(lsn2, chkpt.buf_tab[2].rec_lsn);
    EXPECT_EQ(lsn6, chkpt.buf_tab[4].page_lsn);
    EXPECT_EQ(lsn5, chkpt.buf_tab[4].rec_lsn);
    EXPECT_EQ(lsn7, chkpt.xct_tab[tid_t(1,0)].last_lsn);
    (void) lsn4;

    return RCOK;
}

rc_t cleanerLostUpdate(ss_m*, test_volume_t*)
{
    // Update before page_write log record but after the last update
    // captured by the cleaner
    makeUpdate(1, 1, "key1");
    makeUpdate(1, 2, "key1");

    // Add dummy update so clean page is identified correctly.
    // This is needed because when the clean_lsn is equal to the
    // page_lsn in chkpt_t, the page is treated as dirty to be
    // on the safe side.
    lsn_t lsn3 = logDummy();

    // This update is on that the page_write logrec will miss
    lsn_t lsn4 = makeUpdate(1, 2, "key2");

    // Cleaner missed LSN 3
    sysevent::log_page_write(1, lsn3);
    sysevent::log_page_write(2, lsn3);
    flushLog();

    chkpt_t chkpt;
    chkpt.scan_log();

    // Page 2 must be seen as dirty
    EXPECT_EQ(1, chkpt.buf_tab.size());
    EXPECT_EQ(lsn4, chkpt.get_min_rec_lsn());
    EXPECT_TRUE(chkpt.buf_tab.find(2) != chkpt.buf_tab.end());
    EXPECT_EQ(lsn4, chkpt.buf_tab[2].page_lsn);
    EXPECT_EQ(lsn4, chkpt.buf_tab[2].rec_lsn);

    return RCOK;
}

#define DFT_TEST(name) \
TEST (CheckpointTest, name) { \
    test_env->empty_logdata_dir(); \
    init(); \
    sm_options options; \
    options.set_bool_option("sm_format", true); \
    options.set_bool_option("sm_shutdown_clean", false); \
    EXPECT_EQ(test_env->runBtreeTest(name, options), 0); \
}

DFT_TEST(emptyChkpt);
DFT_TEST(oneUpdateDirtyUncommitted);
DFT_TEST(twoUpdatesDirtyUncommitted);
DFT_TEST(twoUpdatesDirtyCommitted);
DFT_TEST(twoUpdatesDirtyAborting);
DFT_TEST(twoUpdatesDirtyAborted);
DFT_TEST(twoXcts);
DFT_TEST(twoXctsOneCommitted);
DFT_TEST(onePageClean);
DFT_TEST(twoPagesCleanTwoDirty);
DFT_TEST(pagesDirtiedTwice);
DFT_TEST(cleanerLostUpdate);

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
