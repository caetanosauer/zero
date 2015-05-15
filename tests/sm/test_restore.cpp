#include "btree_test_env.h"

#include "logarchiver.h"
#include "restore.h"
#include "vol.h"
#include "sm_options.h"

const size_t RECORD_SIZE = 100;
const size_t BLOCK_SIZE = 8192;
const size_t SEGMENT_SIZE = 8;

char RECORD_STR[RECORD_SIZE + 1];

typedef w_rc_t rc_t;

btree_test_env* test_env;
sm_options options;
stid_t stid;
lpid_t root_pid;

/******************************************************************************
 * Auxiliary functions
 */

rc_t populateBtree(ss_m* ssm, test_volume_t *test_volume, int count)
{
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    std::stringstream ss("key");

    // fill buffer with a valid string
    memset(RECORD_STR, 'x', RECORD_SIZE);
    RECORD_STR[RECORD_SIZE] = '\0';

    W_DO(test_env->begin_xct());
    for (int i = 0; i < count; i++) {
        ss.seekp(3);
        ss << i;
        W_DO(test_env->btree_insert(stid, ss.str().c_str(), RECORD_STR));
    }
    W_DO(test_env->commit_xct());
    return RCOK;
}

rc_t lookupKeys(size_t count)
{
    string str;
    std::stringstream ss("key");

    W_DO(test_env->begin_xct());
    for (size_t i = 0; i < count; i++) {
        ss.seekp(3);
        ss << i;
        W_DO(test_env->btree_lookup(stid, ss.str().c_str(), str));
        EXPECT_TRUE(strncmp(str.c_str(), RECORD_STR, str.length()) == 0);
    }
    W_DO(test_env->commit_xct());

    return RCOK;
}

void archiveLog(ss_m* ssm)
{
    // archive whole log
    ssm->logArchiver->activate(ssm->log->curr_lsn(), true);
    while (ssm->logArchiver->getNextConsumedLSN() < ssm->log->curr_lsn()) {
        usleep(1000);
    }
    ssm->logArchiver->start_shutdown();
    ssm->logArchiver->join();
}

rc_t populatePages(ss_m* ssm, test_volume_t* test_volume, int numPages)
{
    size_t pageDataSize = btree_page_data::data_sz;
    size_t numRecords = (pageDataSize * numPages)/ RECORD_SIZE;

    return populateBtree(ssm, test_volume, numRecords);
}

rc_t lookupPages(size_t numPages)
{
    size_t pageDataSize = btree_page_data::data_sz;
    size_t numRecords = (pageDataSize * numPages)/ RECORD_SIZE;

    return lookupKeys(numRecords);
}

vol_t* failVolume(test_volume_t* test_volume, bool clear_buffer)
{
    vol_t* volume = smlevel_0::vol->get(test_volume->_vid);
    W_COERCE(volume->mark_failed(clear_buffer));
    return volume;
}

rc_t singlePageTest(ss_m* ssm, test_volume_t* test_volume)
{
    W_DO(populateBtree(ssm, test_volume, 3));
    archiveLog(ssm);
    failVolume(test_volume, true);

    W_DO(lookupKeys(3));

    return RCOK;
}

rc_t multiPageTest(ss_m* ssm, test_volume_t* test_volume)
{
    W_DO(populatePages(ssm, test_volume, 3));
    archiveLog(ssm);
    failVolume(test_volume, true);

    W_DO(lookupPages(3));

    return RCOK;
}

rc_t fullRestoreTest(ss_m* ssm, test_volume_t* test_volume)
{
    W_DO(populatePages(ssm, test_volume, 3 * SEGMENT_SIZE));

    archiveLog(ssm);

    vol_t* volume = smlevel_0::vol->get(test_volume->_vid);
    W_DO(volume->mark_failed());

    generic_page page;
    W_DO(volume->read_page(1, page));

    return RCOK;
}

#define DEFAULT_TEST(test, function, option_reuse, option_singlepass) \
    TEST (test, function) { \
        test_env->empty_logdata_dir(); \
        options.set_bool_option("sm_archiving", true); \
        options.set_int_option("sm_archiver_block_size", BLOCK_SIZE); \
        options.set_string_option("sm_archdir", test_env->archive_dir); \
        options.set_int_option("sm_restore_segsize", SEGMENT_SIZE); \
        options.set_bool_option("sm_restore_sched_singlepass", option_singlepass); \
        options.set_bool_option("sm_restore_reuse_buffer", option_reuse); \
        EXPECT_EQ(test_env->runBtreeTest(function, options), 0); \
    }

DEFAULT_TEST(BackupLess, singlePageTest, false, false);
DEFAULT_TEST(BackupLess, multiPageTest, false, false);
DEFAULT_TEST(RestoreTest, fullRestoreTest, true, true);

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
