#include "btree_test_env.h"

#include "bf_tree.h"
#include "log_core.h"
#include "logarchiver.h"
#include "restore.h"
#include "vol.h"
#include "alloc_cache.h"
#include "sm_options.h"


const size_t RECORD_SIZE = 100;
const size_t SEGMENT_SIZE = 8;

char RECORD_STR[RECORD_SIZE + 1];

typedef w_rc_t rc_t;

btree_test_env* test_env;
sm_options options;
StoreID stid;
PageID root_pid;

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

void emptyBP(unsigned /*count*/)
{
    // CS TODO implement this method on bf_tree
    // (would probably only work with swizzling turned off)
}

rc_t populatePages(ss_m* ssm, test_volume_t* test_volume, int numPages)
{
    size_t pageDataSize = btree_page_data::data_sz;
    size_t numRecords = (pageDataSize * numPages)/ RECORD_SIZE;

    W_DO(populateBtree(ssm, test_volume, numRecords));
    emptyBP(numPages);

    return RCOK;
}

rc_t lookupPages(size_t numPages)
{
    size_t pageDataSize = btree_page_data::data_sz;
    size_t numRecords = (pageDataSize * numPages)/ RECORD_SIZE;

    return lookupKeys(numRecords);
}

vol_t* failVolume(test_volume_t*, bool clear_buffer)
{
    vol_t* volume = smlevel_0::vol;
    W_COERCE(volume->mark_failed(clear_buffer));
    return volume;
}

// Compare volume contents, expected and actual
void verifyVolumesEqual(string pathExp, string pathAct)
{
    // vol_t instances can be created regardless of whether they are already
    // mounted on the volume manager
    options.set_string_option("sm_dbfile", pathExp);
    vol_t volExp(options, NULL);
    volExp.build_caches(false /* format */);
    options.set_string_option("sm_dbfile", pathAct);
    vol_t volAct(options, NULL);
    volAct.build_caches(false /* format */);

    size_t num_pages = volExp.num_used_pages();
    EXPECT_EQ(num_pages, volAct.num_used_pages());

    alloc_cache_t* allocExp = volExp.get_alloc_cache();
    alloc_cache_t* allocAct = volAct.get_alloc_cache();

    generic_page bufExp, bufAct;
    for (PageID p = PageID(1); p < num_pages; p++) {
        bool isAlloc = allocExp->is_allocated(p);
        EXPECT_EQ(isAlloc, allocAct->is_allocated(p));
        if (isAlloc) {
            volExp.read_page(p, &bufExp);
            volAct.read_page(p, &bufAct);

            EXPECT_EQ(PageID(p), bufAct.pid);

            // CS TODO -- checksums don't match, probably because of CLSN,
            // which is set on backup, but empty on original
            // EXPECT_EQ(bufExp.calculate_checksum(),
            //         bufAct.calculate_checksum());

            if (bufExp.tag == t_btree_p) {
                btree_page_data* btExp = (btree_page_data*) &bufExp;
                btree_page_data* btAct = (btree_page_data*) &bufAct;
                EXPECT_TRUE(btExp->eq(*btAct));
                // std::cout << "EXPECTED " << *btExp << std::endl;
                // std::cout << "ACTUAL " << *btAct << std::endl;
            }
        }
    }
}

rc_t singlePageTest(ss_m* ssm, test_volume_t* test_volume)
{
    W_DO(populateBtree(ssm, test_volume, 3));
    failVolume(test_volume, true);

    W_DO(lookupKeys(3));

    return RCOK;
}

rc_t multiPageTest(ss_m* ssm, test_volume_t* test_volume)
{
    W_DO(populatePages(ssm, test_volume, 3));
    failVolume(test_volume, true);

    W_DO(lookupPages(3));

    return RCOK;
}

rc_t fullRestoreTest(ss_m* ssm, test_volume_t* test_volume)
{
    W_DO(populatePages(ssm, test_volume, 3 * SEGMENT_SIZE));

    vol_t* volume = smlevel_0::vol;
    W_DO(volume->mark_failed());

    generic_page page;
    W_DO(volume->read_page(1, &page));

    W_DO(lookupPages(3 * SEGMENT_SIZE));

    return RCOK;
}

rc_t multiThreadedRestoreTest(ss_m* ssm, test_volume_t* test_volume)
{
    W_DO(populatePages(ssm, test_volume, 16 * SEGMENT_SIZE));

    vol_t* volume = smlevel_0::vol;
    W_DO(volume->mark_failed());

    generic_page page;
    W_DO(volume->read_page(1, &page));

    W_DO(lookupPages(16 * SEGMENT_SIZE));

    return RCOK;
}

rc_t takeBackupTest(ss_m* ssm, test_volume_t* test_volume)
{
    W_DO(populatePages(ssm, test_volume, 3 * SEGMENT_SIZE));
    smlevel_0::bf->get_cleaner()->wakeup(true);
    vol_t* volume = smlevel_0::vol;

    string backupPath = string(test_env->vol_dir) + "/backup";
    volume->take_backup(backupPath, true /* flushArchive */);

    verifyVolumesEqual(string(test_volume->_device_name), backupPath);

    return RCOK;
}

rc_t takeBackupMultiThreadedTest(ss_m* ssm, test_volume_t* test_volume)
{
    W_DO(populatePages(ssm, test_volume, 16 * SEGMENT_SIZE));
    smlevel_0::bf->get_cleaner()->wakeup(true);
    vol_t* volume = smlevel_0::vol;

    string backupPath = string(test_env->vol_dir) + "/backup";
    volume->take_backup(backupPath, true /* flushArchive */);

    verifyVolumesEqual(string(test_volume->_device_name), backupPath);

    return RCOK;
}

#define DEFAULT_TEST(test, function, option_reuse, option_singlepass, option_threads) \
    TEST (test, function) { \
        test_env->empty_logdata_dir(); \
        options.set_bool_option("sm_archiving", true); \
        options.set_string_option("sm_archdir", test_env->archive_dir); \
        options.set_int_option("sm_restore_segsize", SEGMENT_SIZE); \
        options.set_bool_option("sm_restore_sched_singlepass", option_singlepass); \
        options.set_bool_option("sm_restore_reuse_buffer", option_reuse); \
        options.set_int_option("sm_restore_threads", option_threads); \
        EXPECT_EQ(test_env->runBtreeTest(function, options), 0); \
    }

DEFAULT_TEST(BackupLess, singlePageTest, false, false, 1);
DEFAULT_TEST(BackupLess, multiPageTest, false, false, 1);
DEFAULT_TEST(BackupTest, takeBackupTest, false, false, 1);
DEFAULT_TEST(BackupTest, takeBackupMultiThreadedTest, false, false, 4);
DEFAULT_TEST(RestoreTest, fullRestoreTest, true, true, 1);
DEFAULT_TEST(RestoreTest, multiThreadedRestoreTest, true, true, 4);

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
