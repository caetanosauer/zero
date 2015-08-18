#include "btree_test_env.h"
#include "sm_base.h"
#include "log_core.h"
#include "vol.h"
#include "logarchiver.h"

// use small block to test boundaries
const size_t BLOCK_SIZE = 8192;
char HUNDRED_BYTES[100];

btree_test_env *test_env;
stid_t stid;
lpid_t root_pid;
vol_m* volMgr;

void init() {
    volMgr = smlevel_0::vol;
}

rc_t populateBtree(ss_m* ssm, test_volume_t *test_volume, int count)
{
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    std::stringstream ss("key");

    W_DO(test_env->begin_xct());
    for (int i = 0; i < count; i++) {
        ss.seekp(3);
        ss << i;
        W_DO(test_env->btree_insert(stid, ss.str().c_str(), HUNDRED_BYTES));
    }
    W_DO(test_env->commit_xct());
    return RCOK;
}

rc_t cleaner(ss_m* ssm, test_volume_t* test_vol)
{
    unsigned howManyToInsert = 2000;
    W_DO(populateBtree(ssm, test_vol, howManyToInsert));

    //W_COERCE(volMgr->sx_dismount(test_vol->_device_name, true));
    //init();
    //vid_t vid;
    //uint32_t numpages = volMgr->get(test_vol.vid)->num_pages();
    //W_COERCE(volMgr->sx_format(test_vol->_device_name, shpid_t(numpages), vid, true));
    //W_COERCE(volMgr->sx_mount(test_vol->_device_name, true));  

    LogArchiver::LogConsumer cons(lsn_t(1,0), BLOCK_SIZE);
    LogArchiver::ArchiverHeap heap(BLOCK_SIZE);
    LogArchiver::ArchiveDirectory dir(test_env->archive_dir, BLOCK_SIZE, false);
    LogArchiver::BlockAssembly assemb(&dir);

    LogArchiver la(&dir, &cons, &heap, &assemb);
    la.fork();
    la.activate(lsn_t::null, true /* wait */);
    // wait for logarchiver to consume up to durable LSN,
    while (cons.getNextLSN() < ssm->log->durable_lsn()) {
        usleep(1000); // 1ms
    }
    return RCOK;
}
TEST (CleanerTest, cleaner) {
    test_env->empty_logdata_dir();
    sm_options options;
    options.set_bool_option("sm_testenv_init_vol", true);
    EXPECT_EQ(test_env->runBtreeTest(cleaner, options), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}