#include "btree_test_env.h"
#include "sm_base.h"
#include "log_core.h"
#include "vol.h"
#include "logarchiver.h"
#include "page_cleaner.h"

// use small block to test boundaries
const size_t BLOCK_SIZE = 1024 * 1024;
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
    init();
    unsigned howManyToInsert = 20000;
    W_DO(populateBtree(ssm, test_vol, howManyToInsert));
    ssm->activate_archiver();

    //LogArchiver::LogConsumer cons(lsn_t(1,0), BLOCK_SIZE);
    //LogArchiver::ArchiverHeap heap(BLOCK_SIZE);
    //LogArchiver::ArchiveDirectory dir(test_env->archive_dir, BLOCK_SIZE, true);
    //LogArchiver::BlockAssembly assemb(&dir);

    //LogArchiver la(&dir, &cons, &heap, &assemb);
    //la.fork();
    //la.activate(lsn_t::null, true /* wait */);

    // wait for logarchiver to consume up to durable LSN,
    //if (la.getDirectory()->getLastLSN() < ssm->log->durable_lsn()) {
    //        la.requestFlushSync(ssm->log->durable_lsn());
    //}

    //page_cleaner_mgr cleaner(smlevel_0::bf, &dir);
    //cleaner.start_cleaners();
    //cleaner.force_all();
    //sleep(1);
    //cleaner.request_stop_cleaners();
    //cleaner.join_cleaners();

    return RCOK;
}
TEST (CleanerTest, cleaner) {
    test_env->empty_logdata_dir();
    sm_options options;
    options.set_bool_option("sm_testenv_init_vol", true);
    options.set_bool_option("sm_logging", true);
    options.set_bool_option("sm_archiving", true);
    options.set_bool_option("sm_decoupled_cleaner", true);
    options.set_bool_option("sm_decoupled_cleaner_mode", true);
    options.set_bool_option("sm_archiver_eager", true);
    options.set_string_option("sm_archdir", "/var/tmp/lucas/btree_test_env/archive");
    options.set_string_option("sm_logdir", "/var/tmp/lucas/btree_test_env/log");
    EXPECT_EQ(test_env->runBtreeTest(cleaner, options), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}