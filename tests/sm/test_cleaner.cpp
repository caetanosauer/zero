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
    unsigned howManyToInsert = 2000;
    W_DO(populateBtree(ssm, test_vol, howManyToInsert));
    //W_DO(ss_m::force_buffers());
    vol_t* vol1 = volMgr->get(test_vol->_vid);

    //W_COERCE(volMgr->sx_dismount(test_vol->_device_name, true));
    //init();
    //vid_t vid;
    //uint32_t numpages = volMgr->get(test_vol.vid)->num_pages();
    //W_COERCE(volMgr->sx_format(test_vol->_device_name, shpid_t(numpages), vid, true));
    //W_COERCE(volMgr->sx_mount(test_vol->_device_name, true));  

    LogArchiver::LogConsumer cons(lsn_t(1,0), BLOCK_SIZE);
    LogArchiver::ArchiverHeap heap(BLOCK_SIZE);
    LogArchiver::ArchiveDirectory dir(test_env->archive_dir, BLOCK_SIZE, true);
    LogArchiver::BlockAssembly assemb(&dir);

    LogArchiver la(&dir, &cons, &heap, &assemb);
    la.fork();
    la.activate(lsn_t::null, true /* wait */);
    // wait for logarchiver to consume up to durable LSN,
    if (la.getDirectory()->getLastLSN() < ssm->log->durable_lsn()) {
            la.requestFlushSync(ssm->log->durable_lsn());
    }

    /*
    vid_t vid;
    shpid_t numPages = vol1->num_pages();
    std::string volpath = "device_1"; 
    W_COERCE(volMgr->sx_format(volpath.c_str(), numPages, vid, true));
    W_COERCE(volMgr->sx_mount(volpath.c_str(), true));
    vol_t* vol2 = volMgr->get(vid);
    W_DO(ssm->begin_xct());
    W_DO(ssm->create_index(vid, stid));
    W_DO(ssm->open_store(stid, root_pid));
    W_DO(ssm->commit_xct());
    */

    page_cleaner pc(vol1, &dir, smlevel_0::bf);
    pc.fork();
    pc.activate(ssm->log->durable_lsn());
    while(pc.isActive()){}
    pc.shutdown(); 
    pc.join();

    /*
    EXPECT_EQ(vol1->first_data_pageid(), vol2->first_data_pageid());
    EXPECT_EQ(vol1->num_pages(), vol2->num_pages());

    for(shpid_t p=vol1->first_data_pageid(); p<vol1->num_pages(); ++p){
        generic_page buf1;
        generic_page buf2;
        vol1->read_page(p, buf1);
        vol2->read_page(p, buf2);
        EXPECT_EQ(buf1.lsn, buf2.lsn);
    }
    */

    return RCOK;
}
TEST (CleanerTest, cleaner) {
    test_env->empty_logdata_dir();
    sm_options options;
    options.set_bool_option("sm_testenv_init_vol", true);
    options.set_string_option("sm_archdir", "/tmp");
    EXPECT_EQ(test_env->runBtreeTest(cleaner, options), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}