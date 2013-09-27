
#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include "generic_page.h"
#include "bf.h"
#include "btree_page_h.h"
#include "e_error_def_gen.h"
#include <sstream>

btree_test_env *test_env;

/* Hypothesis: Together, b-link trees + fence keys reduce lock/latch
 *  contention, enabling higher insert rates than original ShoreMT.
 * 
 * Procedure:
 *   For now: Create a b-tree and insert XXXX random keys in a single stream.
 *   Later: Repeat at various MPL.
 * 
 * Measure: 
 *    For now:  total runtime, throughput
 *    Later: I/O, lock and latch contention
 * 
 */

w_rc_t dosome(ss_m* ssm, test_volume_t *test_volume) {
    stid_t stid;
    lpid_t root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    w_keystr_t key;

    // XXXX should move the following out to a separate DataGen class
    typedef unsigned int size_t;
    size_t const domain = 100000;
    size_t const records = 10000;
    size_t produced = 0; // produced output records
    int ibuffer; // hold the random int 
    
    ::srand(12345); // use fixed seed for repeatability and easier debugging
    W_DO(ssm->begin_xct());
    test_env->set_xct_query_lock();
    while ( produced < records ) 
    {
       std::stringstream buffer, buffer2;
       ibuffer = rand () % domain;
       buffer << ibuffer;
       key.construct_regularkey(buffer.str().data(), buffer.str().size());
       ibuffer = rand () % domain;
       buffer2 << ibuffer;
       vec_t data;
       data.set(buffer2.str().data(),buffer2.str().size());


       rc_t rc = ssm->create_assoc(stid, key, data);
       if (rc.is_error()) {
          if (rc.err_num() != ss_m::eDUPLICATE) {
                cerr << "unexpected error";
                return rc;
          }
       }


       if (produced%500  == 1)
        {
          W_DO(ssm->commit_xct());
          W_DO(ssm->begin_xct());
          test_env->set_xct_query_lock();
          printf("Processed %d keys\n",produced);
        }
         
       produced++;
    } 
    W_DO(ssm->commit_xct());
    return RCOK;
}

TEST (BtreeBasicTest2, DoSome) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(dosome, false, default_locktable_size, 4096, 1024), 0);
}
TEST (BtreeBasicTest2, DoSomeLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(dosome, true, default_locktable_size, 4096, 1024), 0);
}


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
