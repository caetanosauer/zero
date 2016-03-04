
#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include <sstream>

//#include <sys/types.h>
#include <sys/stat.h>
//#include <unistd.h>
#include "log_core.h"
#include "partition.h"

#include "logbuf_common.h"

btree_test_env *test_env;

/* Hypothesis: Verify that log entries are persisted.
 *
 * Procedure:
 *   For now: Create a b-tree and insert XXXX random keys in a single stream.
 *   Later: Repeat at various MPL.
 *
 * Measure:
 *    For now, verify that:
 *         1. the log file increases in size
 *         2. the log grows by logpagesize
 *         3. the log rolls over into new partitions
 */

w_rc_t dosome(ss_m* ssm, test_volume_t *test_volume) {
    stid_t stid;
    lpid_t root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    w_keystr_t key;

    // XXXX should move the following out to a separate DataGen class
    typedef unsigned int size_t;
    size_t const domain = 100000;
    size_t const records= 35000;
    off_t  const logpagesize = 8192; // quantum of log file size
    off_t  logsize = 0; // log file size. Should grow monotonically.
    off_t  logsize2 = 0; // log file size. Should grow monotonically.
    int ibuffer; // hold the random int

    // for checking log partition files
    partition_number_t cur_partition_number;
    partition_number_t prev_partition_number, initial_partition_number;
    cur_partition_number = smlevel_0::log->partition_num();
    prev_partition_number = cur_partition_number;
    initial_partition_number = cur_partition_number;
    (void) initial_partition_number; // Used in an assert
    std::cout << "Log starts off in dir: " << test_env->log_dir << " partition"
                    << cur_partition_number << "\n";

    struct stat filestatus;
    char *fname = new char [smlevel_0::max_devname];

    // Get initial size of current log partition
    smlevel_0::log->make_log_name(cur_partition_number, fname, smlevel_0::max_devname);
    assert (0 == stat( fname, &filestatus ));
    logsize = filestatus.st_size;
    std::cout << "Initial log [" << fname << "] " << logsize << " bytes\n";


    ::srand(12345); // use fixed seed for repeatability and easier debugging
    W_DO(ssm->begin_xct());
    test_env->set_xct_query_lock();
    for (size_t produced=1; produced <= records; ++produced) { //produced output records
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
	if (rc.err_num() != eDUPLICATE) {
	  cerr << "unexpected error";
	  return rc;
	}
      }

      // commit every other record and check log growth
      if (produced%2 == 0) {

	W_DO(ssm->commit_xct());

        // Check for roll-over
        cur_partition_number = smlevel_0::log->partition_num();
        if (cur_partition_number != prev_partition_number)
        {
          smlevel_0::log->make_log_name(cur_partition_number, fname, smlevel_0::max_devname);
          std::cout << "Log rolled over. " << test_env->log_dir
                    << " now contains "
                    << fname << "\n";

          stat( fname, &filestatus );
          logsize2 = filestatus.st_size;
	  logsize = logsize2;
          prev_partition_number = cur_partition_number;
        } else
        {
          stat( fname, &filestatus );
          logsize2 = filestatus.st_size;

          // Assert that the log partition increased in size
	  assert (logsize <= logsize2);

	  // Assert that the log grew by logpagesize
	  assert (0 == logsize % logpagesize);

          logsize = logsize2;
        }

	logsize = filestatus.st_size;

      // print informative message every 1000 records
      if (produced%1000 == 0) {
        std::cout << "Log [" << fname << "] " << logsize << " bytes; ";
	std::cout << logsize/logpagesize << " pages; "
		  << " Processed " << produced << " keys" << std::endl;
        }

	W_DO(ssm->begin_xct());
	test_env->set_xct_query_lock();
      }
    }

    // Assert that the log has rolled over at least once
#ifdef LOG_BUFFER
    // this assert would fail with the new log buffer
#else
    assert (initial_partition_number < cur_partition_number);
#endif
    W_DO(ssm->commit_xct());
    return RCOK;
}

sm_options make_options() {
    sm_options options;
    // MUCH larger than usual testcases
    options.set_int_option("sm_locktablesize", 1 << 14);
    options.set_int_option("sm_bufpoolsize", SM_PAGESIZE / 1024 * 1024);
    options.set_int_option("sm_rawlock_lockpool_segsize", 1 << 14);
    return options;
}

TEST (BtreeBasicTest2, DoSomeLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(dosome, true, 4096, make_options()), 0);
}


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
