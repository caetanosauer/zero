
#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include "generic_page.h"
#include "bf.h"
#include "btree_p.h"
#include "e_error_def_gen.h"
#include <sstream>

//#include <sys/types.h>
#include <sys/stat.h>
//#include <unistd.h>

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
    size_t const records= 7500; //26900 is max that currently works 
    off_t  const logpagesize = 8192; // quantum of log file size
    off_t  logsize = 0; // log file size. Should grow monotonically.
    int logfnum = 0; // log file number
    int ibuffer; // hold the random int 
    
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
	if (rc.err_num() != ss_m::eDUPLICATE) {
	  cerr << "unexpected error";
	  return rc;
	}
      }


      if (produced%2 == 1) { // Fails with <=85 for records=10000 
                              //==99 for records=27000
	W_DO(ssm->commit_xct());
	// OZ check for log file growth

	struct stat buf;

	std::ostringstream fname2;
	fname2 << "log/log." << logfnum+1;
	if(0 == stat(fname2.str().c_str(), &buf)) { // next log file appeared
	  std::cout << "log/log." << logfnum   << " ==> " 
		    << "log/log." << logfnum+1 << " rollover" << std::endl; 
	  ++logfnum;
	  logsize = 0;
	}

	std::ostringstream fname;
	fname << "log/log." << logfnum;
	std::cout << fname.str();
	assert (0 == stat(fname.str().c_str(), &buf));
	std::cout << " Verifying " << std::setw(3) << logsize/logpagesize 
		  << (logsize < buf.st_size ? " < " : " = ")        
		  << std::setw(3) << buf.st_size/logpagesize;
	//assert (logsize < buf.st_size); // should grow by logpagesize every logpagesize/512 records
	logsize = buf.st_size;
	assert (0 == logsize % logpagesize);
	std::cout << " Log file size " << logsize/logpagesize
		  << " Processed " << produced << " keys" << std::endl;
	

	W_DO(ssm->begin_xct());
	test_env->set_xct_query_lock();

      }
    }
    W_DO(ssm->commit_xct());
    return RCOK;
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
