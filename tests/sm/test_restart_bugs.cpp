#define SM_SOURCE

#define SM_LEVEL 0

#include "sm_int_1.h"
#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include "bf.h"
#include "xct.h"

#include "smthread.h"
#include "sthread.h"

/* This class contains only test cases that are failing at the time.
 * The issues that cause the test cases to fail are tracked in the bug reporting system,
 * the associated issue ID is noted beside each test case. 
 * Since they would block the check-in process, all test cases are disabled. 
 */

btree_test_env *test_env;
int next_thid = 10;

lsn_t get_durable_lsn() {
    lsn_t ret;
    ss_m::get_durable_lsn(ret);
    return ret;
}
void output_durable_lsn(int W_IFDEBUG1(num)) {
    DBGOUT1( << num << ".durable LSN=" << get_durable_lsn());
}


/* Test case with an uncommitted transaction, no checkpoint, simulated crash shutdown
 * It is currently failing because the current implementation for simulated crash shutdown
 * is unable to handle an in-flight transaction with multiple inserts.
 * A bug report concerning this issue has been submitted. (ZERO-182) 
 */
class restart_complic_inflight_crash_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa4", "data4"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));

        // Start a transaction but no commit, normal shutdown
        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "aa2", "data2"));
        W_DO(test_env->btree_insert(_stid, "aa5", "data5"));
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa4"), s.maxkey);
        return RCOK;
    }
};

/* Disabled because it's failing
 * TEST (RestartTestBugs, InflightCrashShutdownFailing) {
 *   test_env->empty_logdata_dir();
 *   restart_complic_inflight_crash_shutdown context;
 *   EXPECT_EQ(test_env->runRestartTest(&context, true, 10), 0);  // true = simulated crash
 *                                                                // 10 = recovery mode, m1 default serial mode
 * } 
 */


/* Trying to implement thread handling here */

class transact_thread_t : public smthread_t {
public:
	transact_thread_t(stid_t stid, void (*runfunc)(stid_t)) : smthread_t(t_regular, "transact_thread_t"), _stid(stid) {
	    _runnerfunc = runfunc;
	    _thid = next_thid++;
	}
	~transact_thread_t() {}
	

	virtual void run() {
	    std::cout << ":T" << _thid << " starting..";
	    _runnerfunc(_stid);
	    _finished = true;
	    std::cout << ":T" << _thid << " finished.";
	}

	stid_t _stid;
	int _thid;
	void (*_runnerfunc)(stid_t);
	bool _finished;
};



class restart_multithrd_normal_shutdown : public restart_test_base
{
public:
    static void t1Run(stid_t pstid) {
	test_env->btree_insert_and_commit(pstid, "aa1", "data1");
    }

    static void t2Run(stid_t pstid) {
	test_env->btree_insert_and_commit(pstid, "aa2", "data2");
    }

    w_rc_t pre_shutdown(ss_m *ssm) {
	output_durable_lsn(1);
	W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
	output_durable_lsn(2);
	transact_thread_t t1 (_stid, t1Run);
	transact_thread_t t2 (_stid, t2Run);
	output_durable_lsn(3);

	W_DO(t1.fork());
	W_DO(t2.fork());
	W_DO(t1.join());
	W_DO(t2.join());

	EXPECT_TRUE(t1._finished);
	EXPECT_TRUE(t2._finished);
	return RCOK;
    }


    w_rc_t post_shutdown(ss_m *) {
	output_durable_lsn(4);
	x_btree_scan_result s;
	W_DO(test_env->btree_scan(_stid, s));
	EXPECT_EQ (2, s.rownum);
	EXPECT_EQ (std::string("aa1"), s.minkey);
	EXPECT_EQ (std::string("aa2"), s.maxkey);
	return RCOK;
    }
};

TEST (RestartTestBugs, MultithrdNormal) {
    test_env->empty_logdata_dir();
    restart_multithrd_normal_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, false, 10), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
