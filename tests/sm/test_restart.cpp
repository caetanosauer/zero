#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include "bf.h"
#include "xct.h"

btree_test_env *test_env;

// Testcases to test restart.

lsn_t get_durable_lsn() {
    lsn_t ret;
    ss_m::get_durable_lsn(ret);
    return ret;
}
void output_durable_lsn(int W_IFDEBUG1(num)) {
    DBGOUT1( << num << ".durable LSN=" << get_durable_lsn());
}

// Test case without any operation, start and normal shutdown SM
class restart_empty : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *) {
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        return RCOK;
    }
};
/**/
TEST (RestartTest, Empty) {
    test_env->empty_logdata_dir();
    restart_empty context;
    EXPECT_EQ(test_env->runRestartTest(&context, false), 0);  // false = no simulated crash, normal shutdown
}
/**/

// Test case without checkpoint and normal shutdown
class restart_normal_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) 
        {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa4", "data4"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa2", "data2"));
        output_durable_lsn(3);
        return RCOK;
        }

    w_rc_t post_shutdown(ss_m *) 
        {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (4, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa4"), s.maxkey);
        return RCOK;
        }
};

/* Passing */
TEST (RestartTest, NormalShutdown) 
    {
    test_env->empty_logdata_dir();
    restart_normal_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, false), 0);  // false = no simulated crash, normal shutdown
    }
/**/

// Test case with checkpoint and normal shutdown
class restart_checkpoint_normal_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) 
        {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa4", "data4"));
        W_DO(ss_m::checkpoint());
        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa2", "data2"));
        // If enabled the 2nd checkpoint, it is passing also
        // W_DO(ss_m::checkpoint());
        output_durable_lsn(3);
        return RCOK;
        }

    w_rc_t post_shutdown(ss_m *) 
        {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (4, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa4"), s.maxkey);
        return RCOK;
        }
};

/* Passing */
TEST (RestartTest, NormalCheckpointShutdown) 
    {
    test_env->empty_logdata_dir();
    restart_checkpoint_normal_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, false), 0);  // false = no simulated crash, normal shutdown
    }
/**/

// Test case with more than one page of data, without checkpoint and normal shutdown
class restart_many_normal_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) 
        {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);

        // Set the data size is the max. entry size minue key size minus 1
        // because the total size must be smaller than btree_m::max_entry_size()
        const int keysize = 5;
        const int datasize = btree_m::max_entry_size() - keysize - 1;

        vec_t data;
        char datastr[datasize];
        memset(datastr, '\0', datasize);
        data.set(datastr, datasize);
        w_keystr_t key;
        char keystr[keysize];
        keystr[0] = 'k';
        keystr[1] = 'e';
        keystr[2] = 'y';

        // Insert enough records to ensure page split
        // One big transaction with multiple insertions
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        for (int i = 0; i < recordCount; ++i) 
            {
            int num;
            num = recordCount - 1 - i;

            keystr[3] = ('0' + ((num / 10) % 10));
            keystr[4] = ('0' + (num % 10));
            key.construct_regularkey(keystr, keysize);
            W_DO(test_env->begin_xct());
            W_DO(ssm->create_assoc(_stid, key, data));
            W_DO(test_env->commit_xct());
            }
 
        output_durable_lsn(3);
        return RCOK;
        }

    w_rc_t post_shutdown(ss_m *) 
        {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        EXPECT_EQ (recordCount, s.rownum);
        return RCOK;
        }
};

/* Passing */
TEST (RestartTest, NormalManyShutdown) 
    {
    test_env->empty_logdata_dir();
    restart_many_normal_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, false), 0);  // false = no simulated crash, normal shutdown
    }
/**/

// Test case with more than one page of data, with checkpoint and normal shutdown
class restart_many_checkpoint_normal_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) 
        {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);

        // Set the data size is the max. entry size minue key size minus 1
        // because the total size must be smaller than btree_m::max_entry_size()
        const int keysize = 5;
        const int datasize = btree_m::max_entry_size() - keysize - 1;

        vec_t data;
        char datastr[datasize];
        memset(datastr, '\0', datasize);
        data.set(datastr, datasize);
        w_keystr_t key;
        char keystr[keysize];
        keystr[0] = 'k';
        keystr[1] = 'e';
        keystr[2] = 'y';

        // Insert enough records to ensure page split
        // One transaction per insertion
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        for (int i = 0; i < recordCount; ++i) 
            {
            int num;
            num = recordCount - 1 - i;

            keystr[3] = ('0' + ((num / 10) % 10));
            keystr[4] = ('0' + (num % 10));
            key.construct_regularkey(keystr, keysize);

            // Take one checkpoint half way through insertions
            if (num == recordCount/2)
               W_DO(ss_m::checkpoint()); 

            W_DO(test_env->begin_xct());
            W_DO(ssm->create_assoc(_stid, key, data));
            W_DO(test_env->commit_xct());
            }

        // If enabled the 2nd checkpoint, it is passing also
        // W_DO(ss_m::checkpoint()); 

        output_durable_lsn(3);
        return RCOK;
        }

    w_rc_t post_shutdown(ss_m *) 
        {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        EXPECT_EQ (recordCount, s.rownum);
        return RCOK;
        }
};

/* AV: restart.cpp:250, xd->state() == xct_t::xct_active, does not repro every time 
TEST (RestartTest, NormalManyCheckpointShutdown) 
    {
    test_env->empty_logdata_dir();
    restart_many_checkpoint_normal_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, false), 0);  // false = no simulated crash, normal shutdown
    }
**/

// Test case with an uncommitted transaction, no checkpoint, normal shutdown
class restart_inflight_normal_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) 
        {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa4", "data4"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));

        // Start a transaction but no commit before normal shutdown
        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "aa2", "data2"));
        output_durable_lsn(3);
        return RCOK;
        }

    w_rc_t post_shutdown(ss_m *) 
        {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa4"), s.maxkey);
        return RCOK;
        }
};

/* Passing in retail build */
/* Core dump in debug build - /projects/Zero/src/sm/restart.cpp:446, assert: r.is_redo() */
/* Question: can a system normal shutdown with in-flight transaction?  Would restart go through the crash code path? *
TEST (RestartTest, InflightNormalShutdown) 
    {
    test_env->empty_logdata_dir();
    restart_inflight_normal_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, false), 0);  // false = no simulated crash, normal shutdown
    }
**/

// Test case with an uncommitted transaction, checkpoint, normal shutdown
class restart_inflight_checkpoint_normal_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) 
        {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa4", "data4"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
        W_DO(ss_m::checkpoint()); 

        // Start a transaction but no commit, checkpoint, and then normal shutdown
        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "aa2", "data2"));
        W_DO(ss_m::checkpoint()); 
        output_durable_lsn(3);
        return RCOK;
        }

    w_rc_t post_shutdown(ss_m *) 
        {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa4"), s.maxkey);
        return RCOK;
        }
};

/* Passing in retail build */
/* Core dump in debug build - /projects/Zero/src/sm/restart.cpp:446, assert: r.is_redo() */
/* Same behavior as the test case without checkpoint*/
/* Question: can a system normal shutdown with in-flight transaction?  Would restart go through the crash code path? *
TEST (RestartTest, InflightcheckpointNormalShutdown) 
    {
    test_env->empty_logdata_dir();
    restart_inflight_checkpoint_normal_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, false), 0);  // false = no simulated crash, normal shutdown
    }
**/

// Test case with an uncommitted transaction, no checkpoint, simulated crash shutdown
class restart_inflight_crash_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) 
        {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa4", "data4"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));

        // Start a transaction but no commit, normal shutdown
        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "aa2", "data2"));
        output_durable_lsn(3);
        return RCOK;
        }

    w_rc_t post_shutdown(ss_m *) 
        {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa4"), s.maxkey);
        return RCOK;
        }
};

/* Passing *
TEST (RestartTest, InflightCrashShutdown) 
    {
    test_env->empty_logdata_dir();
    restart_inflight_crash_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, true), 0);  // true = simulated crash
    }
**/

// Test case with an uncommitted transaction, checkpoint, simulated crash shutdown
class restart_inflight_checkpoint_crash_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) 
        {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa4", "data4"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
        W_DO(ss_m::checkpoint()); 

        // Start a transaction but no commit, checkpoint, and then normal shutdown
        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "aa2", "data2"));
        W_DO(ss_m::checkpoint()); 
        output_durable_lsn(3);
        return RCOK;
        }

    w_rc_t post_shutdown(ss_m *) 
        {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa4"), s.maxkey);
        return RCOK;
        }
};

/* Passing *
TEST (RestartTest, InflightCheckpointCrashShutdown) 
    {
    test_env->empty_logdata_dir();
    restart_inflight_checkpoint_crash_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, true), 0);  // true = simulated crash
    }
**/

// Test case with an uncommitted transaction, more than one page of data, no checkpoint, simulated crash shutdown
class restart_inflight_many_crash_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) 
        {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);

        // Set the data size is the max. entry size minue key size minus 1
        // because the total size must be smaller than btree_m::max_entry_size()
        const int keysize = 5;
        const int datasize = btree_m::max_entry_size() - keysize - 1;

        vec_t data;
        char datastr[datasize];
        memset(datastr, '\0', datasize);
        data.set(datastr, datasize);
        w_keystr_t key;
        char keystr[keysize];
        keystr[0] = 'k';
        keystr[1] = 'e';
        keystr[2] = 'y';

        // Insert enough records to ensure page split
        // One big transaction with multiple insertions
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        for (int i = 0; i < recordCount; ++i) 
            {
            int num;
            num = recordCount - 1 - i;

            keystr[3] = ('0' + ((num / 10) % 10));
            keystr[4] = ('0' + (num % 10));
            key.construct_regularkey(keystr, keysize);
            W_DO(test_env->begin_xct());
            W_DO(ssm->create_assoc(_stid, key, data));
            W_DO(test_env->commit_xct());
            }
        output_durable_lsn(3);

        // In-flight transaction, no commit
        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "aa1", "data1"));
        
        output_durable_lsn(4);
        return RCOK;
        }

    w_rc_t post_shutdown(ss_m *) 
        {
        output_durable_lsn(5);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        EXPECT_EQ (recordCount, s.rownum);
        return RCOK;
        }
};

/* Passing */
/* if execute test case 'restart_empty' first and then followed by this test case, AV: btree_page.h:518, item>=0 && item<nitems *
TEST (RestartTest, InflightManyCrashShutdown) 
    {
    test_env->empty_logdata_dir();
    restart_inflight_many_crash_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, true), 0);  // true = simulated crash
    }
**/

// Test case with an uncommitted transaction, more than one page of data, checkpoint, simulated crash shutdown
class restart_inflight_ckpt_many_crash_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) 
        {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);

        // Set the data size is the max. entry size minue key size minus 1
        // because the total size must be smaller than btree_m::max_entry_size()
        const int keysize = 5;
        const int datasize = btree_m::max_entry_size() - keysize - 1;

        vec_t data;
        char datastr[datasize];
        memset(datastr, '\0', datasize);
        data.set(datastr, datasize);
        w_keystr_t key;
        char keystr[keysize];
        keystr[0] = 'k';
        keystr[1] = 'e';
        keystr[2] = 'y';

        // Insert enough records to ensure page split
        // One big transaction with multiple insertions
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        for (int i = 0; i < recordCount; ++i) 
            {
            int num;
            num = recordCount - 1 - i;

            keystr[3] = ('0' + ((num / 10) % 10));
            keystr[4] = ('0' + (num % 10));
            key.construct_regularkey(keystr, keysize);

            // Take one checkpoint half way through insertions
            if (num == recordCount/2)
               W_DO(ss_m::checkpoint()); 

            W_DO(test_env->begin_xct());
            W_DO(ssm->create_assoc(_stid, key, data));
            W_DO(test_env->commit_xct());
            }
        output_durable_lsn(3);

        // 2nd checkpoint before the in-flight transaction
        // W_DO(ss_m::checkpoint());

        // In-flight transaction, no commit
        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "aa1", "data1"));
        
        output_durable_lsn(4);
        return RCOK;
        }

    w_rc_t post_shutdown(ss_m *) 
        {
        output_durable_lsn(5);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        EXPECT_EQ (recordCount, s.rownum);
        return RCOK;
        }
};

/* AV, btree_page_h.cpp:961, right_begins_from >= 0 && right_begins_from <= nrecs() *
TEST (RestartTest, InflightCkptManyCrashShutdown) 
    {
    test_env->empty_logdata_dir();
    restart_inflight_ckpt_many_crash_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, true), 0);  // true = simulated crash
    }
**/

// Test case with committed transactions,  no checkpoint, simulated crash shutdown
class restart_crash_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) 
        {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa4", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa5", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa2", "data1"));
        output_durable_lsn(3);
        return RCOK;
        }

    w_rc_t post_shutdown(ss_m *) 
        {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (5, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa5"), s.maxkey);
        return RCOK;
        }
};

/* Passing *
TEST (RestartTest, CrashShutdown) 
    {
    test_env->empty_logdata_dir();
    restart_crash_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, true), 0);  // true = simulated crash
    }
**/

// Test case with committed transactions,  checkpoint, simulated crash shutdown
class restart_checkpoint_crash_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) 
        {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa4", "data1"));
        W_DO(ss_m::checkpoint());
        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa5", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa2", "data1"));
        // If enabled the 2nd checkpoint...
        // W_DO(ss_m::checkpoint());
        output_durable_lsn(3);
        return RCOK;
        }

    w_rc_t post_shutdown(ss_m *) 
        {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (5, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa5"), s.maxkey);
        return RCOK;
        }
};

/* AV: fixable_page_h.h:32, s->tag == t_btree_p *
TEST (RestartTest, CheckpointCrashShutdown) 
    {
    test_env->empty_logdata_dir();
    restart_checkpoint_crash_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, true), 0);  // true = simulated crash
    }
**/

// Test case with committed transactions,  more than one page of data, no checkpoint, simulated crash shutdown
class restart_many_crash_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) 
        {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);

        // Set the data size is the max. entry size minue key size minus 1
        // because the total size must be smaller than btree_m::max_entry_size()
        const int keysize = 5;
        const int datasize = btree_m::max_entry_size() - keysize - 1;

        vec_t data;
        char datastr[datasize];
        memset(datastr, '\0', datasize);
        data.set(datastr, datasize);
        w_keystr_t key;
        char keystr[keysize];
        keystr[0] = 'k';
        keystr[1] = 'e';
        keystr[2] = 'y';

        // Insert enough records to ensure page split
        // One transaction per insertion
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        for (int i = 0; i < recordCount; ++i) 
            {
            int num;
            num = recordCount - 1 - i;

            keystr[3] = ('0' + ((num / 10) % 10));
            keystr[4] = ('0' + (num % 10));
            key.construct_regularkey(keystr, keysize);

            W_DO(test_env->begin_xct());
            W_DO(ssm->create_assoc(_stid, key, data));
            W_DO(test_env->commit_xct());
            }

        output_durable_lsn(3);
        return RCOK;
        }

    w_rc_t post_shutdown(ss_m *) 
        {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        EXPECT_EQ (recordCount, s.rownum);
        return RCOK;
        }
};

/* Passing */
/* if execute test case 'restart_empty' first and then followed by this test case, AV: btree_page.h:518, item>=0 && item<nitems *
TEST (RestartTest, ManyCrashShutdown) 
    {
    test_env->empty_logdata_dir();
    restart_many_crash_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, true), 0);  // true = simulated crash
    }
**/

// Test case with committed transactions,  more than one page of data, checkpoint, simulated crash shutdown
class restart_many_ckpt_crash_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) 
        {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);

        // Set the data size is the max. entry size minue key size minus 1
        // because the total size must be smaller than btree_m::max_entry_size()
        const int keysize = 5;
        const int datasize = btree_m::max_entry_size() - keysize - 1;

        vec_t data;
        char datastr[datasize];
        memset(datastr, '\0', datasize);
        data.set(datastr, datasize);
        w_keystr_t key;
        char keystr[keysize];
        keystr[0] = 'k';
        keystr[1] = 'e';
        keystr[2] = 'y';

        // Insert enough records to ensure page split
        // One transaction per insertion
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        for (int i = 0; i < recordCount; ++i) 
            {
            int num;
            num = recordCount - 1 - i;

            keystr[3] = ('0' + ((num / 10) % 10));
            keystr[4] = ('0' + (num % 10));
            key.construct_regularkey(keystr, keysize);

            // Take one checkpoint half way through insertions
            if (num == recordCount/2)
               W_DO(ss_m::checkpoint()); 

            W_DO(test_env->begin_xct());
            W_DO(ssm->create_assoc(_stid, key, data));
            W_DO(test_env->commit_xct());
            }

        output_durable_lsn(3);
        return RCOK;
        }

    w_rc_t post_shutdown(ss_m *) 
        {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        EXPECT_EQ (recordCount, s.rownum);
        return RCOK;
        }
};

/* AV, btree_page_h.cpp:961, right_begins_from >= 0 && right_begins_from <= nrecs() *
TEST (RestartTest, ManyCkptCrashShutdown) 
    {
    test_env->empty_logdata_dir();
    restart_many_ckpt_crash_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, true), 0);  // true = simulated crash
    }
**/

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
