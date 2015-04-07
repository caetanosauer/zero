#include "btree_test_env.h"

#include <sstream>

#include "logarchiver.h"

btree_test_env* test_env;
stid_t stid;
lpid_t root_pid;
char HUNDRED_BYTES[100];

// use small block to test boundaries
const size_t BLOCK_SIZE = 8192;

class ArchiverTest {
private:
    LogArchiver::ReaderThread* reader;
    AsyncRingBuffer* readbuf;
};

typedef w_rc_t rc_t;

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

rc_t consumerTest(ss_m* ssm, test_volume_t* test_vol)
{
    unsigned howManyToInsert = 1000;
    W_DO(populateBtree(ssm, test_vol, howManyToInsert));
    lsn_t lastLSN = ssm->log->durable_lsn();

    lsn_t prevLSN = lsn_t(1,0);
    LogArchiver::LogConsumer cons(prevLSN, BLOCK_SIZE);
    cons.open(lastLSN);

    logrec_t* lr;
    unsigned int insertCount = 0;
    while (cons.next(lr)) {
        if (lr->type() == logrec_t::t_btree_insert ||
                lr->type() == logrec_t::t_btree_insert_nonghost)
        {
            insertCount++;
        }
        EXPECT_TRUE(lr->lsn_ck() > prevLSN);
        prevLSN = lr->lsn_ck();
    }

    EXPECT_EQ(howManyToInsert, insertCount);

    return RCOK;
}

TEST (LogArchiverTest, ConsumerTest) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(consumerTest), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
