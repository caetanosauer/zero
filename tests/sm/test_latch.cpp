#include "w_defines.h"

#include <cstdlib>
#include <ctime>
#include "w_base.h"

#include "latches.h"
#include "latch.h"
#include "thread_wrapper.h"

#include <iostream>

#define NUM_THREADS 3
#define LAST_THREAD (NUM_THREADS-1)

#include "gtest/gtest.h"

latch_t  the_latch;
queue_based_block_lock_t print_mutex;
// sevsem_t done;
int      done_count;
pthread_cond_t  done; // paried with done_mutex
pthread_mutex_t done_mutex = PTHREAD_MUTEX_INITIALIZER; // paired with done
#if W_DEBUG_LEVEL > 3
bool     verbose(true);
#else // W_DEBUG_LEVEL
bool     verbose(false);
#endif // W_DEBUG_LEVEL

int      testnum = -1;

struct latch_thread_id_t {
    latch_thread_id_t (int x): _id(x) {};
    int _id;
};

class latch_thread_t : public thread_wrapper_t {
public:
    latch_thread_t(int id);
    ~latch_thread_t() {
                CRITICAL_SECTION(cs, lock);
        hasExited = true;
    }

    static void sync_other(latch_thread_t *r);

protected:
    typedef enum { all_sh, one_ex, all_ex } testKind;
    static const char *kind_strings[];
    void run();
    void test1(int i, testKind t);
    void test2(int i, latch_mode_t mode1, latch_mode_t mode2);
    void test3(int i, testKind t);
private:
    latch_thread_id_t     _self;
    bool                  isWaiting;
    bool                  canProceed;
    bool                  hasExited;
    pthread_cond_t        quiesced;
    pthread_cond_t        proceed;
    pthread_mutex_t       lock;

    void sync() {
                CRITICAL_SECTION(cs, lock);
        isWaiting=true;
                DO_PTHREAD(pthread_cond_signal(&quiesced));
        while(!canProceed) {
                        DO_PTHREAD(pthread_cond_wait(&proceed, &lock));
                }
        canProceed = false;
    }
};

latch_thread_t::latch_thread_t(int _id)
:
  _self(_id),
  isWaiting(false),canProceed(false),hasExited(false)
{
    DO_PTHREAD(pthread_cond_init(&quiesced, NULL));
    DO_PTHREAD(pthread_cond_init(&proceed, NULL));
    DO_PTHREAD(pthread_mutex_init(&lock, NULL));
}

const char *latch_thread_t::kind_strings[] = {
    "all_sh",
    "one_ex",
     "all_ex"
};

void latch_thread_t::sync_other(latch_thread_t *r)
{
        CRITICAL_SECTION(cs, &r->lock);
    while(!(r->isWaiting || r->hasExited)) {
                DO_PTHREAD(pthread_cond_wait(&r->quiesced, &r->lock));
        }
    r->isWaiting = false;
    r->canProceed = true;
        DO_PTHREAD(pthread_cond_signal(&r->proceed));
}

void
dump(ostream &o, latch_t &l)
{
    o << " mode " << latch_t::latch_mode_str[ l.mode() ]
    << " num_holders " << l.num_holders()
    << " latch_cnt " << l.latch_cnt()
    << " is_latched " << (const char *)(l.is_latched()?"true":"false")
    << " is_mine " << (const char *)(l.is_mine()?"true":"false")
    << " held_by_me " << l.held_by_me()
    << endl;
}

void
check(  int line,
    const char *msg,
    latch_t &l,
    latch_mode_t expected,
    latch_mode_t m1,
    latch_mode_t m2,
    int holders,
    int latch_cnt,
    bool is_latched,
    bool is_mine,
    int held_by_me
)
{
        {
                CRITICAL_SECTION(cs, print_mutex);
                if (verbose) {
                    cout << endl;
                    cout << " {---------------------- "
                    << latch_t::latch_mode_str[m1] << " / " << latch_t::latch_mode_str[m2]
                    << "  ------------------" << line << endl; /*}*/
                    cout << "\t" << msg << endl;
                    dump(cout, l);
                }
        }

    bool do_asserts(true);
    if(do_asserts)
    {
    int last_failure(0);
    int failure(0);
    if(!(l.mode() == expected)) { failure++; last_failure=__LINE__; }
    if(!(l.num_holders() == holders)) { failure++; last_failure=__LINE__; }
    if(!(l.latch_cnt() == latch_cnt)) { failure++; last_failure=__LINE__; }
    if(!(l.is_latched() == is_latched)) { failure++; last_failure=__LINE__; }
    if(!(l.is_mine() == is_mine)) { failure++; last_failure=__LINE__; }
    if(!(l.held_by_me() == held_by_me)) { failure++; last_failure=__LINE__; }
    if(failure > 0) {
                CRITICAL_SECTION(cs, print_mutex);
        EXPECT_TRUE(false) << l << endl
            << "# failures: " << failure
            << " last @" << last_failure
            << endl << endl;
    }
    }
    if (verbose) {
        cout << " ---------------------------------------------------"
        << line << "}" << endl;
    }
}

ostream &
operator << (ostream &o, const latch_thread_id_t &ID)
{
    int j = ID._id;
    for(int i=0; i < j; i++) {
    o << "-----";
    }
    o << "----- " << ID._id << ":" ;
    return o;
}

// int i gives an id of the thread that's supposed to acquire an EX-mode
// if anyone is to do so.
//  -1 means noone, else it should be the id of some thread in the range
void latch_thread_t::test1(int i, testKind t)
{
    if(verbose)  {
                CRITICAL_SECTION(cs, print_mutex);
        cout << _self << " starting test 1 id=" << i << "  " << kind_strings[int(t)]
    << endl;
        cout << _self << " await sync @" << __LINE__ << endl;
    }

    sync(); // test1 #1

    if(verbose)  {
                CRITICAL_SECTION(cs, print_mutex);
        cout << _self << " test 1 got sync " <<  endl;
        cout << _self << " await acquire @" << __LINE__ << endl;
    }

    latch_mode_t mode = LATCH_SH;
    switch(t) {
    case one_ex:
        if(i == _self._id) mode = LATCH_EX;
        break;
    case all_ex:
        mode = LATCH_EX;
        break;
    case all_sh:
    default:
        break;
    }
    // START test 1: everyone acquire in given mode
    if(verbose)  {
                CRITICAL_SECTION(cs, print_mutex);
        cout << _self << " latch_acquire mode= "
            << mode  << "  @" << __LINE__ << endl;
        // print_all_latches();
    }
    the_latch.latch_acquire(mode);

    // yield();

    EXPECT_TRUE(the_latch.mode() == mode);
    EXPECT_TRUE(the_latch.num_holders() > 0);
    EXPECT_TRUE(the_latch.latch_cnt() > 0);
    EXPECT_TRUE(the_latch.is_latched() == true);
    EXPECT_TRUE(the_latch.is_mine() == (mode == LATCH_EX));
    EXPECT_TRUE(the_latch.held_by_me() == 1);

    if(verbose)  {
                CRITICAL_SECTION(cs, print_mutex);
        cout << _self << " latch_release @" << __LINE__ << endl;
    }

    the_latch.latch_release();
    // yield();

    EXPECT_TRUE(the_latch.is_mine() == false);
    EXPECT_TRUE(the_latch.held_by_me() == 0);

    if(verbose)  {
                CRITICAL_SECTION(cs, print_mutex);
        cout << _self << " end test 1 " <<  i << endl;
    }

        {
                CRITICAL_SECTION(cs, done_mutex);
                done_count++;
        }
        DO_PTHREAD(pthread_cond_signal(&done));
}

const char* const  latch_mode_str[3] = { "NL", "SH", "EX" };

//
// test2 is performed by only one thread, so there are
// no races in checking the status of the latch (check(), assertions)
//
void latch_thread_t::test2(int i, latch_mode_t mode1,
    latch_mode_t mode2)
{
    // Make only one thread do anything here
    if(verbose)  {
                CRITICAL_SECTION(cs, print_mutex);
        cout << _self
        << "--------- test2 STARTING " << " modes="
        << latch_mode_str[int(mode1)] << ", " << latch_mode_str[int(mode2)]
        << endl;
        cout << _self << " await sync @" << __LINE__ << endl;
    }
    sync();  // test2 #1

    if(verbose)  {
                CRITICAL_SECTION(cs, print_mutex);
        cout << _self << "--------- test2 got sync " << endl;
    }

    if(i == _self._id)
    {
        bool is_upgrade(false);
        // NOTE: this use of the word "upgrade" is a little misleading.
        // The counts don't get increased for a "real" upgrade, but they
        // do for a duplicate latch, regardless of the mode.
        if(mode2 > mode1)
        {
            is_upgrade=true;
        }

        check(__LINE__,
            "before first acquire ",
            the_latch,
            LATCH_NL /* expected */,
            mode1, mode2,
            0 /* holders */,
            0 /* latch_cnt */,
            false /* is_latched */,
            false /* is_mine */,
            0 /* # held_by_me */
         );

        if(verbose)  {
                        CRITICAL_SECTION(cs, print_mutex);
            cout << _self << " latch_acquire mode= "
                << mode1  << "  @" << __LINE__ << endl;
        }

        // FIRST ACQUIRE
        the_latch.latch_acquire(mode1);

        check(__LINE__,
            " after first acquire ",
            the_latch,
            (mode1==LATCH_NL)?LATCH_EX:mode1 /* expected */,
            mode1, mode2,
            1 /* holders */,
            1 /* latch_cnt */,
            true /* is_latched */,
            (mode1==LATCH_EX) /* is_mine */,
            (mode1==LATCH_NL)?0:1 /* # held_by_me */
         );

        if(verbose)  {
                        CRITICAL_SECTION(cs, print_mutex);
            cout << _self << " double latch_acquire mode= "
                << mode2  << "  @" << __LINE__ << endl;
        }

        // 2nd ACQUIRE
        the_latch.latch_acquire(mode2);
        if(is_upgrade)
        {
            check(__LINE__,
            " after 2nd acquire (upgrade) ",
            the_latch,
            mode2 /* expected */,
            mode1, mode2,
            1 /* holders */,
            2 /* latch_cnt */,
            true /* is_latched */,
            (mode2==LATCH_EX) /* is_mine */,
            2 /* # held_by_me */
             );
        }
        else
        {
            check(__LINE__,
            " after 2nd acquire (duplicate) ",
            the_latch,
            mode2 /* expected */,
            mode1, mode2,
            1 /* holders */,
            2 /* latch_cnt */,
            true /* is_latched */,
            (mode2==LATCH_EX) /* is_mine */,
            2 /* # held_by_me */
             );
        }

        if(verbose)  {
                        CRITICAL_SECTION(cs, print_mutex);
            cout << _self << " latch_release @" << __LINE__ << endl;
        }

        // FIRST RELEASE
        the_latch.latch_release();
        if(is_upgrade)
        {
            check(__LINE__,
            " after first release (from upgrade) ",
            the_latch,
            LATCH_EX /* expected */,
            mode1, mode2,
            1 /* holders */,
            1 /* latch_cnt */,
            true /* is_latched */,
            true /* is_mine */, // --- note that this is still in EX mode
            1 /* # held_by_me */
             );
        }
        else
        {
            check(__LINE__,
            " after first release (from duplicate) ",
            the_latch,
            mode1 /* expected */,
            mode1, mode2,
            1 /* holders */,
            1 /* latch_cnt */,
            true /* is_latched */,
            (mode1==LATCH_EX) /* is_mine */,
            1 /* # held_by_me */
             );
        }

        if(verbose)  {
                        CRITICAL_SECTION(cs, print_mutex);
            cout << _self << " 2nd latch_release @" << __LINE__ << endl;
        }
        // 2nd RELEASE
        the_latch.latch_release();
        check(__LINE__,
            " after 2nd release",
            the_latch,
            LATCH_NL /* expected */,
            mode1, mode2,
            0 /* holders */,
            0 /* latch_cnt */,
            false /* is_latched */,
            false /* is_mine */,
            0 /* # held_by_me */
         );

    }
    else
    {
        if(verbose)  {
                        CRITICAL_SECTION(cs, print_mutex);
            cout << _self << "--------- test2 VACUOUS"  << endl;
        }
    }
    if(verbose)  {
                CRITICAL_SECTION(cs, print_mutex);
        cout << _self << "--------- test2 END  " << mode1 << ", " << mode2 << endl;
    }

        {
                CRITICAL_SECTION(cs, done_mutex);
                done_count++;
        }
        DO_PTHREAD(pthread_cond_signal(&done));
}

void latch_thread_t::test3(int i, testKind t)
{
    if(verbose)  {
                CRITICAL_SECTION(cs, print_mutex);
        cout << _self << " starting test 3 id=" << i << " " << kind_strings[int(t)]
    << endl;
        cout << _self << " await sync @" << __LINE__ << endl;
    }

    sync(); // test3 #1

    if(verbose)  {
                CRITICAL_SECTION(cs, print_mutex);
        cout << _self << " test 3 got sync " <<  endl;
        cout << _self << " await acquire @" << __LINE__ << endl;
    }

    latch_mode_t mode = LATCH_SH;
    switch(t) {
    case one_ex:
        if(i == _self._id) mode = LATCH_EX;
        break;
    case all_ex:
        mode = LATCH_EX;
        break;
    case all_sh:
    default:
        break;
    }
    // START test 3: everyone acquire in given mode
    if(verbose)  {
                CRITICAL_SECTION(cs, print_mutex);
        cout << _self << " latch_acquire mode= "
            << mode  << "  @" << __LINE__ << endl;
    }
    the_latch.latch_acquire(mode);

    // yield();

    EXPECT_TRUE(the_latch.mode() == mode);
    EXPECT_TRUE(the_latch.num_holders() > 0);
    EXPECT_TRUE(the_latch.latch_cnt() > 0);
    EXPECT_TRUE(the_latch.is_latched() == true);
    EXPECT_TRUE(the_latch.is_mine() == (mode == LATCH_EX));
    EXPECT_TRUE(the_latch.held_by_me() == 1);

    if(verbose)  {
                CRITICAL_SECTION(cs, print_mutex);
        cout << _self << " upgrade_if_not_block @" << __LINE__ << endl;
    }

    bool would_block(false);
    // This use of is_upgrade refers to a real upgrade
    // If we started with SH or NL mode, then upgrade will be true
    bool is_real_upgrade = (mode != LATCH_EX);

    W_COERCE(the_latch.upgrade_if_not_block(would_block));
    if(verbose)  {
                CRITICAL_SECTION(cs, print_mutex);
        cout << _self << " upgrade_if_not_block would "
        << (const char *)(would_block?"":" NOT ")
        << " have blocked "
        << __LINE__ << endl;
    }
    if(would_block) {
        // At least *I* should hold it in SH mode, even if the
        // others have released it already:
        EXPECT_TRUE(the_latch.mode() == LATCH_SH);
        EXPECT_TRUE(the_latch.num_holders() > 0);
        EXPECT_TRUE(the_latch.latch_cnt() > 0);
        EXPECT_TRUE(the_latch.is_latched() == true);
        EXPECT_TRUE(the_latch.is_mine() == (mode == LATCH_EX));
        EXPECT_TRUE(the_latch.held_by_me() == 1);
    } else if(is_real_upgrade)
    {
        // upgrade worked
        EXPECT_TRUE(the_latch.mode() == LATCH_EX);
        EXPECT_TRUE(the_latch.num_holders() == 1);
        EXPECT_TRUE(the_latch.latch_cnt() == 1);
        EXPECT_TRUE(the_latch.is_latched() == true);
        EXPECT_TRUE(the_latch.is_mine() == true);
        EXPECT_TRUE(the_latch.held_by_me() == 1);
    } else
    {
        EXPECT_TRUE(!is_real_upgrade);
        EXPECT_TRUE(mode == LATCH_EX);
        // not an upgrade because original mode was LATCH_EX
        EXPECT_TRUE(the_latch.mode() == mode);
        EXPECT_TRUE(the_latch.num_holders() == 1);
        EXPECT_TRUE(the_latch.latch_cnt() == 1);
        EXPECT_TRUE(the_latch.is_latched() == true);
        EXPECT_TRUE(the_latch.is_mine() == true);
        EXPECT_TRUE(the_latch.held_by_me() == 1);
    }

    if(verbose)  {
                CRITICAL_SECTION(cs, print_mutex);
        cout << _self << " latch_release @" << __LINE__ << endl;
    }

    if( is_real_upgrade && !would_block ) {
        // upgrade worked, so these shouldn't have changed
        EXPECT_TRUE(the_latch.mode() == LATCH_EX);
        EXPECT_TRUE(the_latch.num_holders() == 1);
        EXPECT_TRUE(the_latch.latch_cnt() == 1);
        EXPECT_TRUE(the_latch.is_latched() == true);
        EXPECT_TRUE(the_latch.is_mine() == true);
        EXPECT_TRUE(the_latch.held_by_me() == 1);

        // first release
        the_latch.latch_release();
        // yield();

        EXPECT_TRUE(the_latch.is_mine() == false);
        EXPECT_TRUE(the_latch.held_by_me() == 0);
        // RACY EXPECT_TRUE(the_latch.latch_cnt() == 0);
        // RACY EXPECT_TRUE(the_latch.mode() == LATCH_NL);
    } else  if(would_block) {
        // would have blocked - hold it only once
        // first release
        the_latch.latch_release();
        // yield();

        EXPECT_TRUE(the_latch.is_mine() == false);
        EXPECT_TRUE(the_latch.held_by_me() == 0);
    } else {
        EXPECT_TRUE(!is_real_upgrade);
        EXPECT_TRUE(!would_block);

        // Did not upgrade but if mode is SH, it
        // first release
        the_latch.latch_release();
        // yield();

        EXPECT_TRUE(the_latch.is_mine() == false);
        EXPECT_TRUE(the_latch.held_by_me() == 0);
    }

    if(verbose)  {
                CRITICAL_SECTION(cs, print_mutex);
        cout << _self << " end test 3 " <<  i << endl;
    }

        {
                CRITICAL_SECTION(cs, done_mutex);
                done_count++;
        }
        DO_PTHREAD(pthread_cond_signal(&done));
}

void latch_thread_t::run()
{
    // latch_t::on_thread_init(this);
    switch(testnum)
    {
    case 1:
        test1(-1, all_sh);
        test1(2,  one_ex);
        test1(1,  one_ex); // ....
        test1(0,  one_ex);
        test1(-1, all_ex);
        break;

    case 2:
        test2(1, LATCH_SH, LATCH_SH); // only thread 1 does this test
        test2(1, LATCH_SH, LATCH_EX); // only thread 1 does this test
        test2(1, LATCH_EX, LATCH_EX); // only thread 1 does this test
        // Original SM w_assert9s that it never latches in
        // LATCH_NL mode.
        // Assertions are in bf, latch
        // The code in bf with predicates if mode != LATCH_NL
        // should be removed.
        //
        // shore-mt version lets you latch in NL mode but hangs on
        // an upgrade attempt
        // So I'm inserting an assert in latch.cpp to the extent that
        // you never latch in NL mode
        // test2(1, LATCH_NL, LATCH_SH);
        // test2(1, LATCH_NL, LATCH_EX);
        break;

    case 3:
        test3(-1, all_sh);
        test3(2,  one_ex);
        test3(1,  one_ex); // ....
        test3(0,  one_ex);
        test3(-1, all_ex);
        break;
    }

    // Say we have exited.
        {
                CRITICAL_SECTION(cs, lock);
                hasExited = true;
        }
    // latch_t::on_thread_destroy(this);
}

void sync_all (latch_thread_t **t)
{
        {
                CRITICAL_SECTION(cs, done_mutex);
                done_count=0;
        }

    if(verbose)  {
                CRITICAL_SECTION(cs, print_mutex);
        cout << "{ sync_all START  syncing all " << endl; /*}*/
    }
    for(int i=0; i < NUM_THREADS; i++)
    {
        latch_thread_t::sync_other(t[i]);
    }

    if(verbose)  {
                CRITICAL_SECTION(cs, print_mutex);
        cout << "sync_all  synced all; awaiting them  " << endl;
    }

        {
                CRITICAL_SECTION(cs, done_mutex);
                do {
                        DO_PTHREAD(pthread_cond_wait(&done, &done_mutex));
                } while(done_count < NUM_THREADS-1);

        }
    if(verbose)  {
                CRITICAL_SECTION(cs, print_mutex);
        /*{*/ cout << "sync_all  done }" << endl;
    }
}

latch_thread_t** init (int testnum_) {
    latch_thread_t **latch_thread = new latch_thread_t *[NUM_THREADS];
    testnum = testnum_;
    for (int i = 0; i < NUM_THREADS; i++)  {
        latch_thread[i] = new latch_thread_t(i);
        EXPECT_TRUE(latch_thread[i] != NULL);
        EXPECT_FALSE(latch_thread[i]->fork().is_error());
    }
    return latch_thread;
}
void terminate (latch_thread_t **latch_thread) {
    for (int i = 0; i < NUM_THREADS; i++)  {
        EXPECT_FALSE( latch_thread[i]->join().is_error());
        delete latch_thread[i];
        latch_thread[i] = NULL;
    }

    delete [] latch_thread;
    latch_thread = NULL;
}

TEST(LatchTest, Latch1) {
    latch_thread_t **latch_thread = init (1);
    sync_all(latch_thread);
    sync_all(latch_thread);
    sync_all(latch_thread);
    sync_all(latch_thread);
    sync_all(latch_thread);
    terminate(latch_thread);
}

TEST(LatchTest, Latch2) {
    latch_thread_t **latch_thread = init (2);
    sync_all(latch_thread);
    sync_all(latch_thread);
    sync_all(latch_thread);
    terminate(latch_thread);
}

TEST(LatchTest, Latch3) {
    latch_thread_t **latch_thread = init (3);
    sync_all(latch_thread);
    sync_all(latch_thread);
    sync_all(latch_thread);
    sync_all(latch_thread);
    sync_all(latch_thread);
    terminate(latch_thread);
}
