#define SM_SOURCE
#define HTAB_UNIT_TEST_C

#include "w.h"
typedef w_rc_t rc_t;
typedef unsigned short uint16_t;

#include "sm_int_4.h"
#include "bf_core.h"
#include "w_getopt.h"
#include "rand48.h"
#include <pthread.h>
#include "btree_test_env.h"
#include "gtest/gtest.h"

btree_test_env *test_env;

rand48  tls_rng  = RAND48_INITIALIZER;
bool    debug(false);
bool    Random(false);
bool    Random_uniq(false);
int     tries(10);
signed48_t pagebound(1000);
uint16_t vol(1);
uint    storenum(5);
uint    bufkb(1024);
uint32_t    npgwriters(1);

uint32_t  nbufpages = 0;

class bfcb_t; // forward

// This has to derive from smthread_t because the buffer-pool has to
// be used in an smthread_t::run() context, in order that its statistics
// can be gathered in a per-thread structure.  If we don't do it this way,
// we'll croak in INC_TSTAT deep in the buffer pool.
class htab_tester : public smthread_t
{
    typedef enum { Zero, Init, Inserted, Evicted, Returned, Removed } Status;
    // Zero: not used yet
    // Init : initialized with a pid
    // Inserted : Inserted into the htab
    // Evicted: we noticed it's not in the ht any more - 
    //    bf_core::replacement evicted it
    // Returned: got moved by an insert (we don't get told about every move)  
    // Removed: we removed it
    bf_m *     _bfmgr;
    int        _tries;
    signed48_t _pagebound;
    uint16_t    _vol;
    uint       _store;
    bf_core_m *core;

protected:
    struct pidinfo 
    {
        lpid_t pid; // index i -> pid
        lpid_t returned; // return value from insert
        Status status;
        int    count; // # times random gave us this pid
        int    count_removes; // # times returned/evicted/removed
        int    inserts;
        int    evicts;

        pidinfo() : status(Zero),count(0),count_removes(0),
        inserts(0), evicts(0) {}
        friend ostream & operator<< (ostream &o, const struct pidinfo &info);
    };
    friend ostream & operator<< (ostream &o, const struct pidinfo &info);
private:

    lpid_t  *_i2pid; // indexed by i 
    pidinfo *_pid2info; // indexed by pid

    bf_core_m::Tstats S;

public:
    htab_tester(int i, signed48_t pb, uint16_t v, uint    s) : 
         _bfmgr(NULL),
         _tries(i),
         _pagebound(pb),
         _vol(v), _store(s)
    {
        {
            long  space_needed = bf_m::mem_needed(nbufpages);
            /*
             * Allocate the buffer-pool memory
             */ 
            char    *shmbase;
            w_rc_t    e;
#ifdef HAVE_HUGETLBFS
            // fprintf(stderr, "setting path to  %s\n", _hugetlbfs_path->value());
             e = smthread_t::set_hugetlbfs_path(HUGETLBFS_PATH);
#endif
            e = smthread_t::set_bufsize(space_needed, shmbase);

            EXPECT_FALSE(e.is_error());
            EXPECT_TRUE(is_aligned(shmbase));
            _bfmgr = new bf_m(nbufpages, shmbase, npgwriters);

            EXPECT_TRUE(_bfmgr != NULL);
        }

        _pid2info = new pidinfo[int(pb)];
        _i2pid = new lpid_t[i];
        core = _bfmgr->_core;
        memset(&S, '\0', sizeof(S));
    }
    ~htab_tester() 
    {
        sthread_t::do_unmap();
        delete _bfmgr;
    }

    void run();
    void run_inserts();
    void run_lookups();
    void run_removes();
    void cleanup();
    pidinfo & pid2info(const lpid_t &p) { return _pid2info[p.page]; }
    pidinfo &i2info(int i) { return pid2info(i2pid(i)); }
    lpid_t &i2pid(int i) { return _i2pid[i]; }

    bool was_returned(lpid_t &p) 
    {
        for(int i=0; i < _tries; i++)
        {
            if(_pid2info[i].returned == p) return true;
        }
        return false;
    }
    // non-const b/c it updates the stats
    void printstats(ostream &o, bool final=false);
    void print_bf_fill(ostream &o) const;

};

void
htab_tester::print_bf_fill(ostream &o) const
{
    int frames, slots;
    htab_count(core, frames, slots);

    o << "BPool HTable stats part2: #buffers "  << frames 
        << " #slots " << slots 
    << "  " <<  (slots*100.0)/(float)frames 
        << "% full "  
    << endl;
}

void
htab_tester::printstats(ostream &o, bool final) 
{
    if(final) o << "FINAL ";

    o << "NEW htab stats:" << endl;

#define D(x) if(S.bf_htab_##x > 0) o << S.bf_htab_##x << " " << #x << endl;
    if(!final)
    {
        D(insertions);
        D(ensures);
        D(cuckolds);
        D(slow_inserts);
        D(slots_tried);
        D(probes);
        D(harsh_probes);
        D(probe_empty);
        D(hash_collisions);
        D(harsh_lookups);
        D(lookups);
        D(lookups_failed);
        D(removes);
        D(max_limit);
        D(limit_exceeds);
    }
#undef D
#define D(x) if(S.x > 0) o << #x << " " << S.x << endl;
    else
    {
        _bfmgr->htab_stats(S);
        S.compute();

        D(bf_htab_insertions);
        // D(bf_htab_slots_tried);
        D(bf_htab_slow_inserts);
        D(bf_htab_probe_empty);
        // D(bf_htab_hash_collisions); (depends on hash funcs, optimiz level)
        D(bf_htab_harsh_lookups);
        D(bf_htab_lookups);
        D(bf_htab_lookups_failed);
        D(bf_htab_removes);
    }
#undef D

    print_bf_fill(o);
}

void htab_tester::cleanup()
{
    run_removes();
    delete[] _pid2info;
    delete[] _i2pid;
    // delete core;
    delete   _bfmgr;
    _bfmgr=0;
}

void htab_tester::run()
{
    signed48_t    pgnum(0); 

    // Create the set of page ids
    // Either sequential or random.
    for(int i=0; i < _tries; i++)
    {
        pidinfo &info = i2info(i);
        EXPECT_TRUE(info.status == Zero);
    }
    for(int i=0; i < _tries; i++)
    {
        if(Random) {
            // If Random_uniq, we have to look through all
            // the already-created pids and if this pgnum is
            // already there, we have to jettison it and
            // try another.
            pgnum = tls_rng.randn(_pagebound);

            if(Random_uniq)  {
                // give it at most _pagebound tries
                int j= int(_pagebound);
                while (j-- > 0) 
                {
                    pidinfo &info = _pid2info[pgnum];
                    // Is it already in use?
                    if(info.status == Zero) break;
                    // yes -> try again
                    pgnum = tls_rng.randn(_pagebound);
                }
                EXPECT_FALSE(j == 0)
                    << " Could not create unique random set ";
            }
        } else {
            // sequential
            pgnum = i % _pagebound; 
        }
        // Create a pid based on pgnum and store it

#define START 0
    // Well, there IS a page 0...
        pgnum += START;

        lpid_t p(_vol, _store, pgnum);
        pidinfo &info = pid2info(p);

        _i2pid[i] = p;
        info.pid = p;
        info.count ++;
        // info.returned is null pid
        info.status = Init;
        info.inserts = info.evicts = 0;
    }

    // let's verify that we have no dups if we don't want dups
    if(debug || Random_uniq) for(int i=0; i < _tries; i++)
    {
        pidinfo &info=_pid2info[_i2pid[i].page];
        if(Random_uniq) EXPECT_TRUE(int(info.pid.page) == i || info.pid.page == 0);
    }
    if(Random_uniq) {
        cout << "verified no dups" << endl;
    }

    // do the test
    run_inserts();
    run_lookups();
    // Don't remove: just see how the hash table used the
    // available buffers and entries.
    // run_removes();

    {
        int evicted(0), returned(0), inserted(0), inited(0), removed(0);
        for(int i=0; i < _tries; i++)
        {
            pidinfo &info = i2info(i);
            if(info.status == Init) inited++;
            if(info.status == Inserted) inserted++;
            if(info.status == Returned) returned++;
            if(info.status == Evicted) evicted++;
            if(info.status == Removed) removed++;
        }
        int unaccounted = _tries - (inited + inserted+ returned + evicted
                + removed);
        cout << "Remaining Init " << inited
            << " Inserted " << inserted
            << " Evicted " << evicted
            << " Returned " << returned
            << " Removed " << removed
            << " Unaccounted " << unaccounted
            << endl;

    }
    printstats(cout, true);
    cleanup();
}

void htab_tester::run_inserts()
{
    for(int i=0; i < _tries; i++)
    {
        pidinfo &info = i2info(i);

        lpid_t pid = info.pid;
        if(info.status == Inserted)  
        if(debug) {
            cout 
            << "i=" << i
            << " pid=" << pid
            << " ALREADY INSERTED " 
                << endl;
            // This COULD trigger an assert in bf_core.cpp
            cout << " #inserts " << info.inserts
            << " #evicts " << info.evicts
            << " count " << info.count
            << endl;
        }

        int slots(0);
        int frames(0); 
        if(debug) 
        {
            htab_count(core, frames, slots);
            cout << "before htab_insert:\t frames =" << frames 
            << " slots in use " << slots <<endl;;
        }

        bfcb_t *p = htab_insert(core, pid, S);
            
        if(p) 
        {
            if(debug) {
                cout << "Possible move: : pid= "
                << pid
                << " returned pid "
                << p->pid()
                << endl; 

                printstats(cout); 
            }

            info.returned = p->pid();

            pidinfo &info_returned = pid2info(p->pid());
            info_returned.status = Returned;
            info_returned.evicts++ ;

            // make sure the pin count is zero so that
            // the sm has a possibility of evicting it later.
            p->zero_pin_cnt();
        }
        info.status = Inserted;
        info.inserts++;
        if(debug) {
            if(i % 10) cout << "." ;
            else cout << i << endl;
        }
        bfcb_t *p2 = htab_lookup(core, pid, S);
        EXPECT_TRUE(p2 != NULL)
            << " Cannot find just-inserted page " << pid << endl;
        // correct the pin-count, since the lookup incremented it...
        // This is to avoid an assert at shut-down
        // and to give the old bf htab half a chance to evict a
        // page.
        p2->zero_pin_cnt();
        EXPECT_TRUE(p2->pid() == pid);

        if(debug) {
            int slots2;
            htab_count(core, frames, slots2);
            if(slots2 <  slots) 
            {
                cout << "htab_insert reduced # entries in use:\t frames =" 
                << frames << " slots in use " << slots2 <<endl;
                // w_assert1(0);
            }
        }

    }
    if(debug) {
        cout <<endl <<  " after insertions : " << endl; printstats(cout);
    }
}

void htab_tester::run_lookups()
{
    for(int i=0; i < _tries; i++)
    {
        pidinfo &info = i2info(i);
        lpid_t pid=info.pid;
        bfcb_t *p = htab_lookup(core, pid, S);
        if(!p) {
            if(info.status != Returned) 
            {
                // NOTE: the hash table at this writing gives
                // no indication when it cannot insert because
                // it ran out of room.   The assumption is that
                // there is room for everything because in the
                // sm the number of page buffers in the pool limits
                // the number of things you would have inserted 
                // at any time.
                // What happens here is that the bf_core::replacement()
                // pitches out what it finds that has pin_cnt of 0,
                // which is the case for every one of these we are inserting.
                if(debug) {
                    cout << pid << " was evicted; "
                    << " status = " << info.status
                    << " was_returned = " << was_returned(pid)
                    << endl;
                }
                info.status = Evicted; // without explanation 
                info.evicts++; // without explanation 
            }
        }
    }
    if(debug) {
    cout <<endl <<  " after lookups : " << endl; printstats(cout);
    }
}

void htab_tester::run_removes()
{
    for(int i=0; i < _tries; i++)
    {
        pidinfo &info = i2info(i);
        lpid_t pid=info.pid;
        if(info.count_removes == 0)
        {
            // Don't try to remove it twice
            /*bool b =*/ htab_remove(core, pid, S);
            info.status = Removed;
            // note: returns false if pin count was zero, which
            // it will be for this test.
            info.count_removes++;
        }
    }
    if(debug) {
        cout << endl << " after removes : " << endl; printstats(cout);
        cout << endl;
    }
}

void runtest (bool random, int n) {     
    const int page_sz = SM_PAGESIZE;

    Random = random;
    tries = n;

    if(Random_uniq && (tries > pagebound)) {
        // NOTE: we now have to do this because we index
        // the info structures on the page #
        cerr << "For " << tries 
            << " page bound (-p) is too low: "
            << int(pagebound) << ". Using " << tries << endl;
        pagebound = tries;
    }
    
    nbufpages = (bufkb * 1024 - 1) / page_sz + 1;
    if (nbufpages < 10)  {
        cerr << error_prio << "ERROR: buffer size ("
             << bufkb
             << "-KB) is too small" << flushl;
        cerr << error_prio << "       at least " << 10 * page_sz / 1024
             << "-KB is needed" << flushl;
        W_FATAL(fcOUTOFMEMORY);
    }

    latch_t::on_thread_init(me());
    {
        cout <<"creating tests with " 
             << tries << " tries, "
             << pagebound << " upper bound on pages, "
        << " volume " << vol 
        << ", store " << storenum 
        << "."
        << endl;
        htab_tester anon(tries, pagebound, vol, storenum);
        anon.fork();
        anon.join();
    }
    // cerr << endl << flushl;
    latch_t::on_thread_destroy(me());
}

TEST (HtabTest, Fixed500) {
    runtest(false, 500);
}
TEST (HtabTest, Random10000) {
    runtest(true, 10000);
}
TEST (HtabTest, Random100000) {
    runtest(true, 100000);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
