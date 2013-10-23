#include "w_defines.h"

/* Not a test, but give us some info for debugging */ 
/* Also finds out os-dependent maximum # threads per process */

#include "w_pthread.h"
#include <iostream>
#include "w_defines.h"
#include "w.h"
#include "sthread.h"
// for PTHREAD_THREAD_MAX:
#include <limits.h>
// for _SC_PTHREAD_THREAD_MAX:
#include <unistd.h>
#include "gtest/gtest.h"

static pthread_barrier_t b;

class simple_thread_t : public sthread_t {
public:
        simple_thread_t() {};
protected:
        void run();
private:
};

#include <iostream>
#include <sstream>
#include "w_strstream.h"



__thread stringstream *_safe_io(NULL);
void safe_io_init() 
{ 
        if (_safe_io==NULL) _safe_io = new stringstream; 
}

#define SAFE_IO(XXXX) { safe_io_init(); \
        *_safe_io <<  XXXX; \
        fprintf(stdout, "%s", _safe_io->str().c_str()); }

void simple_thread_t::run() 
{ 
    SAFE_IO( "IDs" << endl
    << "thread ptr " << ::hex << uint64_t(this) << endl
    << "self " << uint64_t(pthread_self()) << endl
    << "sthread_t::id " << ::dec << uint64_t(this->id) << endl
    << endl << flush
    ) 
    pthread_barrier_wait(&b);
}

#undef TEST_FIND_PTHREAD_RUNTIMEMAX
#ifdef TEST_FIND_PTHREAD_RUNTIMEMAX
void* dummy(void *) {return NULL;}
#endif

TEST (PthreadTest, All) {
    // At one point we had a bug in that tatas lock relied on
    // the impelemtation's sizeof pthread_t
    tatas_lock lock; 
    cout << "Sizes: " << endl;
    cout << "pthread_t: " << sizeof(pthread_t) << endl;
    cout << "lock._holder: " << sizeof(lock._holder) << endl;
    cout << "lock._holder.bits: " << sizeof(lock._holder.bits) << endl;
    EXPECT_EQ(sizeof(lock._holder.handle), sizeof(lock._holder.bits));
    cout << endl;

    int threads = 2;

        /* Determine how many threads we can create in this process
         * First, see if there is a limit in <limits.h>
         *   This doesn't have to be defined if the value is determined by
         *   available memeory.
         * Second, see if there is a limit available with sysconf.
         *   This does have to be define by a POSIX 1003.1c-1995-compliant system.
         * */

        long m;
    cout << "_POSIX_VERSION/_SC_VERSION ----------------------------- " <<endl;
#ifdef _POSIX_VERSION
    cout << "POSIX_VERSION " << _POSIX_VERSION << endl;
#endif
#ifdef _SC_VERSION
        m = sysconf(_SC_VERSION);
    cout << "sysconf: _SC_VERSION " << 
                        _SC_VERSION << endl;
#endif

    cout << "_POSIX2_VERSION/_SC_2_VERSION ----------------------------- " <<endl;
#ifdef _POSIX2_VERSION
    cout << "POSIX2_VERSION " << _POSIX2_VERSION << endl;
#endif
#ifdef _SC_2_VERSION
        m = sysconf(_SC_2_VERSION);
    cout << "sysconf: _SC_2_VERSION " << 
                        _SC_2_VERSION << endl;
#endif

    cout << "_XOPEN_VERSION/_SC_XOPEN_VERSION ----------------------------- " <<endl;
#ifdef _XOPEN_VERSION
    cout << "XOPEN_VERSION " << _XOPEN_VERSION << endl;
#endif
#ifdef _SC_XOPEN_VERSION
        m = sysconf(_SC_XOPEN_VERSION);
    cout << "sysconf: _SC_XOPEN_VERSION " << 
                        _SC_XOPEN_VERSION << endl;
#endif

    cout << "_POSIX_THREADS/_SC_THREADS ----------------------------- " <<endl;
#ifdef _POSIX_THREADS
    cout << "_POSIX_THREADS " << _POSIX_THREADS << endl;
#endif
#ifdef _SC_THREADS
        m = sysconf(_SC_THREADS);
    cout << "sysconf: _SC_THREADS " << 
                        _SC_THREADS << endl;
#endif

    cout << "_POSIX_THREAD_ATTR_STACKSIZE/_SC_THREAD_ATTR_STACKSIZE ----------------------------- " <<endl;
#ifdef _POSIX_THREAD_ATTR_STACKSIZE
    cout << "POSIX_THREAD_ATTR_STACKSIZE " << _POSIX_THREAD_ATTR_STACKSIZE << endl;
#endif
#ifdef _SC_THREAD_ATTR_STACKSIZE
        m = sysconf(_SC_THREAD_ATTR_STACKSIZE);
    cout << "sysconf: _SC_THREAD_ATTR_STACKSIZE " << 
                        _SC_THREAD_ATTR_STACKSIZE << endl;
#endif

    cout << "_POSIX_THREAD_ATTR_STACKADDR/_SC_THREAD_ATTR_STACKADDR ----------------------------- " <<endl;
#ifdef _POSIX_THREAD_ATTR_STACKADDR
    cout << "POSIX_THREAD_ATTR_STACKADDR " << _POSIX_THREAD_ATTR_STACKADDR << endl;
#endif
#ifdef _SC_THREAD_ATTR_STACKADDR
        m = sysconf(_SC_THREAD_ATTR_STACKADDR);
    cout << "sysconf: _SC_THREAD_ATTR_STACKADDR " << 
                        _SC_THREAD_ATTR_STACKADDR << endl;
#endif

#ifdef _POSIX_BARRIERS
    cout << "POSIX_BARRIERS " << _POSIX_BARRIERS << endl;
#endif
#ifdef _POSIX_READER_WRITER_LOCKS
    cout << "POSIX_READER_WRITER_LOCKS " << _POSIX_READER_WRITER_LOCKS << endl;
#endif

#ifdef _POSIX_THREAD_THREADS_MAX
    cout << "POSIX_THREAD_THREADS_MAX " << _POSIX_THREAD_THREADS_MAX << endl;
#endif

#ifdef PTHREAD_THREADS_MAX
        threads = PTHREADS_MAX;
    cout << "limits.h: maximum pthreads per process is " << 
                        threads << endl;
#else
    cout 
        << "limits.h: maximum pthreads per process is not defined." 
        << endl;
#endif
#ifdef _SC_THREAD_THREADS_MAX
        m = sysconf(_SC_THREAD_THREADS_MAX);
    cout << "limits.h: maximum pthreads per process is " << 
                        m << endl;
        threads = int(m);
#else
    cout 
        << "NOT COMPLIIANT: sysconf: maximum _SC_PTHREAD_THREADS_MAX is not defined." 
        << endl;
#endif

        // create and fork as many threads as the system will allow,

#ifdef TEST_FIND_PTHREAD_RUNTIMEMAX
        threads=0;
    while(true) {
                threads++;
                // pthread_attr_t attr;
                pthread_t thr;
                int e= pthread_create(&thr, NULL, dummy, NULL);
                if(e!=0) {
                        if(e==EAGAIN) {
                                cout << "runtime: maximum pthreads " << threads << endl;
                                break;
                        }
                        perror("perror ");
                        cout << "ERROR " << e << endl;
                        cout << "threads " << threads << endl;
                        break;
                }
    }
        // On RHEL 5 I got 32748 with the runtime test despite having
        // the stated limit of POSIX_THREAD_THREADS_MAX of 64.
#endif

        threads=2;
        // They all wait on this barrier.
    pthread_barrier_init(&b, NULL, threads+1);
    sthread_t **t = new sthread_t *[threads];

    for (int i = 0; i < threads; i++)
        t[i] = new simple_thread_t();

    for (int i = 0; i < threads; i++)
        EXPECT_TRUE(t[i] != NULL);

    for (int i = 0; i < threads; i++)
        EXPECT_FALSE(t[i]->fork().is_error());

    pthread_barrier_wait(&b);

    for (int i = 0; i < threads; i++)
        EXPECT_FALSE(t[i]->join().is_error());

    pthread_barrier_destroy(&b);
    
    delete[] t;
}
