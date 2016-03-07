#include "w_defines.h"

#include <cstdlib>
#include <ctime>

#include "w.h"
#include "sthread.h"

#include <iostream>
#include <sstream>
#include "w_strstream.h"
#include "gtest/gtest.h"

__thread stringstream *_safe_io(NULL);
void safe_io_init() 
{ 
        if (_safe_io==NULL) _safe_io = new stringstream; 
}
void safe_io_fini() 
{ 
        if (_safe_io!=NULL) delete _safe_io; _safe_io=NULL;
}

#define SAFE_IO(XXXX) { safe_io_init(); \
        *_safe_io <<  XXXX; \
        fprintf(stdout, "%s", _safe_io->str().c_str()); }

class timer_thread_t : public sthread_t {
public:
        timer_thread_t(unsigned ms);

protected:
        virtual void run();

private:
        unsigned _ms;
};

unsigned default_timeout[] = { 
        400, 500, 100, 600, 450, 440, 430, 420, 410, 900
};

bool    verbose = false;


TEST (ThreadTest4, All) {
    int             timeouts;
    unsigned        *timeout;

    timeouts = sizeof(default_timeout) /sizeof(default_timeout[0]);
    timeout = default_timeout;

    timer_thread_t **timer_thread = new timer_thread_t *[timeouts];

    int i;
    for (i = 0; i < timeouts; i++)  {
            timer_thread[i] = new timer_thread_t(timeout[i]);
            EXPECT_TRUE(timer_thread[i] != NULL);
            EXPECT_FALSE(timer_thread[i]->fork().is_error());
    }

    for (i = 0; i < timeouts; i++)  {
            EXPECT_FALSE( timer_thread[i]->join().is_error());
            delete timer_thread[i];
    }

    delete [] timer_thread;
    if (timeout != default_timeout)
            delete [] timeout;

    if (verbose)
            sthread_t::dump_stats(cout);

    delete _safe_io; _safe_io = NULL;
}

    

timer_thread_t::timer_thread_t(unsigned ms)
: sthread_t(t_regular),
  _ms(ms)
{
        w_ostrstream_buf        s(40);          // XXX magic number
        s << "timer_thread(" << ms << ')' << ends;
        rename(s.c_str());
}

void timer_thread_t::run()
{
        stime_t start, stop;

        SAFE_IO( "[" << setw(2) << setfill('0') << id 
                << "] going to sleep for " << _ms << "ms" << endl;)

        if (verbose)
                start = stime_t::now();

        sthread_t::sleep(_ms);

        if (verbose)
                stop = stime_t::now();
        
        SAFE_IO(
        "[" << setw(2) << setfill('0') << id 
                << "] woke up after " << _ms << "ms";
        )
        if (verbose) SAFE_IO(
                 "; measured " << (sinterval_t)(stop-start)
                << " seconds.";
        cout << endl;
        )
        safe_io_fini();
}

