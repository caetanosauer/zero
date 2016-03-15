#ifndef BASETHREAD_H
#define BASETHREAD_H


#include "sm_base.h"
#include "vol.h"
#include "logarchiver.h"
#include "xct.h"

#include <stdexcept>
#include <queue>

using namespace std;

class basethread_t : public smthread_t {
public:
    basethread_t();

    virtual ~basethread_t();

    bool finished;

    static void start_base();
    static void start_buffer();
    static void start_log(string logdir);
    // default archiver workspace size = 800MB
    static void start_archiver(string archdir, size_t wsize, size_t bsize);
    static void start_merger(string archdir);
    static void start_other();
    static void print_stats();

protected:
    void begin_xct();
    void commit_xct();
    virtual void before_run();
    virtual void after_run();

private:
    pthread_mutex_t running_mutex;
    xct_t* current_xct;

    static sm_options _options;
};

#endif
