#include "w_defines.h"
#include "errlog.h"
#include "w_debug.h"
#include "gtest/gtest.h"

typedef void (*testfunc)(ErrLog &);

void test1(ErrLog &e) { 
    e.setloglevel(log_debug);
    e.log( log_error, "test1 printf.");
    e.clog << __LINE__<< " " << "test1 operator << endl " << endl; 
    e.clog << __LINE__<< " " << "test1 operator << flush - no newline " << flush; 
    e.clog << __LINE__<< " " << "test1 operator << flushl" << flushl; 
    cout << __LINE__<< " " << "cout test1 operator << flushl" << flushl; 
    cerr << __LINE__<< " " << "cerr test1 operator << flush" << flushl; 
    e.log( log_error, "test1 printf after clog used.");
}
void test2(ErrLog &e) { 
    e.clog << error_prio << __LINE__<< " " << "This is test 2, error prio." << flushl;
}
void test3(ErrLog &e) { 
    e.clog << info_prio << __LINE__<< " " << "This is test 3, info prio." << flushl;
}
void final(ErrLog &e) { 
    e.clog << fatal_prio << __LINE__<< " " << "This is test final, fatal prio." 
        << flushl << "after fatal" ;
}

void mixed(ErrLog &e) { 
    e.clog << error_prio << __LINE__<< " " << "This is test mixed, error prio." ;
    e.clog << error_prio << __LINE__<< " " << "Using output operator. " << error_prio; 
    e.log(log_error, "Using syslog style");
    e.clog << error_prio << __LINE__<< " " << "Using output operator again. " << error_prio; 
    e.log(log_error, "Using syslog style again");
    e.clog << flushl;
}

testfunc array[] = {
    test1, test2, test3, mixed, final, 0
};

void 
alltests(ErrLog *e, void *arg)
{
    int i = (int) (long) arg;
    testfunc f = array[i-1];
    w_assert9(f);
    cerr << __LINE__<< " " << "alltests: test " << i  << " on " << e->ident() << "...";
    (*f)(*e);
    cerr << __LINE__<< " " << " DONE" << endl;
}

ErrLog *tfile, *topen, *tsyslog, *terr, *tether;

TEST (ErrorlogTest, All){
    const char *path = "tfile";

/*
    tether = new ErrLog("to-ether", log_to_ether, 0, log_debug);
    cerr << "tether created." << endl;

    tsyslog = 
        new ErrLog("to-syslog", log_to_syslogd, (void *)LOG_USER, log_debug);
    cerr << "tsyslog created." << endl;

    terr = new ErrLog("to-err", log_to_stderr, 0, log_debug);
    cerr << "terr created." << endl;

    topen = new ErrLog("to-open", log_to_open_file, stdout, log_debug);
    cerr << "topen created." << endl;
*/

    tfile = new ErrLog("to-file", log_to_unix_file, path, log_debug);
    cerr << "tfile created." << endl;
    
#define NITERS 10
for(int j=0; j<NITERS; j++) {
    DBG(<<"DBG ITERATION #" << j);
    //  ErrLog::apply(alltests, (void *)1);
        alltests(tfile, (void*)1);
}
    /*
    ErrLog::apply(alltests, (void *)2);
    ErrLog::apply(alltests, (void *)3);
    ErrLog::apply(alltests, (void *)4);
    ErrLog::apply(alltests, (void *)5);
    */

delete tfile;
}

