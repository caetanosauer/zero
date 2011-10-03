/*<std-header orig-src='shore'>

 $Id: rusage.cpp,v 1.2 2010/12/08 17:37:37 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

/*  -- do not edit anything above this line --   </std-header>*/

#include "w_rusage.h"
#include "w_gettimeofday.h"
#include <iostream>

const int MIL     = 1000000;

unix_stats::unix_stats()  { who = RUSAGE_SELF; }
unix_stats::unix_stats(int _who)  { who = (who_t) (_who); }

void
unix_stats::start() 
{
    iterations = 0; 
    /*
     * Save the current stats in buffer area 1.
     */
    gettimeofday(&time1, NULL);
    getrusage(who, &rusage1);
    sbrk1 = sbrk(0);

    return;
}

void
unix_stats::stop(int iter) 
{
    /*
     * Save the final stats in buffer area 2.
     */
    gettimeofday(&time2, NULL);
    getrusage(who, &rusage2);
    sbrk2 = sbrk(0);

    if(iter < 1) {
        iterations = 0;
    } else {
        iterations = iter;
    }

    return;
}

int 
unix_stats::clocktime()  const  // in microseconds
{
    return (MIL * (time2.tv_sec - time1.tv_sec)) + 
           (time2.tv_usec - time1.tv_usec);
}

int 
unix_stats::usertime()  const // in microseconds
{
    return (MIL * (rusage2.ru_utime.tv_sec - rusage1.ru_utime.tv_sec)) +
           (rusage2.ru_utime.tv_usec - rusage1.ru_utime.tv_usec);
}

int 
unix_stats::systime()  const // in microseconds
{
    return (MIL * (rusage2.ru_stime.tv_sec - rusage1.ru_stime.tv_sec)) +
           (rusage2.ru_stime.tv_usec - rusage1.ru_stime.tv_usec);
}

int
unix_stats::page_reclaims()  const
{
    return rusage2.ru_minflt - rusage1.ru_minflt;
}

int
unix_stats::page_faults()  const
{
    return rusage2.ru_majflt - rusage1.ru_majflt;
}

int
unix_stats::swaps()  const
{
    return rusage2.ru_nswap - rusage1.ru_nswap;
}

int
unix_stats::vcsw()  const
{
    return rusage2.ru_nvcsw - rusage1.ru_nvcsw;
}

int
unix_stats::invcsw()  const
{
    return rusage2.ru_nivcsw - rusage1.ru_nivcsw;
}
int
unix_stats::inblock()  const
{
    return rusage2.ru_inblock - rusage1.ru_inblock;
}

int
unix_stats::oublock()  const
{
    return rusage2.ru_oublock - rusage1.ru_oublock;
}
int
unix_stats::drss()  const
{
    return rusage2.ru_idrss - rusage1.ru_idrss;
}
int
unix_stats::srss()  const
{
    return rusage2.ru_isrss - rusage1.ru_isrss;
}
int
unix_stats::xrss()  const
{
    return rusage2.ru_ixrss - rusage1.ru_ixrss;
}
int
unix_stats::msgsent()  const
{
    return rusage2.ru_msgsnd - rusage1.ru_msgsnd;
}
int
unix_stats::msgrecv()  const
{
    return rusage2.ru_msgrcv - rusage1.ru_msgrcv;
}

int
unix_stats::mem()  const
{
    return int((ptrdiff_t)sbrk2 - (ptrdiff_t)sbrk1);
}

ostream &unix_stats::print(ostream &o) const
{
    int   i;
    double clk,usr,sys;

    if(iterations== 0) {
        o << "error: unix_stats was never properly \"stop()\"-ed. ";
        return o;
    }
    clk=(double) clocktime()/MIL;
    usr=(double) usertime()/MIL;
    sys=(double) systime()/MIL;
    o << "seconds/iter: ";
    W_FORM2(o,("  clk: %3.2f ",(clk/iterations)));
    W_FORM2(o,("  usr: %3.2f ",(usr/iterations)));
    W_FORM2(o,("  sys: %3.2f ",(sys/iterations)));
    o << endl;

    if( (i=mem()) != 0) {
        W_FORM2(o,("memory: %16d bytes",i));
        o << endl;
    }

    if( (i=page_faults()) != 0) {
        W_FORM2(o,("%6d maj ",i));
    }
    if( (i=page_reclaims()) != 0) {
        W_FORM2(o,("%6d min ",i));
    }
    if( (i=swaps()) != 0) {
        W_FORM2(o,("%6d swp ",i));
    }
    if( (i=vcsw()) != 0) {
        W_FORM2(o,("%6d csw",i));
    }
    if( (i=invcsw()) != 0) {
        W_FORM2(o,("%6d inv",i));
    }
    if( (i=inblock()) != 0) {
        W_FORM2(o,("%6d inblk",i));
    }
    if( (i=oublock()) != 0) {
        W_FORM2(o,("%6d oublk",i));
    }
    if( (i=xrss()) != 0) {
        W_FORM(o)("%6d xrss",i);
    }
    if( (i=drss()) != 0) {
        W_FORM(o)("%6d drss",i);
    }
    if( (i=srss()) != 0) {
        W_FORM(o)("%6d srss",i);
    }
    if( (i=msgsent()) != 0) {
        W_FORM2(o,("%6d snd",i));
    }
    if( (i=msgrecv()) != 0) {
        W_FORM2(o,("%6d rcv",i));
    }
    return o;
}

ostream& operator<<( ostream& o, const unix_stats &s)
{
    return s.print(o);
}

int 
unix_stats::s_clocktime()  const  
{
    return time2.tv_sec - time1.tv_sec;
}

int 
unix_stats::s_usertime()  const 
{
    return rusage2.ru_utime.tv_sec - rusage1.ru_utime.tv_sec;
}

int 
unix_stats::s_systime()  const 
{
    return rusage2.ru_stime.tv_sec - rusage1.ru_stime.tv_sec;
}

int 
unix_stats::us_clocktime()  const  // in microseconds
{
    return (time2.tv_usec - time1.tv_usec);
}

int 
unix_stats::us_usertime()  const // in microseconds
{
    return (rusage2.ru_utime.tv_usec - rusage1.ru_utime.tv_usec);
}

int 
unix_stats::us_systime()  const // in microseconds
{
    return (rusage2.ru_stime.tv_usec - rusage1.ru_stime.tv_usec);
}

int 
unix_stats::signals()  const 
{
    return (rusage2.ru_nsignals- rusage1.ru_nsignals);
}


extern float compute_time(const struct timeval*, const struct timeval*);

float compute_time(const struct timeval* start_time, const struct timeval* end_time)
{
    double seconds, useconds;

    seconds = (double)(end_time->tv_sec - start_time->tv_sec);
    useconds = (double)(end_time->tv_usec - start_time->tv_usec);

    if (useconds < 0.0) {
        useconds = 1000000.0 + useconds;
        seconds--;
    }
    return (float)(seconds + useconds/1000000.0);
}

float 
unix_stats::compute_time() const
{
    return ::compute_time(&time1, &time2);
}

