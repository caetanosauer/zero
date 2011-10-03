/*<std-header orig-src='shore'>

 $Id: stime.cpp,v 1.40 2010/12/08 17:37:37 nhall Exp $

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

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/**\cond skip */
#include <ctime>
#include <cstring>
#include <w_base.h>
#include <stime.h>
#include <w_stream.h>
#include <climits>

/*
   All this magic is to allow either timevals or timespecs
   to be used without code change.  It's disgusting, but it
   avoid templates.  Templates are evil.

   st_tod    == time of day part of the time
   st_hires    == "higher resolution" part of the time
   HR_SECOND    == high-resolution units in a second
 */  

#define    NS_SECOND    1000000000    /* nanoseconds in a second */
#define    US_SECOND    1000000    /* microseconds in a second */
#define    MS_SECOND    1000    /* millisecs in a second */

#ifdef USE_POSIX_TIME
typedef    struct timespec _stime_t;
#define    st_tod        tv_sec
#define    st_hires    tv_nsec
#define    HR_SECOND    NS_SECOND
#else
typedef struct timeval    _stime_t;
#define    st_tod        tv_sec
#define    st_hires    tv_usec
#define    HR_SECOND    US_SECOND
#endif

#define    HR_MAX        (HR_SECOND-1)




/*
   XXX problems

   Rounding policy is currently ill-defined.  I need to choose one,
   and make sure the various input and output (to/from other type)
   operators implement it uniformly.  XXX This really needs to be
   fixed.
 */

/* Internal constructor, exposes implementation */
stime_t::stime_t(time_t tod, long hires)
{
    _time.st_tod = tod;
    _time.st_hires = hires;

    normalize();
}


stime_t::stime_t(int secs)
{
    _time.st_tod = secs;
    _time.st_hires = 0;

    /* the conversion automagically normalizes */
}

stime_t::stime_t(long secs)
{
    _time.st_tod = secs;
    _time.st_hires = 0;

    /* the conversion automagically normalizes */
}


stime_t::stime_t(double secs)
{
    _time.st_tod = (long) secs;
    _time.st_hires = (long) ((secs - _time.st_tod) * HR_SECOND);

    /* the conversion automagically normalizes */
}


#ifdef USE_POSIX_TIME
stime_t::stime_t(const struct timespec &tv)
{
    _time.st_tod = tv.tv_sec;
    _time.st_hires = tv.tv_nsec * (HR_SECOND / NS_SECOND);

    normalize();
}
#endif

stime_t::stime_t(const struct timeval &tv)
{
    _time.st_tod = tv.tv_sec;
    _time.st_hires = tv.tv_usec * (HR_SECOND / US_SECOND);

    normalize();
}


bool    stime_t::operator==(const stime_t &r) const
{
    return _time.st_tod == r._time.st_tod &&
        _time.st_hires == r._time.st_hires;
}


bool    stime_t::operator<(const stime_t &r) const
{
    if (_time.st_tod == r._time.st_tod)
        return _time.st_hires < r._time.st_hires;
    return _time.st_tod < r._time.st_tod;
}


bool    stime_t::operator<=(const stime_t &r) const
{
    return *this == r  ||  *this < r;
}


static inline int sign(const int i)
{
    return i > 0 ? 1 : i < 0 ? -1 : 0;
    
}

/* Put a stime into normal form, where the HIRES part
   will contain less than a TODs worth of HIRES time.
   Also, the signs of the TOD and HIRES parts should
   agree (unless TOD==0) */

void stime_t::signs()
{
    if (_time.st_tod  &&  _time.st_hires
        && sign(_time.st_tod) != sign(_time.st_hires)) {

        if (sign(_time.st_tod) == 1) {
            _time.st_tod--;
            _time.st_hires += HR_SECOND;
        }
        else {
            _time.st_tod++;
            _time.st_hires -= HR_SECOND;
        }
    }
}

/* off-by one */
void stime_t::_normalize()
{
    if (abs(_time.st_hires) >= HR_SECOND) {
        _time.st_tod += sign(_time.st_hires);
        _time.st_hires -= sign(_time.st_hires) * HR_SECOND;
    }
    signs();
}
   

/* something that could be completely wacked out */
void stime_t::normalize()
{
    int    factor;

    factor = _time.st_hires / HR_SECOND;
    if (factor) {
        _time.st_tod += factor;
        _time.st_hires -= HR_SECOND * factor;
    }

    signs();
}


stime_t    stime_t::operator-() const
{
    stime_t    result;

    result._time.st_tod = -_time.st_tod;
    result._time.st_hires = -_time.st_hires;

    return result;
}


stime_t    stime_t::operator+(const stime_t &r) const
{
    stime_t    result;

    result._time.st_tod  = _time.st_tod  + r._time.st_tod;
    result._time.st_hires = _time.st_hires + r._time.st_hires;

    result._normalize();

    return result;
}


stime_t    stime_t::operator-(const stime_t &r) const
{
    return *this + -r;
}


stime_t stime_t::operator*(const int factor) const
{
    stime_t    result;

    result._time.st_tod = _time.st_tod * factor;
    result._time.st_hires = _time.st_hires * factor;
    result.normalize();

    return result;
}

/* XXX
   Float scaling is stupid for the moment.  It doesn't need
   to use double arithmetic, instead it should use an
   intermediate normalization step which moves
   lost TOD units into the HIRES range.

   The double stuff at least makes it seem to work right.
 */  


stime_t stime_t::operator/(const int factor) const
{
    return *this / (double)factor;
}


stime_t    stime_t::operator*(const double factor) const
{
    double d = *this;
    d *= factor;
    stime_t result(d); 
    result.normalize();

    return result;
}


stime_t    stime_t::operator/(const double factor) const
{
    return *this * (1.0 / factor);
}


/* The operator X and operator X= can be written in terms of each other */
stime_t &stime_t::operator+=(const stime_t &r)
{
    _time.st_tod  += r._time.st_tod;
    _time.st_hires += r._time.st_hires;

    _normalize();
    
    return *this;
}


stime_t &stime_t::operator-=(const stime_t &r)
{
    _time.st_tod  -= r._time.st_tod;
    _time.st_hires -= r._time.st_hires;

    _normalize();
    
    return *this;
}


stime_t::operator double() const
{
    return _time.st_tod + _time.st_hires / (double) HR_SECOND;
}


stime_t::operator float() const
{
    double res = (double) *this;
    return (float)res;
//    return _time.st_tod + _time.st_hires / (float) HR_SECOND;
}




/* XXX do we want this conversion even if we are using timeval
   implementation on systems that have timespec? */
#ifdef USE_POSIX_TIME
stime_t::operator struct timespec() const
{
    struct    timespec tv;
    tv.tv_sec = _time.st_tod;
    tv.tv_nsec = _time.st_hires;
    return tv;
}
#endif

// for type conversion
stime_t::operator struct timeval() const
{
    struct    timeval tv;
    tv.tv_sec = _time.st_tod;
    /* This conversion may prevent overflow which may
       occurs with some values on some systems. */
    tv.tv_usec = _time.st_hires / (HR_SECOND / US_SECOND);
    return tv;
}


void    stime_t::gettime()
{
    int    kr;
#ifdef USE_POSIX_TIME
    kr = clock_gettime(CLOCK_REALTIME, &_time);
#else
    kr = gettimeofday(&_time, 0);
#endif
    if (kr == -1)
        W_FATAL(fcOS);
}


ostream    &stime_t::print(ostream &s) const
{
    ctime(s);

    if (_time.st_hires) {
        stime_t    tod(_time.st_tod, 0);

        s << " and " << sinterval_t(*this - tod);
    }

    return s;
}


ostream &stime_t::ctime(ostream &s) const
{
    /* the second field of the time structs should be a time_t */
    time_t    kludge = _time.st_tod;
    const   int buflen(26); 
    char    buf[buflen];    /* XXX well known magic number */

    /// See Makefile.generic for note about this
#ifdef _POSIX_PTHREAD_SEMANTICS 
    char    *when = ctime_r(&kludge, buf);
#elif defined(SOLARIS2)
    char    *when = ctime_r(&kludge, buf, buflen);
#else
    char    *when = ctime_r(&kludge, buf);
#endif

    /* chop the newline */
    char *nl = strchr(when, '\n');
    if (nl)
        *nl = '\0';

    return s << when;
}


static void factor_print(ostream &s, long what)
{
    struct {
        const char    *label;
        int        factor;
    } factors[] = {
        {"%02d:", 60*60},
        {"%02d:", 60},
        {0, 0}
    }, *f = factors;
    long    mine;
    bool    printed = false;
    bool    negative = what < 0;

    if (negative) {
        s << '-';
        what = -what;
    }

    for (f = factors; f->label; f++) {
        mine = what / f->factor;
        what = what % f->factor;
        if (mine || printed) {
            W_FORM(s)(f->label, mine);
            printed = true;
        }
    }

    /* always print a seconds field */
    W_FORM(s)(printed ? "%02d" : "%d", what);
}


ostream    &sinterval_t::print(ostream &s) const
{
    factor_print(s, _time.st_tod);

    if (_time.st_hires) {
#ifdef USE_POSIX_TIME
        W_FORM(s)(".%09ld", _time.st_hires);
#else
        W_FORM(s)(".%06ld", _time.st_hires);
#endif
    }

    return s;
}


ostream &operator<<(ostream &s, const stime_t &t)
{
    return t.print(s);
}


ostream &operator<<(ostream &s, const sinterval_t &t)
{
    return t.print(s);
}


/* Input Conversion operators */

static inline void from_linear(int sec, int xsec,
                int linear_secs, _stime_t &_time)
{
    _time.st_tod = sec + xsec / linear_secs;
    xsec = xsec % linear_secs;
    if (linear_secs > HR_SECOND)
        _time.st_hires = xsec / (linear_secs / HR_SECOND);
    else
        _time.st_hires = xsec * (HR_SECOND / linear_secs);
}


stime_t stime_t::sec(int sec)
{
    stime_t    r;

    r._time.st_tod = sec;
    r._time.st_hires = 0;

    return r;
}


stime_t    stime_t::msec(int ms, int sec)
{
    stime_t    r;

    from_linear(sec, ms, MS_SECOND, r._time);
    /* conversion normalizes */

    return r;
}

    
stime_t    stime_t::usec(int us, int sec)
{
    stime_t    r;

    from_linear(sec, us, US_SECOND, r._time);
    /* conversion normalizes */

    return r;
}


stime_t    stime_t::nsec(int ns, int sec)
{
    stime_t    r;

    from_linear(sec, ns, NS_SECOND, r._time);
    /* conversion normalizes */

    return r;
}


stime_t    stime_t::now()
{
    stime_t    now;
    now.gettime();

    return now;
}


/* More conversion operators */
/* For now, only the seconds conversion does rounding */ 

/* roundup #seconds if hr_seconds >= this value */
#define    HR_ROUNDUP    (HR_SECOND / 2)

static    inline long to_linear(const _stime_t &_time, const int linear_secs)
{
    long    result;
    int    factor;

    result = _time.st_tod * linear_secs;

    if (linear_secs > HR_SECOND) {
        factor = linear_secs / HR_SECOND;
        result += _time.st_hires * factor;
    }
    else {
        factor = HR_SECOND / linear_secs;
        result += _time.st_hires / factor;
    }

    return result;
}


long    stime_t::secs() const
{
    long    result;

    result = _time.st_tod;
    if (_time.st_hires >= HR_ROUNDUP)
        result++;

    return result;
}

long    stime_t::msecs() const
{
    return to_linear(_time, MS_SECOND);
}

long    stime_t::usecs() const
{
    return to_linear(_time, US_SECOND);
}

long    stime_t::nsecs() const
{
    return to_linear(_time, NS_SECOND);
}

/**\endcond skip */
