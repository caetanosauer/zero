#include "w_defines.h"

#include "w_stream.h"
#include <cstddef>
#include <new>

#define __STIME_TESTER__

#include "w.h"
#include "stime.h"
#include "gtest/gtest.h"

/*
   Test edge conditions in stime_t normal form conversion

   The following edge conditions exist seperately:

   1) sign of TOD matches sign of HIRES
   2) HIRES truncated and TOD fixed

   */


#ifndef HR_SECOND
#ifdef USE_POSIX_TIME
#define    HR_SECOND    1000000000
#define    st_tod    tv_sec
#define    st_hires    tv_nsec
#else
#define    HR_SECOND    1000000
#define    st_tod    tv_sec
#define    st_hires    tv_usec
#endif
#endif


/* XXX Should be stime_test_t */
class stime_test_t : public stime_t {
public:
    stime_test_t() : stime_t() { } 
    stime_test_t(const stime_t &time) : stime_t(time) { }

    /* Like the interior constructor, but it doesn't normalize */
    stime_test_t(time_t tod, long hr) : stime_t() {
        _time.st_tod = tod;
        _time.st_hires = hr;
    }

    /* Expose interior constructs for testing */
    void    signs() { stime_t::signs(); }
    void    _normalize() { stime_t::_normalize(); }
    void    normalize() { stime_t::normalize(); }

    ostream &print(ostream &) const;
};

ostream &stime_test_t::print(ostream &s) const
{
    W_FORM(s)("(%d %d)", _time.st_tod, _time.st_hires);
    return s;
}

ostream &operator<<(ostream &s, const stime_test_t &gr)
{
    return gr.print(s);
}


ostream &operator<<(ostream &s, const struct timeval &tv)
{
    W_FORM(s)("timeval(sec=%d  usec=%d)", tv.tv_sec, tv.tv_usec);
    return s;
}

#ifdef USE_POSIX_TIME
ostream &operator<<(ostream &s, const struct timespec &ts)
{
    W_FORM(s)("timespec(sec=%d  nsec=%d)", ts.tv_sec, ts.tv_nsec);
    return s;
}
#endif


int    tod;
int    hires;


void normal()
{
    int    i;
    int    n;
    stime_test_t    *raw;

    cout << "============ NORMAL ==================" << endl;

    raw = new stime_test_t[100];
    if (!raw)
        W_FATAL(fcOUTOFMEMORY);
    n = 0;

    /* normal form w/ tod == 0 */
    new (raw + n++) stime_test_t(0, 8);
    new (raw + n++) stime_test_t(0, -8);
    new (raw + n++) stime_test_t(0, hires + 8);
    new (raw + n++) stime_test_t(0, -hires - 9);

    /* normal form w/ non-zero tod */
    new (raw + n++) stime_test_t(tod, 8);
    new (raw + n++) stime_test_t(tod, hires + 8);
    new (raw + n++) stime_test_t(-tod, -8);
    new (raw + n++) stime_test_t(-tod, -hires - 8);

    /* sign correction w/out normal */
    new (raw + n++) stime_test_t(tod, 9);
    new (raw + n++) stime_test_t(tod, -9);
    new (raw + n++) stime_test_t(-tod, -9);
    new (raw + n++) stime_test_t(-tod, 9);

    /* sign correction w/ normal */
    new (raw + n++) stime_test_t(tod, hires + 7);
    new (raw + n++) stime_test_t(tod, -hires - 7);
    new (raw + n++) stime_test_t(-tod, -hires - 7);
    new (raw + n++) stime_test_t(-tod, hires + 7);

    for (i = 0; i < n; i++) {
        stime_test_t    _norm, norm;

        _norm = norm = raw[i];

        _norm._normalize();
        norm.normalize();

        W_FORM(cout)("[%d] == ", i);
        cout << raw[i];
        cout << " to " << _norm;
        if (norm != _norm)
            cout << " AND " << norm << "   XXX";

        cout << endl;
    }
        
    delete [] raw;
}


struct stime_pair {
    stime_test_t    l;
    stime_test_t     r;
};


void arithmetic()
{
    int    i;
    int    n;
    stime_pair    *raw;

    cout << "============ ARITHMETIC ==================" << endl;

    raw = new stime_pair[100];
    if (!raw)
        W_FATAL(fcOUTOFMEMORY);
    n = 0;

    raw[n].l = stime_test_t(0,  8);
    raw[n++].r = stime_test_t(0,  5);

    raw[n].l = stime_test_t(1, hires-7);
    raw[n++].r = stime_test_t(1, 8);

    raw[n].l = stime_test_t(1, hires-7);
    raw[n++].r = stime_test_t(0, 8);

    raw[n].l = stime_test_t(1, 8);
    raw[n++].r = stime_test_t(-2, -9);

    cout << "================== add / subtract =============" << endl;
    for (i = 0; i < n; i++) {
        W_FORM(cout)("[%d] => ", i);
        cout << raw[i].l << " op ";
        cout << raw[i].r << ": ";

        cout << " +: " << (stime_test_t)(raw[i].l + raw[i].r);
        cout << ", +': " << (stime_test_t)(raw[i].r + raw[i].l);
        cout << ", -: " << (stime_test_t)(raw[i].l - raw[i].r);
        cout << ", -': " << (stime_test_t)(raw[i].r - raw[i].l);

        cout << endl;
    }

    cout << "================== scaling ==============" << endl;
    for (i = 0; i < n; i++) {
        W_FORM(cout)("[%d] => ", i);
        cout << raw[i].l;

        cout << "  * " << (stime_test_t)(raw[i].l * 2);
        cout << "  *' " << (stime_test_t)(raw[i].l * 2.0);

        cout << "  / " << (stime_test_t)(raw[i].l / 2);
        cout << "  /' " << (stime_test_t)(raw[i].l / 2.0);

        cout << endl;
    }

        
    delete [] raw;
}


void conversion()
{
    sinterval_t a(stime_t::msec(hires));
    sinterval_t b(stime_t::usec(hires));
    sinterval_t c(stime_t::nsec(hires));

    cout << "hires == " << hires << endl;
    cout << "====== conversion statics ====" << endl;
    cout << "msec " << (stime_test_t)a << ' ' << a << endl;
    cout << "usec " << (stime_test_t)b << ' ' << b << endl;
    cout << "nsec " << (stime_test_t)c << ' ' << c << endl;

    cout << "====== output conversion integers ======" << endl;
    cout << "msec " << a.msecs() << endl;
    cout << "usec " << b.usecs() << endl;
    cout << "nsec " << c.nsecs() << endl;

    a = stime_t::msec(25);
    b = stime_t::usec(25);
    c = stime_t::nsec(25);
    cout << "====== output conversion integers 2 ======" << endl;
cout << "msec " << a.msecs() << ' ' << b.msecs() << ' ' << c.msecs() << endl;
cout << "usec " << a.usecs() << ' ' << b.usecs() << ' ' << c.usecs() << endl;
cout << "nsec " << a.nsecs() << ' ' << b.nsecs() << ' ' << c.nsecs() << endl;

    cout << "======= output casts =======" << endl;
    a = stime_test_t(tod, hires);
    struct timeval tv;
    tv = a;
    cout << (stime_test_t)a << ": " << tv << endl;
#ifdef USE_POSIX_TIME
    struct timespec ts;
    ts = a;
    cout << (stime_test_t)a << ": " << ts << endl;
#endif
    cout << "test overflow of output casts" << endl;
    a = stime_test_t(0, HR_SECOND-1);
    tv = a;
    cout << (stime_test_t)a << ": " << tv << endl;
#ifdef USE_POSIX_TIME
    ts = a;
    cout << (stime_test_t)a << ": " << ts << endl;
#endif

    /* XXX what is this supposed to test? */
    stime_test_t ten(10,0);
    stime_test_t tenn(ten);
    tenn.normalize();
    cout << "stime_t(10,0) == " << ten;
    cout << tenn << endl;
}


void output()
{
    cout << "========== FORMAT OUTPUT ========" << endl;
    stime_t now = stime_t::now();

    cout << "now            == " << now << endl;
    cout << "now.ctime      == ";
    now.ctime(cout);
    cout << endl;

    /* This tests stripping off the fraction. */
    stime_t nows = now.secs();
    cout << "now.secs       == " << nows << endl;
    cout << "now.secs.ctime == ";
    nows.ctime(cout);
    cout << endl;

    stime_t    later = stime_t::now();
    cout << "later          == " << later << endl;
    sinterval_t delta = later - now;
    cout << "delta          == " << delta << endl;
}

void misc()
{
    cout << "========== MISCELLANY  ========" << endl;
    stime_t        now = stime_t::now();

    cout << "now       = " << now << endl;
}


TEST(NtimeTest, All) {
    tod = 1;
    hires = HR_SECOND;

    normal();

    arithmetic();

    conversion();

    output();

    misc();
}

