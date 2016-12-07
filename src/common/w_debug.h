/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager

                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne

                         All Rights Reserved.

   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.

   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

/*<std-header orig-src='shore' incl-file-exclusion='W_DEBUG_H'>

 $Id: w_debug.h,v 1.1 2010/12/09 15:29:05 nhall Exp $

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

#ifndef W_DEBUG_H
#define W_DEBUG_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifndef W_BASE_H
/* NB: DO NOT make this include w.h -- not yet */
#include <w_base.h>
#endif /* W_BASE_H */

/**\file w_debug.h
 *\ingroup MACROS
 *
*  This is a set of macros for use with C or C++. They give various
*  levels of debugging printing when compiled with --enable-trace.
*  With tracing, message printing is under the control of an environment
*  variable DEBUG_FLAGS (see debug.cpp).
*  If that variable is set, its value must
*  be  a string.  The string is searched for __FILE__ and the function name
*  in which the debugging message occurs.  If either one appears in the
*  string (value of the env variable), or if the string contains the
*  word "all", the message is printed.
*
*
*/
#include <cassert>
#include <pthread.h>
#include <sstream>

#undef USE_REGEX

#ifdef USE_REGEX
#include "regex_posix.h"
#endif /* USE_REGEX */

/* XXX missing type in vc++, hack around it here too, don't pollute
   global namespace too badly. */
typedef    ios::fmtflags    w_dbg_fmtflags;


#ifdef W_TRACE

// Turns full path from __FILE__ macro into just name of the file
// CS: This was not necessary in Shore-MT because gcc was invoked
// not on the full path, but on the file directly
#define _strip_filename(f) \
    (strrchr(f, '/') ? strrchr(f, '/') + 1 : f)


#endif  /* W_TRACE*/

/* ************************************************************************  */

/* ************************************************************************
 *
 * Class w_debug, macros DBG, DBG_NONL, DBG1, DBG1_NONL:
 */


/**\brief An ErrLog used for tracing (configure --enable-trace)
 *
 * For tracing to be used, you must set the environment variable
 * DEBUG_FLAGS to a string containing the names of the files you
 * want traced, and
 *
 * DEBUG_FILE to the name of the output file to which the output
 * should be sent. If DEBUG_FILE is not set, the output goes to
 * stderr.
 */
class w_debug {
    private:
        char *_flags;
        enum { _all = 0x1, _none = 0x2 };
        unsigned int        mask;
        int            _trace_level;

#ifdef USE_REGEX
        static regex_t        re_posix_re;
        static bool        re_ready;
        static char*        re_error_str;
        static char*        re_comp_debug(const char* pattern);
        static int        re_exec_debug(const char* string);
#endif /* USE_REGEX */

        int            all(void) { return (mask & _all) ? 1 : 0; }
        int            none(void) { return (mask & _none) ? 1 : 0; }

    public:
        w_debug(const char *n, const char *f);
        ~w_debug();
        int flag_on(const char *fn, const char *file);
        const char *flags() { return _flags; }
        void setflags(const char *newflags);
        void memdump(void *p, int len); // hex dump of memory
        int trace_level() { return _trace_level; }
};
extern w_debug _w_debug;


// I wanted to use google-logging (glog), but changing all of the existing code
// takes time. So, currently it's just std::cout.
#define ERROUT(a) std::cerr << "[" << hex << pthread_self() << dec << "] " << __FILE__ << " (" << __LINE__ << ") " a << endl;
//#define DBGOUT(a) std::cout << "[" << pthread_self() << "] " << __FILE__ << " (" << __LINE__ << ") " a << endl;

// CS: reverted back to shore's old debug mechanism, which allows us
// to select only output from certain source files. The current mechanism
// dumps way to much debug information, which makes it hard to perform
// actual debugging focused only on certain components.
#define DBGPRINT(a, file, line) \
       std::stringstream ss; \
       ss << "[" << hex << pthread_self() << dec << "] " \
            << _strip_filename(file) << " (" << line << ") " a; \
       std::cerr << ss.str() << endl;

#define DBGOUT(a) do { \
    if(_w_debug.flag_on(__func__,_strip_filename(__FILE__))) { \
        DBGPRINT(a, __FILE__, __LINE__); \
    } \
 } while (0);

#define DBGOUT0(a) DBGOUT(a)

#if 1
#if W_DEBUG_LEVEL >= 1
#define DBGOUT1(a) DBGOUT(a)
#else
#define DBGOUT1(a)
#endif


#if W_DEBUG_LEVEL >= 2
#define DBGOUT2(a) DBGOUT(a)
#else
#define DBGOUT2(a)
#endif

#if W_DEBUG_LEVEL >= 3
#define DBGOUT3(a) DBGOUT(a)
#else
#define DBGOUT3(a)
#endif


#if W_DEBUG_LEVEL >= 4
#define DBGOUT4(a) DBGOUT(a)
#else
#define DBGOUT4(a)
#endif


#if W_DEBUG_LEVEL >= 5
#define DBGOUT5(a) DBGOUT(a)
#else
#define DBGOUT5(a)
#endif


#if W_DEBUG_LEVEL >= 6
#define DBGOUT6(a) DBGOUT(a)
#else
#define DBGOUT6(a)
#endif


#if W_DEBUG_LEVEL >= 7
#define DBGOUT7(a) DBGOUT(a)
#else
#define DBGOUT7(a)
#endif


#if W_DEBUG_LEVEL >= 8
#define DBGOUT8(a) DBGOUT(a)
#else
#define DBGOUT8(a)
#endif


#if W_DEBUG_LEVEL >= 9
#define DBGOUT9(a) DBGOUT(a)
#else
#define DBGOUT9(a)
#endif

#define DBG1(a) DBGOUT1(a)
#define DBG2(a) DBGOUT2(a)
#define DBG3(a) DBGOUT3(a)
#define DBG5(a) DBGOUT5(a)

#else

#define DBGOUT1(a)
#define DBGOUT2(a)
#define DBGOUT3(a)
#define DBGOUT4(a)
#define DBGOUT5(a)
#define DBGOUT6(a)
#define DBGOUT7(a)
#define DBGOUT8(a)
#define DBGOUT9(a)

#endif

// the old "DBG" idiom is level=3
#define DBG(a) DBGOUT3(a)
/*
#if defined(W_TRACE)

#    define DBG2(a,file,line) \
        w_dbg_fmtflags old = _w_debug.clog.setf(ios::dec, ios::basefield); \
        _w_debug.clog  << _strip_filename(file) << ":" << line << ":" ; \
        _w_debug.clog.setf(old, ios::basefield); \
        _w_debug.clog  a    << endl;

#    define DBG1(a) do {\
    if(_w_debug.flag_on(__func__,__FILE__)) {                \
        DBG2(a,__FILE__,__LINE__) \
    } } while(0)

#    define DBG(a) DBG1(a)

#else
#    define DBG(a)
#endif *//* defined(W_TRACE) */
/* ************************************************************************  */

// #define DBG2(a,f,l) DBGPRINT(a,f,l) // used by smthread.h

#include <thread>
#define DBGTHRD(arg) DBG(<<" th."<< std::this_thread::get_id() << " " arg)

/*<std-footer incl-file-exclusion='W_DEBUG_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
