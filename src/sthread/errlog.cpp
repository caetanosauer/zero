/*<std-header orig-src='shore'>

 $Id: errlog.cpp,v 1.1 2010/12/09 15:29:05 nhall Exp $

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

#define __ERRLOG_C__
/* errlog.cpp -- error logging functions */

#include <cstdarg>
#include <cstdlib>
#include <cstddef>
#include <cstring>
#include <cassert>
#include <cstdio>
#include <iostream>

#include "w.h"
#include "errlog.h"
#include <errlog_s.h>
#include <w_strstream.h>

#ifdef EXPLICIT_TEMPLATE
template class w_list_t<ErrLogInfo,unsafe_list_dummy_lock_t>;
template class w_list_i<ErrLogInfo,unsafe_list_dummy_lock_t>;
template class w_keyed_list_t<ErrLogInfo,,unsafe_list_dummy_lock_t simple_string>;
#endif

/** \cond skip */
// DEAD static char __c[100];
// This stream is used *only* to tell if an ostream is a log stream; 
// all log streams are tied to this even though this is only a pseudo-stream.
// Let's hope that the
// DEAD w_ostrstream  logstream::static_stream(__c,sizeof(__c));
/** \endcond skip */

ostream &operator<<(ostream &out, const simple_string x) {
    out << x._s;
    return out;
}

/* A buffer large enough for any result that can fit on a page. */
#if SM_PAGESIZE < 8192
#define    ERRLOG_BUF_SIZE    8192
#else
#define    ERRLOG_BUF_SIZE    SM_PAGESIZE
#endif
static char buffer[ERRLOG_BUF_SIZE];


/** \cond skip */
ErrLogInfo::ErrLogInfo(ErrLog *e)
    : _ident(e->ident()), _e(e)

{ 
}
/** \endcond skip */

// grot- have to wrap w_keyed_list_t to get its destructor
static w_keyed_list_t<ErrLogInfo,unsafe_list_dummy_lock_t,simple_string> 
        _tab(W_KEYED_ARG(ErrLogInfo, _ident, hash_link), unsafe_nolock);


/**\cond skip */
class errlog_dummy {
    // class exists *JUST* to get rid of all the logs
    // so that the hash_t code doesn't choke on exit().
    // 
    // ... and for debugging purposes

    friend class ErrLog;

protected:
    bool table_cleared;

public:
    errlog_dummy(){ 
        table_cleared = false;
#ifdef ZERO_INIT
        memset(buffer, '\0', sizeof(buffer));
#endif
    }
    ~errlog_dummy();
    void dump();
}_d;

errlog_dummy::~errlog_dummy() {
    ErrLogInfo *ei;

    while((ei = _tab.pop())) {
        delete ei;
    }
    table_cleared = true;
}

void
errlog_dummy::dump() {
    ErrLogInfo *ei;
    w_list_i <ErrLogInfo,unsafe_list_dummy_lock_t> iter(_tab);
    while((ei=iter.next())) {
        ei->dump();
    }
}
/**\endcond skip */

// called by flush_and_set_prio, friend of ErrLog
logstream *
is_logstream(std::basic_ostream<char, std::char_traits<char > > &o)
{
    logstream *l=0;
    const ostream *tied = NULL;
	if((&o)->ios::good()) {
		tied =  o.ios::tie();
	}
    // cerr << "tied " << ::hex((unsigned int)tied) << endl;
    // DEAD if(tied == &logstream::static_stream) 
	{
        l = (logstream *)&o;
    }
        //Kill warning
        (void) tied;
    if(l) {
        // cerr << "magic1 " << (unsigned int)l->__magic1 << endl;
        // cerr << "magic2 " << (unsigned int)l->__magic1 << endl;
        // cerr << "_prio" << l->_prio << endl;
    }
    if(l && 
        (l->__magic1 == logstream::LOGSTREAM__MAGIC) &&
        (l->__magic2 == logstream::LOGSTREAM__MAGIC) &&
        (l->_prio >= log_none) &&
        (l->_prio <= log_all) &&
        (l->_log->_magic == ErrLog::ERRORLOG__MAGIC)
       ) {
        // cerr << " IS log stream" << endl;
        return l;
    } else {
        // cerr << " NOT log stream" << endl;
        return (logstream *)0;
    }
}

ostream & 
flush_and_setprio(ostream& o, LogPriority p)
{
    // cerr << "flush_and_setprio o=" << &o << endl;
    // Not entirely thread-safe
    logstream *l = is_logstream(o);
    if(l) {
        l->_log->_flush(); 
        if(p != log_none) {
            l->_prio =  p;
        }
    } else {
        o << flush;
    }
    return o;
}

ostream& flushl(ostream& out)
{
    out << endl;
    return flush_and_setprio(out, log_none); 
}
ostream& emerg_prio(ostream& o){return flush_and_setprio(o, log_emerg); }
ostream& fatal_prio(ostream& o){return flush_and_setprio(o, log_fatal); }
ostream& internal_prio(ostream& o){ return flush_and_setprio(o, log_internal); }
ostream& error_prio(ostream& o){return flush_and_setprio(o, log_error); }
ostream& warning_prio(ostream& o){ return flush_and_setprio(o, log_warning); }
ostream& info_prio(ostream& o){ return flush_and_setprio(o, log_info); }
ostream& debug_prio(ostream& o){ return flush_and_setprio(o, log_debug); }

#ifdef USE_REGEX
#include "regex_posix.h"
#endif
#include "w_debug.cpp"

#if W_DEBUG_LEVEL > 3
void dummy() { DBG(<<""); } // to keep gcc quiet about _fname_debug_
#endif

#include <critical_section.h>

LogPriority 
ErrLog::setloglevel( LogPriority prio) 
{
    CRITICAL_SECTION(cs, _errlog_mutex);
    LogPriority old = _level;
    _level =  prio;
    return old;
}

// static
LogPriority
ErrLog::parse(const char *arg, bool *ok)
    //doesn't change *ok if no errors
{
    LogPriority level = log_none;

    if(strcmp(arg, "off")==0) {
        level = log_none;
    } else
    if(strcmp(arg, "trace")==0 || strcmp(arg,"debug")==0) {
        level = log_debug;
    } else
    if(strcmp(arg, "info")==0) {
        level = log_info;
    } else
    if(strcmp(arg, "warning")==0) {
        level = log_warning;
    } else
    if(strcmp(arg, "error")==0) {
        level = log_error;
    } else
    if(strcmp(arg, "internal")==0 || strcmp(arg,"critical")==0) {
        level = log_internal;
    } else
    if(strcmp(arg, "fatal")==0 || strcmp(arg,"alert")==0) {
        level = log_fatal;
    } else
    if(strcmp(arg, "emerg")==0) {
        level = log_emerg;
    } else {
        if (ok) *ok = false;
    }
    return level;
}

void 
ErrLog::_closelogfile() 
{ 
    // called in cs
    assert(_file != NULL);
    fclose(_file);
}

void
ErrLog::_openlogfile(
    const char *fn      
) 
{
    // called in cs
    const char *filename=fn;
    if(strcmp(filename, "-")==0) {
        // cerr << "log to stderr" << endl;
        _destination = log_to_stderr;
        _file = stderr;
        return;
    }
    if(filename) {
        _destination = log_to_unix_file;
        if(strncmp(filename, "unix:", 5) == 0) {
            filename += 5;
        } else if (strncmp(filename, "shore:", 6) == 0) {
            filename += 6;
        }
        _file = fopen(filename, "a+");
        if(_file == NULL) {
            w_rc_t e = RC(fcOS);
            cerr << "Cannot fopen Unix file " << filename << endl;
            cerr << e << endl;
            W_COERCE(e);
        }
    } else {
        cerr << "Unknown logging destination." << endl;
        W_FATAL(fcINTERNAL);
    }

}

void
ErrLog::_init1()
{
    // called in critical section
    clog.init_errlog(this);
    w_reset_strstream(this->clog);
}

void
ErrLog::_init2()
{
    // called in critical section
    ErrLogInfo *ei;
    if((ei = _tab.search(this->_ident)) == 0) {
        ei = new ErrLogInfo(this);
        _tab.put_in_order(ei); // not really ordered
    } else {
        cerr <<  "An ErrLog called " << _ident << " already exists." << endl;
        W_FATAL(fcINTERNAL);
    }
}

ErrLog::ErrLog(
    const char *ident,        // required
    LoggingDestination dest,     // required
    const char *filename,
    LogPriority level,         //  = log_error
    char *ownbuf,         //  = 0
    int  ownbufsz         //  = 0

) :
    _destination(dest),
    _level(level), 
    _file(0), 
    _ident(ident), 
    _buffer(ownbuf?ownbuf:buffer),
    _bufsize(ownbuf?ownbufsz:sizeof(buffer)),
    _magic(ERRORLOG__MAGIC),
    _errlog_mutex(new pthread_mutex_t),
    clog(ownbuf?ownbuf:buffer, ownbuf?ownbufsz:sizeof(buffer))
{
    DO_PTHREAD(pthread_mutex_init(_errlog_mutex, NULL));
    CRITICAL_SECTION(cs, _errlog_mutex);
    _init1();

    switch( dest ) {
    case log_to_unix_file: 
        {     
            if(!filename) {
                filename = "-"; // stderr
            }
            _openlogfile(filename);
        }
        break;

    case log_to_stderr:
        _file = stderr;
        break;

    case log_to_ether:
        _file = 0;
        break;

    default:
        // fatal error
        cerr << "Bad argument 2 to ErrLog constructor" <<endl;
        W_FATAL_MSG(fcINTERNAL, << "Bad argument 2 to ErrLog constructor");
        break;
    }
    _init2();
}

ErrLog::ErrLog(
    const char *ident,        // required
    LoggingDestination dest,     // required
    FILE *file,             
    LogPriority level,         //  = log_error
    char *ownbuf,         //  = 0
    int  ownbufsz         //  = 0

) :
    _destination(dest),
    _level(level), 
    _file(file), 
    _ident(ident), 
    _buffer(ownbuf?ownbuf:buffer),
    _bufsize(ownbuf?ownbufsz:sizeof(buffer)),
    _magic(ERRORLOG__MAGIC),
    _errlog_mutex(new pthread_mutex_t),
    clog(ownbuf?ownbuf:buffer, ownbuf?ownbufsz:sizeof(buffer))
{
    DO_PTHREAD(pthread_mutex_init(_errlog_mutex, NULL));
    CRITICAL_SECTION(cs, _errlog_mutex);
    _init1();
    w_assert9( dest == log_to_open_file );
    _init2();
}

ErrLog::~ErrLog() 
{
    {
        CRITICAL_SECTION(cs, _errlog_mutex);
        switch(_destination) {
            case log_to_unix_file:
            case log_to_open_file:
                _closelogfile();
                break;

            case log_to_stderr: 
                // let global destructors 
                // do the close - we didn't open
                // it, we shouldn't close it!
                break;

            case log_to_ether:
                break;
        }
        if( !_d.table_cleared ) {
            ErrLogInfo *ei = _tab.search(this->_ident);
            assert(ei!=NULL);
            // remove from the list
            (void) ei->hash_link.detach();
            delete ei;
        }
    }
    DO_PTHREAD(pthread_mutex_destroy(_errlog_mutex));
    delete _errlog_mutex; // pthread_mutex_destroy() just uninitializes it. we still have to delete the pointer!
    _errlog_mutex = NULL; // cleanup for valgrind
}

void 
ErrLog::log(enum LogPriority prio, const char *format, ...) 
{
    if(_magic != ERRORLOG__MAGIC) {
        cerr << "Trying to use uninitialized ErrLog." <<endl;
        ::exit(1);
    }
    va_list ap;
    va_start(ap, format);

    _flush(); 

    if (prio > _level) {
        return;
    }

    CRITICAL_SECTION(cs, _errlog_mutex);
    switch(_destination) {

        case log_to_unix_file:
        case log_to_open_file:
        case log_to_stderr:

#ifdef HAVE_VPRINTF
            (void) vfprintf(_file,format, ap);
#else
#error need vfprintf
#endif
            fputc('\n', _file);
            fflush(_file);
            break;
            
        case log_to_ether:
            break;
    }
    va_end(ap);

    // clear the slate for the next use of operator<<
    w_reset_strstream(this->clog);
}

void 
ErrLog::_flush() 
{ 
    CRITICAL_SECTION(cs, _errlog_mutex);
    if(_magic != ERRORLOG__MAGIC) {
        cerr << "Fatal error: Trying to use uninitialized ErrLog." <<endl;
        ::exit(1);
    }
    this->clog << ends ;

    if (this->clog._prio <= _level) {
        switch(_destination) {

            case log_to_unix_file:
            case log_to_open_file:
            case log_to_stderr:
                fprintf(_file, "%s", this->clog.c_str());
                // fprintf(_file, "%s\n", this->clog.c_str());
                fflush(_file);
                break;
                
            case log_to_ether:
                break;
        }
    } 
    this->clog.flush();

    // reset to beginning of buffer
    w_reset_strstream(this->clog);
}
