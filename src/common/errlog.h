/*<std-header orig-src='shore' incl-file-exclusion='ERRLOG_H'>

 $Id: errlog.h,v 1.1 2010/12/09 15:29:05 nhall Exp $

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

#ifndef ERRLOG_H
#define ERRLOG_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/* errlog.h -- facilities for logging errors */

#include <cassert>
#include <cstdlib>
#include <cstddef>
#include <w.h>
#include <iostream>
#include <cstdio>    // XXX just needs a forward decl
#include <w_pthread.h> 

class ErrLog; // forward
class logstream; // forward

#ifndef    _SYSLOG_H
#define LOG_EMERG 0 
#define LOG_ALERT 1
#define LOG_CRIT  2
#define LOG_ERR   3
#define LOG_WARNING 4
#define LOG_NOTICE  5
#define LOG_INFO  6
#define LOG_DEBUG 7
#define LOG_USER  8
#endif

/** \brief A namespace for errlog-related types.
 */
namespace shore_errlog {
using namespace std;

/*!\enum LogPriority
 * \brief Enumeration that enables filtering of messages by priority.
 *
 * The following __omanip functions are defined to correspond 
 * to the LogPriority:
 * emerg_prio, fatal_prio, internal_prio, 
 * error_prio, warning_prio, info_prio, debug_prio
 *
 * Log messages must end with the new __omanip function
 * flushl.
 */
enum LogPriority {
    log_none = -1,    // none (for global variable logging_level only)
    log_emerg = LOG_EMERG,        // no point in continuing (syslog's LOG_EMERG)
    log_fatal = LOG_ALERT,        // no point in continuing (syslog's LOG_ALERT)
    log_alert = log_fatal,        // alias
    log_internal = LOG_CRIT,    // internal error 
    log_error = LOG_ERR,        // client error 
    log_warning = LOG_WARNING,    // client error 
    log_info = LOG_INFO,        // just for yucks 
    log_debug=LOG_DEBUG,        // for debugging gory details 
    log_all,
    default_prio = log_error
};

} /* namespace syslog_compat */

using namespace shore_errlog;

extern ostream& flushl(ostream& o);
extern ostream& emerg_prio(ostream& o);
extern ostream& fatal_prio(ostream& o);
extern ostream& internal_prio(ostream& o);
extern ostream& error_prio(ostream& o);
extern ostream& warning_prio(ostream& o);
extern ostream& info_prio(ostream& o);
extern ostream& debug_prio(ostream& o);
extern void setprio(ostream&, LogPriority);
extern logstream *is_logstream(std::basic_ostream<char, std::char_traits<char > > &);
/** \brief A strstream-based log output stream. 
 * \details
 * Responds to these iomanip functions :
 *  emerg_prio, 
 *  fatal_prio, 
 *  internal_prio, 
 *  error_prio, 
 *  warning_prio, 
 *  info_prio, 
 *  debug_prio
 */
class logstream : public w_ostrstream {
    friend class ErrLog;
    friend ostream &flush_and_setprio(ostream& o, LogPriority p);
    friend ostream& emerg_prio(ostream& o);
    friend ostream& fatal_prio(ostream& o);
    friend ostream& internal_prio(ostream& o);
    friend ostream& error_prio(ostream& o);
    friend ostream& warning_prio(ostream& o);
    friend ostream& info_prio(ostream& o);
    friend ostream& debug_prio(ostream& o);

    unsigned int     __magic1;
    LogPriority     _prio;
    ErrLog*        _log;
    unsigned int     __magic2;

public:
/** \cond skip */
	friend logstream *is_logstream(std::basic_ostream<char, std::char_traits<char > > &);

    enum { LOGSTREAM__MAGIC = 0xad12bc45 };
private:
    // DEAD static w_ostrstream static_stream;
public:
    logstream(char *buf, size_t bufsize = 1000)
    : w_ostrstream(buf, bufsize),
      __magic1(LOGSTREAM__MAGIC),
      _prio(log_none), 
      __magic2(LOGSTREAM__MAGIC)
        {
            // tie this to a hidden stream; 
			// DEAD ios::tie(&static_stream);
            // DEAD assert(ios::tie() == &static_stream) ;
            assert(__magic1==LOGSTREAM__MAGIC);
        }

protected:
    void  init_errlog(ErrLog* mine) { _log = mine; }
/** \endcond skip
 */
};

/** \enum LoggingDestination Describes the possible log destinations,
 * used for the ErrLog constructor.
 */
enum LoggingDestination {
    log_to_ether, /*! no logging */
    log_to_unix_file, /*! sent to a unix file identified by name */
    log_to_open_file, /*! sent to an open unix FILE* */
    log_to_stderr  /*! sent to stderr */
}; 

typedef void (*ErrLogFunc)(ErrLog *, void *);


/** \brief A syslog-style output stream for logging errors, information, etc.
 *
 * This output stream is used for issuing errors, "information",
 * debugging tracing, etc. to the operator (e.g., stderr) or to
 * a file,  somewhat like syslog.
 * \attention This predates true multi-threading and is thus not thread-safe.
 * We have not yet replaced this code, with a thread-safe version.
 * It is still useful for debugging non-timing-dependent issues,
 * for issuing operator messages before multithreading really starts, e.g.,
 * during recovery.
 *
 * Example:
 * \code
 * ErrLog errlog(log_to_unix_file, "sm.errlog");
 * errlog->clog << info_prio << "Restart recovery : analysis phase " << flushl;
 * \endcode
 */
class ErrLog {
    friend class logstream;
    friend logstream *is_logstream(std::basic_ostream<char, std::char_traits<char > > &);
    friend ostream &flush_and_setprio(ostream& o, LogPriority p);

    LoggingDestination _destination;
    LogPriority       _level;
    FILE*             _file;        // if local file logging is used
    const char *      _ident; // identity for syslog & local use
    char *            _buffer; // default is static buffer
    size_t            _bufsize; 
    unsigned int      _magic;
    pthread_mutex_t*  _errlog_mutex;

    enum { ERRORLOG__MAGIC = 0xa2d29754 };

public:

    /** Create a log.
     * @param[in] ident  The name of the log.
     * @param[in] dest   Indicates destination (unix file, stderr, etc).
     * @param[in] filename Name of destination or "-' .
     * @param[in] level  Minimum priority level of messages to be sent to the file. For filtering.
     * @param[in] ownbuf Buffer to use. Default is NULL.
     * @param[in] ownbufsz Size of given buffer. Default is 0.
     *
     * Using the name "-" is the same as specifying log_to_stderr
     */
    ErrLog(
        const char *ident,
        LoggingDestination dest, // required
        const char *filename = 0,             
        LogPriority level =  default_prio,
        char *ownbuf = 0,
        int  ownbufsz = 0  // length of ownbuf, if ownbuf is given
    );

    /** Create a log.
     * @param[in] ident  The name of the log.
     * @param[in] dest   Indicates destination (unix file, stderr, etc).
     * @param[in] file   Already open FILE*.  Default is NULL.
     * @param[in] level  Minimum priority level of messages to be sent to the file. For filtering.
     * @param[in] ownbuf Buffer to use. Default is NULL.
     * @param[in] ownbufsz Size of given buffer. Default is 0.
     */
    ErrLog(
        const char *ident,
        LoggingDestination dest, // required
        FILE *file = 0,             
        LogPriority level =  default_prio,
        char *ownbuf = 0,
        int  ownbufsz = 0  // length of ownbuf, if ownbuf is given
    );

    ~ErrLog();

    /** Convert a char string to an enumeration value.  
     * @param[in] arg The string to parse.
     * @param[out] ok Returns true/false if parse worked/not (optional)
     */
    static LogPriority parse(const char *arg, bool *ok=0);

    /** A stream that can be used with operator<<.
     * Example:
     * \code
     * ErrLog E("XXX", log_to_unix_file, "XXX.out");
     * E->clog << obj << endl;
     * \endcode
     */
    logstream    clog;

    /** Format and issue a message with the given priority, that
     * is, don't issue it unless this priority is equal to or higher 
     * than the priority of this error log.
     */
    void log(enum LogPriority prio, const char *format, ...);

    /** Return the name of the file if known */
    const char * ident() { 
        return _ident;
    }
    LoggingDestination    destination() { return _destination; };

    /** Return the current logging level */
    LogPriority getloglevel() { return _level; }

    /** Return a static string describing the current logging level */
    const char * getloglevelname() {
        switch(_level) {
            case log_none:
                return "log_none";
            case log_emerg:
                return "log_emerg";
            case log_fatal:
                return "log_fatal";
            case log_internal:
                return "log_internal";
            case log_error:
                return "log_error";
            case log_warning:
                return "log_warning";
            case log_info:
                return "log_info";
            case log_debug:
                return "log_debug";
            case log_all:
                return "log_all";
            default:
                return "error: unknown";
                // w_assert1(0);
        }
    }

    /** Change the current logging level */
    LogPriority setloglevel( LogPriority prio);

private:
    void _init1();
    void _init2();
    void _flush();
    void _openlogfile( const char *filename );
    void _closelogfile();
    NORET ErrLog(const ErrLog &); // disabled
    

} /* ErrLog */;

/*<std-footer incl-file-exclusion='ERRLOG_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
