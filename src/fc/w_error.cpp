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

/*<std-header orig-src='shore'>

 $Id: w_error.cpp,v 1.64 2010/12/08 17:37:37 nhall Exp $

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

#if defined(__GNUC__)
#pragma implementation "w_error.h"
#endif

#include <cstring>

#include <w_base.h>
const
#include <fc_einfo_gen.h>
#if USE_BLOCK_ALLOC_FOR_W_ERROR_T
#include "block_alloc.h"
#endif

//
// Static equivalent of insert(..., error_info, ...)
//
const 
w_error_t::info_t*        w_error_t::_range_start[w_error_t::max_range] = {
    w_error_t::error_info, 0 
};
uint32_t        w_error_t::_range_cnt[w_error_t::max_range] = {
    fcERRMAX - fcERRMIN + 1, 0
};

const char *w_error_t::_range_name[w_error_t::max_range]= { 
    "Foundation Classes",
    0
};
uint32_t        w_error_t::_nreg = 1;

const w_error_t w_error_t::no_error_instance(__FILE__, __LINE__, 0, 0, 0);

w_error_t* const w_error_t::no_error = const_cast<w_error_t *>(&no_error_instance);

static void w_error_t_no_error_code()
{
}
w_error_t&
w_error_t::add_trace_info(
        const char* const    filename,
        uint32_t        line_num)
{
    if (_trace_cnt < max_trace)  {
        _trace_file[_trace_cnt] = filename;
        _trace_line[_trace_cnt] = line_num;
        ++_trace_cnt;
    }

    return *this;
}

#if W_DEBUG_LEVEL > 1
#define CHECK_STRING(x) if((x) != NULL) w_assert2(*(x) != 0)
#else
#define CHECK_STRING(x) 
#endif

w_error_t&
w_error_t::clear_more_info_msg()
{
    delete[] more_info_msg;
    more_info_msg = NULL;
    return *this;
}

w_error_t&
w_error_t::append_more_info_msg(const char* more_info)
{
    CHECK_STRING(more_info);
    if (more_info)  
    {
        int more_info_len = strlen(more_info);
        if(more_info_len > 0)
        {
            if(more_info[more_info_len-1] == '\n') more_info_len--;

            int more_info_msg_len = more_info_msg?strlen(more_info_msg):0;
            char* new_more_info_msg = new 
                char[more_info_len + more_info_msg_len + 2];
            if(more_info_msg) { 
                strcpy(new_more_info_msg, more_info_msg);
            }
            strcpy(new_more_info_msg + more_info_msg_len, more_info);
            new_more_info_msg[more_info_msg_len + more_info_len] = '\n';
            new_more_info_msg[more_info_msg_len + more_info_len + 1] = '\0';

            if(more_info_msg) delete[] more_info_msg;
            more_info_msg = new_more_info_msg;

            CHECK_STRING(more_info_msg);
        }
    }

    return *this;
}

const char*
w_error_t::get_more_info_msg() const
{ 
    CHECK_STRING(more_info_msg);
    return more_info_msg;
}

/* automagically generate a sys_err_num from an errcode */
inline uint32_t w_error_t::classify(int er)
{
    uint32_t    sys = 0;
    switch (er) {
    case fcOS:
        sys = errno;
        break;
    }
    return sys;
}

#if USE_BLOCK_ALLOC_FOR_W_ERROR_T
DEFINE_TLS_SCHWARZ(block_alloc<w_error_t>, w_error_alloc);
// This operator delete doesn't do anything automagic.
// It's just a little more convenient for the caller than
// calling block_alloc<w_error_t>::destroy_object.
// Only a little. 
// It still has to be called iff USE_BLOCK_ALLOC_FOR_W_ERROR_T
// because we have to avoid doing any ::delete, which will call
// the destructor implicitly. Destroy_object calls the destructor
// explicitly.
void w_error_t::operator delete(void* p) {
    DEBUG_BLOCK_ALLOC_MARK_FOR_DELETION((w_error_t *)p)
    block_alloc<w_error_t>::destroy_object((w_error_t*) p);
}
#endif


inline
w_error_t::w_error_t(const char* const        fi,
                 uint32_t        li,
                 err_num_t        er,
                 w_error_t*        list,
                 const char*    more_info)
: err_num(er),
  file(fi),
  line(li), 
  sys_err_num(classify(er)),
  more_info_msg(more_info),
  _trace_cnt(0),
  _next(list)
{
    CHECK_STRING(more_info_msg);
    CHECKIT;
}


w_error_t*
w_error_t::make(
        const char* const    filename,
        uint32_t              line_num,
        err_num_t            err_num,
        w_error_t*           list,
        const char*          more_info)
{
#if USE_BLOCK_ALLOC_FOR_W_ERROR_T
    return new (*w_error_alloc) w_error_t(filename, line_num, err_num, list, more_info);
#else
    return new  w_error_t(filename, line_num, err_num, list, more_info);
#endif

}

inline NORET
w_error_t::w_error_t(
        const char* const    fi,
        uint32_t        li,
        err_num_t      er,
        uint32_t        sys_er,
        w_error_t*        list,
        const char*        more_info)
        : err_num(er),
          file(fi), line(li), 
          sys_err_num(sys_er),
          more_info_msg(more_info),
          _trace_cnt(0),
          _next(list)
{
    CHECK_STRING(more_info_msg);
    CHECKIT;
}

w_error_t*
w_error_t::make(
        const char* const    filename,
        uint32_t              line_num,
        err_num_t            err_num,
        uint32_t              sys_err,
        w_error_t*           list,
        const char*          more_info)
{
    CHECK_STRING(more_info);
// Template version causes gcc to choke on strict-aliasing warning
#if USE_BLOCK_ALLOC_FOR_W_ERROR_T
  return  new (*w_error_alloc) w_error_t(filename, line_num, 
          err_num, sys_err, list, more_info);
  // void * ptr = (*w_error_alloc).alloc(); 
  // return  new (ptr)  w_error_t(filename, line_num, err_num, sys_err, list, more_info);
#else
    return new  w_error_t(filename, line_num, err_num, sys_err, list, more_info);
#endif
}

// Application has to be careful not to let these be called by 
// multiple threads at once.
// The SM protects calls to this (which are made via init_errorcodes()) 
// with a mutex in the ss_m constructor and destructor.
bool
w_error_t::insert(
        const char *        modulename,
        const info_t    info[],
        uint32_t        count)
{
    if (_nreg >= max_range)
        return false;

    err_num_t start = info[0].err_num;

    for (uint32_t i = 0; i < _nreg; i++)  {
        if (start >= _range_start[i]->err_num && start < _range_cnt[i])
            return false;
        uint32_t end = start + count;
        if (end >= _range_start[i]->err_num && end < _range_cnt[i])
            return false;
    }
    _range_start[_nreg] = info;
    _range_cnt[_nreg] = count;
    _range_name[_nreg] = modulename;

    ++_nreg;
    return true;
}

const char* 
w_error_t::error_string(err_num_t err_num)
{
    if(err_num ==  w_error_t::no_error->err_num ) {
        return "no error";
    }
    uint32_t i;
    for (i = 0; i < _nreg; i++)  {
        if (err_num >= _range_start[i]->err_num && 
            err_num <= _range_start[i]->err_num + _range_cnt[i]) {
            break;
        }
    }
        
    if (i == _nreg)  {
        w_error_t_no_error_code();
        return error_string( fcNOSUCHERROR );
        // return "unknown error code";
    }

    const uint32_t j = CAST(int, err_num - _range_start[i]->err_num);
    return _range_start[i][j].errstr;
}

const char*
w_error_t::module_name(err_num_t err_num)
{
    if(err_num ==  w_error_t::no_error->err_num ) {
            return "all modules";
    }
    uint32_t i;
    for (i = 0; i < _nreg; i++)  {
        if (err_num >= _range_start[i]->err_num && 
            err_num <= _range_start[i]->err_num + _range_cnt[i]) {
            break;
        }
    }
    
    if (i == _nreg)  {
        return "unknown module";
    }
    return _range_name[i];
}

void format_unix_error(int err, char *buf, int bufsize)
{
#ifdef HAVE_STRERROR
    char    *s = strerror(err);
#else
    char    *s = "No strerror function. Cannot format unix error.";
#endif
    strncpy(buf, s, bufsize);
    buf[bufsize-1] = '\0';
}

ostream& w_error_t::print_error(ostream &o) const
{
    if (this == w_error_t::no_error) {
        return o << "no error";
    }

    int cnt = 1;
    for (const w_error_t* p = this; p; p = p->_next, ++cnt)  {

        const char* f = strrchr(p->file, '/');
        f ? ++f : f = p->file;
        o << cnt << ". error in " << f << ':' << p->line << " ";
        if(cnt > 1) {
            if(p == this) {
                o << "Error recurses, stopping" << endl;
                break;
            } 
            if(p->_next == p) {
                o << "Error next is same, stopping" << endl;
            break;
            }
        }
        if(cnt > 20) {
            o << "Error chain >20, stopping" << endl;
            break;
        }
        o << p->error_string(p->err_num);
        o << " [0x" << hex << p->err_num << dec << "]";

        /* Eventually error subsystems will have their own interfaces
           here. */
        switch (p->err_num) {
        case fcOS: {
            char buf[1024];
            format_unix_error(p->sys_err_num, buf, sizeof(buf));
            o << " --- " << buf;
            break;
            } 
        }

        o << endl;

        if (more_info_msg)  {
            o << "\tadditional information: " << more_info_msg << endl;
        }

        if (p->_trace_cnt)  {
            o << "\tcalled from:" << endl;
            for (unsigned i = 0; i < p->_trace_cnt; i++)  {
                f = strrchr(p->_trace_file[i], '/');
                f ? ++f : f = p->_trace_file[i];
                o << "\t" << i << ") " << f << ':' 
                  << p->_trace_line[i] << endl;
            }
        }
    }

    return o;
}

ostream &operator<<(ostream &o, const w_error_t &obj)
{
        return obj.print_error(o);
}

ostream &
w_error_t::print(ostream &out)
{
    for (unsigned i = 0; i < _nreg; i++)  {
        err_num_t first    = _range_start[i]->err_num;
        unsigned int last    = first + _range_cnt[i] - 1;

        for (unsigned j = first; j <= last; j++)  {
            const char *c = module_name(j);
            const char *s = error_string(j);

            out <<  c << ":" << j << ":" << s << endl;
        }
    }
        
    return out;
}

/* Here we insert documentation for generation of error codes,
 * to be picked up by doxygen:
 */

/**\page ERRNUM Error Codes
 * This page describes the error codes used in 
 * the various Shore Storage Manager modules.
 *
 * These numbers are generated by the Perl script 
 * \code
 * tools/errors.pl
 * \endcode
 *
 * This page is of interest to those who wish to use this tool to
 * generate their own sets of error codes.
 *
 * \section ERRNUM1 Error Codes and Modules
 * Error codes are unsigned integers.
 * Each error code has associated metadata, which consists of 
 * a descriptive string and a name 
 * (either by way of an enumeration, or by a C-preprocessor-defined name).
 *
 * The integer values associated with error code names,
 * the descriptive strings, the enumerations, and the
 * C Preprocessor macros are generated by the Perl script.
 * Error codes are grouped into modules, so that all the error codes
 * for a software module and their metadata are kept together.
 * Each module is given a mask, which is folded into the
 * values assigned to the errorcodes.
 * This keeps the error codes for different software modules distinct.  
 * The software that manages error codes keeps a (global) list
 * of all the modules of error codes.
 * Each software layer that uses the error codes must 
 * invoke a method to `install' its module in the global list, 
 * preferably at server start-up time, in the main thread, using w_error_t::insert,
 * which is called by a Perl-generated method \<class\>::init_errorcodes();-
 *
 * \section ERRNUM2 Generating Sets of Error Codes
 * Generating the codes is best described by way of an example.
 * The following example is taken from the Shore Storage Manager.
 *
 * The script takes one of two mutually exclusive options, and a file name.  
 * One or the other of the options (-d, -e) is required:
 * \code
 * $(SHORE_SOURCES)/tools/errors.pl -d <input-file>
 *    or
 * $(SHORE_SOURCES)/tools/errors.pl -e <input-file>
 * \endcode
 *
 * In the first case (-d) the named constants are generated as 
 * C preprocessor defined constants.
 * The prefix of the generated names is capitalized and 
 * separated from the rest of the name by an underscore character. 
 *
 * In the second case (-e) the named constants are generated as
 * members of an anonymous enumeration.  The prefix of the generated names is
 * taken, case unchanged, from the input file.
 * \code
 * e = 0x00080000 "Storage Manager" smlevel_0 {
 * 
 * ASSERT          Assertion failed
 * USERABORT       User initiated abort
 * ... and so on ...
 * }
 * \endcode
 *
 * The input is parsed as follows.
 * On the first line: 
 * - \b e 
    A prefix used to generate the names of the constants for the
    error codes for this module. 
    This prefix must not conflict with prefixes for other
    modules.
 * - \b = 
 *   Separates the name prefix from the mask.
 * - \b 0x00080000 
 *   This mask is added into each named constant generated for this module.
 *   This mask must not conflict with the masks used in other modules.
 * - \b "Storage Manager"
 *   The name of the module.
 * - \b smlevel_0
 *   The name of a C++ class. If a class name is present, certain generated
 *   data structures and methods will be members of the class, such as
 *   init_errorcodes(), which will be \<class\>::init_errorcodes().
 *   If no class name appears, these entities will have global namescope.
 * - \b {
 *   Begins the set of error codes and descriptions for this module.
 *
 * - The next two lines define error codes:
 *   - ASSERT
 *   - USERABORT
 *   These cause the named constants eASSERT and eUSERABORT to appear 
 *   in an anonymous enumeration type.
 *   The values associated with eASSERT and eUSERABORT will contain the mask:
 *   \code
 *   enum {
 *     eASSERT    = 0x80000,
 *     eUSERABORT = 0x80001,
 *     ...
 *   }
 *   \endcode
 *   The string \b "Assertion failed" is the descriptive string associated with
 *   eASSERT.  These descriptive strings will appear in arrays generated by 
 *   the Perl script.
 * - \b }
 *   Ends the set of error codes and descriptions for this module.
 *
 *   Blank lines may appear anywhere.
 *   Lines beginning with \b # are comments.
 *
 *
 * \section ERRNUM3 Generated Files
 * The names of the files generated contain the prefix given on the first line
 * of the module's input.  In the above example, that prefix is \b e.
 *
 * The set of files generated is determined by the arguments with which the 
 * script is invoked:
 * \subsection errorspld errors.pl -d <file> :
 *     - \<prefix\>_error_def_gen.h
 *        - defines C Preprocessor macros for the \<prefix\>_ERROR values, e.g.,
 *        \code
 *        #define E_ASSERT 0x80000
 *        \endcode
 *     - \<prefix\>_errmsg_gen.h (same as with -e option)
 *        - defines an array \code static char* <prefix>_errmsg[] \endcode
 *          containing the descriptive strings and
 *          \code const <prefix>_msg_size = <number of error codes in this module> \endcode.
 *     - \<prefix\>_einfo_gen.h (same as with -e option)
 *        - defines an array \code <class>::error_info[] \endcode
 *          of { integer,  descriptive-string } pairs.
 *        - defines  the method \code void <class>::init_errorcodes() \endcode
 *          to be called by the server at start-up to insert the 
 *          module information into the global list; this is generated only if 
 *          the module contains a class name.
 *     - \<prefix\>_einfo_bakw_gen.h
 *        - defines an array \code  <prefix>_error_info_backw[] \endcode 
 *          of { integer, string-name-of-macro } pairs.
 *
 * \subsection errorsple errors.pl -e <file> :
 *     - \<prefix\>_error_enum_gen.h
 *        - defines an enumeration for the \<prefix\>_ERROR values, e.g.,
 *        \code
 *        enum {
 *         eASSERT         = 0x80000,
 *         eUSERABORT      = 0x80001,
 *         ...
 *        }
 *        \endcode
 *     - \<prefix\>_errmsg_gen.h (same as with -d option)
 *        - defines an array \code static char* <prefix>_errmsg[] \endcode
 *          containing the descriptive strings and
 *          \code const <prefix>_msg_size = <number of error codes in this module> \endcode.
 *     - \<prefix\>_einfo_gen.h (same as with -d option)
 *        - defines an array \code <class>::error_info[] \endcode
 *          of { integer,  descriptive-string } pairs.
 *        - defines  the method \code void <class>::init_errorcodes() \endcode
 *          to be called by the server at start-up to insert the 
 *          module information into the global list. This is generated only
 *          if the module contains a class name.
 *
 */

