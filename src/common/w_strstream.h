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

/*<std-header orig-src='shore' incl-file-exclusion='W_STRSTREAM_H'>

 $Id: w_strstream.h,v 1.19 2010/12/08 17:37:37 nhall Exp $

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

#ifndef W_STRSTREAM_H
#define W_STRSTREAM_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include <w_workaround.h>

/*
 * Shore only supports "shore strstreams", which are a compatability
 * layer which
 *
 *   1) works with either strstream (or eventually) stringstream.
 *   2) provides automatic string deallocation, the most error-prone
 *      use of strstreams with allocated memory.
 *   3) Provides for static buffer non-dynamic streams to eliminate
 *      a large amount of duplicate shore code (forthcoming).
 *
 * The c_str() method _guarantees_ that the returned c-string WILL
 * be nul-terminated.  This prevents unterminated string bugs.  This
 * occurs at the expense of something else.  That cost is increasing
 * the buffer size on a dynamically allocated string.  Or, with a
 * fixed buffer, over-writing the last valid character (at the end
 * of the buffer) with a nul.   The implementation has low
 * overhead in the expected case that the string is nul-terminated
 * by 'ends'.
 *
 * To access the contents without any funky appending behavior, the
 * proper way to do that is with address+length.  That is done with
 * the data() method and then pcount() for the length of the buffer.
 *
 * The auto-deallocation mimics the behavior of newer stringstreams,
 * 
 */

#ifdef W_USE_COMPAT_STRSTREAM
#include "w_compat_strstream.h"
#else
#include <strstream>
#endif
#include <cstring>

#if defined(W_USE_COMPAT_STRSTREAM)
/* #define instead of typedef so everything is hidden, and not available
   or conflicting with other users. */
#define    istrstream       shore_compat::istrstream
#define    ostrstream       shore_compat::ostrstream
#define    strstreambuf     shore_compat::strstreambuf
#endif

/**\brief Input string stream based on shore_compat::istrstream
 */
class w_istrstream : public istrstream {
public:
    /// Construct using strlen(s)
    w_istrstream(const char *s)
    : istrstream(s, strlen(s))
    {
    }

    /// Construct using a given length
    w_istrstream(const char *s, size_t l)
    : istrstream(s, l)
    {
    }

};

/**\brief Output string stream based on shore_compat::ostrstream
 */
class w_ostrstream : public ostrstream {

public:
    /// Construct w/o any length limit
    w_ostrstream()
    : ostrstream()
    {
    }

    /// Construct with given limit
    w_ostrstream(char *s, size_t l)
    : ostrstream(s, l)
    {
    }

    ~w_ostrstream()
    {
    }

    /// Return a pointer to nul-terminated output string.
    const char *c_str()
    {
        const char    *s = str();
        int        l = pcount();
        int        last = (l == 0 ? 0 : l-1);

        // ensure it is nul-terminated
        if (s[last] != '\0') {
            *this << ends;

            // it could move with the addition
            s = str();
            int    l2 = pcount();
            last = (l2 == 0 ? 0 : l2-1);

            // no growth ... the end string didn't fit
            // over-write the last valid char.
            // a throw would be a possibility too.
            if (l == l2 || s[last] != '\0')
                ((char *)s)[last] = '\0';
        }
        return s;
    }

    /* Data() + size() allows access to buffer with nulls */
    /// Return a pointer to buffer.
    const char *data()
    {
        return str();
    }

    /// Return a pointer to copy of nul-terminated string. Delegates
    /// responsibility for freeing to the caller.
    const char *new_c_str()
    {
        /* A snapshot of the buffer as it currently is .. but still frozen */
        const char *s = c_str();
        char *t = new char[strlen(s) + 1]; 
        if (t)
            strcpy(t, s);
        return t;
    }

};


#if defined(__GNUG__)
/* Older strstreams have different buffer semantics than newer ones */
#if W_GCC_THIS_VER < W_GCC_VER(3,0)
#define W_STRSTREAM_NEED_SETB
#endif
#endif


/* XXX this would be easy as a template, but too much effort to maintain,
   and the stack issue would still be there. */

/// Fixed-len buffer-based w_ostrstream
class w_ostrstream_buf : public w_ostrstream {
    // maximum stack frame impingement
    enum { default_buf_size = 128 };

    char    *_buf;
    size_t    _buf_size;

    char    _default_buf[default_buf_size];

    // access underlying functions ... disgusting, but scoped funky
    class w_ostrstreambuf : public strstreambuf {
    public:
        void public_setbuf(char *start, size_t len)
        {
            // nothing to read
            setg(start, start, start);

            // just the buf to write
            setp(start, start + len);

#ifdef W_STRSTREAM_NEED_SETB
            // and the underlying buffer too
            setb(start, start+len);
#undef W_STRSTREAM_NEED_SETB
#endif
        }
    };

public:
    w_ostrstream_buf(size_t len)
    : w_ostrstream(_default_buf, default_buf_size),
      _buf(_default_buf),
      _buf_size(len <= default_buf_size ? len : (size_t)default_buf_size) // min
    {
        if (len > _buf_size)
            _buf = new char[len];

        if (len != _buf_size) {
            _buf_size = len;
            ((w_ostrstreambuf*) rdbuf())->public_setbuf(_buf, _buf_size);
        }
    }

    ~w_ostrstream_buf()
    {
        if (_buf != _default_buf)
            delete [] _buf;
        _buf = 0;
        _buf_size = 0;
    }
};

#ifdef W_USE_COMPAT_STRSTREAM
#undef istrstream
#undef ostrstream
#undef strstreambuf
#endif



/*<std-footer incl-file-exclusion='W_STRSTREAM_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
