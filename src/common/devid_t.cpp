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

 $Id: devid_t.cpp,v 1.17 2010/05/26 01:20:11 nhall Exp $

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

#define DEVID_T_C

#ifdef __GNUC__
#pragma implementation
#endif

#include <cstdlib>
#include <w_stream.h>
#include <w_base.h>
#include "basics.h"
#include "devid_t.h"

#include <sthread.h>

devid_t::devid_t(const char* path)
: id(0),
  dev(0)
{
    // generate a device id by stat'ing the path and using the
    // inode number

    int    fd;
    w_rc_t    e;
#if W_DEBUG_LEVEL > 2
    const    char    *what = "open";
#endif

    e = sthread_t::open(path, sthread_t::OPEN_RDONLY, 0, fd);
    if (!e.is_error()) {
        sthread_t::filestat_t    st;
#if W_DEBUG_LEVEL > 2
        what = "fstat";
#endif
        e = sthread_t::fstat(fd, st);
        if (!e.is_error()) {
            id = st.st_file_id;
            dev = st.st_device_id;
        }
        W_COERCE(sthread_t::close(fd));
    }
    /* XXX don't complain if file doesn't exist?  IDs should only
       be generated in a valid state. */
    if (e.is_error()) {
#if W_DEBUG_LEVEL > 2
        // this is NOT an error as far as we return appropriate return value.
        // see smsh/scripts/vol.init. it calls format_dev to check the file exists!
        cout // cerr
        << "warning - devid_t::devid_t(" << path 
            << "): " << what << ":" << endl << e << endl;
#endif
        *this = null;
    }
}

ostream& operator<<(ostream& o, const devid_t& d)
{
    return o << d.dev << "." << d.id;
}

const devid_t devid_t::null;

