/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

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

 $Id: log.cpp,v 1.137 2010/12/08 17:37:42 nhall Exp $

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

#define SM_SOURCE
#define LOG_C

#include "sm_base.h"
#include "logdef_gen.cpp"
#include "log.h"
#include "log_core.h"
#include "log_lsn_tracker.h"
#include "lock_raw.h"
#include "bf_tree.h"
#include <algorithm> // for std::swap
#include <stdio.h> // snprintf
#include <boost/static_assert.hpp>
#include <vector>


typedef smlevel_0::fileoff_t fileoff_t;


/*********************************************************************
 *
 *  log_i::xct_next(lsn, r)
 *
 *  Read the next record into r and return its lsn in lsn.
 *  Return false if EOF reached. true otherwise.
 *
 *********************************************************************/
bool log_i::xct_next(lsn_t& lsn, logrec_t& r)
{
    // Initially (before the first xct_next call,
    // 'cursor' is set to the starting point of the scan
    // After each xct_next call,
    // 'cursor' is set to the lsn of the next log record if forward scan
    // or the lsn of the fetched log record if backward scan
    // log.fetch() returns eEOF when it reaches the end of the scan

    bool eof = (cursor == lsn_t::null);

    if (! eof) {
        lsn = cursor;
        logrec_t* b;
        rc_t rc = log.fetch(lsn, b, &cursor, forward_scan);  // Either forward or backward scan

        if (!rc.is_error())  {
            memcpy(&r, b, b->length());
        }
        // release right away, since this is only
        // used in recovery.
        log.release();

        if (rc.is_error())  {
            last_rc = RC_AUGMENT(rc);
            RC_APPEND_MSG(last_rc, << "trying to fetch lsn " << cursor);

            if (last_rc.err_num() == eEOF)
                eof = true;
            else  {
                cerr << "Fatal error : " << last_rc << endl;
            }
        }
    }

    return ! eof;
}
