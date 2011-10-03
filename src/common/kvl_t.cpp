/*<std-header orig-src='shore'>

 $Id: kvl_t.cpp,v 1.12 2010/05/26 01:20:12 nhall Exp $

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

#ifdef __GNUC__
#pragma implementation "kvl_t.h"
#endif

#define VEC_T_C
#include <cstdlib>
#include <cstring>
#include <w_stream.h>
#include <w_base.h>
#include "basics.h"
#include "kvl_t.h"

const cvec_t         kvl_t::eof("\0255EOF", 4); // the lexical order doesn't really matter;
                                        // it's the likelihood of a user coming up with
                                        // this as a legit key that matters
const cvec_t         kvl_t::bof("\0BOF", 4); // not used


/*********************************************************************
 *
 *  operator<<(ostream, kvl)
 *
 *  Pretty print "kvl" to "ostream".
 *
 *********************************************************************/
ostream& 
operator<<(ostream& o, const kvl_t& kvl)
{
    return o << "k(" << kvl.stid << '.' << kvl.h << '.' << kvl.g << ')';
}

/*********************************************************************
 *
 *  operator>>(istream, kvl)
 *
 *  Read a kvl from istream into "kvl". Format of kvl is a string
 *  of format "k(stid.h.g)".
 *
 *********************************************************************/
istream& 
operator>>(istream& i, kvl_t& kvl)
{
    char c[6];
    i >> c[0] >> c[1] >> kvl.stid >> c[2]
      >> kvl.h >> c[3]
      >> kvl.g >> c[4];
    c[5] = '\0';
    if (i) {
        if (strcmp(c, "k(..)"))  {
            i.clear(ios::badbit|i.rdstate());  // error
        }
    }
    return i;
}

