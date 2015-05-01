/*<std-header orig-src='shore'>

 $Id: lid_t.cpp,v 1.38 2010/05/26 01:20:12 nhall Exp $

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

#define LID_T_C

#include <cstdlib>
#include <w_stream.h>
#include <cstring>
#include "basics.h"
#include "tid_t.h"
#include "lid_t.h"

const lvid_t lvid_t::null(0,0);

ostream& operator<<(ostream& o, const lvid_t& lvid)
{
    const u_char* p = (const u_char*) &lvid.high;
    //(char*)inet_ntoa(lvid.net_addr)

        // WARNING: byte-order-dependent
    return o << u_int(p[0]) << '.' << u_int(p[1]) << '.'
             << u_int(p[2]) << '.' << u_int(p[3]) << ':'
             <<        lvid.low;
}

istream& operator>>(istream& is, lvid_t& lvid)
{
    is.clear();
    uint32_t i;
    char c;
    const uint32_t parts = sizeof(lvid.high); // should be 4
    int  temp[parts];

    // in case not all fields are represented
    // in the input string
    for (i=0; i<parts; i++) { temp[i] = 0; }

    // read each part of the lvid "address" and stop if
    // it ends early
    for (i=0, c='.'; i<parts && c!='\0'; i++) {
        is >> temp[i];        
        // peek to see the delimiters of lvid pieces.
        if (is.peek() == '.' || is.peek()== ':') {
            is >> c;
        } else {
            c = '\0';
            break;
        }
    }
    if (i==1) {
        // we had a simple integer: put it
        // int the low part
        lvid.low = temp[0];
        temp[0]=0;
    } else if (c == ':') {
        // we had a.b.c.d:l
        // we had a.b.c:l
        // we had a.b:l
        // we had a:l
        if(i!=parts) {
            is.clear(ios::badbit);
        } else {
            is >> lvid.low;
        }
    } else  {
        // we had
        // a.b
        // a.b.c
        // a.b.c.d
        is.clear(ios::badbit);
    }

    ((char*)&lvid.high)[0] = temp[0];
    ((char*)&lvid.high)[1] = temp[1];
    ((char*)&lvid.high)[2] = temp[2];
    ((char*)&lvid.high)[3] = temp[3];

    return is;
}
