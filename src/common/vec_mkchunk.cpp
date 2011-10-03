/*<std-header orig-src='shore'>

 $Id: vec_mkchunk.cpp,v 1.15 2010/05/26 01:20:12 nhall Exp $

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

/**\cond skip */
#ifdef __GNUC__
     #pragma implementation
#endif

#define VEC_T_C
#include <cstdlib>
#include <w_stream.h>
#include <w_base.h>
#include <w_minmax.h>
#include "basics.h"
#include "vec_t.h"
#include "umemcmp.h"
#include "w_debug.h"


/////////////////////////////////////////////////////
// "result" is a vector with size() no larger than 
// maxsize, whose contents are taken from the part
// of *this that's left after you skip the first
// "offset" bytes of *this.
// CALLER provides result, which is reset() right
// away, and re-used each time this is called.
//
// The general idea is that this allows you to take
// a vector of arbitrary configuration (in the context
// of writes, say) and break it up into vectors of
// "maxsize" sizes so that you can limit the sizes of
// writes.
/////////////////////////////////////////////////////
void
vec_t::mkchunk(
    int                maxsize,
    int                offset, // # skipped
    vec_t            &result // provided by the caller
) const
{
    int i;

    w_assert1( _base[0].ptr != zero_location );

    DBG(<<"offset " << offset << " in vector :");
    result.reset();

    // return a vector representing the next
    // maxsize bytes starting at the given offset
    // from the data represented by the input vector
    int        first_chunk=0, first_chunk_offset=0, first_chunk_len=0;
    {
        // find first_chunk
        int skipped=0, skipping;

        for(i=0; i<this->count(); i++) {
            skipping = this->len(i);
            if(skipped + skipping > offset) {
                // found
                first_chunk = i;
                first_chunk_offset = offset - skipped;
                first_chunk_len = skipping - first_chunk_offset;
                if(first_chunk_len > maxsize) {
                    first_chunk_len = maxsize;
                }

        DBG(<<"put " << W_ADDR(this->ptr(i)) << 
            "+" << first_chunk_offset << ", " << first_chunk_len);

                result.put((char*)this->ptr(i)+first_chunk_offset,first_chunk_len);
                break;
            }
            skipped += skipping;
        }
        if(first_chunk_len == 0) return;
    }

    if(first_chunk_len < maxsize) {
        // find next chunks up to the last
        int used, is_using ;

        used = first_chunk_len;
        for(i=first_chunk+1; i<this->count(); i++) {
            is_using = this->len(i);
            if(used + is_using <= maxsize) {
                // use the whole thing
                used += is_using;

                DBG(<<"put " << W_ADDR(this->ptr(i)) << ", " << is_using);
                result.put(this->ptr(i),is_using);
            } else {
                // gotta use part
                result.put(this->ptr(i),maxsize-used);
                used = maxsize;
                break;
            }
        }
    }
}

/**\endcond skip */
