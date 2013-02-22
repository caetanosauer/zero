/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-kits -- Benchmark implementations for Shore-MT
   
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

/** @file:   countdown.h
 *
 *  @brief:  Atomic countdown class
 *
 *  @author: Ryan Johnson
 *
 */

#ifndef __UTIL_COUNTDOWN_H
#define __UTIL_COUNTDOWN_H

#include <cassert>

#include "k_defines.h"

struct countdown_t 
{
public:

    // constructor - sets the number to count down
    countdown_t(int count=0) : _state(count*CD_NUMBER) { }

    // reduce thread count by one (or post an error) and return true if
    // the operation completed the countdown
    bool post(bool is_error=false);

    // return remaining threads or -1 if error
    int remaining() const;
 
    // reset - should be called only when count == 0
    void reset(const int newcount); 

private:

    enum { CD_ERROR=0x1, CD_NUMBER=0x2 };
    unsigned int volatile _state;

    // copying not allowed
    countdown_t(countdown_t const &);
    void operator=(countdown_t const &);

}; // EOF: struct countdown_t


typedef countdown_t* countdownPtr;


#endif /* __UTIL_COUNTDOWN_H */
