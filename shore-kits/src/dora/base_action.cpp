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

/** @file:   base_action.cpp
 *
 *  @brief:  Implementation of Actions for DORA
 *
 *  @author: Ippokratis Pandis, Nov 2008
 */


#include "dora/base_action.h"


ENTER_NAMESPACE(dora);


// copying allowed
base_action_t& base_action_t::operator=(const base_action_t& rhs)
{
  if (this != &rhs) {
    _prvp = rhs._prvp;
    _xct = rhs._xct;
    _tid = rhs._tid;
    _keys_needed = rhs._keys_needed;
    _keys_set = rhs._keys_set;
  }
  return (*this);
}



EXIT_NAMESPACE(dora);
