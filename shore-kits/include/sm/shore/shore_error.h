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

/** @file:   shore_error.h
 *
 *  @brief:  Enumuration of Shore-related errors
 *
 *  @author: Mengzhi Wang, April 2001
 *  @author: Ippokratis Pandis, January 2008
 *
 */

#ifndef __SHORE_ERROR_H
#define __SHORE_ERROR_H

#include "util/namespace.h"


ENTER_NAMESPACE(shore);

/* error codes returned from shore toolkit */

enum {
  se_NOT_FOUND                = 0x810001,
  se_VOLUME_NOT_FOUND         = 0X810002,
  se_INDEX_NOT_FOUND          = 0x810003,
  se_TABLE_NOT_FOUND          = 0x810004,
  se_TUPLE_NOT_FOUND          = 0x810005,
  se_NO_CURRENT_TUPLE         = 0x810006,
  se_CANNOT_INSERT_TUPLE      = 0x810007,
  
  se_SCAN_OPEN_ERROR          = 0x810010,
  se_INCONSISTENT_INDEX       = 0x810012,
  se_OPEN_SCAN_ERROR          = 0x810020,
  
  se_LOAD_NOT_EXCLUSIVE       = 0x810040,
  se_ERROR_IN_LOAD            = 0x810041,
  se_ERROR_IN_IDX_LOAD        = 0x810042,

  se_WRONG_DISK_DATA          = 0x810050,

  se_INVALID_INPUT            = 0x810060
};



EXIT_NAMESPACE(shore);

#endif /* __SHORE_ERROR_H */
