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

/** @file:   dora_error.h
 *
 *  @brief:  Enumuration of DORA-related errors
 *
 *  @author: Ippokratis Pandis, Oct 2008
 *
 */

#ifndef __DORA_ERROR_H
#define __DORA_ERROR_H

#include "util/namespace.h"

ENTER_NAMESPACE(dora);

/* error codes returned from DORA */

/** note: All DORA-related errors start with 0x82.. */

enum {
    /** Problems generating */
    de_GEN_WORKER              = 0x820001,
    de_GEN_PRIMARY_WORKER      = 0x820002,
    de_GEN_STANDBY_POOL        = 0x820003,
    de_GEN_PARTITION           = 0x820004,
    de_GEN_TABLE               = 0x820005,

    /** Problems run-time */
    de_PROBLEM_ENQUEUE         = 0x820011,
    de_WRONG_ACTION            = 0x820011,
    de_WRONG_PARTITION         = 0x820012,
    de_WRONG_WORKER            = 0x820013,

    de_WORKER_ATTACH_XCT       = 0x820021,
    de_WORKER_DETACH_XCT       = 0x820022,
    de_WORKER_RUN_XCT          = 0x820023,
    de_WORKER_RUN_RVP          = 0x820024,

    de_INTERMEDIATE_XCT        = 0x820025,
    de_TERMINAL_XCT            = 0x820026,
    de_NOTIFY_COMMITTED        = 0x820028,

    de_INCOMPATIBLE_LOCKS      = 0x820031,
    de_WRONG_IDX_DATA          = 0x820032,

    de_EARLY_ABORT             = 0x820041,
    de_MIDWAY_ABORT            = 0x820042,

    /** Problem in PLP/MRBTrees */
    de_PLP_NOT_FOUND           = 0x820051,
    de_LPID_NOT_FOUND          = 0x820052,
    de_PARTID_NOT_FOUND        = 0x820053
};



EXIT_NAMESPACE(dora);

#endif /* __DORA_ERROR_H */
