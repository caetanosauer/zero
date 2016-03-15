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

/** @file:   shore_file_desc.h
 *
 *  @brief:  Descriptors for Shore files/indexes, and structures that help in
 *           keeping track of the created files/indexes.
 *
 *  @author: Ippokratis Pandis, January 2008
 *
 */

#ifndef __SHORE_FILE_DESC_H
#define __SHORE_FILE_DESC_H

#include "sm_vas.h"

#include <list>

using std::list;


/******** Exported constants ********/

const unsigned int MAX_FNAME_LEN     = 40;
const unsigned int MAX_TABLENAME_LEN = 40;
const unsigned int MAX_FIELDNAME_LEN = 40;

const unsigned int MAX_KEYDESC_LEN   = 40;
const unsigned int MAX_FILENAME_LEN  = 100;

const unsigned int MAX_BODY_SIZE     = 1024;

#define  DELIM_CHAR            '|'
#define  ROWEND_CHAR            '\r'

const unsigned int COMMIT_ACTION_COUNT           = 2000;
const unsigned int COMMIT_ACTION_COUNT_WITH_ITER = 500000;

#define  MIN_SMALLINT     0
#define  MAX_SMALLINT     1<<15
#define  MIN_INT          0
#define  MAX_INT          1<<31
#define  MIN_FLOAT        0
#define  MAX_FLOAT        1<<10



/* ---------------------------------------------------------------
 *
 *  @enum:  file_type_t
 *
 *  @brief: A file can be either a table, an index,
 *          a (secondary) index, etc...
 *
 * --------------------------------------------------------------- */

enum file_type_t  { FT_TABLE        = 0x1,
                    FT_PRIMARY_IDX  = 0x2,
                    FT_IDX          = 0x4,
                    FT_NONE         = 0x8
};



/* ---------------------------------------------------------------
 *
 * @enum:  physical_design_t
 *
 * @brief: There are different options for the physical design. The
 *         currently supported options:
 *         PD_NORMAL      - vanilla structures
 *         PD_PADDED      - use padding to reduce contention
 *         PD_MRBT_NORMAL - use MRBTrees with normal heap files
 *         PD_MRBT_PART   - use MRBTrees with partitioned heap files
 *         PD_MRBT_LEAF   - use MRBTrees with each heap page corresponding
 *                          to one leaf MRBTree index page
 *         PD_NOLOCK      - have indexes without CC
 *         PD_NOLATCH     - have indexes without even latching
 *
 * --------------------------------------------------------------- */

enum physical_design_t { PD_NORMAL      = 0x1,
                         PD_PADDED      = 0x2,
                         PD_NOLOCK      = 0x4,
                         PD_NOLATCH     = 0x8
};

#endif /* __SHORE_FILE_DESC_H */
