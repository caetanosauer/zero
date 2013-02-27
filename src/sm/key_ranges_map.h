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

/** @file:   key_ranges_map.h
 *
 *  @brief:  Definition of a map of key ranges to partitions used by
 *           baseline MRBTrees.
 *
 *  @notes:  The keys are Shore-mt cvec_t. Thread-safe.  
 *
 *  @date:   July 2010
 *
 *  @author: Pinar Tozun (pinar)
 *  @author: Ippokratis Pandis (ipandis)
 *  @author: Ryan Johnson (ryanjohn)
 */


#ifndef _KEY_RANGES_MAP_H
#define _KEY_RANGES_MAP_H


/******************************************************************** 
 *
 * @class: key_ranges_map
 *
 * @brief: A map of key ranges to partitions. This structure is used
 *         by the multi-rooted B-tree (MRBTree). 
 *
 * @note:  The specific implementation indentifies each partition through
 *         the lpid_t of the root of the corresponding sub-tree. Hence,
 *         this implementation is for the Baseline MRBTree (non-DORA).
 *
 ********************************************************************/

class key_ranges_map
{
}; // EOF: key_ranges_map

#endif
