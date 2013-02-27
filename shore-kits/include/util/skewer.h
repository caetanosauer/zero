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

/** @file skewer.h
 *
 *  @brief Definition for the class skewer
 *
 *  @author: Pinar Tozun, Feb 2011
 */

#ifndef __UTIL_SKEWER_H
#define __UTIL_SKEWER_H

#include <iostream>
#include <vector>

/* ---------------------------------------------------------------
 *
 * @enum:  skew_type_t
 *
 * @brief: There are different options for handling dynamic skew
 *         SKEW_NONE    - means there is no data skew
 *         SKEW_NORMAL  - the skew given in the input command will be applied
 *         SKEW_DYNAMIC - the initial area of the skew will be changed in random time durations
 *         SKEW_CHAOTIC - both the initial area and load of the skew will be changed in random time durations
 *
 * --------------------------------------------------------------- */

enum skew_type_t {
    SKEW_NONE      = 0x0,
    SKEW_NORMAL    = 0x1,
    SKEW_DYNAMIC   = 0x2,
    SKEW_CHAOTIC   = 0x4
};

using namespace std;

/*********************************************************************
 * 
 * @class skewer_t
 *
 * @brief Class that keeps the skew information for creating dynamic skew
 *        With zipf we can just create a static skew on some area
 *
 *********************************************************************/

class skewer_t {
  
private:

    // the % of the total area that the skew will be applied
    int _area;
    
    // the % of the load to apply to _area 
    int _load;
    
    // the boundaries of the whole area
    int _lower;
    int _upper;
    
    // the boundaries of the area that the load will be applied to
    // this area doesn't have to be continuous 
    vector<int> _interval_l;
    vector<int> _interval_u;

    // the boundaries of the area that the remaining load will be applied to
    // this area doesn't have to be continuous 
    vector<int> _non_interval_l;
    vector<int> _non_interval_u;
    
    // indicates whether the set intervals was used once
    bool _is_used;
    
public:

    // empty constructor, things should be set later
    skewer_t() { _is_used = false; }

    // initialization
    void set(int area, int lower, int upper, int load);

    // cleans the intervals
    void clear();

    // re-decides on intervals
    void reset(skew_type_t type);

    // gives input to the input creators
    int get_input();

    // to be called on deciding whether to set or reset
    bool is_used();

    // for debugging
    void print_intervals();
    
private:

    void _set_intervals();
    
    void _add_interval(int interval_lower, int interval_upper);
    
};

#endif // __UTIL_SKEW_INFO_H
