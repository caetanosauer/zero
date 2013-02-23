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

/** @file skewer.cpp
 *
 *  @brief Implementation for the class skewer_t
 *
 *  @author: Pinar Tozun, Feb 2011
 */

#include "util/skewer.h"
#include "util/random_input.h"

/*********************************************************************
 * 
 * @class skewer
 *
 * @brief Class that keeps the skew information for creating dynamic skew
 *        With zipf we can just create a static skew on some area
 *
 *********************************************************************/


/******************************************************************** 
 *
 *  @fn:    set()
 *
 *  @brief: Initialization of the field values
 *
 ********************************************************************/

void skewer_t::set(int area, int lower, int upper, int load)
{
    _area = area;
    _lower = lower;
    _upper = upper;
    _load = load;
    clear();
    _set_intervals();
}


/******************************************************************** 
 *
 *  @fn:    clear()
 *
 *  @brief: Clean the intervals for the area
 *
 ********************************************************************/

void skewer_t::clear() {
    _interval_l.clear();
    _interval_u.clear();
    _non_interval_l.clear();
    _non_interval_u.clear();
    _is_used = false;
}


/******************************************************************** 
 *
 *  @fn:    reset()
 *
 *  @brief: Reset the intervals for the area
 *          Recalculate area and load if necessary
 *
 ********************************************************************/

void skewer_t::reset(skew_type_t type) {
    clear();
    if(type == SKEW_CHAOTIC) {
	_load = URand(0,100);
	_area = URand(1,100);
	// pin: to debug
	//cout << "load: " << _load << " area: " << _area << endl;
    }
    _set_intervals();
    _is_used = true;
}


/******************************************************************** 
 *
 *  @fn:    is_used()
 *
 *  @brief: Indicates whether the intervals are used once before
 *
 ********************************************************************/

bool skewer_t::is_used() {
    if(!_is_used) {
	_is_used = true;
	return false;
    } else return true;
}


/******************************************************************** 
 *
 *  @fn:    _set_intervals()
 *
 *  @brief: sets the intervals for the area
 *          the intervals don't have to be continuous
 *          either one or two random points are set and based on the
 *          the given % area the interval(s) boundaries are determined
 *          in a way that the chosen random point(s) are the initial
 *          points of the interval(s)
 *
 ********************************************************************/

void skewer_t::_set_intervals() {    
    // for intervals
    if(URand(1,100) < 70) { // a continuous spot
	int interval_lower = URand(_lower,_upper);
	int interval_upper = interval_lower + ceil(((double) ((_upper - _lower + 1) * _area))/100);
	_add_interval(interval_lower, interval_upper);
    } else { // for possibility of a non continious two spots
	int interval = ceil(((double) ((_upper - _lower + 1) * _area))/200);
	int interval_lower = URand(_lower,_upper);
	int interval_upper = interval_lower + interval;
	int interval_lower_2 = URand(interval_lower,_upper);
	int interval_upper_2 = interval_lower_2 + interval;
	_add_interval(interval_lower, interval_upper);
	_add_interval(interval_lower_2, interval_upper_2);
    }
    // for outside of intervals
    for(int i=0; i<_interval_u.size(); i++) {
	if(i==0 && _interval_l[i] > _lower) {
	    _non_interval_l.push_back(_lower);
	    _non_interval_u.push_back(_interval_l[i]-1);
	}
	if(i+1<_interval_u.size()) {
	    if(_interval_u[i]+1 < _interval_l[i+1]) {
		_non_interval_l.push_back(_interval_u[i]+1);
		_non_interval_u.push_back(_interval_l[i+1]-1);
	    }
	}  else if(_interval_u[i] < _upper) {
	    _non_interval_l.push_back(_interval_u[i]+1);
	    _non_interval_u.push_back(_upper);
	}
    }
    // pin: to debug
    //print_intervals();
}


/******************************************************************** 
 *
 *  @fn:    _add_interval()
 *
 *  @brief: adds one interval to the interval vectors
 *          the reason this is made a seperate function is that
 *          if the chosen interval is not inside the boundaries it's
 *          split into two and two intervals; one finishes in the upper
 *          boundary and one startw in the lower boundary is made
 *
 ********************************************************************/

void skewer_t::_add_interval(int interval_lower, int interval_upper) {
    // pin: a very ugly check for a special case, try to get rid of this later
    if(interval_lower == _upper && interval_upper-1>_upper) {
	interval_lower--;
	interval_upper--;
    }
    if(interval_upper-1 > _upper) {
	if(_interval_l.size() == 1) {
	    _interval_l.insert(_interval_l.begin(), _lower);
	    _interval_u.insert(_interval_u.begin(), interval_upper - _upper - 2 + _lower);
	    _interval_l.push_back(interval_lower + 1);
	    _interval_u.push_back(_upper);
	} else if(_interval_l.size() == 2) {
	    if(_interval_u[0] > interval_upper - _upper - 2 + _lower) {
		_interval_l.insert(_interval_l.begin(), _lower);
		_interval_u.insert(_interval_u.begin(), interval_upper - _upper - 2 + _lower);
	    } else {
		vector<int>::iterator iter = _interval_l.begin();
		_interval_l.insert(++iter, _lower);
		iter = _interval_u.begin();
		_interval_u.insert(++iter, interval_upper - _upper - 2 + _lower);
	    }
	    if(_interval_l[2] < interval_lower+1) {
		_interval_l.push_back(interval_lower+1);
		_interval_u.push_back(_upper);
	    } else {
		_interval_l.push_back(_interval_l[2]);
		_interval_u.push_back(_interval_u[2]);
		_interval_l[2] = interval_lower+1;
		_interval_u[2] = _upper;
	    }
	} else {
	    _interval_l.push_back(_lower);
	    _interval_u.push_back(interval_upper - _upper - 2 + _lower);
	    _interval_l.push_back(interval_lower+1);
	    _interval_u.push_back(_upper);
	}
    } else {
	_interval_l.push_back(interval_lower);
	_interval_u.push_back(interval_upper - 1);
    }
}


/******************************************************************** 
 *
 *  @fn:    get_input()
 *
 *  @brief: creates the input for the input creators when skew is enabled
 *
 ********************************************************************/

int skewer_t::get_input() {
    int input = 0;
    bool is_set = false;
    int rand = URand(1,100);
    int load = _load / _interval_u.size();
    for(int i=0; !is_set && i<_interval_u.size(); i++) {
	if(rand < load * (i+1)) {
	    input = UZRand(_interval_l[i],_interval_u[i]);
	    is_set = true;
	}
    }
    rand = rand - _load;
    load = (100 - _load) / _non_interval_u.size();
    for(int i=0; !is_set && i<_non_interval_u.size(); i++) {
	if(rand < load * (i+1)) {
	    input = UZRand(_non_interval_l[i],_non_interval_u[i]);
	    is_set = true;
	}
    }
    if(!is_set || (input < _lower) || (input > _upper)) {
	input = UZRand(_lower, _upper);
    }
    return input;
}


/******************************************************************** 
 *
 *  @fn:    print_intervals()
 *
 *  @brief: prints the intervals the (load) and the (100 - load)
 *          will be applied for debugging purposes
 *
 ********************************************************************/

void skewer_t::print_intervals() {
    cout << "print intervals for (load)" << endl;
    for(int i=0; i<_interval_u.size(); i++) {
	cout << _interval_l[i] << " - " <<  _interval_u[i] << endl;
    }
    cout << "print intervals for (100-load)" << endl;
    for(int i=0; i<_non_interval_u.size(); i++) {
	cout << _non_interval_l[i] << " - " <<  _non_interval_u[i] << endl;
    }
}
