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

#ifndef __UTIL_CONDEX_H
#define __UTIL_CONDEX_H

#include <cstdlib>


/******************************************************************** 
 *
 * @struct: condex
 *
 * @brief:  Struct of a cond var with a flag indicating if it was fired
 *
 ********************************************************************/

struct condex 
{
    pthread_cond_t _cond;
    pthread_mutex_t _lock;
    long _signals;
    long _waits;

    condex() : _signals(0), _waits(0) { 
        if (pthread_cond_init(&_cond,NULL)) {
            assert (0); // failed to init cond var
        }
        if (pthread_mutex_init(&_lock,NULL)) {
            assert (0); // failed to init mutex
        }
    }

    ~condex() {
        pthread_cond_destroy(&_cond);
        pthread_mutex_destroy(&_lock);
    }

    void signal() {
	CRITICAL_SECTION(cs, _lock);
	_signals++;
	pthread_cond_signal(&_cond);
    }

    void wait() {
	CRITICAL_SECTION(cs, _lock);
	_waits++;
	while(_waits > _signals)
	    pthread_cond_wait(&_cond,&_lock);
    }

}; // EOF: condex



/******************************************************************** 
 *
 * @struct: condex_pair
 *
 * @brief:  Struct of two condexes with a ready flag and an pointer to
 *          the active condex
 *
 ********************************************************************/

struct condex_pair 
{
    condex _wait[2];
    long _requested;
    long _wanted;
    long _waited;

    condex_pair() : _requested(0), _wanted(0), _waited(0) { }

    ~condex_pair() { }

    void please_take_one() {
	++_wanted;
    }
    condex* take_one() {
	if(_wanted > _requested) {
	    return &_wait[_requested++ % 2];
	}
	return NULL;
    }
    void wait() {
	w_assert1(_waited < _requested);
	_wait[_waited++ %2].wait();
    }

};  // EOF: condex_pair


// my_condex = {
//     {
// 	{PTHREAD_COND_INITIALIZER, PTHREAD_MUTEX_INITIALIZER, false},
// 	{PTHREAD_COND_INITIALIZER, PTHREAD_MUTEX_INITIALIZER, false},
//     },
//     0, false
// };



#endif /** __UTIL_CONDEX_H */
