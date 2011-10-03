/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager
   
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

// -*- mode:c++; c-basic-offset:4 -*-
/*<std-header orig-src='shore' incl-file-exclusion='STHREAD_H'>

 $Id: critical_section.h,v 1.1 2010/12/09 15:20:17 nhall Exp $

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

/*  -- do not edit anything above this line --   </std-header>*/

#ifndef CRIT_SEC_H
#define CRIT_SEC_H

/**\def CRITICAL_SECTION(name, lock)
 *
 * This macro starts a critical section protected by the given lock
 * (2nd argument).  The critical_section structure it creates is
 * named by the 1st argument.
 * The rest of the scope (in which this macro is used) becomes the
 * scope of the critical section, since it is the destruction of this
 * critical_section structure that releases the lock.
 *
 * The programmer can release the lock early by calling \<name\>.pause()
 * or \<name\>.exit().
 * The programmer can reacquire the lock by calling \<name\>.resume() if
 * \<name\>.pause() was called, but not after \<name\>.exit().
 *
 * \sa critical_section
 */
#define CRITICAL_SECTION(name, lock) critical_section<__typeof__(lock)&> name(lock)

template<class Lock>
struct critical_section;

/**\brief Helper class for CRITICAL_SECTION idiom (macro).
 *
 * This templated class does nothing; its various specializations 
 * do the work of acquiring the given lock upon construction and
 * releasing it upon destruction. 
 * See the macros:
 * - SPECIALIZE_CS(Lock, Extra, ExtraInit, Acquire, Release)  
 * - CRITICAL_SECTION(name, lock) 
 */
template<class Lock>
struct critical_section<Lock*&> : public critical_section<Lock&> {
    critical_section<Lock*&>(Lock* mutex) : critical_section<Lock&>(*mutex) { }
};

/*
 * NOTE: I added ExtraInit to make the initialization happen so that
 * assertions about holding the mutex don't fail.
 * At the same time, I added a holder to the w_pthread_lock_t
 * implementation so I could make assertions about the holder outside
 * the lock implementation itself.  This might seem like doubly
 * asserting, but in the cases where the critical section isn't
 * based on a pthread mutex, we really should have this clean
 * initialization and the check the assertions.
 */

/**\def SPECIALIZE_CS(Lock, Extra, ExtraInit, Acquire, Release) 
 * \brief Macro that enables use of CRITICAL_SECTION(name,lock)
 *\addindex SPECIALIZE_CS
 * 
 * \details
 * Create a templated class that holds 
 *   - a reference to the given lock and
 *   - the Extra (2nd macro argument)
 *
 *  and it
 *   - applies the ExtraInit and Acquire commands upon construction,
 *   - applies the Release command upon destruction.
 *
 */
#define SPECIALIZE_CS(Lock,Extra,ExtraInit,Acquire,Release) \
template<>  struct critical_section<Lock&> { \
critical_section(Lock &mutex) \
    : _mutex(&mutex)          \
    {   ExtraInit; Acquire; } \
    ~critical_section() {     \
        if(_mutex)            \
            Release;          \
            _mutex = NULL;    \
        }                     \
    void pause() { Release; } \
    void resume() { Acquire; }\
    void exit() { Release; _mutex = NULL; } \
    Lock &hand_off() {        \
        Lock* rval = _mutex;  \
        _mutex = NULL;        \
        return *rval;         \
    }                         \
private:                      \
    Lock* _mutex;             \
    Extra;                    \
    void operator=(critical_section const &);   \
    critical_section(critical_section const &); \
}


// I undef-ed this and found all occurrances of CRITICAL_SECTION with this.
// and hand-checked them.
SPECIALIZE_CS(pthread_mutex_t, int _dummy,  (_dummy=0), 
    pthread_mutex_lock(_mutex), pthread_mutex_unlock(_mutex));


#endif          /*</std-footer>*/
