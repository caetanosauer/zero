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

/*<std-header orig-src='shore' incl-file-exclusion='AUTO_RELEASE_H'>

 $Id: auto_release.h,v 1.4 2010/12/08 17:37:50 nhall Exp $

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

#ifndef AUTO_RELEASE_H
#define AUTO_RELEASE_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/**\brief Template class that releases a held resource upon destruction.
 *
 * This template class is an analog of auto_ptr<T>  from the C++ standard
 * template library, but rather than freeing a heap object, it releases a
 * resource by calling the resource's "release" method. This only works if
 * the resource has a method
 * \code
 * void release();
 * \endcode
 *
 * For more complex releasing requirements, see the specializations 
 * auto_release_t<queue_based_lock_t>,
 * auto_release_t<pthread_mutex_t>, and
 * the analogous templates for read-write synchronization primitives
 * auto_release_r_t<>, and
 * auto_release_w_t<>
 *
 * Used in the storage manager by buffer manager and io layer.
 */
template <class T> class auto_release_t {
public:
    NORET                       auto_release_t(T& t)  
        : obj(t)  {};
    NORET                       ~auto_release_t()  {
        obj.release();
    }
private:
    T&                          obj;
};

     
/**\brief Template class that releases a held queue_based_lock_t upon destruction.
 *
 * \sa auto_release_t<>
 */
template<>
class auto_release_t<w_pthread_lock_t> 
{
 public:
    /// construct with a reference to the lock and a pointer to the Queue-node upon which this thread spins
    NORET            auto_release_t(w_pthread_lock_t& t, w_pthread_lock_t::ext_qnode* me)
        : obj(t), _me(me) { }
    NORET            ~auto_release_t() {
        obj.release(_me);
    }
 private:
    w_pthread_lock_t&                obj;
    w_pthread_lock_t::ext_qnode*    _me;
};

/**\brief Template class that releases a held pthread mutex upon destruction.
 * 
 * \sa auto_release_t<>
 */
template<>
class auto_release_t<pthread_mutex_t> 
{
 public:
    /// construct with a pointer to the mutex
    NORET            auto_release_t(pthread_mutex_t* t) : obj(t) {}
    void            exit() { if(obj) pthread_mutex_unlock(obj); obj=NULL; }
    NORET            ~auto_release_t() { exit(); }
 private:
    pthread_mutex_t*                obj;
};

/**\brief Template class that releases a held latch upon destruction.
 * 
 * \sa auto_release_t<>
 */
template<>
class auto_release_t<latch_t> 
{
 public:
    /// construct with a pointer to the mutex
    NORET            auto_release_t(latch_t* t) : obj(t) {}
    void             exit() { if(obj) obj->latch_release(); obj=NULL; }
    NORET            ~auto_release_t() { exit(); }
 private:
    latch_t*                obj;
};

/**\brief Template class that, upon destruction, releases a read-write lock held for read.
 *
 * This template class is an analog of auto_ptr<T>  from the C++ standard
 * template library, but rather than freeing a heap object, it releases a
 * lock (thread-synchronization primitive, not a database lock)
 * by calling the lock's "release_read" method. This only works if
 * the resource has a method
 * \code
 * void release_read();
 * \endcode
 *
 * \sa auto_release_w_t<>
 */
template<class T>
class auto_release_r_t {
 public:
    NORET            auto_release_r_t(T& t) 
        : _disabled(false), _obj(t)    { }
    NORET            ~auto_release_r_t() {
        release_disable();
    }
    void             release_disable() { 
                        if(!_disabled) _obj.release_read(); 
                        _disabled=true; }
    void             disable() { _disabled=true; }
 private:
    bool       _disabled;
    T&         _obj;
};

/**\brief Template class that, upon destruction, releases a read-write lock held for write.
 *
 * This template class is an analog of auto_ptr<T>  from the C++ standard
 * template library, but rather than freeing a heap object, it releases a
 * lock (thread-synchronization primitive, not a database lock)
 * by calling the lock's "release_write" method. This only works if
 * the resource has a method
 * \code
 * void release_write();
 * \endcode
 *
 * \sa auto_release_r_t<>
 */
template<class T>
class auto_release_w_t
{
 public:
    NORET            auto_release_w_t(T& t)
        : _disabled(false), _obj(t)    { }
    NORET            ~auto_release_w_t() {
        release_disable();
    }
    void             release_disable() { 
                        if(!_disabled) _obj.release_write(); 
                        _disabled=true; }
    void             disable() { _disabled=true; }
 private:
    bool       _disabled;
    T&         _obj;
};

    
/*<std-footer incl-file-exclusion='AUTO_RELEASE_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
