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

/*<std-header orig-src='shore' incl-file-exclusion='W_AUTODEL_H'>

 $Id: w_autodel.h,v 1.19 2010/05/26 01:20:23 nhall Exp $

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

#ifndef W_AUTODEL_H
#define W_AUTODEL_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/**\brief Delete object when this leaves scope (a la STL auto_ptr)
 *
 *  This class is used to ensure that a "new"ed object of type T 
 *  will be "delete"d when the scope is closed.
 *  During destruction, automatically call "delete" on the pointer
 *  supplied during construction.
 *
 *  eg. f()
 *  \code
 *    {
 *          int* p = new int;
 *        if (!p)  return OUTOFMEMORY;
 *        w_auto_delete_t<int> autodel(p);
 *
 *         ... do work ...
 *
 *        if (error)  {    // no need to call delete p
 *        return error;
 *        }
 *
 *        // no need to call delete p
 *        return OK;
 *    }
 *    \endcode
 *
 *  delete p will be called by the autodel object. Thus users do 
 *    not need to code 'delete p' explicitly, and can be assured
 *    that p will be deleted when the scope in which autodel 
 *  was constructed is closed.
 *
 *  This code predates the STL.
 *
 *********************************************************************/
template <class T>
class w_auto_delete_t {
public:
    NORET            w_auto_delete_t()
    : obj(0)  {};
    NORET            w_auto_delete_t(T* t)
    : obj(t)  {};
    NORET            ~w_auto_delete_t()  {
    if (obj) delete obj;
    }
    w_auto_delete_t&        set(T* t)  {
    return obj = t, *this;
    }
    T* operator->() { return obj; }
    T &operator*() { return *obj; }
    operator T*() { return obj; }
    T const* operator->() const { return obj; }
    T const &operator*() const { return *obj; }
    operator T const*() const { return obj; }
private:
    T*                obj;

    // disabled
    NORET            w_auto_delete_t(const w_auto_delete_t&) {};
    w_auto_delete_t&        operator=(const w_auto_delete_t &) {return *this;};
};



/**\brief Delete array object when this leaves scope.
 *  
 *
 *  Same as w_auto_delete_t, except that this class operates on
 *  arrays (i.e. the destructor calls delete[] instead of delete.)
 *
 *  eg. f()
 *    {
 *          int* p = new int[20];
 *        if (!p)  return OUTOFMEMORY;
 *        w_auto_delete_array_t<int> autodel(p);
 *
 *         ... do work ...
 *
 *        if (error)  {    // no need to call delete[] p
 *        return error;
 *        }
 *
 *        // no need to call delete[] p
 *        return OK;
 *    }
 *
 *    This code predates STL.
 *
 *********************************************************************/
template <class T>
class w_auto_delete_array_t {
public:
    NORET            w_auto_delete_array_t()
    : obj(0)  {};
    NORET            w_auto_delete_array_t(T* t)
    : obj(t)  {};
    NORET            ~w_auto_delete_array_t()  {
    if (obj) delete [] obj;
    }
    w_auto_delete_array_t&    set(T* t)  {
    return obj = t, *this;
    }
    T* operator->() { return obj; }
    T &operator*() { return *obj; }
    operator T*() { return obj; }
    //    T &operator[](int idx) { return obj[idx]; }
private:
    T*                obj;

    // disabled
    NORET            w_auto_delete_array_t(
    const w_auto_delete_array_t&)  {}
    w_auto_delete_array_t&    operator=(const w_auto_delete_array_t &) {return *this;};
};

/*<std-footer incl-file-exclusion='W_AUTODEL_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
