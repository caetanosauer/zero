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

/*<std-header orig-src='shore' incl-file-exclusion='W_WORKAROUND_H'>

 $Id: w_workaround.h,v 1.61 2010/12/08 17:37:37 nhall Exp $

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

#ifndef W_WORKAROUND_H
#define W_WORKAROUND_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/**\file w_workaround.h 
 * Macros that allow workarounds for different compilers.
 */


#ifdef __GNUC__

/* Mechanism to make disjoint gcc numbering appear linear for comparison
   purposes.  Without this all the Shore gcc hacks break when a new major
   number is encountered. 
*/

#define    W_GCC_VER(major,minor)    (((major) << 16) + (minor))

#ifndef __GNUC_MINOR__    /* gcc-1.something -- No minor version number */
#define    W_GCC_THIS_VER    W_GCC_VER(__GNUC__,0)
#else
#define    W_GCC_THIS_VER    W_GCC_VER(__GNUC__,__GNUC_MINOR__)
#endif


#if     W_GCC_THIS_VER < W_GCC_VER(2,5)
/* XXX all the following tests assume this filter is used */
#error    This software requires gcc 2.5.x or a later release.
#error  Gcc 2.6.0 is preferred.
#endif


#if W_GCC_THIS_VER < W_GCC_VER(2,6)

    /*
     * G++ also has a bug in calling the destructor of a template
     */
#   define GNUG_BUG_2 1

    /*
     * G++ seems to have a problem calling ::operator delete 
     */
#   define GNUG_BUG_3 1

    /*
     * G++ version 2.4.5 has problems with templates that don't have
     * destructors explicitly defined.  It also seems to have problems
     * with classes used to instantiate templates if those classes
     * do not have destructors.
     */
#   define GNUG_BUG_7 1

    /* bug #8:
     * gcc include files don't define signal() as in ANSI C.
     * we need to get around that
     */
#   define GNUG_BUG_8 1

#endif /* gcc < 2.6 */

    /*
     * #12
     * This is a bug in parsing specific to gcc 2.6.0.
     * The compiler misinterprets:
     *    int(j)
     * to be a declaration of j as an int rather than the correct
     * interpretation as a cast of j to an int.  This shows up in
     * statements like:
     *     istrstream(c) >> i;
    */

/* see below for more info on GNUG_BUG_12 */
#define GNUG_BUG_12(arg) arg
#if W_GCC_THIS_VER > W_GCC_VER(2,5)
#    undef GNUG_BUG_12    
#       define GNUG_BUG_12(arg) (arg)
#endif

#if W_GCC_THIS_VER > W_GCC_VER(2,5)
/*
 *     GNU 2.6.0  : template functions that are 
 *  not member functions don't get exported from the
 *  implementation file.
 */
#define     GNUG_BUG_13 1

/*
 * Cannot explicitly instantiate function templates.
 */
#define     GNUG_BUG_14 1
#endif

#if W_GCC_THIS_VER > W_GCC_VER(2,6)
/* gcc 2.7.2 has bogus warning messages; it doesn't inherit pointer
   properties correctly */
#define        GNUG_BUG_15  1
#endif

/* Gcc 64 bit integer math incorrectly optimizes range comparisons such as
   if (i64 < X || i64 > Y)
    zzz;
   This should be re-examined when we upgrade to 2.8, perhaps it is fixed and
   we can make this a 2.7 dependency.
 */
#define    GNUG_BUG_16

/******************************************************************************
 *
 * Migration to standard C++
 *
 ******************************************************************************/
#if W_GCC_THIS_VER >= W_GCC_VER(2,90)
/*
 * EGCS is 2.90 (which really screws up any attempt to fix 
 * things based on __GNUC_MINOR__ and __GNUC__
 * and egcs does not define any different macro to identify itself.
 */
#endif

#if W_GCC_THIS_VER < W_GCC_VER(2,8)

#   define BIND_FRIEND_OPERATOR_PART_1(TYP,L,TMPLa,TMPLb) /**/
#   define BIND_FRIEND_OPERATOR_PART_1B(TYP1,TYP3,TYP2,TMPLa,TMPLc,TMPLb) /**/
#   define BIND_FRIEND_OPERATOR_PART_2(TYP) /**/
#   define BIND_FRIEND_OPERATOR_PART_2B(TYP1,TYP2) /**/

#   else

#   define BIND_FRIEND_OPERATOR_PART_1(TYP,L,TMPLa,TMPLb) \
    template <class TYP, class L> \
    ostream & operator<<(ostream&o, const TMPLa,TMPLb& l);

#   define BIND_FRIEND_OPERATOR_PART_1B(TYP1,TYP3, TYP2,TMPLa,TMPLc,TMPLb) \
    template <class TYP1, class TYP3, class TYP2> \
                ostream & operator<<(ostream&o, const TMPLa,TMPLc,TMPLb& l);

#   define BIND_FRIEND_OPERATOR_PART_2(TYP, L)\
    <TYP, L>

#   define BIND_FRIEND_OPERATOR_PART_2B(TYP1,L,TYP2)\
    <TYP1, L, TYP2>

#   endif

/* XXX
 *  The gcc-3.x object model has changes which allow THIS to change
 *  based upon inheritance and such.   That isn't a problem.  However,
 *  they added a poor warning which breaks ANY use of offsetof(), even
 *  legitimate cases where it is THE ONLY way to get the correct result and
 *  where the result would be correct with the new model.  This offsetof
 *  implementation is designed to avoid that particular compiler warning.
 *  Until the GCC guys admit they are doing something dumb, we need to do this.
 *
 *  This could arguably belong in w_base.h, I put it here since w_base.h
 *  always sucks this in and it is a compiler-dependency.
 */
#if W_GCC_THIS_VER >= W_GCC_VER(3,0)
#define    w_offsetof(t,f)    \
    ((size_t)((char*)&(*(t*)sizeof(t)).f - (char *)&(*(t*)sizeof(t))))
#endif

#endif /* __GNUC__ */

/******************************************************************************
 *
 * C string bug
 *
 ******************************************************************************/

/*
 * a C string constant is of type (const char*).  some routines assume that
 * they are (char*).  this is to cast away the const in those cases.
 */
#define C_STRING_BUG (char *)



/* This is really a library problem; stream.form() and stream.scan()
   aren't standard, but GNUisms.  On the other hand, they should
   be in the standard, because they save us from static form() buffers.
   Using the W_FORM() and W_FORM2() macros instead of
   stream.form() or stream << form() encapsulates this use, so the
   optimal solution can be used on each platform.
   If a portable scan() equivalent is written, a similar set
   of W_SCAN macros could encapuslate input scanning too.
 */  
#define    W_FORM(stream)        stream << form

// in w_form.cpp
extern const char *form(const char *, ...);

#define    W_FORM2(stream,args)    W_FORM(stream) args

/*
 * Try to use the system definition of offsetof, and provide one here
 * if the system's isn't in the standard place.
 */
#ifndef offsetof
#include <cstddef>
#endif
#ifndef offsetof
#define offsetof(type,member)    ((size_t)((&(type *)0)->member))
#endif

#ifndef w_offsetof
/* FRJ: Sun's CC returns an address near the top of the stack when
   given ``offsetof(a, b.c())'', where c() returns a reference to a
   private member of b. This seems to work around the issue (bug?).
   OLD:
   //template<class T>
   //static T* get_null() { return NULL; }
   //#define    w_offsetof(class,member)    ((size_t) &get_null<class>()->member)
   NEW: below
 */
#define w_offsetof(class,member) offsetof(class,member)
#endif

/*<std-footer incl-file-exclusion='W_WORKAROUND_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
