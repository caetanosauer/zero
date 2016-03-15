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
/*<std-header orig-src='shore' incl-file-exclusion='TLS_H'>

 $Id: tls.h,v 1.3 2010/06/23 23:42:57 nhall Exp $

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
#ifndef __TLS_H
#define __TLS_H

#include <pthread.h>
#include <new>

/**\file tls.h
 * Cause macro definitions to show up in doxygen-generated
 * pages
 *\ingroup MACROS
 * */

/**\addtogroup TLS 
 * The following are some of the thread-local variables 
 * defined in the storage manager libraries. (This is far from
 * a complete list.)
 *
 * Each use of \ref TLS_STRUCT creates a thread_local object.
 *
 * See also \ref tls_tricks.
 *
 */

/**\brief A namespace for thread-local storage tricks.
 *
 * See also the following macros:
 * - #DECLARE_TLS(Type,Name)
 * - #DECLARE_TLS_SCHWARZ(Name)
 * - #DEFINE_TLS_SCHWARZ(Type,Name)
 */
namespace tls_tricks {

/**\brief A management class for non-POD thread-local storage. 
 *
 * The programmer
 * declares thread-local variables via DECLARE_TLS macro, and every
 * thread which calls thread_init/fini at entry/exit will have its
 * thread-local variables initialized and destroyed by their no-arg
 * constructor and destructor.
 *
 * \attention NOTE: the main thread automatically initializes its own TLS before
 * entering main(), but if the main thread exits without ending the
 * program it should call tls_manager::thread_fini like any other
 * thread.
 * 
 * \attention WARNING: for now this API does not support TLS declared within a
 * function.
 * 
 * \attention WARNING: Similar to static initialization, the programmer cannot
 * make any assumptions about the order in which two thread-local
 * variables get initialized unless they are declared in the same file
 * or declared using a DECLARE_TLS_SCHWARZ/DEFINE_TLS_SCHWARZ pair.
*/
class tls_manager {
public:
    static void global_init();
    static void global_fini();
    static void register_tls(void (*init)(), void (*fini)());
    static void thread_init();
    static void thread_fini();
    static __thread bool _thread_initialized;
};

/**\brief Static struct to make
 * sure tls_manager's global init() and fini() are called.
 *
 * \relates tls_manager
 * This is a so-called "schwarz counter", included in the .h file
 * and included here (first) so that it guarantees that the
 * tls_manager is indeed initialized before any other
 * schwarz counter or static declaration uses register_tls.
 */
struct tls_manager_schwarz {
    /**\brief Constructor: invokes global init of all registered tls initializers */
    tls_manager_schwarz() { tls_manager::global_init(); }
    /**\brief Destructor: invokes global init of all registered tls destructors */
    ~tls_manager_schwarz() { tls_manager::global_fini(); }
} ;
static struct tls_manager_schwarz tlsm_schwarz_one_and_only;

/** \brief Wrapper for a type, used by TLS_STRUCT helper macro
 *
 * All thread-local variables declared by TLS_STRUCT are actually
 * just a bunch of bytes... until init() and fini() are called,
 * that is. After that the tls_blob acts like a smart pointer.
 */
template<typename T>
struct tls_blob {
    enum { MAX_BYTES_NEEDED = sizeof(T)+sizeof(long)-1,
       ARRAY_SIZE = MAX_BYTES_NEEDED/sizeof(long) };

    // force proper alignment...
    long _reserved_space[ARRAY_SIZE];

    /** Placement new, using _reserved_space, to make the type T's
    * constructor get called.
    */
    void init() { new (get()) T; }
    /** 
     * Call T's destructor */
    void fini() { get()->~T();   }
    
    
    /** \brief Used by fini() and init() and directly by macros */
    T* get() {
        union { long* a; T* t; } u = {_reserved_space};
        return u.t;
    }
};

/* WARNING: These thread-local variables essentially use the namespace
   of types, not variables for the purposes of detecting naming
   collisions. So, just as the following two declarations could lead
   to serious and hard-to-identify bugs: 

   -- file1.cpp --
   struct foo {
       int a; int b;
   };
   -- file2.cpp --
   
   struct foo {
    double a; char* b;
   };

   So, too, would the following:
   
   -- file1.cpp -- 
   DECLARE_TLS(foo, my_tls);
   -- file2.cpp --
   DECLARE_TLS(bar, my_tls);

   If you are lucky and the two types have different names the
   compiler may notice, otherwise you're on your own.
*/

/**\def TLS_STRUCT(Type,Name,InitFn)
 *\brief Helper macro for DECLARE_TLS. Do not use directly.
 *
 *\addindex TLS_STRUCT
 *\relates tls_blob
 *\relates DECLARE_TLS
 *\relates DEFINE_TLS_SCHWARZ
 *
 *
 * A helper macro for DECLARE_TLS.
 * \attention Do not use this macro directly.
 *
 * Creates a "smart pointer" structure with the given Name;
 * the pointer is to an object of the given Type. Actually,
 * it's a pointer to a tls_blob, which is a bunch of untyped
 * bytes, but they get initialized via placement new when
 * a thread starts and 
 * "destructed" when the thread goes away.
 * Passing in the InitFn allows us to register a
 * non-trivial constructor, i.e., it allows us to
 * use non-POD types in thread-local storage.
 * This *only* lets us use a default constructor, but
 * the compiler's idea of trivial is more strict than
 * just having a default constructor.
 */
#define TLS_STRUCT(Type, Name, InitFn)                    \
struct Name {                                            \
    typedef tls_tricks::tls_blob< Type > Wrapper;                \
    Type &operator*() { return *get(); }                \
    Type* operator->() { return get(); }                \
    operator Type*() { return get(); }                \
    static Wrapper* get_wrapper() {                    \
        static __thread Wrapper val;                \
        return &val;                        \
    }                                \
    static Type* get() { return get_wrapper()->get(); }        \
    static void init() { get_wrapper()->init(); }            \
    static void fini() { get_wrapper()->fini(); }            \
    InitFn() {                            \
        static bool initialized = false;                \
        if(initialized)                        \
        return;                            \
        tls_tricks::tls_manager::register_tls(&init, &fini);            \
        initialized = true;                        \
    }                                \
}


/**\def DECLARE_TLS(Type,Name)
 *\brief Cause non-POD TLS object of Type to be created and initialized.
 *
 * This macro declares a static "smart pointer" 
 * named * Name_tls_wrapper to a
 * thread-local variable of the given Type.
 * When this static struct get initialized at static-init time,
 * it registers with the tls manager its init and fini methods.
 * Those methods invoke the init and fini methods of the item to
 * which this "smart pointer" points, which is the actual TLS entity:
 * a tls_tricks::tls_blobType.
 *
 *\addindex DECLARE_TLS
 */ 
#define DECLARE_TLS(Type, Name) \
    static \
    TLS_STRUCT(Type, Name##_tls_wrapper, Name##_tls_wrapper) Name


/**\def DECLARE_TLS_SCHWARZ(Name)
 *\brief Cause a Schwarz counter to be declared (for use in header files).
 *
 *\relates DEFINE_TLS_STRUCT
 *\addindex DECLARE_TLS_SCHWARZ
 *
 * Make a Swatchz counter (in a .h) 
 * to force initialization of the TLS
 * defined in a .cpp by DEFINE_TLS_SCHWARZ. This is useful if there is
 * a dependency between 2+ TLS variables so the correct one is
 * initialized first.
 * The only way you can control their order is to make sure their
 * DECLARE_TLS_SCHWARZ macros are in the correct order b/c C++ guarantees
 * static init order only for objects in the same translation unit.
 * Note that the counter is really in the tls wrapper.
 */
#define DECLARE_TLS_SCHWARZ(Name)        \
    static struct Name##_tls_wrapper_schwarz {    \
            Name##_tls_wrapper_schwarz();        \
    } Name##_schwarz

/**\def DEFINE_TLS_SCHWARZ(Type,Name)
 *\brief Cause a Schwarz counter to be defined (for use in .cpp files).
 *
 *\addindex DEFINE_TLS_SCHWARZ
 *\relates DECLARE_TLS_STRUCT
 *
 * Define the TLS struct that DECLARE_TLS_SCHWARZ expects to initialize.
 */
#define DEFINE_TLS_SCHWARZ(Type, Name)                    \
    static TLS_STRUCT(Type, Name##_tls_wrapper, static void init_wrapper) Name; \
    Name##_tls_wrapper_schwarz::Name##_tls_wrapper_schwarz() {        \
        Name##_tls_wrapper::init_wrapper();                \
    }

} /* namespace tls_tricks */

#endif
