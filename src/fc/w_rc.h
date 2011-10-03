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

/*<std-header orig-src='shore' incl-file-exclusion='W_RC_H'>

 $Id: w_rc.h,v 1.73 2010/12/08 17:37:37 nhall Exp $

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

#ifndef W_RC_H
#define W_RC_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

#include "w_error.h"


/**\file w_rc.h
 *\ingroup MACROS
 */


/**\addtogroup IDIOMS 
 * \details
 *
 * The storage manager is written with programming idioms to make sure 
 * all return codes are checked, and as a user of the storage manager, you
 * strongly encouraged to use these idioms.
 *
 * It is especially important that you understand the storage manager's
 * \e return \e code type, \ref w_rc_t.
 * Most of the storage manager methods return this type.  The return code
 * type is a dynamically-allocated class instance (except when RCOK, the
 * default, non-error code, is returned: this is a static constant).
 * Because it is heap-allocated, when the compiler generates code for its 
 * destruction, certain programming errors are nastier
 * than in the case of returning an atomic type (e.g., int).  For example,
 * if you write a function with a w_rc_t return type and neglect to give
 * it a return value, your program will probably fail catastrophically.
 * (You can avoid this problem by compiling with compiler warnings enabled.)
 *
 * The return code from a storage manager method should \e always be checked:
 * \code
 w_rc_t func(...)
 {
     ...
     w_rc_t rc = ss_m::create_file(...);
     if(rc.is_error()) {
        ...
     }
     return RCOK;
 }
 \endcode
 * The act of calling \code rc.is_error() \endcode flags the return code as
 * having been checked. 
 * The destructor for \ref w_rc_t can be made to issue an error if the
 * return code was never checked for destruction. The message takes the
 * form:
 * \code
 Error not checked: rc=1. error in j_create_rec.cpp:176 Timed out waiting for resource [0x40000]
 * \endcode
 *
 * (When a return code is sent to an output stream, it prints a 
 * stack trace. The above message contains a stack trace with one level.)
 *
 * The error-not-checked checking happens if 
 * the storage manager is configured and built with 
 * \code --enable-checkrc \endcode .
 * By default this configuration option is off so that it is off
 * for a "production", that is, optimized build.  When you are 
 * debugging your code, it is a good idea to configure the storage manager
 * with it enabled. 
 *
 * Several  macros defined in w_rc.h 
 * support writing and using methods and functions that
 * return a w_rc_t.
 * \code
 w_rc_t
 func(...)
 {
     W_DO(ss_m::create_file(...));
     return RCOK;
 }
 \endcode
 *
 * The \ref W_DO macro returns whatever the called function returns if
 * that return code was an error code, otherwise, it falls through
 * to the code below the macro call. This is the most-often used
 * idiom.
 *
 * The RC_* macros  let you construct a return code for a return value
 * from a function.  The normal, non-error return code is \ref RCOK.
 *
 * Return  codes are described in some detail \ref ERRNUM.  
 * There you may also
 * see how to create your own return codes for server modules.
 *
 * See the examples as well as \ref MACROS in w_rc.h.
 *
 */
class w_rc_i; // forward

/**\brief Return code for most functions and methods.
 *
 * \note
 *  w_error_t::errcode_t sometimes used deep in storage manager rather
 * than w_rc_t for one or more of these reasons:
 *  - w_rc_t is costly 
 *  - The w_error_t's are allocated off a heap and if that is a
 *    per-thread heap, we don't want them to cross thread boundaries.
 *
 *  An error code must be checked by some code, else it will report an
 *  "error-not-checked" when the system is built with
 *  \code
 *    configure --enable-checkrc
 *  \endcode
 *  This is costly but useful for checking that code copes with
 *  errors.   Of course, it does not do a static analysis; rather it
 *  is a dynamic check and so it cannot catch all code 
 *  that ignores return codes.
 */
class w_rc_t 
{
    friend class w_rc_i;
public:
 /**\brief
  * w_error_t::errcode_t sometimes used deep in storage manager rather
 * than w_rc_t. See detailed description, above.
 */
    typedef w_error_t::err_num_t  errcode_t;
    /// Static const return code meaning "no error"
    static const w_rc_t    rc_ok;

    /// Default constructor: "no error"
    NORET            w_rc_t();

    /// Copy constructor: does a deep copy
    explicit NORET            w_rc_t(w_error_t* e);

    /// Construct a return code with the given info. For use by macros.
    explicit NORET            w_rc_t(
            const char* const        filename,
            uint32_t        line_num,
            errcode_t                err_num);

    /// Construct a return code with the given info. For use by macros.
    explicit NORET            w_rc_t(
            const char* const        filename,
            uint32_t        line_num,
            errcode_t                err_num,
            int32_t         sys_err);

    /// Copy constructor: does a deep copy, does not delegate.
    w_rc_t(const w_rc_t &other) : _err(other.clone()) { 
        set_unchecked(); 
    }

    /// Copy operator: does a deep copy; does not delegate
    w_rc_t &operator=(w_rc_t const &other) { 
        return (this == &other)?  *this : _assign(other.clone()); }

    /// Will croak if is_error() has not been called (configure --enable-checkrc)
    NORET                    ~w_rc_t();

    /// Not for general use. Used by configure --enable-checkrc.
    static void             set_return_check(bool on_off, bool fatal);

    /**\brief True if this return code is not rc_ok or equivalent.
     * 
     * This \b must   be called for every w_rc_t before destruction.
     * The idiomatic macros W_DO and its companions do that check for you, and
     * should be used most of the time.
     *
     * See also the following macros:
     * - #W_DO(x)
     * - #W_DO_MSG(x,m)
     */
    bool                    is_error() const;

    /// return the integer error code, as found in the Perl-generated *_gen* header files
    errcode_t               err_num() const;

    /// return the (optional) system error code, if there is one
    int32_t        sys_err_num() const;

    /// Re-initialize this return code to rc_ok equivalence.
    w_rc_t&                 reset();

    /// Add tracing info on the stack for the top error number.  Used by macros.
    w_rc_t&                add_trace_info(
            const char* const        filename,
            uint32_t        line_num);

    /// Push another error number onto the stack.  Used by macros.
    w_rc_t&                push(
            const char* const        filename,
            uint32_t        line_num,
            errcode_t                err_num);

    void                   verify();

    /// Issue error when a return code leaves scope without being checked.  
    void                   error_not_checked();

    /// Choke.
    void                   fatal();

    w_error_t const* operator->() const { return ptr(); }

    /// non-const needed for rc->append_more_info in the RC_APPEND macros
    w_error_t* operator->() { return ptr(); }

    /// public so that sm code and be explicit about delegating if need be
    w_error_t*            delegate();
private:
    w_error_t &operator*() { return *ptr(); }
    w_error_t const &operator*() const { return *ptr(); }

    w_error_t*            clone() const {
      return (ptr() == w_error_t::no_error ) ?  
            w_error_t::no_error: _clone();
    }
    w_error_t*            _clone() const;

    /*
     *  streams
     */
    friend ostream&             operator<<(
        ostream&                    o,
        const w_rc_t&                obj);

private:
    mutable w_error_t*     _err;
    static bool            do_check;
    static bool            unchecked_is_fatal;

    // W_DEBUG_RC is defined as 0 or 1 by --enable-checkrc
    // It tracks whether return codes get checked.
#if W_DEBUG_RC
    static ptrdiff_t get_flag() { return 0x1; }
#else
    static ptrdiff_t get_flag() { return 0x0; }
#endif

    /*
      Access to the w_error_t pointer
      ************************************************************************
      */
    void set_unchecked() {
        if(_err == w_error_t::no_error || !get_flag())
            return;
        union {w_error_t* e; long n; } u={_err};
        u.n |= get_flag();
        _err = u.e;
    }

public: 
    /// turned public for testing
    bool is_unchecked() {
        union {w_error_t* e; long n; } u={_err};
        return u.n & get_flag();
    }
private:
    w_error_t* ptr() { return get(); }
    w_error_t const* ptr() const { return get(); }

    // return the error and mark it checked
    w_error_t* get() const {
        union {w_error_t* e; long n; } u={_err};
        if(get_flag()) {
            u.n &= ~get_flag(); // remove the flag so we don't seg fault
            _err=u.e;
        }
        return _err;
    }

private:

    // delete my current error (if any) and take ownership of a new one
    // other had better not belong to any other rc.
    w_rc_t &_assign(w_error_t* other) {
        w_assert2(other);
        verify();
        w_error_t* err = ptr();
        if((const w_error_t *)err != w_error_t::no_error)
#if USE_BLOCK_ALLOC_FOR_W_ERROR_T
            w_error_t::operator delete(err); // make sure the right delete is used.
#else
            delete err;
#endif
        _err = other;
        return reset();
    }

};


/**\brief Iterator over w_error_t list : helper for w_rc_t.
 *
 *  Allows you to iterate  over the w_error_t structures hanging off a
 *  w_rc_t.
 *
 *  Not generally used by server writers; used by w_rc_t implementation.
 */
class w_rc_i {
    w_error_t const    *_next;
public:
    w_rc_i(w_rc_t const &x) : _next(x.ptr()) {};

    int32_t    next_errnum() {
        w_rc_t::errcode_t temp = 0;
        if(_next) {
            temp = _next->err_num;
            _next = _next->next();
        }
        return temp;
    }
    w_error_t const     *next() {
        w_error_t const *temp = _next;
        if(_next) {
            _next = _next->next();
        }
        return temp;
    }
private:
    // disabled
    w_rc_i(const w_rc_i &x);
//    : _rc( w_rc_t(w_error_t::no_error)),
//        _next(w_error_t::no_error) {};
};



/*********************************************************************
 *
 *  w_rc_t::w_rc_t()
 *
 *  Create an rc with no error. Mark as checked.
 *
 *********************************************************************/
inline NORET
w_rc_t::w_rc_t() 
    : _err(w_error_t::no_error)
{
}


/*********************************************************************
 *
 *  w_rc_t::w_rc_t(e)
 *
 *  Create an rc for error e. Rc is not checked.
 *
 *********************************************************************/
inline NORET
w_rc_t::w_rc_t(w_error_t* e)
    : _err(e)
{
    set_unchecked();
}


/*********************************************************************
 *
 *  w_rc_t::reset()
 *
 *  Mark rc as not checked.
 *
 *********************************************************************/
inline w_rc_t&
w_rc_t::reset()
{
    set_unchecked();
    return *this;
}


/*********************************************************************
 *
 *  w_rc_t::verify()
 *
 *  Verify that rc has been checked. If not, call error_not_checked().
 *
 *********************************************************************/
inline void
w_rc_t::verify()
{
    // W_DEBUG_RC is defined as 0 or 1 by --enable-checkrc
    // It tracks whether return codes get checked.
#if W_DEBUG_RC
    if (do_check && is_unchecked())
        error_not_checked();
#endif
#if W_DEBUG_LEVEL > 2
    w_rc_i it(*this);
    while(w_error_t const* e = it.next()) {
        (void) e->get_more_info_msg(); // Just for assertion checking
    }
#endif
}


/*********************************************************************
 *
 *  w_rc_t::delegate()
 *
 *  Give up my error code. Set self as checked.
 *
 *********************************************************************/
inline w_error_t*
w_rc_t::delegate()
{
    w_error_t* t = ptr();
    _err = w_error_t::no_error;
    return t;
}

/*********************************************************************
 *
 *  w_rc_t::~w_rc_t()
 *
 *  Destructor. Verify status.
 *
 *********************************************************************/
inline NORET
w_rc_t::~w_rc_t()
{
    _assign(w_error_t::no_error);
}


/*********************************************************************
 *
 *  w_rc_t::is_error()
 *
 *  Return true if pointing to an error. Set self as checked.
 *
 *********************************************************************/
inline bool
w_rc_t::is_error() const
{
    // strongly encourage the user to use no_error when they mean "no error"
    return ptr() != w_error_t::no_error; 
}


/*********************************************************************
 *
 *  w_rc_t::err_num()
 *
 *  Return the error code in rc.
 *
 *********************************************************************/
inline w_rc_t::errcode_t
w_rc_t::err_num() const
{
    return ptr()->err_num;
}


/*********************************************************************
 *
 *  w_rc_t::sys_err_num()
 *
 *  Return the system error code in rc.
 *
 *********************************************************************/
inline int32_t
w_rc_t::sys_err_num() const
{
    return ptr()->sys_err_num;
}



/*********************************************************************
 *
 *  Basic macros for using rc.
 *
 *  RC(e)   : create an rc for error code e.
 *
 *  RC2(e,s)   : create an rc for error code e, sys err num s
 *
 *  RCOK    : create an rc for no error.
 *
 *  MAKERC(bool, x):    create an rc if true, else RCOK
 *
 *  e.g.  if (eof) 
 *            return RC(eENDOFFILE);
 *        else
 *            return RCOK;
 *  With MAKERC, this can be converted to
 *       return MAKERC(eof, eENDOFFILE);
 *
 *********************************************************************/
/**\def  RC(e)  
 * \brief Normal error-case return. 
 *
 * Create a return code with the current file, line, and error code x.
 * This is the normal way to return from a method or function.
 */
#define RC(e)       w_rc_t(__FILE__, __LINE__, e)

/**\def  RC2(e,s)  
 * \brief Normal error-case return with sys_error.
 *
 * Create a return code with the current file, line, and error code e,
 * and system error number s.
 * This is the normal way to return an 
 * error indication from a method or function that encountered a system error.
 * The value \b s allows the user to convey an ::errno value in the return code.
 */
#define RC2(e,s)    \
    w_rc_t(__FILE__, __LINE__, e, s)

// This, at least, avoids one constructor call
/**\def  RCOK
 * \brief Normal return value for no-error case.
 *
 * Const return code that indicates no error.
 * This is the normal way to return from a method or function.
 */
#define RCOK        w_rc_t::rc_ok

/**\def  MAKERC(condition,e)
 * \brief Return error \b e if \b condition is false.
 *
 * Create a return code that indicates an error iff the condition is false,
 * otherwise return with no-error indication.
 */
#define MAKERC(condition,e)    ((condition) ? RC(e) : RCOK)



/********************************************************************
 *
 *  More Macros for using rc.
 *
 *  RC_AUGMENT(rc)   : add file and line number to the rc
 *  RC_PUSH(rc, e)   : add a new error code to rc
 *
 *  e.g. 
 *    w_rc_t rc = create_file(f);
 *      if (rc)  return RC_AUGMENT(rc);
 *    rc = close_file(f);
 *    if (rc)  return RC_PUSH(rc, eCANNOTCLOSE)
 *
 *********************************************************************/

#ifdef __GNUC__
/**\def  W_EXPECT(rc)
 * \brief Give the compiler a hint that we expect to take the branch
 *
 * This macro is meaningful only with the GNU C++ compiler.
 */
#define W_EXPECT(rc)    __builtin_expect(rc,1)
/**\def  W_EXPECT_NOT(rc)
 * \brief Give the compiler a hint that we expect not to take the branch
 *
 * This macro is meaningful only with the GNU C++ compiler.
 */
#define W_EXPECT_NOT(rc)    __builtin_expect(rc,0)
#else
/**\def  W_EXPECT(rc)
 * \brief Give the compiler a hint that we expect to take the branch
 *
 * This macro is meaningful only with the GNU C++ compiler.
 */
#define W_EXPECT(rc)    rc            
/**\def  W_EXPECT_NOT(rc)
 * \brief Give the compiler a hint that we expect not to take the branch
 *
 * This macro is meaningful only with the GNU C++ compiler.
 */
#define W_EXPECT_NOT(rc) rc    
#endif
 
/**\def  RC_AUGMENT(rc)
 * \brief Augment stack trace.
 *
 * Add stack trace information (file, line) to a return code. 
 * This is the normal way to return from a method or function upon
 * receiving an error from a method or function that it called.
 * Used by \ref #W_DO(x), \ref #W_DO_MSG(x,m), \ref #W_DO_GOTO(rc,x),
 * and \ref #W_COERCE(x)
 */
#if defined(DEBUG_DESPERATE)
// This can be useful when you are desperate to see where
// some sequence of event happened, as it prints the rc at each 
// augment. 
#define RC_AUGMENT(rc)                    \
    (rc.add_trace_info(__FILE__, __LINE__), (cerr << rc << endl), rc)
#else
#define RC_AUGMENT(rc)                    \
    rc.add_trace_info(__FILE__, __LINE__)
#endif

/**\def  RC_PUSH(rc, e)
 * \brief Augment stack trace with another error code.
 *
 * Add stack trace informatin (file, line, error) to a return code.
 * This is to return from a method or function upon
 * receiving an error from a method or function that it called, when
 * a what you want to return to your caller is a
 * different error code from that returned by the method just called.
 * Used by \ref #W_DO_PUSH(x, e) and
 * \ref #W_DO_PUSH_MSG(x,e, m)
 */
#define RC_PUSH(rc, e)                    \
    rc.push(__FILE__, __LINE__, e)


/**\def  RC_APPEND_MSG(rc, m)
 * \brief Augment stack trace with more arbitrary string information.
 *
 * Add a char string representing more information to a return code.
 * Used by \ref W_RETURN_RC_MSG(e, m),
 * \ref W_DO_MSG(x, m), 
 * \ref W_DO_PUSH_MSG(x, m), and
 * \ref W_COERCE_MSG(x, m)
 */
#define RC_APPEND_MSG(rc, m)                \
do {                            \
    w_ostrstream os;                    \
    os  m << ends;                    \
    rc->append_more_info_msg(os.c_str());        \
} while (0)

/**\def  W_RETURN_RC_MSG(e, m)
 * \brief Retrun with a return code that contains the given error code and additional message.
 */
#define W_RETURN_RC_MSG(e, m)                \
do {                            \
    w_rc_t __e = RC(e);                    \
    RC_APPEND_MSG(__e, m);                    \
    return __e;                        \
} while (0)

/**\def  W_EDO(x)
 * \brief Call a method or function \b x that returns a lightweight error code from a method that returns a w_rc_t.
 *
 * This macro is used deep in the storage manager to call a 
 * method or function that returns a (lightweight) error code rather than
 * a \ref #w_rc_t.
 * It checks the returned code for the error case, and if it finds an
 * error, it creates a w_rc_t with the error code returned by the called
 * function or method.
 */
#define W_EDO(x)                      \
do {                            \
    w_rc_t::errcode_t __e = (x);                    \
    if (W_EXPECT_NOT(__e)) return RC(__e);        \
} while (0)

/**\def  W_DO(x)
 * \brief Call a method or function \b x. 
 *
 * This macro is the normal idiom for calling a method or function.
 * Most methods and functions return a w_rc_t. This macro calls \b x 
 * and checks its returned value.  If an error is encountered, it
 * immediately returns from the current function or method, augmenting
 * the stack trace held by the return code.
 */
#define W_DO(x)                      \
do {                            \
    w_rc_t __e = (x);                    \
    if (W_EXPECT_NOT(__e.is_error())) return RC_AUGMENT(__e); \
} while (0)

/**\def  W_DO_MSG(x)
 * \brief Call a method or function \b x. 
 *
 * Like \ref #W_DO, but any error returned contains
 * the additional information message \b m.
 */
#define W_DO_MSG(x, m)                    \
do {                            \
    w_rc_t __e = (x);                    \
    if (W_EXPECT_NOT(__e.is_error())) {                \
        RC_AUGMENT(__e);                \
        RC_APPEND_MSG(__e, m);                \
        return __e;                    \
    }                            \
} while (0)

/**\def  W_DO_GOTO(rc, x)
 * \brief Idiom for unusual error-handling  before returning.
 *
 *  This macro is used to process errors that require special handling before
 *  the calling function can return.
 *  It calls the method or function \b x, and if \b x returns an error, it
 *  transfers control to the label \b failure.
 *
 * Like \ref #W_DO, but: 
 * - rather than defining a local w_rc_t, it uses an elsewhere-defined
 *   w_rc_t instance; this is because: 
 * - rather than returning in the error case, it branches to the label
 *   \b failure.
 *
 * \note: the label \b failure must exist in the the calling function, and
 * the argument \b rc must have been declared in the calling scope. 
 * Presumably the argument \b rc is declared in the scope of \b failure:
 * as well, so that it can process the error.
 */
// W_DO_GOTO assumes the rc was already declared 
#define W_DO_GOTO(rc/*w_rc_t*/, x)          \
do {                            \
    (rc) = (x);    \
    if (W_EXPECT_NOT(rc.is_error())) { \
        RC_AUGMENT(rc);              \
        goto failure;    \
    } \
} while (0)

/**\def  W_DO_PUSH(x, e)
 * \brief Call a function or method \b x, if error, push error code \b e on the stack and return.
 *
 * This macro is like \ref #W_DO(x), but it adds an error code \b e to 
 * the stack trace before returning.
 */
#define W_DO_PUSH(x, e)                    \
do {                            \
    w_rc_t __e = (x);                    \
    if (W_EXPECT_NOT(__e.is_error()))  { return RC_PUSH(__e, e); }    \
} while (0)

/**\def  W_DO_PUSH_MSG(x, e, m)
 * \brief Call a function or method \b x, if error, push error code \b e on the stack and return.
 *
 * This macro is like \ref #W_DO_PUSH(x, e), but it adds an additional
 * information message \b m to
 * the stack trace before returning.
 */
#define W_DO_PUSH_MSG(x, e, m)                \
do {                            \
    w_rc_t __e = (x);                    \
    if (W_EXPECT_NOT(__e.is_error()))  {                \
        RC_PUSH(__e, e);                \
        RC_APPEND_MSG(__e, m);                \
    return __e;                    \
    }                            \
} while (0)

/**\def  W_COERCE(x)
 * \brief Call a function or method \b x, fail catastrophically if error is returned.
 *
 * This macro is like \ref #W_DO(x), but instead of returning in the error
 * case, it fails catastrophically.
 * It is used in cases such as these:
 * - Temporary place-holder where the coder hasn't written 
 *   the failure-handling code
 * - The calling function or method API has no means to return 
 *   error information, and this case hasn't yet been accommodated.
 * - The called \b x should never return an error 
 *   in this case, and doing so would indicate a programming error.
 * - The called \b x never returns an error at the time the calling code 
 *   is written, and should the called code \b x change, 
 *   the calling code should probably be adjusted to handle any new error.
 *
 *   The call to __e.fatal() prints the stack trace and additional information
 *   associated with the w_rc_t before it croaks.
 */
#define W_COERCE(x)                      \
do {                            \
    w_rc_t __e = (x);                    \
    if (W_EXPECT_NOT(__e.is_error()))  {                \
    RC_AUGMENT(__e);                \
    __e.fatal();                    \
    }                            \
} while (0)

/**\def  W_COERCE_MSG(x, m)
 * \brief Same as \ref #W_COERCE(x) but adds a string message before croaking. 
 */
#define W_COERCE_MSG(x, m)                \
do {                            \
    w_rc_t __em = (x);                    \
    if (W_EXPECT_NOT(__em.is_error()))  {                \
    RC_APPEND_MSG(__em, m);                \
    W_COERCE(__em);                    \
    }                            \
} while (0)

/**\def  W_FATAL(e)
 * \brief Croak with the error code \b e.
 */
#define W_FATAL(e)           W_COERCE(RC(e))

/**\def  W_FATAL_RC(rc)
 * \brief Croak with the return code \b rc.
 */
#define W_FATAL_RC(rc)        W_COERCE(rc)

/**\def  W_FATAL_MSG(e, m)
 * \brief Croak with the error code \b e and message \b m.
 */
#define W_FATAL_MSG(e, m)    W_COERCE_MSG(RC(e), m)

/**\def  W_IGNORE(x, m)
 * \brief Invoke \b x and ignore its result.
 */
#define W_IGNORE(x)    ((void) x.is_error())

/*<std-footer incl-file-exclusion='W_RC_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
