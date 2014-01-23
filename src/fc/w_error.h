#ifndef W_ERROR_H
#define W_ERROR_H
/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */

/**
 * \defgroup ERRORCODES Error code and error messages
 * \brief Error codes and corresponding error messages defined in w_error_xmacro.h.
 * \ingroup IDIOMS
 * \details
 * \section Basics Quick-start
 * To return an error, just put "return RC(error_code_here);".
 * Whenever you want a new error message, append a new line in w_error_xmacro.h like existing lines.
 * This file is completely independent and header-only. Just include w_error.h to use.
 *
 * \section History History
 * Shore-MT had a perl script to generate equivalent codes from tabular data files.
 * The reason for doing so back then was (I guess) to keep the definition of
 * error code in the same line as the definition of its error message for better
 * readability and maintainability. This is where C++ sucks where pretty-print
 * enum like Java is not available.
 *
 * \section XMacros X-Macros
 * However, the code generator unfortunately caused another readability and maintainability
 * issue just for this trivial goal. Foster-Btree changed it to the so-called "X Macro" style.
 *   http://en.wikipedia.org/wiki/X_Macro
 *   http://www.drdobbs.com/the-new-c-x-macros/184401387
 * The goal is same, but by using this approach, we don't need code generator.
 *
 * \section Pollution Pollution
 * As you might notice, this enum is directly exposed in global namespace.
 * Not a good thing, but otherwise existing code will break.
 * We might move it to some namespace or class member when we are ready to do wide refactoring.
 */

#define X(a, b) /** b. */ a,
/**
 * \enum w_error_codes
 * \ingroup ERRORCODES
 * \brief Enum of error codes defined in w_error_xmacro.h.
 */
enum w_error_codes {
    /** 0 means no-error. */
    w_error_ok = 0,
#include "w_error_xmacro.h"
    /** dummy entry to get the count of error codes. */
    w_error_count
};
#undef X

// A bit tricky to get "a" from a in C macro.
#define X_QUOTE(str) #str
#define X_EXPAND_AND_QUOTE(str) X_QUOTE(str)
#define X(a, b) case a: return X_EXPAND_AND_QUOTE(a);
/**
 * \brief Returns the names of w_error_code enum defined in w_error_xmacro.h.
 * \ingroup ERRORCODES
 */
inline const char* w_error_name(w_error_codes error_code) {
    switch (error_code) {
        case w_error_ok: return "w_error_ok";
        case w_error_count: return "w_error_count";
#include "w_error_xmacro.h"
    }
    return "Unexpected error code";
};
#undef X
#undef X_EXPAND_AND_QUOTE
#undef X_QUOTE

#define X(a, b) case a: return b;
/**
 * \brief Returns the error messages corresponding to w_error_code enum defined in w_error_xmacro.h.
 * \ingroup ERRORCODES
 */
inline const char* w_error_message(w_error_codes error_code) {
    switch (error_code) {
        case w_error_ok: return "no_error";
        case w_error_count: return "error-count";
#include "w_error_xmacro.h"
    }
    return "Unexpected error code";
};
#undef X

#endif // W_ERROR_H
