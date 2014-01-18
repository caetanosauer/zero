#ifndef W_ERROR_H
#define W_ERROR_H
/*
 * (c) Copyright 2014-, Hewlett-Packard Development Company, LP
 */

#define X(a, b, c, d) a b,
/**
 * \enum w_error_codes
 * \file w_error.h
 * \brief Defines all error codes and corresponding error messages.
 * \details
 * \section Basics Quick-start
 * To return an error, just put "return RC(error_code_here);".
 * Whenever you want a new error message, append a new line in w_error.dat like existing lines.
 *
 * \section History History
 * Shore-MT had a perl script to generate equivalent codes from tabular data files.
 * The reason for them to do that was (I guess) to keep the definition of
 * error code in the same line as the definition of its error message for better
 * readability and maintainability. This is where C++ sucks where pretty-print
 * enum like Java is not available.
 *
 * \section XMacros X-Macros
 * However, the code generator unfortunately caused another readability and maintainability
 * issue just for this trivial goal. Foster-Btree changed it to the so-called "X Macros" style.
 *   http://www.drdobbs.com/the-new-c-x-macros/184401387
 * The goal is same, but by using this approach, we don't need code generator.
 *
 * \section Notes Notes
 * As you might notice, this enum is directly exposed in global namespace.
 * Not a good thing, but otherwise existing code will break.
 * We might move it to some namespace or class member when we are ready to do wide refactoring.
 */
enum w_error_codes {
#include "w_error.dat"
};
#undef X

#define X(a, b, c, d) [a]=c,
const char *w_error_names[] = {
#include "w_error.dat"
};
#undef X

#define X(a, b, c, d) [a]=d,
const char *w_error_messages[] = {
#include "w_error.dat"
};
#undef X

#endif // W_ERROR_H
