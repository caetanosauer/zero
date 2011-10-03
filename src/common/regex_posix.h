/*<std-header orig-src='regex' incl-file-exclusion='REGEX_POSIX_H'>

 $Id: regex_posix.h,v 1.6 2010/05/26 01:20:12 nhall Exp $


*/

#ifndef REGEX_POSIX_H
#define REGEX_POSIX_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/*
 * hpux does not have re_comp and re_exec.  Instead there is
 * regcomp() and regexec().
 */

#    define _INCLUDE_XOPEN_SOURCE
#    include "regex.h"

#include <cassert>

#    define re_comp re_comp_posix
#    define re_exec re_exec_posix

extern "C" {
    char* re_comp_posix(const char* pattern);
    int    re_exec_posix(const char* string);
}

/*<std-footer incl-file-exclusion='REGEX_POSIX_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
