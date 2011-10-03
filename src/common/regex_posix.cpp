/*<std-header orig-src='regex'>

 $Id: regex_posix.cpp,v 1.6 2010/05/26 01:20:12 nhall Exp $


*/

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/*
 * hpux does not have re_comp and re_exec.  Instead there is
 * regcomp() and regexec().
 */

#include <cstddef>
#include <w_base.h>
#include "regex_posix.h"

static regex_t     re_posix_re;
static bool     re_ready = false;
static char    re_error_str[200] = { '\0' };

char* 
re_comp_posix(const char* pattern)
{
    // assert(!re_ready);

    if (re_ready) {
    regfree(&re_posix_re);
    }
    long flags = REG_NOSUB; //ignore last 3 args of regexec();

    int res = regcomp(&re_posix_re, pattern, flags);
    if (res != 0) {
    (void) regerror(res, &re_posix_re, re_error_str, sizeof(re_error_str));
#ifdef DEBUG_REGEX_POSIX
    cerr << "re_comp_posix: error = " << re_error_str <<endl;
#endif
     
    return re_error_str;
    }

    re_ready = true;
    return NULL;
}

int    
re_exec_posix(const char* string)
{
    int status;

    if(!re_ready) {
    return -1; // no string compiled
        // possibly because of previous error
    }
    status = regexec(&re_posix_re, string, (size_t)0, NULL, 0);
    if (status != 0) {
    (void) regerror(status, &re_posix_re, 
        re_error_str, sizeof(re_error_str));
#ifdef DEBUG_REGEX_POSIX
    cerr << "re_exec_posix: error = " << re_error_str <<endl;
    cerr << "re_exec_posix: string = " << string <<endl;
#endif
    regfree(&re_posix_re);
        re_ready = false;
    return 0;
    }
    w_assert9 (status == 0);
    return 1; // found match
}

