/*<std-header orig-src='regex' incl-file-exclusion='REGEX_UTILS_H'>

 $Id: regex_utils.h,v 1.11 2010/05/26 01:20:12 nhall Exp $


*/

#ifndef REGEX_UTILS_H
#define REGEX_UTILS_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/*
Copyright 1992, 1993, 1994, 1997 Henry Spencer.  All rights reserved.
This software is not subject to any license of the American Telephone
and Telegraph Company or of the Regents of the University of California.

Permission is granted to anyone to use this software for any purpose on
any computer system, and to alter it and redistribute it, subject
to the following restrictions:

1. The author is not responsible for the consequences of use of this
   software, no matter how awful, even if they arise from flaws in it.

2. The origin of this software must not be misrepresented, either by
   explicit claim or by omission.  Since few users ever read sources,
   credits must appear in the documentation.

3. Altered versions must be plainly marked as such, and must not be
   misrepresented as being the original software.  Since few users
   ever read sources, credits must appear in the documentation.

4. This notice may not be removed or altered.

*/

/* 
  NOTICE of alterations in Spencer's regex implementation :
  The following alterations were made to Henry Spencer's regular 
  expressions implementation, in order to make it build in the
  Shore configuration scheme:

  1) the generated .ih files are no longer generated. They are
    considered "sources".  Likewise for regex.h.
  2) names were changed to w_regexex, w_regerror, etc by i
    #define statements in regex.h
  3) all the c sources were protoized and gcc warnings were 
    fixed.
  4) This entire notice was put into the .c, .ih, and .h files
*/

/* utility definitions */
#ifdef _POSIX2_RE_DUP_MAX
#define    DUPMAX    _POSIX2_RE_DUP_MAX
#else
#define    DUPMAX    255
#endif
#define    REGEX_INFINITY    (DUPMAX + 1)
#define    NC        (CHAR_MAX - CHAR_MIN + 1)
typedef unsigned char uch;

/* switch off assertions (if not already off) if no REDEBUG */
#ifndef REDEBUG
#define    re_assert(EX)    do { } while(0)
#else
/* This is a workaround for a bug in some c++ cassert wrappers that
   have an incorrect multiple include prevention for assert.  The
   bug still is a problem, but at least it won't leave REDEBUG enabled
   continously when it shouldn't be */
/* XXX if you are having problems debuging regular expression code,
   you may need to do an assert-by-hand below (use w_assert) */
#include <cassert>
#define    re_assert(EX)    assert(EX)
#endif

/* for old systems with bcopy() but no memmove() */
#ifdef USEBCOPY
#define    memmove(d, s, c)    bcopy(s, d, c)
#endif

/*<std-footer incl-file-exclusion='REGEX_UTILS_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
