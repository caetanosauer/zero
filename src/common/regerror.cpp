/*<std-header orig-src='shore'>

 $Id: regerror.cpp,v 1.19 2010/05/26 01:20:12 nhall Exp $

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
    *.ih is now called *_i.h, for NT purposes.
  2) names were changed to w_regexex, w_regerror, etc by i
    #define statements in regex.h
  3) all the c sources were protoized and gcc warnings were 
    fixed.
  4) This entire notice was put into the .c, .ih, and .h files
*/

#include <os_types.h>
#include <cstdio>
#include <cstring>
#include <cctype>
#include <climits>
#include <cstdlib>
#include <w_debug.h>
#include <regex.h>

#include "regex_utils.h"
#include "regerror_i.h"

extern int w_force_shore_regcomp; // GROT
int w_force_shore_regerror=0; // GROT
int &_w_force_shore_regerror=w_force_shore_regcomp; // GROT

/*
 = #define    REG_OKAY     0
 = #define    REG_NOMATCH     1
 = #define    REG_BADPAT     2
 = #define    REG_ECOLLATE     3
 = #define    REG_ECTYPE     4
 = #define    REG_EESCAPE     5
 = #define    REG_ESUBREG     6
 = #define    REG_EBRACK     7
 = #define    REG_EPAREN     8
 = #define    REG_EBRACE     9
 = #define    REG_BADBR    10
 = #define    REG_ERANGE    11
 = #define    REG_ESPACE    12
 = #define    REG_BADRPT    13
 = #define    REG_EMPTY    14
 = #define    REG_ASSERT    15
 = #define    REG_INVARG    16
 = #define    REG_ATOI    255    // convert name to number (!)
 = #define    REG_ITOA    0400    // convert number to name (!)
 */
static struct rerr {
    int code;
    const char *name;
    const char *explain;
} rerrs[] = {
    { REG_OKAY,    "REG_OKAY",    "no errors detected"},
    { REG_NOMATCH,    "REG_NOMATCH",    "regexec() failed to match"},
    { REG_BADPAT,    "REG_BADPAT",    "invalid regular expression"},
    { REG_ECOLLATE,    "REG_ECOLLATE",    "invalid collating element"},
    { REG_ECTYPE,    "REG_ECTYPE",    "invalid character class"},
    { REG_EESCAPE,    "REG_EESCAPE",    "trailing backslash (\\)"},
    { REG_ESUBREG,    "REG_ESUBREG",    "invalid backreference number"},
    { REG_EBRACK,    "REG_EBRACK",    "brackets ([ ]) not balanced"},
    { REG_EPAREN,    "REG_EPAREN",    "parentheses not balanced"},
    { REG_EBRACE,    "REG_EBRACE",    "braces not balanced"},
    { REG_BADBR,    "REG_BADBR",    "invalid repetition count(s)"},
    { REG_ERANGE,    "REG_ERANGE",    "invalid character range"},
    { REG_ESPACE,    "REG_ESPACE",    "out of memory"},
    { REG_BADRPT,    "REG_BADRPT",    "repetition-operator operand invalid"},
    { REG_EMPTY,    "REG_EMPTY",    "empty (sub)expression"},
    { REG_ASSERT,    "REG_ASSERT",    "\"can't happen\" -- you found a bug"},
    { REG_INVARG,    "REG_INVARG",    "invalid argument to regex routine"},
    { -1,        "",        "*** unknown regexp error code ***"},
};

/*
 - regerror - the interface to error numbers
 = extern size_t regerror(int, const regex_t *, char *, size_t);
 */
/* ARGSUSED */
size_t
regerror(int errcode, const regex_t *preg, char *errbuf, size_t errbuf_size)
{
    register struct rerr *r;
    register size_t len;
    register int target = errcode &~ REG_ITOA;
    register const char *s;
    char convbuf[50];

    if (errcode == REG_ATOI)
        s = regatoi(preg, convbuf);
    else {
        for (r = rerrs; r->code >= 0; r++)
            if (r->code == target)
                break;
    
        if (errcode&REG_ITOA) {
            if (r->code >= 0)
                (void) strcpy(convbuf, r->name);
            else
                sprintf(convbuf, "REG_0x%x", target);
            re_assert(strlen(convbuf) < sizeof(convbuf));
            s = convbuf;
        } else
            s = r->explain;
    }

    len = strlen(s) + 1;
    if (errbuf_size > 0) {
        if (errbuf_size > len)
            (void) strcpy(errbuf, s);
        else {
            (void) strncpy(errbuf, s, errbuf_size-1);
            errbuf[errbuf_size-1] = '\0';
        }
    }

    return(len);
}

/*
 - regatoi - internal routine to implement REG_ATOI
 == static char *regatoi(const regex_t *preg, char *localbuf);
 */
static const char *
regatoi(const regex_t *preg, char *localbuf)
{
    register struct rerr *r;

    for (r = rerrs; r->code >= 0; r++)
        if (strcmp(r->name, preg->re_endp) == 0)
            break;
    if (r->code < 0)
        return("0");

    sprintf(localbuf, "%d", r->code);
    return(localbuf);
}

