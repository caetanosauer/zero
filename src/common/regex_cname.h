/*<std-header orig-src='regex' incl-file-exclusion='REGEX_CNAME_H'>

 $Id: regex_cname.h,v 1.10 2010/05/26 01:20:12 nhall Exp $


*/

#ifndef REGEX_CNAME_H
#define REGEX_CNAME_H

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

/* character-name table */
static struct cname {
    const char *name;
    char code;
} cnames[] = {
    { "NUL",    '\0'},
    { "SOH",    '\001'},
    { "STX",    '\002'},
    { "ETX",    '\003'},
    { "EOT",    '\004'},
    { "ENQ",    '\005'},
    { "ACK",    '\006'},
    { "BEL",    '\007'},
    { "alert",    '\007'},
    { "BS",        '\010'},
    { "backspace",    '\b'},
    { "HT",        '\011'},
    { "tab",        '\t'},
    { "LF",        '\012'},
    { "newline",    '\n'},
    { "VT",        '\013'},
    { "vertical-tab",    '\v'},
    { "FF",        '\014'},
    { "form-feed",    '\f'},
    { "CR",        '\015'},
    { "carriage-return",    '\r'},
    { "SO",    '\016'},
    { "SI",    '\017'},
    { "DLE",    '\020'},
    { "DC1",    '\021'},
    { "DC2",    '\022'},
    { "DC3",    '\023'},
    { "DC4",    '\024'},
    { "NAK",    '\025'},
    { "SYN",    '\026'},
    { "ETB",    '\027'},
    { "CAN",    '\030'},
    { "EM",    '\031'},
    { "SUB",    '\032'},
    { "ESC",    '\033'},
    { "IS4",    '\034'},
    { "FS",    '\034'},
    { "IS3",    '\035'},
    { "GS",    '\035'},
    { "IS2",    '\036'},
    { "RS",    '\036'},
    { "IS1",    '\037'},
    { "US",    '\037'},
    { "space",        ' '},
    { "exclamation-mark",    '!'},
    { "quotation-mark",    '"'},
    { "number-sign",        '#'},
    { "dollar-sign",        '$'},
    { "percent-sign",        '%'},
    { "ampersand",        '&'},
    { "apostrophe",        '\''},
    { "left-parenthesis",    '('},
    { "right-parenthesis",    ')'},
    { "asterisk",    '*'},
    { "plus-sign",    '+'},
    { "comma",    ','},
    { "hyphen",    '-'},
    { "hyphen-minus",    '-'},
    { "period",    '.'},
    { "full-stop",    '.'},
    { "slash",    '/'},
    { "solidus",    '/'},
    { "zero",        '0'},
    { "one",        '1'},
    { "two",        '2'},
    { "three",    '3'},
    { "four",        '4'},
    { "five",        '5'},
    { "six",        '6'},
    { "seven",    '7'},
    { "eight",    '8'},
    { "nine",        '9'},
    { "colon",    ':'},
    { "semicolon",    ';'},
    { "less-than-sign",    '<'},
    { "equals-sign",        '='},
    { "greater-than-sign",    '>'},
    { "question-mark",    '?'},
    { "commercial-at",    '@'},
    { "left-square-bracket",    '['},
    { "backslash",        '\\'},
    { "reverse-solidus",    '\\'},
    { "right-square-bracket",    ']'},
    { "circumflex",        '^'},
    { "circumflex-accent",    '^'},
    { "underscore",        '_'},
    { "low-line",        '_'},
    { "grave-accent",        '`'},
    { "left-brace",        '{'},
    { "left-curly-bracket",    '{'},
    { "vertical-line",    '|'},
    { "right-brace",        '}'},
    { "right-curly-bracket",    '}'},
    { "tilde",        '~'},
    { "DEL",    '\177'},
    { NULL,    0},
};

/*<std-footer incl-file-exclusion='REGEX_CNAME_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
