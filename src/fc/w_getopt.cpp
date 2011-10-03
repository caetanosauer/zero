/*<std-header orig-src='shore'>

 $Id: w_getopt.cpp,v 1.2 2010/05/26 01:20:25 nhall Exp $

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

#include "w_getopt.h"
#ifndef HAVE_GETOPT
#include <w_stream.h>
#include <cstring>

char*        optarg = 0;
int        optind = 1;
int        opterr = 1;    /* print error message by default */
int        optopt = 0;

/* implements unix (3C) getopt function

   getopt takes the argc and argv variables passed to a program and
   extracts option flags and values from them.

   it does this by checking to see if the next argument exists and
   begins with a '-'.  if it does then the argument is assumed to
   contain a set of flags.  if it does not them the processing returns
   EOF and optind is the index into argv where processing stopped.

   if the argument contains a set of flags the value of the flag
   is checked to see if it is in the optstring parameter, and if it
   is then the flag is valid, if not it is an error.  if the next
   character in optstring is a ':' then this signifies that the option
   takes a value, which is considered to be the rest of the argument
   if there is any or the next argument (it is an error if there is no
   next argument) if the flag is at the end of the argument.

   the function returns the option flag if no error occurred, a '?' if
   an error occurred, or EOF if the end of the options was found.  optarg
   contains the value of the option if the option has one and no error
   occurred.  optopt always contains the character for the flag
   regardless of an error.  optind contains the argv index where the
   next flag will be found or one place beyond where options processing
   concluded.

   an argument of "--" signals the end of options processing and causes
   getopt to return EOF and to set optind to one beyond where "--" was
   found.

   this function is not thread safe since if relies on global variables.
   also it is not safe to restart option processing by resetting optind
   to 1, until getopt has returned EOF.

   to be compatible with the unix version of getopt, setting optind to
   0 now resets option processing to begin at argv[1].  if you wish to
   restart option processing at some other argv index you need to first
   call getopt repeatedly until it returns EOF, which sets the argIndex
   to 0, and then you can set optind to the index value.  both of these
   behaviors are non-portable.
*/

int PrintError(const char* errMsg, const char* progName, char optChar);

int getopt(int argc, char* const * argv, const char* optstring)
{
    /* used to keep track of where we are in the arg when multiple flags
       are combined in the same argument */
    static int    argIndex = 0;

    /* restart option processing if optind is 0 */
    if (optind == 0)  {
    optind = 1;
    argIndex = 0;
    }

    optarg = 0;
    optopt = 0;

    /* if we've run out of args exit */
    if (optind == argc)  {
    return EOF;
    }

    /* if the current arg doesn't start with '-' exit */
    if (argIndex == 0 && argv[optind][0] != '-')  {
    return EOF;
    }

    /* skip past the '-', if we're looking at one */
    if (argIndex == 0)  {
    argIndex++;

    /* getopt man page says "--" signals end of args, so check for it */
    if (argv[optind][argIndex] == '-' && argv[optind][argIndex + 1] == 0)  {
        ++optind;
        argIndex = 0;
        return EOF;
    }
    }

    optopt = argv[optind][argIndex++];

    const char* optStringPos = 0;
    if ((optStringPos = strchr(optstring, optopt)))  {
    if (optStringPos[1] == ':')  {

        /* flag has a value */
        if (argv[optind][argIndex] != 0)  {
        /* value is the rest of the arg */
        optarg = &argv[optind][argIndex];
        ++optind;
        argIndex = 0;

        }  else  {
        /* value is next arg if there is one */
        ++optind;
        argIndex = 0;
        if (argv[optind] != 0)  {
            optarg = &argv[optind++][argIndex];
        }  else  {
            return PrintError("option requires an argument", argv[0], optopt);
        }
        }
    }  else  {
        /* flag is just a flag */
        if (argv[optind][argIndex] == 0)  {
        /* advance to next arg if we are at the end of the current arg */
        ++optind;
        argIndex = 0;
        }
    }
    }  else  {
    /* bad flag */
    if (argv[optind][argIndex] == 0)  {
        ++optind;
        argIndex = 0;
    }
    return PrintError("illegal option", argv[0], optopt);
    }

    return optopt;
}


int PrintError(const char* errMsg, const char* progName, char optChar)
{
    if (opterr)
        cerr << progName << ": " << errMsg << " -- " << optChar << endl;

    return '?';
}

#endif
