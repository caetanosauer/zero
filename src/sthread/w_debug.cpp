/*<std-header orig-src='shore'>

 $Id: w_debug.cpp,v 1.1 2010/12/09 15:29:05 nhall Exp $

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

#ifdef __GNUG__
#pragma implementation
#endif 
#include <w_debug.h>


#ifdef __ERRLOG_C__

// gcc implementation in errlog.cpp since it is in #included there

/* compile this stuff even if -UDEBUG because
 * other layers might be using -DDEBUG
 * and we want to be able to link, in any case
 */

#include <w_stream.h>
#include <iostream>
#include <cstring>
#include <cstdlib>
#include "w_debug.h"

#ifdef W_TRACE
w_debug _w_debug("debug", getenv("DEBUG_FILE"));
#endif


#ifdef USE_REGEX
bool        _w_debug::re_ready = false;
regex_t     _w_debug::re_posix_re;
char*       _w_debug::re_error_str = "Bad regular expression";
#endif /* USE_REGEX */

// I'm ambivalent about making this thread-safe.
// To do so would require moving this and all related tests into
// sthread/.
// The disadvantage is that it would that much more change the
// timing of things.
// The errlog and such is most useful for debugging single-threaded
// tests anyway, and still somewhat useful for mt stuff despite this
// being not-safe; it would clearly change the timing for mt situations
// we're trying to debug; I think it's probably more useful to
// decipher mixed-up debugging output for those cases.
//

w_debug::w_debug(const char *n, const char *f) : 
    ErrLog(n, log_to_unix_file, f?f:"-")
{
#ifdef USE_REGEX
    //re_ready = false;
    //re_error_str = "Bad regular expression";
    re_syntax_options = RE_NO_BK_VBAR;
#endif /* USE_REGEX */

    mask = 0;
    const char *temp_flags = getenv("DEBUG_FLAGS");
    // malloc the flags so it can be freed
    if(!temp_flags) {
        temp_flags = "";
        mask = _none;
    }

    // make a copy of the flags so we can delete it later
    _flags = new char[strlen(temp_flags)+1];
    strcpy(_flags, temp_flags);
    assert(_flags != NULL);

    if(!strcmp(_flags,"all")) {
    mask |= _all;
#ifdef USE_REGEX
    } else if(!none()) {
    char *s;
    if((s=re_comp_debug(_flags)) != 0) {
        if(strstr(s, "No previous regular expression")) {
        // this is ok
        } else {
        cerr << "Error in regex, flags not set: " <<  s << endl;
        }
        mask = _none;
    }
#endif /* USE_REGEX */
    }

    assert( !( none() && all() ) );
}

w_debug::~w_debug()
{
    if(_flags) delete [] _flags;
    _flags = NULL;

}

void
w_debug::setflags(const char *newflags)
{
    if(!newflags) return;
#ifdef USE_REGEX
    {
        char *s;
        if((s=re_comp_debug(newflags)) != 0) {
            cerr << "Error in regex, flags not set: " <<  s << endl;
            mask = _none;
            return;
        }
    }
#endif /* USE_REGEX */

    mask = 0;
    if(_flags) delete []  _flags;
    _flags = new char[strlen(newflags)+1];
    strcpy(_flags, newflags);
    if(strlen(_flags)==0) {
        mask |= _none;
    } else if(!strcmp(_flags,"all")) {
        mask |= _all;
    }
    assert( !( none() && all() ) );
}

#ifdef USE_REGEX
int 
w_debug::re_exec_debug(const char* string)
{
    if (!re_ready)  {
        cerr << __LINE__ 
        << " " << __FILE__ 
        << ": No compiled string." <<endl;
        return 0;
    }
    int match = (re_exec_posix(string)==1);
    return  match;
}

char* 
w_debug::re_comp_debug(const char* pattern)
{
    if (re_ready)
        regfree(&re_posix_re);
    char *res;

    res = re_comp_posix(pattern);
    if(res) {
        cerr << __LINE__ 
        << " " << __FILE__ 
        << " Error in re_comp_debug: " << res << endl;
    }
    re_ready = true;
    return NULL;
}
#endif /* USE_REGEX */


int
w_debug::flag_on(
    const char *fn,
    const char *file
)
{
    int res = 0;
    assert( !( none() && all() ) );
    if(_flags==NULL) {
    res = 0; //  another constructor called this
            // before this's constructor got called. 
    } else if(none())     {
    res = 0;
    } else if(all())     {
    res = 1;
#ifdef USE_REGEX
    } else  if(file && re_exec_debug(file)) {
    res = 1;
    } else if(fn && re_exec_debug(fn)) {
    res = 1;
#endif /* USE_REGEX */
    } else
    // if the regular expression didn't match,
    // try searching the strings
    if(file && strstr(_flags,file)) {
    res = 1;
    } else if(fn && strstr(_flags,fn)) {
    res = 1;
    }
    return res;
}

/* This function prints a hex dump of (len) bytes at address (p) */
void
w_debug::memdump(void *p, int len)
{
    register int i;
    char *c = (char *)p;
    
    clog << "x";
    for(i=0; i< len; i++) {
        W_FORM2(clog,("%2.2x", (*(c+i))&0xff));
        if(i%32 == 31) {
            clog << endl << "x";
        } else if(i%4 == 3) {
            clog <<  " x";
        }
    }
    clog << "--done" << endl;
}
#endif /* __ERRLOG_C__ */

