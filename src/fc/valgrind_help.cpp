/*<std-header orig-src='shore' incl-file-exclusion='W_BASE_H'>

 $Id: valgrind_help.cpp,v 1.2 2010/05/26 01:20:22 nhall Exp $

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

#ifdef USING_VALGRIND
#include "valgrind_help.h"

static valgrind_check VGC;

#include <assert.h>
#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

void stop_on_vg_err() { 
    // core dump
    (void) kill(getpid(), SIGABRT);
}

/* If you've got reproducible errors reported by valgrind,
 * you have to insert calls to this method somewhere near but
 * after the suspected place, so you can create a core dump there.
 * Then rebuild & rerun.
 */
void check_valgrind_errors(int /*line*/, const char * /*file*/) { 
    VGC.check();
    /*
    fprintf(stderr, "check_valgrind_errors: more %s diff %d curr %d line %d file %s\n",
            (const char *)(VGC.more()?"true":"false"),
            VGC.diff(), VGC.curr(), line, file );
    */
    if(VGC.more()) stop_on_vg_err();
}

class dummyfile {
    int _fd;
public:
    // I'm using /tmp for the benefit of tmpfs
    dummyfile() { _fd = open("/tmp/vgdummy", O_CREAT|O_TRUNC|O_RDWR, S_IRWXU);
                   assert(_fd>0);
                }
    ~dummyfile() { close(_fd); _fd=0; }
    int        fd() { return _fd; }
};

void check_definedness(void *v, size_t len)
{
    // Valgrind doesn't check for defined-ness until
    // the program behavior would be affected by it, so
    // what we have to do here is write to a dummy file.
    // We used to cause a branch based on the value in each char,
    // but valgrind doesn't report that with the address of the
    // datum in question.
    //
    static dummyfile D;
    lseek( D.fd(), 0, SEEK_SET);
    write( D.fd(), v, len);
}
#endif
