/*<std-header orig-src='shore' incl-file-exclusion='W_BASE_H'>

 $Id: valgrind_help.h,v 1.2 2010/05/26 01:20:22 nhall Exp $

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

#ifndef VALGRIND_HELP_H
#define VALGRIND_HELP_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/**\cond skip */

#include <sys/types.h>

class valgrind_check
{
    int  _current_errors;
    int  _errors_last_check;
    int  _check_failed;

public:
    valgrind_check(): _current_errors(0),
                        _errors_last_check(0), _check_failed(0) {}

    void check() {
        _current_errors = VALGRIND_COUNT_ERRORS;
        _check_failed = _current_errors - _errors_last_check;
        if(_check_failed > 0) {
            _errors_last_check = _current_errors;
        }
    }
    bool more() const { return (_check_failed>0); }
    int diff() const { return _check_failed; }
    int curr() const { return _current_errors; }
    int prev() const { return _current_errors - _check_failed; } };


extern "C" void stop_on_vg_err();
extern "C" void check_valgrind_errors(int l, const char *f);
extern "C" void check_definedness(void *v, size_t len);

/**\endcond skip */

#endif  /* VALGRIND_HELP_H */
