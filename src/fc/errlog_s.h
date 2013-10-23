/*<std-header orig-src='shore' incl-file-exclusion='ERRLOG_S_H'>

 $Id: errlog_s.h,v 1.10 2010/05/26 01:20:21 nhall Exp $

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

#ifndef ERRLOG_S_H
#define ERRLOG_S_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include <w_base.h>

/** \cond skip */
class simple_string {
    const char *_s;
public:
    // friend w_base_t uint32_t hash(const simple_string &);
    simple_string(const char *s) { _s = s; }
    bool operator==(const simple_string &another) const {
        return strcmp(this->_s,another._s)==0; 
    }
    bool operator!=(const simple_string &another) const {
        return strcmp(this->_s,another._s)!=0; 
    }
    friend ostream &operator<<(ostream &out, const simple_string x);
};

// uint32_t w_hash(const simple_string &s){
//    return (uint32_t) w_hash(s._s); 
//}

extern uint32_t w_hash(const char *); // in stringhash.C

class ErrLog;
struct ErrLogInfo {
public:
    simple_string _ident; // identity for syslog & local use
    w_link_t    hash_link;

    ErrLog *_e;
    // const simple_string & hash_key() { return _ident; } 
    ErrLogInfo(ErrLog *e);
    void dump() const {
        cerr <<  _ident;
        cerr << endl;
    };
private:
    NORET ErrLogInfo(const ErrLogInfo &); // disabled
    NORET ErrLogInfo(ErrLogInfo &); // disabled
};
/** \endcond skip */

/*<std-footer incl-file-exclusion='ERRLOG_S_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
