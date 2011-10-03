/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager
   
                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne
   
                         All Rights Reserved.
   
   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.
   
   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

/*<std-header orig-src='shore' incl-file-exclusion='CHKPT_H'>

 $Id: chkpt.h,v 1.23 2010/06/08 22:28:55 nhall Exp $

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

#ifndef CHKPT_H
#define CHKPT_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

#include "sm_int_1.h"
class chkpt_thread_t;

/*********************************************************************
 *
 *  class chkpt_m
 *
 *  Checkpoint Manager. User calls spawn_chkpt_thread() to fork
 *  a background thread to take checkpoint every now and then. 
 *  User calls take() to take a checkpoint immediately.
 *
 *  User calls wakeup_and_take() to wake up the checkpoint 
 *  thread to checkpoint soon.
 *
 *********************************************************************/
class chkpt_m : public smlevel_1 {
public:
    NORET            chkpt_m();
    NORET            ~chkpt_m();

public:
    void             wakeup_and_take();
    void             spawn_chkpt_thread();
    void             retire_chkpt_thread();
    void             take();

private:
    chkpt_thread_t*  _chkpt_thread;
    long             _chkpt_count;


public:
    // These functions are for the use of chkpt -- to serialize
    // logging of chkpt and prepares 
};

/*<std-footer incl-file-exclusion='CHKPT_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
