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

/*<std-header orig-src='shore'>

 $Id: chkpt_serial.cpp,v 1.8 2010/05/26 01:20:37 nhall Exp $

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

#define SM_SOURCE
#define CHKPT_SERIAL_C

#ifdef __GNUG__
#   pragma implementation
#endif


#include <sm_int_0.h>
#include <chkpt_serial.h>


/*********************************************************************
 *
 *  Fuzzy checkpoints and prepares cannot be inter-mixed in the log.
 *  This mutex is for serializing them.
 *  Note that transactions acquire the mutex in read mode; the
 *  checkpoint acquires it in write mode. Since the chkpt thread is the
 *  ONLY one that acquires in write mode, it's safe to use an occ_rwlock.
 *
 *********************************************************************/
static occ_rwlock _chkpt_mutex;

void
chkpt_serial_m::trx_release()
{
  _chkpt_mutex.release_read();
}

void
chkpt_serial_m::trx_acquire()
{
  _chkpt_mutex.acquire_read();
}

void
chkpt_serial_m::chkpt_release()
{
  _chkpt_mutex.release_write();
}

void
chkpt_serial_m::chkpt_acquire()
{
  _chkpt_mutex.acquire_write();
}

