/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

/*<std-header orig-src='shore' incl-file-exclusion='LID_H'>

 $Id: lid.h,v 1.66 2010/05/26 01:20:39 nhall Exp $

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

#ifndef LID_H
#define LID_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/*********************************************************************

   IMPLEMENTATION LIMITATIONS
   --------------------------
   Logical volume IDs are made unique by making them a combination of
   the network address of the machine they are formatted on, and the
   physical ID given to the volume when it is formatted.

**********************************************************************/

/*
 * Logical ID Manager class.
 */
class lid_m : public smlevel_4 
{
public:

    NORET            lid_m() {};
    NORET            ~lid_m() {};

    rc_t            generate_new_volid(lvid_t& lvid);

private:

    // disabled
    NORET              lid_m(const lid_m&);
    lid_m&             operator=(const lid_m&);
};


/*<std-footer incl-file-exclusion='LID_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
