/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

/*<std-header orig-src='shore' incl-file-exclusion='DEVICE_H'>

 $Id: device.h,v 1.20 2010/06/08 22:28:55 nhall Exp $

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

#ifndef DEVICE_H
#define DEVICE_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/


// must be outside of device_m due to HP CC limitation
struct device_s {
    char       name[smlevel_0::max_devname+1];
    shpid_t    quota_pages;
    devid_t    id;        // unique device id since name may
                // not be unique
    w_link_t    link;
};

struct device_hdr_s {
    device_hdr_s(uint32_t version, 
        sm_diskaddr_t quota_in_KB, 
        lvid_t v) :
    format_version(version), quota_KB(quota_in_KB), lvid(v) {}
    uint32_t        format_version;
    sm_diskaddr_t    quota_KB;
    lvid_t        lvid;
};

class device_m : public smlevel_0 {
public:
    enum { max = smlevel_0::max_vols };
    device_m();
    ~device_m();

    rc_t mount(const char* dev_name, const device_hdr_s& dev_hdr, u_int& vol_cnt);
    bool is_mounted(const char* dev_name);
    rc_t quota(const char* dev_name, smksize_t& quota_KB);
    rc_t dismount(const char* dev_name);
    rc_t dismount_all();
    rc_t list_devices(const char**& dev_list, devid_t*& devs, u_int& dev_cnt);
    void dump() const;
    
private:
    device_s*    _find(const char* dev_name);
    device_s*    _find(const devid_t& devid);

    // table of all devices currently mounted
    static queue_based_lock_t             _table_lock;
    w_list_t<device_s,queue_based_lock_t> _tab;

    // disabled
    device_m(const device_m&);
    device_m& operator=(const device_m&);
};

/*<std-footer incl-file-exclusion='DEVICE_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
