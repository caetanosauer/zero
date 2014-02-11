/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#ifndef BACKUP_H
#define BACKUP_H

#include "w_defines.h"


/*
 *  Backup manager interface.  
 */

#include "vid_t.h"
#include "basics.h"

class generic_page;

class backup_m {

public:
    NORET                        backup_m();
    NORET                        ~backup_m();
	
	bool 						 backup_m_valid();

	w_rc_t						 retrieve_page(generic_page *page, volid_t vid, shpid_t shpid);

};

#endif // BACKUP_H

