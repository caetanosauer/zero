/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#ifndef BACKUP_H
#define BACKUP_H

#include "w_defines.h"

/**
 * \defgroup SSMBCK Backup Module
 * \brief \b Backup \b Manager manipulates backup files that can be used for recovery from
 * failures.
 * \ingroup SSMAPI
 * \details
 * TODO Put detailed description of Backup module here.
 * \section SUMMARY Overview
 * bluh
 * \section FUNC Functionalities
 * bluh
 * \section LIMIT Limitations
 * bluh
 * \section FUTURE Planned Extension
 * bluh
 */

#include "vid_t.h"
#include "basics.h"

class generic_page;

/**
 * \brief The API class of Backup Manager.
 * \ingroup SSMBCK
 * \details
 * TODO Put detailed description of backup_m here.
 */
class backup_m {

public:
    NORET                        backup_m();
    NORET                        ~backup_m();
	
	bool 						 backup_m_valid();

	w_rc_t						 retrieve_page(generic_page &page, volid_t vid, shpid_t shpid);
    
    /**
     * \brief Tells whether the given page exists in this backup.
     * @param[in] vol volume ID
     * @param[in] shpid page ID
     * @return true if the given page exists in this backup
     */
    bool                          page_exists(volid_t vol, shpid_t shpid); 

private:
    /**
     * \brief This one simply retrieves the page as a byte array, no content checking,
     * so it can be used for any type of data region.
     */
    w_rc_t                       _retrieve_page(generic_page &page, volid_t vid, shpid_t shpid);
    
};

#endif // BACKUP_H

