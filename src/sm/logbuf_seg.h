/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */
#ifndef LOGBUF_SEG_H
#define LOGBUF_SEG_H

// LOG_BUFFER switch
#include "logbuf_common.h"


#include "w_defines.h"
#include "w_list.h"
#include "tatas.h"
#include "mcs_lock.h"

#include "lsn.h"


class logbuf_core; 

// segment descriptor
class logbuf_seg {
    friend class logbuf_core;  // logbuf_core needs access to _link

public:
    logbuf_seg(uint32_t size = LOGBUF_SEG_SIZE
#ifdef LOG_DIRECT_IO
, uint32_t align = LOG_DIO_ALIGN
#endif
);
    ~logbuf_seg();


    char *buf;  // memory space for the segment
    lsn_t base_lsn;  // starting lsn of this segment, as key in the hashtable

private:

    w_link_t _link;  // node for the doubly linked list


};


#endif // LOGBUF_SEG_H
