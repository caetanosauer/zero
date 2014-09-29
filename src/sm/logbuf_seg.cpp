/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */
#include "w_defines.h"
#include "w_base.h"
#include "w_error.h"
#include "w_debug.h"

#include "logbuf_seg.h"

logbuf_seg::logbuf_seg(uint32_t size
#ifdef LOG_DIRECT_IO
, uint32_t align
#endif
) {
#ifdef LOG_DIRECT_IO
    posix_memalign((void**)&buf, align, size);
#else
    buf = new char[size];
#endif
    if (buf == NULL) {
        ERROUT("logbuf_seg: out of memory for new segment");
        W_FATAL(fcOUTOFMEMORY);
    }
    else {
        ::memset (buf, 0, size);
    }

    base_lsn = lsn_t::null;

}

logbuf_seg::~logbuf_seg() {
    if (buf != NULL) {
#ifdef LOG_DIRECT_IO
        free(buf);
#else
        delete[] buf;
#endif
        buf = NULL;
    }
}
