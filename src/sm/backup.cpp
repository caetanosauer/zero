/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#include "backup.h"
#include "generic_page.h"
#include "vid_t.h"

#include <boost/static_assert.hpp>
#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sstream>
#include <stdlib.h>
#include <memory.h>

#include "alloc_page.h"

#define BACKUP_MEM_ALIGNMENT 4096


char* allocate_aligned_memory(size_t size_to_allocate) {
    w_assert1(size_to_allocate % BACKUP_MEM_ALIGNMENT == 0);
    char* buffer = NULL;
    int ret = ::posix_memalign(reinterpret_cast<void**>(&buffer),
                                BACKUP_MEM_ALIGNMENT, size_to_allocate);
    w_assert1(ret == 0);
    return buffer;
}

backup_m::backup_m() {

}

backup_m::~backup_m() {

}

bool backup_m::backup_m_valid(){
	return true;
}

bool backup_m::page_exists(volid_t vol, shpid_t shpid) {
    generic_page buffer;
    shpid_t alloc_pid = alloc_page_h::pid_to_alloc_pid(shpid);
    W_COERCE(_retrieve_page(buffer, vol, alloc_pid));
    alloc_page_h alloc_p(&buffer);
    return alloc_p.is_bit_set(shpid);
}


w_rc_t backup_m::_retrieve_page(generic_page& page, volid_t vid, shpid_t shpid) {
    std::stringstream file_name;
    file_name << "backup_" << vid;
    int fd = ::open(file_name.str().c_str(), O_RDONLY|O_DIRECT|O_NOATIME);
    if (fd == -1) {
        return RC (eBADCHECKSUM);
    }

    /* NB: If sizeof(generic_page) (== 8K) changes to a non-multiple of 512/1K/4K,
     * the read length will need to be explicitly realigned.
     */
    char *aligned = allocate_aligned_memory(sizeof(generic_page));
    ::lseek(fd, shpid*sizeof(generic_page), SEEK_SET);
    size_t read_bytes = ::read(fd, aligned, sizeof(generic_page));
    w_assert1(read_bytes == sizeof(generic_page));
    ::memcpy(&page, aligned, sizeof(generic_page));
    ::free(aligned);
    return RCOK;
}


w_rc_t backup_m::retrieve_page(generic_page &page, volid_t vid, shpid_t shpid) {
    W_DO(_retrieve_page(page, vid, shpid));
    uint32_t checksum = page.calculate_checksum();
    if (checksum != page.checksum) {
        return RC (eBADCHECKSUM);
    }

    if ((page.pid.page != shpid) || (page.pid.vol().vol != vid)) {
        return RC (eBADCHECKSUM);
    }

    return RCOK;
}
