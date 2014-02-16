/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#include "backup.h"
#include "generic_page.h"
#include "vid_t.h"

#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sstream>
#include <stdlib.h>
#include <memory.h>

#include "alloc_page.h"

bool BackupManager::volume_exists(volid_t vid) {
    BackupFile file(vid);
    file.open();
    return file.is_opened();
}

bool BackupManager::page_exists(volid_t vid, shpid_t shpid) {
    BackupFile file(vid);
    file.open();
    if (!file.is_opened()) {
        return false;
    }
    generic_page buffer;
    shpid_t alloc_pid = alloc_page_h::pid_to_alloc_pid(shpid);
    W_COERCE(_retrieve_page(file, buffer, alloc_pid));
    alloc_page_h alloc_p(&buffer);
    return alloc_p.is_bit_set(shpid);
}

w_rc_t BackupManager::_retrieve_page(BackupFile &file, generic_page& page, shpid_t shpid) {
    w_assert1(file.is_opened());
    w_assert1(page_exists(file.get_vid(), shpid));
    AlignedMemory aligned(sizeof(generic_page));
    W_DO(file.read_page(aligned, shpid));
    ::memcpy(&page, aligned.get_buffer(), sizeof(generic_page));
    return RCOK;
}

w_rc_t BackupManager::retrieve_page(generic_page &page, volid_t vid, shpid_t shpid) {
    BackupFile file(vid);
    file.open();
    if (!file.is_opened()) {
        W_RETURN_RC_MSG(eNO_BACKUP_FILE, << " vid=" << vid << ", errno=" << errno);
    }
    W_DO(_retrieve_page(file, page, shpid));

    uint32_t checksum = page.calculate_checksum();
    if (checksum != page.checksum) {
        return RC (eBADCHECKSUM);
    }

    if ((page.pid.page != shpid) || (page.pid.vol().vol != vid)) {
        return RC (eBADCHECKSUM);
    }

    return RCOK;
}

AlignedMemory::AlignedMemory(size_t size) : _size(size), _buffer(NULL) {
    w_assert1(size % POSIX_MEM_ALIGNMENT == 0);
    int ret = ::posix_memalign(reinterpret_cast<void**>(&_buffer),
                                POSIX_MEM_ALIGNMENT, size);
    w_assert1(ret == 0);
}
void AlignedMemory::release() {
    if (_buffer != NULL) {
        ::free(_buffer);
        _buffer = NULL;
    }
}

void BackupFile::open() {
    w_assert1(_fd == -1); // not yet opened
    std::stringstream file_name;
    file_name << "backup_" << _vid;
    _fd = ::open(file_name.str().c_str(), O_RDONLY|O_DIRECT|O_NOATIME);
}
void BackupFile::close() {
    if (is_opened()) {
        int ret = ::close(_fd);
        w_assert1(ret == 0);
        _fd = -1;
    }
}
w_rc_t BackupFile::read_page(AlignedMemory& buffer, shpid_t shpid) {
    w_assert1(buffer.get_size() >= sizeof(generic_page));
    char* memory = buffer.get_buffer();
    __off_t seeked = ::lseek(_fd, shpid * sizeof(generic_page), SEEK_SET);
    if (seeked != static_cast<__off_t>(shpid * sizeof(generic_page))) {
        W_RETURN_RC_MSG(eBACKUP_SHORTSEEK, << " vid=" << _vid
            << ", shpid=" << shpid << ", seeked=" << seeked << ", errno=" << errno);
    }
    size_t read_bytes = ::read(_fd, memory, sizeof(generic_page));
    if (read_bytes != sizeof(generic_page)) {
        W_RETURN_RC_MSG(eBACKUP_SHORTIO, << " vid=" << _vid
            << ", shpid=" << shpid << ", read_bytes=" << read_bytes << ", errno=" << errno);
    }
    return RCOK;
}
