/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */
#include "backup.h"
#include "generic_page.h"
#include "alloc_page.h"
#include "sm.h"
#include "vol.h"

#include <fcntl.h>
#include <unistd.h>
#include <sstream>
#include <memory.h>
#include <boost/concept_check.hpp>

std::string BackupManager::get_backup_path(volid_t vid) const {
    std::stringstream file_name;
    file_name << _backup_folder << "/backup_" << vid;
    return file_name.str();
}

bool BackupManager::volume_exists(volid_t vid) {
    BackupFile file(vid, get_backup_path(vid));
    file.open();
    return file.is_opened();
}

bool BackupManager::page_exists(volid_t vid, shpid_t shpid) {
    BackupFile file(vid, get_backup_path(vid));
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
    AlignedMemory aligned(sizeof(generic_page));
    W_DO(file.read_page(aligned, shpid));
    ::memcpy(&page, aligned.get_buffer(), sizeof(generic_page));
    return RCOK;
}

w_rc_t BackupManager::retrieve_page(generic_page &page, volid_t vid, shpid_t shpid) {
    BackupFile file(vid, get_backup_path(vid));
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
    _fd = ::open(_path.c_str(), O_RDONLY|O_DIRECT|O_NOATIME);
    if (is_opened()) {
        vid_t vid = vid_t(_vid);
        vol_t* vol = smlevel_0::io->get_volume(vid); // The backup and volume share the same volid.

        volhdr_t backup_hdr;
        rc_t rc = vol_t::read_vhdr(_path.c_str(), backup_hdr); 
        if (!rc.is_error())  {
           struct timespec backup_ctime; int backup_salt;
           backup_hdr.get_ctime(backup_ctime, backup_salt);
           struct timespec vol_ctime; int vol_salt;
           vol->get_vol_ctime(vol_ctime, vol_salt);  
           
           if ((backup_ctime.tv_sec != vol_ctime.tv_sec) ||
               (backup_ctime.tv_nsec != vol_ctime.tv_nsec) ||
               (backup_salt != vol_salt) ) {
               this->close();
               DBGOUT1(<<"Backup open: ctime mismatch");
           }          
        } else {
            this->close();
            DBGOUT1(<<"Backup open: failed to read volume header");
        }
    }
}
void BackupFile::close() {
    if (is_opened()) {
        int ret = ::close(_fd);
        w_assert1(ret == 0); // close called from destructor, no other error handling
        _fd = -1;
        
    }
}
w_rc_t BackupFile::read_page(AlignedMemory& buffer, shpid_t shpid) {
    w_assert1(buffer.get_size() >= sizeof(generic_page));
    char* memory = buffer.get_buffer();
    __off_t seeked = ::lseek(_fd, shpid * sizeof(generic_page), SEEK_SET);
    if (seeked != static_cast<__off_t>(shpid * sizeof(generic_page))) {
        W_RETURN_RC_MSG(eBACKUP_SHORTSEEK, << " _path=" << _path
            << ", shpid=" << shpid << ", seeked=" << seeked << ", errno=" << errno);
    }
    size_t read_bytes = ::read(_fd, memory, sizeof(generic_page));
    if (read_bytes != sizeof(generic_page)) {
        W_RETURN_RC_MSG(eBACKUP_SHORTIO, << " _path=" << _path
            << ", shpid=" << shpid << ", read_bytes=" << read_bytes << ", errno=" << errno);
    }
    return RCOK;
}
