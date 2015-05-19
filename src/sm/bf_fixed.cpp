/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"
#define SM_SOURCE

#include "sm_base.h"

#include "bf_fixed.h"
#include "alloc_page.h"
#include "vol.h"

bf_fixed_m::bf_fixed_m(vol_t* parent, int unix_fd, shpid_t max_pid)
    : _parent(parent), _unix_fd(unix_fd),
    _page_cnt(0), _pages(NULL), _dirty_flags(NULL)
{
    // volume has 1 volume header (pid=0), then a few allocation pages first,
    // then 1 stnode_page and then data pages.
    shpid_t alloc_pages = alloc_page::num_alloc_pages(max_pid);
    _page_cnt = alloc_pages + 1; // +1 for stnode_page
    // use posix_memalign to allow unbuffered disk I/O
    void *buf = NULL;
    w_assert0(::posix_memalign(&buf, SM_PAGESIZE, SM_PAGESIZE * _page_cnt)==0);
    if (buf == NULL) {
        ERROUT (<< "failed to reserve " << _page_cnt << " blocks of " << SM_PAGESIZE << "-bytes pages. ");
        W_FATAL(eOUTOFMEMORY);
    }
    _pages = reinterpret_cast<generic_page*>(buf);
    _dirty_flags = new bool[_page_cnt];
    ::memset (_dirty_flags, 0, sizeof(bool) * _page_cnt);
}


bf_fixed_m::~bf_fixed_m() {
    if (_pages != NULL) {
        void *buf = reinterpret_cast<void*>(_pages);
        // note we use free(), not delete[], which corresponds to posix_memalign
        ::free (buf);
        _pages = NULL;
    }

    if (_dirty_flags != NULL) {
        delete[] _dirty_flags;
        _dirty_flags = NULL;
    }
}

w_rc_t bf_fixed_m::init()
{
    // this is called on vol_t::mount(). no other thread is reading this volume concurrently!
    smthread_t* st = me();
    w_assert1(st);
    W_DO(st->lseek(_unix_fd, sizeof(generic_page), sthread_t::SEEK_AT_SET)); // skip first page
    W_DO(st->read(_unix_fd, _pages, sizeof(generic_page) * _page_cnt));

    for (uint32_t i=0; i<_page_cnt; i++) {
        if (_pages[i].checksum !=_pages[i].calculate_checksum()) {
            return RC(eBADCHECKSUM);
        }
    }

    return RCOK;
}


w_rc_t bf_fixed_m::flush(bool toBackup) {
    spinlock_write_critical_section cs(&_checkpoint_lock); // protect against modifications.

    if (toBackup) {
        // if writing to backup, just dump buffer into file and exit
        W_DO(_parent->write_backup(shpid_t(1), _page_cnt, _pages));
        return RCOK;
    }

    // write at once as much as possible
    uint32_t cur = 0;
    while (true) {
        // skip non-dirty pages
        for (; cur < _page_cnt && !_dirty_flags[cur]; ++cur);

        if (cur == _page_cnt) break;

        w_assert1(_dirty_flags[cur]);

        uint32_t next = cur;
        for (; next < _page_cnt && _dirty_flags[next]; ++next) {
            _pages[next].checksum = _pages[next].calculate_checksum();
        }
        DBG(<< "bf_fixed_m::flush " << _pages[next]);

        shpid_t begin_pid = cur + 1; // +1 for volume header

        W_DO(_parent->write_many_pages(begin_pid, _pages + cur, next - cur));
        cur = next;
    }
    ::memset (_dirty_flags, 0, sizeof(bool) * _page_cnt);
    return RCOK;
}
