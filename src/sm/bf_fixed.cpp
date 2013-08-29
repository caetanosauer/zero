/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"
#define SM_SOURCE

#include "sm_int_1.h"

#include "bf_fixed.h"
#include "alloc_page.h"
#include "vol.h"

bf_fixed_m::bf_fixed_m()
    : _parent(NULL), _unix_fd(0), _page_cnt(0), _pages(NULL), _dirty_flags(NULL) {
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

w_rc_t bf_fixed_m::init(vol_t* parent, int unix_fd, uint32_t max_pid) {
    w_assert0 (_parent == NULL); // otherwise double init
    _parent = parent;
    _unix_fd = unix_fd;
    // volume has 1 volume header (pid=0), then a few allocation pages first,
    // then 1 stnode_page and then data pages.
    shpid_t alloc_pages = (max_pid / alloc_page_h::bits_held) + 1;
    _page_cnt = alloc_pages + 1; // +1 for stnode_page
    // use posix_memalign to allow unbuffered disk I/O
    void *buf = NULL;
    ::posix_memalign(&buf, SM_PAGESIZE, SM_PAGESIZE * _page_cnt);
    if (buf == NULL) {
        ERROUT (<< "failed to reserve " << _page_cnt << " blocks of " << SM_PAGESIZE << "-bytes pages. ");
        W_FATAL(smlevel_0::eOUTOFMEMORY);
    }
    _pages = reinterpret_cast<generic_page*>(buf);
    _dirty_flags = new bool[_page_cnt];
    ::memset (_dirty_flags, 0, sizeof(bool) * _page_cnt);

    // this is called on vol_t::mount(). no other thread is reading this volume concurrently!
    smthread_t* st = me();
    w_assert1(st);
    W_DO(st->lseek(unix_fd, sizeof(generic_page), sthread_t::SEEK_AT_SET)); // skip first page
    W_DO(st->read(unix_fd, _pages, sizeof(generic_page) * _page_cnt));

    return RCOK;
}


w_rc_t bf_fixed_m::flush()
{
    spinlock_write_critical_section cs(&_checkpoint_lock); // protect against modifications.
    // write at once as much as possible
    uint32_t cur = 0;
    while (true) {
        // skip non-dirty pages
        for (; cur < _page_cnt && !_dirty_flags[cur]; ++cur);
        
        if (cur == _page_cnt) break;
        w_assert1(_dirty_flags[cur]);
        uint32_t next = cur + 1;
        for (; next < _page_cnt && _dirty_flags[next]; ++next);

        shpid_t begin_pid = cur + 1; // +1 for volume header
        
        W_DO(_parent->write_many_pages(begin_pid, _pages + cur, next - cur));
        cur = next;
    }
    ::memset (_dirty_flags, 0, sizeof(bool) * _page_cnt);
    return RCOK;
}
