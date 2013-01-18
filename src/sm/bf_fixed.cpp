#include "w_defines.h"
#define SM_SOURCE

#include "sm_int_1.h"

#include "bf_fixed.h"
#include "alloc_p.h"
#include "vol.h"

bf_fixed_m::bf_fixed_m()
    : _parent(NULL), _unix_fd(0), _page_cnt(0), _pages(NULL), _dirty_flags(NULL) {
}
bf_fixed_m::~bf_fixed_m() {
    if (_pages != NULL) {
        delete[] _pages;
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
    // then 1 stnode_p and then data pages.
    shpid_t alloc_pages = (max_pid / alloc_p::alloc_max) + 1;
    _page_cnt = alloc_pages + 1; // +1 for stnode_p
    _pages = new page_s[_page_cnt];
    _dirty_flags = new bool[_page_cnt];
    ::memset (_dirty_flags, 0, sizeof(bool) * _page_cnt);

    // this is called on vol_t::mount(). no other thread is reading this volume concurrently!
    smthread_t* st = me();
    w_assert1(st);
    W_DO(st->lseek(unix_fd, sizeof(page_s), sthread_t::SEEK_AT_SET)); // skip first page
    W_DO(st->read(unix_fd, _pages, sizeof(page_s) * _page_cnt));

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
