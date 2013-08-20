/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"
#define SM_SOURCE

#ifdef __GNUG__
#       pragma implementation "alloc_p.h"
#endif

#include "sm_int_2.h"

#include "alloc_cache.h"
#include "smthread.h"
#include "bf_fixed.h"

rc_t alloc_cache_t::load_by_scan (shpid_t max_pid) {
    w_assert1(_fixed_pages != NULL);
    spinlock_write_critical_section cs(&_queue_lock);
    _non_contiguous_free_pages.clear();
    _contiguous_free_pages_begin = max_pid;
    _contiguous_free_pages_end = max_pid;
    
    // at this point, no other threads are accessing the volume. we don't need any synchronization.
    uint32_t alloc_pages_cnt = _fixed_pages->get_page_cnt() - 1; // -1 for stnode_p
    page_s *pages = _fixed_pages->get_pages();
    for (uint32_t i = 0; i < alloc_pages_cnt; ++i) {
        alloc_p al (pages + i);
        w_assert1(al.generic_page()->pid.vol() == _vid);

        shpid_t hwm = al.get_pid_highwatermark();
        shpid_t offset = al.get_pid_offset();
        for (shpid_t pid = offset; pid < hwm; ++pid) {
            if (!al.is_set_bit(pid)) {
                _non_contiguous_free_pages.push_back(pid);
            }
        }

        if (hwm < offset + alloc_p::bits_held) {
            // from this pid, no pages are allocated yet
            _contiguous_free_pages_begin = hwm;
            // if this whole page is not entirely used (at least once),
            // all subsequent pages are not used either.
            break;
        }
    }
#if W_DEBUG_LEVEL > 1
    cout << "init alloc_cache: _contiguous_free_pages_begin=" << _contiguous_free_pages_begin
        << ", _contiguous_free_pages_end=" << _contiguous_free_pages_end
        << ", _non_contiguous_free_pages.size()=" << _non_contiguous_free_pages.size()
    << endl;
#endif // W_DEBUG_LEVEL > 1
    return RCOK;
}

rc_t alloc_cache_t::allocate_one_page (shpid_t &pid) {
    pid = 0;
    spinlock_write_critical_section cs(&_queue_lock);
    shpid_t pid_to_return = 0;
    if (_non_contiguous_free_pages.size() > 0) {
        pid_to_return = _non_contiguous_free_pages.back();
        _non_contiguous_free_pages.pop_back();
    } else if (_contiguous_free_pages_begin < _contiguous_free_pages_end) {
        pid_to_return = _contiguous_free_pages_begin;
        ++_contiguous_free_pages_begin;    
    } else {
        return RC(smlevel_0::eOUTOFSPACE);
    }

    W_DO(apply_allocate_one_page(pid_to_return));
    pid = pid_to_return;
    return RCOK;
}

rc_t alloc_cache_t::allocate_consecutive_pages (shpid_t &pid_begin, size_t page_count) {
    pid_begin = 0;
    
    spinlock_write_critical_section cs(&_queue_lock);
    shpid_t pid_to_begin = 0;
    if (_contiguous_free_pages_begin  + page_count < _contiguous_free_pages_end) {
        pid_to_begin = _contiguous_free_pages_begin;
        _contiguous_free_pages_begin += page_count;
    } else {
        return RC(smlevel_0::eOUTOFSPACE);
    }
    W_DO(apply_allocate_consecutive_pages(pid_to_begin, page_count));
    pid_begin = pid_to_begin;
    return RCOK;
}

inline bool check_not_contain (const std::vector<shpid_t> &list, shpid_t pid) {
    const size_t cnt = list.size();
    for (size_t i = 0; i < cnt; ++i) {
        if (list[i] == pid) return false;
    }
    return true;
}


rc_t alloc_cache_t::deallocate_one_page (shpid_t pid) {
    spinlock_write_critical_section cs(&_queue_lock);

    w_assert1(pid < _contiguous_free_pages_begin);
    w_assert1(check_not_contain(_non_contiguous_free_pages, pid));
    _non_contiguous_free_pages.push_back(pid);
    
    W_DO(apply_deallocate_one_page(pid));
    return RCOK;
}

rc_t alloc_cache_t::redo_allocate_one_page (shpid_t pid)
{
    // REDO is always single-threaded. so no critical section
    if (_contiguous_free_pages_begin == pid) {
        ++_contiguous_free_pages_begin;
    } else if (_contiguous_free_pages_begin > pid) {
        for (int i = _contiguous_free_pages_begin; i < (int) pid; ++i) {
            _non_contiguous_free_pages.push_back(i);
        }
        _contiguous_free_pages_begin = pid + 1;
    } else {
        bool found = false;
        for (int i = _non_contiguous_free_pages.size() - 1; i >= 0; --i) {
            if (_non_contiguous_free_pages[i] == pid) {
                found = true;
                _non_contiguous_free_pages.erase(_non_contiguous_free_pages.begin() + i);
                break;
            }
        }
        if (!found) {
            //weird, but the REDO is not needed
            cerr << "REDO: page " << pid << " is already allocated??" << endl;
            return RCOK;
        }
    }
    W_DO(apply_allocate_one_page(pid, false)); // REDO doesn't generate log
    return RCOK;
}
rc_t alloc_cache_t::redo_allocate_consecutive_pages (shpid_t pid_begin, size_t page_count)
{
    if (_contiguous_free_pages_begin == pid_begin) {
        _contiguous_free_pages_begin += page_count;
    } else if (_contiguous_free_pages_begin > pid_begin) {
        for (int i = _contiguous_free_pages_begin; i < (int) pid_begin; ++i) {
            _non_contiguous_free_pages.push_back(i);
        }
        _contiguous_free_pages_begin = pid_begin + page_count;
    } else {
        // then the REDO order is wrong.
        w_assert0(false);
    }
    
    W_DO(apply_allocate_consecutive_pages(pid_begin, page_count, false));
    return RCOK;
}
rc_t alloc_cache_t::redo_deallocate_one_page (shpid_t pid)
{
    w_assert1(pid < _contiguous_free_pages_begin);
    w_assert1(check_not_contain(_non_contiguous_free_pages, pid));
    _non_contiguous_free_pages.push_back(pid);    
    W_DO(apply_deallocate_one_page(pid, false));
    return RCOK;
}

rc_t alloc_cache_t::apply_allocate_one_page (shpid_t pid, bool logit)
{
    spinlock_read_critical_section cs(&_fixed_pages->get_checkpoint_lock()); // protect against checkpoint. see bf_fixed_m comment.
    shpid_t alloc_pid = alloc_p::pid_to_alloc_pid(pid);
    uint32_t buf_index = alloc_pid - 1; // -1 for volume header
    w_assert1(buf_index < _fixed_pages->get_page_cnt() - 1); // -1 for stnode_p
    page_s* pages = _fixed_pages->get_pages();
    alloc_p al (pages + buf_index);
    if (logit) {
        if (smlevel_0::log != NULL) {
            W_DO(log_alloc_a_page (_vid, pid));
        } else {
            ERROUT(<< "WARNING! apply_allocate_one_page: logging is disabled. skipped logging");
        }
    }
    al.set_bit(pid);
    _fixed_pages->get_dirty_flags()[buf_index] = true;
    return RCOK;
}
rc_t alloc_cache_t::apply_allocate_consecutive_pages (shpid_t pid_begin, size_t page_count, bool logit)
{
    spinlock_read_critical_section cs(&_fixed_pages->get_checkpoint_lock()); // protect against checkpoint. see bf_fixed_m comment.
    const shpid_t pid_to_end = pid_begin + page_count;
    shpid_t alloc_pid = alloc_p::pid_to_alloc_pid(pid_begin);
    page_s* pages = _fixed_pages->get_pages();
    
    shpid_t cur_pid = pid_begin;

    // log and apply per each alloc_p
    while (cur_pid < pid_to_end) {    
        uint32_t buf_index = alloc_pid - 1; // -1 for volume header
        w_assert1(buf_index < _fixed_pages->get_page_cnt() - 1); // -1 for stnode_p
        alloc_p al (pages + buf_index);

        // log it
        size_t this_page_count;
        if (pid_to_end > al.get_pid_offset() + alloc_p::bits_held) {
            this_page_count = al.get_pid_offset() + alloc_p::bits_held - cur_pid;
        } else {
            this_page_count = pid_to_end - cur_pid;
        }
        w_assert1(this_page_count > 0);

        if (logit) {
            if (smlevel_0::log != NULL) {
                W_DO(log_alloc_consecutive_pages (_vid, cur_pid, this_page_count));
            } else {
                ERROUT(<< "WARNING! apply_allocate_consecutive_pages: logging is disabled. skipped logging");
            }
        }
        al.set_consecutive_bits(cur_pid, cur_pid + this_page_count);
        _fixed_pages->get_dirty_flags()[buf_index] = true;

        cur_pid += this_page_count;
        // if more pages to be allocated, move on to next alloc_p
        if (cur_pid < pid_to_end) {
            // move on to next alloc_p
            ++alloc_pid;
        }
    }
    return RCOK;
}
rc_t alloc_cache_t::apply_deallocate_one_page (shpid_t pid, bool logit)
{
    spinlock_read_critical_section cs(&_fixed_pages->get_checkpoint_lock()); // protect against checkpoint. see bf_fixed_m comment.
    shpid_t alloc_pid = alloc_p::pid_to_alloc_pid(pid);
    uint32_t buf_index = alloc_pid - 1; // -1 for volume header
    w_assert1(buf_index < _fixed_pages->get_page_cnt() - 1); // -1 for stnode_p
    page_s* pages = _fixed_pages->get_pages();
    alloc_p al (pages + buf_index);
    // log it
    if (logit) {
        if (smlevel_0::log != NULL) {
            W_DO(log_dealloc_a_page (_vid, pid));
        } else {
            ERROUT(<< "WARNING! apply_deallocate_one_page: logging is disabled. skipped logging");
        }
    }
    // then update the bitmap
    al.unset_bit(pid);
    _fixed_pages->get_dirty_flags()[buf_index] = true;
    return RCOK;
}

size_t alloc_cache_t::get_total_free_page_count () const {
    spinlock_read_critical_section cs(&_queue_lock);
    return _non_contiguous_free_pages.size() + (_contiguous_free_pages_end - _contiguous_free_pages_begin);
}
size_t alloc_cache_t::get_consecutive_free_page_count () const {
    spinlock_read_critical_section cs(&_queue_lock);
    return (_contiguous_free_pages_end - _contiguous_free_pages_begin);
}

bool alloc_cache_t::is_allocated_page (shpid_t pid) const {
    spinlock_read_critical_section cs(&_queue_lock);
    if (pid >= _contiguous_free_pages_begin) {
        return false;
    }
    return check_not_contain(_non_contiguous_free_pages, pid);
}
