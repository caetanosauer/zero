/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"
#define SM_SOURCE

#include "sm_int_1.h"

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
    uint32_t alloc_pages_cnt = _fixed_pages->get_page_cnt() - 1; // -1 for stnode_page
    generic_page *pages = _fixed_pages->get_pages();
    for (uint32_t i = 0; i < alloc_pages_cnt; ++i) {
        alloc_page_h al (pages + i);
        w_assert1(al.vol() == _vid);

        shpid_t hwm = al.get_pid_highwatermark();
        shpid_t offset = al.get_pid_offset();
        for (shpid_t pid = offset; pid < hwm; ++pid) {
            if (!al.is_bit_set(pid)) {
                _non_contiguous_free_pages.push_back(pid);
            }
        }

        if (hwm < offset + alloc_page_h::bits_held) {
            // from this pid, no pages are allocated yet
            _contiguous_free_pages_begin = hwm;
            // if this whole page is not entirely used (at least once),
            // all subsequent pages are not used either.
            break;
        }
    }
    DBGOUT1(<< "init alloc_cache: _contiguous_free_pages_begin=" << _contiguous_free_pages_begin
        << ", _contiguous_free_pages_end=" << _contiguous_free_pages_end
        << ", _non_contiguous_free_pages.size()=" << _non_contiguous_free_pages.size());
    return RCOK;
}

rc_t alloc_cache_t::allocate_one_page (shpid_t &pid) {
    pid = 0;
    spinlock_write_critical_section cs(&_queue_lock);
    shpid_t pid_to_return = 0;

    if (_non_contiguous_free_pages.size() > 0) {
        // if there is a free page in the non-contiguous region, return it
        pid_to_return = _non_contiguous_free_pages.back();
        _non_contiguous_free_pages.pop_back();
    } else if (_contiguous_free_pages_begin < _contiguous_free_pages_end) {
        pid_to_return = _contiguous_free_pages_begin;
        ++_contiguous_free_pages_begin;
    } else {
        return RC(eOUTOFSPACE);
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
        return RC(eOUTOFSPACE);
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
        // pid exactly at the contiguous range begin -- simply increment it
        ++_contiguous_free_pages_begin;
    } else if (_contiguous_free_pages_begin > pid) {
        // all slots between _contiguous_begin and pid are now part of the
        // non-contiguous area
        for (int i = _contiguous_free_pages_begin; i < (int) pid; ++i) {
            _non_contiguous_free_pages.push_back(i);
        }
        _contiguous_free_pages_begin = pid + 1;
    } else {
        // pid is on the non-contiguous area -- remove it from the list of free
        // slots
        bool found = false;
        for (int i = _non_contiguous_free_pages.size() - 1; i >= 0; --i) {
            if (_non_contiguous_free_pages[i] == pid) {
                found = true;
                _non_contiguous_free_pages.erase(_non_contiguous_free_pages.begin() + i);
                break;
            }
        }
        if (!found) {
            // CS (TODO) How come this isn't an error? Redoing a page allocation
            // MUST find the slot free, because REDO of a specific PID (thus a specific
            // slot) is always performed sequentially. Thus, even if a page
            // is allocated and freed multiple times, this kind of inconsistency cannot
            // happen.
            //
            // CS: Apparently it does happen and it's not an error. I guess it depends
            // on when and how the alloc pages get flushed -- study this issue!
            //
            // W_FATAL_MSG(eINTERNAL,
            //         << "Allocation REDO found the slot already allocated");

            // Page might be allocated already due to Single Page Recovery
            // used during Restart operation, the REDO is not needed
            // generate a debug output instead of error log
            DBGOUT1(<<"REDO: page  " << pid << " is already allocated??");
            // cerr << "REDO: page " << pid << " is already allocated??" << endl;
            return RCOK;
        }
    }
    W_DO(apply_allocate_one_page(pid)); // REDO doesn't generate log
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
        W_FATAL_MSG(eINTERNAL, << "REDO of contiguous allocation "
                    << " found the slots already allocated");
    }

    W_DO(apply_allocate_consecutive_pages(pid_begin, page_count));
    return RCOK;
}
rc_t alloc_cache_t::redo_deallocate_one_page (shpid_t pid)
{
    w_assert1(pid < _contiguous_free_pages_begin);
    // CS: Ideally, this could be checked always, with a faster data structure
    w_assert1(check_not_contain(_non_contiguous_free_pages, pid));
    _non_contiguous_free_pages.push_back(pid);
    W_DO(apply_deallocate_one_page(pid));
    return RCOK;
}

rc_t alloc_cache_t::apply_allocate_one_page (shpid_t pid)
{
    spinlock_read_critical_section cs(&_fixed_pages->get_checkpoint_lock()); // protect against checkpoint. see bf_fixed_m comment.
    shpid_t alloc_pid = alloc_page_h::pid_to_alloc_pid(pid);
    uint32_t buf_index = alloc_pid - 1; // -1 for volume header
    w_assert1(buf_index < _fixed_pages->get_page_cnt() - 1); // -1 for stnode_page
    generic_page* pages = _fixed_pages->get_pages();
    alloc_page_h al (pages + buf_index);
    al.set_bit(pid);
    _fixed_pages->get_dirty_flags()[buf_index] = true;
    return RCOK;
}
rc_t alloc_cache_t::apply_allocate_consecutive_pages (shpid_t pid_begin, size_t page_count)
{
    // CS (TODO)
    // 1) Why do we have to be in mutual exclusion with checkpoints??
    // 2) I thought this was done with chkpt_serial_m?? Do we have multiple
    // latches to protect the same critical section?
    spinlock_read_critical_section cs(&_fixed_pages->get_checkpoint_lock()); // protect against checkpoint. see bf_fixed_m comment.
    const shpid_t pid_to_end = pid_begin + page_count;
    shpid_t alloc_pid = alloc_page_h::pid_to_alloc_pid(pid_begin);
    generic_page* pages = _fixed_pages->get_pages();

    shpid_t cur_pid = pid_begin;

    // log and apply per each alloc_page
    while (cur_pid < pid_to_end) {
        uint32_t buf_index = alloc_pid - 1; // -1 for volume header
        w_assert1(buf_index < _fixed_pages->get_page_cnt() - 1); // -1 for stnode_page
        alloc_page_h al (pages + buf_index);

        size_t this_page_count;
        if (pid_to_end > al.get_pid_offset() + alloc_page_h::bits_held) {
            this_page_count = al.get_pid_offset() + alloc_page_h::bits_held - cur_pid;
        } else {
            this_page_count = pid_to_end - cur_pid;
        }
        w_assert1(this_page_count > 0);
        al.set_consecutive_bits(cur_pid, cur_pid + this_page_count);
        _fixed_pages->get_dirty_flags()[buf_index] = true;

        cur_pid += this_page_count;
        // if more pages to be allocated, move on to next alloc_page
        if (cur_pid < pid_to_end) {
            // move on to next alloc_page
            ++alloc_pid;
        }
    }
    return RCOK;
}
rc_t alloc_cache_t::apply_deallocate_one_page (shpid_t pid)
{
    spinlock_read_critical_section cs(&_fixed_pages->get_checkpoint_lock()); // protect against checkpoint. see bf_fixed_m comment.
    shpid_t alloc_pid = alloc_page_h::pid_to_alloc_pid(pid);
    uint32_t buf_index = alloc_pid - 1; // -1 for volume header
    w_assert1(buf_index < _fixed_pages->get_page_cnt() - 1); // -1 for stnode_page
    generic_page* pages = _fixed_pages->get_pages();
    alloc_page_h al (pages + buf_index);
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
