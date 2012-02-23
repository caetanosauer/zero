#include "w_defines.h"
#define SM_SOURCE

#ifdef __GNUG__
#       pragma implementation "alloc_p.h"
#endif

#include "sm_int_2.h"

#include "alloc_cache.h"
#include "smthread.h"

rc_t alloc_cache_t::load_by_scan (int unix_fd, shpid_t max_pid) {
    w_assert1(unix_fd >= 0);
    CRITICAL_SECTION(cs, _queue_lock.write_lock());
    shpid_t al_pid = 1; // the first alloc_p is always pid=1
    shpid_t al_pid_end = 1 + (max_pid / alloc_p::alloc_max) + 1;
    _non_contiguous_free_pages.clear();
    _contiguous_free_pages_begin = max_pid;
    _contiguous_free_pages_end = max_pid;
    
    page_s buf;
    smthread_t* st = me();
    w_assert1(st);
    // at this point, the volume is likely not initialized.
    // so, we need to directly use lseek(),read().
    // instead no other threads are accessing it. we are safe.
    W_DO(st->lseek(unix_fd, sizeof(page_s) * al_pid, sthread_t::SEEK_AT_SET)); // skip first page
    while (al_pid < al_pid_end) {
        // minor TODO should read multiple pages at once for efficient startup
        W_DO(st->read(unix_fd, &buf, sizeof(buf)));
        alloc_p al (&buf, smlevel_0::st_regular);
        
        shpid_t hwm = al.get_pid_highwatermark();
        shpid_t offset = al.get_pid_offset();
        for (shpid_t pid = offset; pid < hwm; ++pid) {
            if (!al.is_set_bit(pid)) {
                _non_contiguous_free_pages.push_back(pid);
            }
        }

        if (hwm < offset + alloc_p::alloc_max) {
            // from this pid, no pages are allocated yet
            _contiguous_free_pages_begin = hwm;
            // if this whole page is not entirely used (at least once),
            // all subsequent pages are not used either.
            break;
        }
        
        ++al_pid;
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
    CRITICAL_SECTION(cs, _queue_lock.write_lock());
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
    
    CRITICAL_SECTION(cs, _queue_lock.write_lock());
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
    CRITICAL_SECTION(cs, _queue_lock.write_lock());

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
    shpid_t alloc_pid = alloc_p::pid_to_alloc_pid(pid);
    alloc_p al;
    lpid_t alloc_lpid (_vid, 0, alloc_pid);
    W_DO(al.fix(alloc_lpid, LATCH_EX));
    if (logit) {
        W_DO(log_alloc_a_page (al, pid));
    }
    al.set_bit(pid);
    al.unfix_dirty();
    return RCOK;
}
rc_t alloc_cache_t::apply_allocate_consecutive_pages (shpid_t pid_begin, size_t page_count, bool logit)
{
    const shpid_t pid_to_end = pid_begin + page_count;
    alloc_p al;
    shpid_t alloc_pid = alloc_p::pid_to_alloc_pid(pid_begin);
    lpid_t alloc_lpid (_vid, 0, alloc_pid);
    W_DO(al.fix(alloc_lpid, LATCH_EX));
    
    shpid_t cur_pid = pid_begin;

    // log and apply per each alloc_p
    while (cur_pid < pid_to_end) {    
        // log it
        size_t this_page_count;
        if (pid_to_end > al.get_pid_offset() + alloc_p::alloc_max) {
            this_page_count = al.get_pid_offset() + alloc_p::alloc_max - cur_pid;
        } else {
            this_page_count = pid_to_end - cur_pid;
        }
        w_assert1(this_page_count > 0);

        if (logit) {
            W_DO(log_alloc_consecutive_pages (al, cur_pid, this_page_count));
        }
        al.set_consecutive_bits(cur_pid, cur_pid + this_page_count);

        cur_pid += this_page_count;
        // if more pages to be allocated, move on to next alloc_p
        if (cur_pid < pid_to_end) {
            // move on to next alloc_p
            ++alloc_lpid.page;
            al.unfix_dirty();
            W_DO(al.fix(alloc_lpid, LATCH_EX));
        }
    }
    return RCOK;
}
rc_t alloc_cache_t::apply_deallocate_one_page (shpid_t pid, bool logit)
{
    shpid_t alloc_pid = alloc_p::pid_to_alloc_pid(pid);
    alloc_p al;
    lpid_t alloc_lpid (_vid, 0, alloc_pid);
    W_DO(al.fix(alloc_lpid, LATCH_EX));
    // log it
    if (logit) {
        W_DO(log_dealloc_a_page (al, pid));
    }
    // then update the bitmap
    al.unset_bit(pid);
    al.unfix_dirty();
    return RCOK;
}

size_t alloc_cache_t::get_total_free_page_count () const {
    CRITICAL_SECTION(cs, _queue_lock.read_lock());
    return _non_contiguous_free_pages.size() + (_contiguous_free_pages_end - _contiguous_free_pages_begin);
}
size_t alloc_cache_t::get_consecutive_free_page_count () const {
    CRITICAL_SECTION(cs, _queue_lock.read_lock());
    return (_contiguous_free_pages_end - _contiguous_free_pages_begin);
}

bool alloc_cache_t::is_allocated_page (shpid_t pid) const {
    CRITICAL_SECTION(cs, _queue_lock.read_lock());
    if (pid >= _contiguous_free_pages_begin) {
        return false;
    }
    return check_not_contain(_non_contiguous_free_pages, pid);
}
