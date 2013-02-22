/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-kits -- Benchmark implementations for Shore-MT
   
                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne
   
                         All Rights Reserved.
   
   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.
   
   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

#include "qpipe/core/tuple_fifo.h"
#include "qpipe/core/tuple_fifo_directory.h"
#include "util/trace.h"
#include "util/acounter.h"
#include <algorithm>


ENTER_NAMESPACE(qpipe);



/* debugging */
static int TRACE_MASK_WAITS = TRACE_COMPONENT_MASK_NONE;
static int TRACE_MASK_DISK  = TRACE_COMPONENT_MASK_NONE;
static const bool FLUSH_TO_DISK_ON_FULL = false;



/* Global tuple_fifo statistics */

/* statistics data structures */
static pthread_mutex_t tuple_fifo_stats_mutex = thread_mutex_create();
static int open_fifo_count = 0;
static int total_fifos_created = 0;
static int total_fifos_experienced_read_wait = 0;
static int total_fifos_experienced_write_wait = 0;
static int total_fifos_experienced_wait = 0;



/* statistics methods */

/**
 * @brief Return the number of currently open tuple_fifos. Not
 * synchronized.
 */
int tuple_fifo::open_fifos() {
    return open_fifo_count;
}



/**
 * @brief Reset global statistics to initial values. This method _is_
 * synchronized.
 */
void tuple_fifo::clear_stats() {
    critical_section_t cs(tuple_fifo_stats_mutex);
    open_fifo_count = 0;
    total_fifos_created = 0;
    total_fifos_experienced_read_wait = 0;
    total_fifos_experienced_write_wait = 0;
}



/**
 * @brief Dump stats using TRACE.
 */
void tuple_fifo::trace_stats() {
    TRACE(TRACE_ALWAYS,
          "--- Since the last clear_stats\n");
    TRACE(TRACE_ALWAYS,
          "%d tuple_fifos created\n", total_fifos_created);
    TRACE(TRACE_ALWAYS,
          "%d currently open\n", open_fifo_count);
    TRACE(TRACE_ALWAYS,
          "%lf experienced read waits\n",
          (double)total_fifos_experienced_read_wait/total_fifos_created);
    TRACE(TRACE_ALWAYS,
          "%lf experienced write waits\n",
          (double)total_fifos_experienced_write_wait/total_fifos_created);
}



/* Sentinal page pool. */

/**
 * @brief Every page within a tuple_fifo needs to be part of a
 * page_pool that manages its deletion. The sentinal_page_pool
 * allocates a single global "sentinal page" that is never freed. This
 * page is inserted into every tuple_fifo to reduce the number of
 * corner cases it needs to deal with.
 *
 * The sentinal page does not go away when it is "freed".
 */
struct sentinel_page_pool : page_pool {
    
    /* We will use this single buffer for our global page instance */
    char _data[sizeof(qpipe::page)];

    sentinel_page_pool()
        : page_pool(sizeof(qpipe::page))
    {
    }

    virtual void* alloc() {
        return _data;
    }

    virtual void free(void*) {
        /* do nothing */
    }
};



/* sentinal page data structures */

static sentinel_page_pool SENTINEL_POOL;
static qpipe::page* SENTINEL_PAGE = qpipe::page::alloc(1, &SENTINEL_POOL);



/* definitions of exported methods */

/*
 * Creates a new (temporary) file in the BDB bufferpool for the FIFO
 * to use as a backing store. The file will be deleted automatically
 * when the FIFO is destroyed.
 */
void tuple_fifo::init() {

    /* Check on the tuple_fifo directory. */
    tuple_fifo_directory_t::open_once();

    _reader_tid = pthread_self();

    /* Prepare for reading. */
    _set_read_page(SENTINEL_PAGE);

    /* Prepare for writing. */
    _write_page = SENTINEL_PAGE;
    
    /* update state */
    _state.transition(tuple_fifo_state_t::IN_MEMORY);

    /* Update statistics. */
    critical_section_t cs(tuple_fifo_stats_mutex);
    open_fifo_count++;
    total_fifos_created++;
    cs.exit();
}



/**
 * @brief Should be invoked by the writer before writing tuples.
 */
void tuple_fifo::writer_init() {
    /* set up the _writer_tid field to make debugging easier */
    _writer_tid = pthread_self();
}



/**
 * @brief Function object. Wrapper around p->free() that can be passed
 * to std::for_each.
 */
struct free_page {
    void operator()(qpipe::page* p) {
	p->free();
    }
};



/**
 * @brief Deallocate the pags in this tuple_fifo and add our local
 * statistics to global ones.
 */
void tuple_fifo::destroy() {

    std::for_each(_pages.begin(), _pages.end(), free_page());
    std::for_each(_free_pages.begin(), _free_pages.end(), free_page());
	
    /* update stats */
    critical_section_t cs(tuple_fifo_stats_mutex);
    open_fifo_count--;
    bool write_wait = _num_waits_on_insert > 0;
    bool read_wait  = _num_waits_on_remove > 0;
    if (read_wait)
        total_fifos_experienced_read_wait++;
    if (write_wait)
        total_fifos_experienced_write_wait++;
    if (read_wait || write_wait)
        total_fifos_experienced_wait++;

    TRACE(TRACE_MASK_WAITS & TRACE_ALWAYS,
          "Blocked on insert %.2f\n",
          (double)_num_waits_on_insert/_num_inserted);
    TRACE(TRACE_MASK_WAITS & TRACE_ALWAYS,
          "Blocked on remove %.2f\n",
          (double)_num_waits_on_remove/_num_removed);
}



/**
 *  @brief Only the consumer may call this method. Retrieve a page
 *  of tuples from the buffer in one operation.
 *
 *  WARNING: Do not mix calls to copy_page() and get_tuple() on the
 *  same tuple_fifo.
 *
 *  @param timeout The maximum number of milliseconds to wait for a
 *  tuple.
 *
 *  @throw BufferTerminatedException if the producer has
 *  terminated the buffer.
 */
bool tuple_fifo::copy_page(page* dst, int timeout_ms) {
    
    if (!ensure_read_ready(timeout_ms))
        /* how to check for timeout? */
        return false;

    /* no partial pages allowed! */
    assert(_read_iterator == _read_page->begin());

    /* copy over tuples */
    dst->clear();
    size_t capacity = dst->capacity();
    for(size_t i = 0; i < capacity; i++) {
        tuple_t tuple;
        if (!get_tuple(tuple))
            break;
        dst->append_tuple(tuple);
    }

    /* return page */
    _num_removed += dst->tuple_count();
    return true;
}



/**
 * @brief Only the producer may call this method. Notify the
 * tuple_fifo that the caller will be inserting no more data.  The
 * FIFO will close normally once the last tuple has been read; the
 * reader is responsible for deleting it. The writer must not access
 * the FIFO again if send_eof() returns true.
 *
 * @return True if we successfully send and EOF. False if the fifo has
 * been terminated.
 */
bool tuple_fifo::send_eof() {
    try {
        // make sure not to drop a partial page
        _flush_write_page(true);
        return true;
    }
    catch(TerminatedBufferException &e) {
        // oops! reader terminated first
        return false;
    }
}



/**
 * @brief Both producer and consumer may call this method. This method
 * is used to notify the "other side" that we are terminating
 * abnormally. The other side should receive a
 * TerminatedBufferException on the next tuple_fifo operation.
 *
 * If successful, the other end will be notified through an
 * exception at some point in the future and will be responsible
 * for deleting the FIFO. The caller must not access the FIFO
 * again if terminate() returns true.
 *
 * @return True if we successfully terminate. False if the other side
 * has already terminated.
 */
bool tuple_fifo::terminate() {
    
    // * * * BEGIN CRITICAL SECTION * * *
    critical_section_t cs(_lock);


    // did the other end beat me to it? Note: if _done_writing is true
    // the reader is responsible for deleting the buffer; either the
    // the reader doesn't know yet or the writer is making an illegal
    // access. False is the right return value for both cases.
    if(is_terminated() || is_done_writing())
        return false;
    
    // make sure nobody is sleeping (either the reader or writer could
    // be calling this)
    _state.transition(tuple_fifo_state_t::TERMINATED);
    thread_cond_signal(_reader_notify);
    thread_cond_signal(_writer_notify);
    
    // * * * END CRITICAL SECTION * * *
    return true;
}



/* definitions of helper methods */

inline void tuple_fifo::wait_for_reader() {
    _num_waits_on_insert++;
    thread_cond_wait(_writer_notify, _lock);
}

inline void tuple_fifo::ensure_reader_running() {
    thread_cond_signal(_reader_notify);
}

inline bool tuple_fifo::wait_for_writer(int timeout_ms) {
    _num_waits_on_remove++;
    return thread_cond_wait(_reader_notify, _lock, timeout_ms);
}

inline void tuple_fifo::ensure_writer_running() {
    thread_cond_signal(_writer_notify);
}



/**
 * @brief Get a page from the tuple_fifo.
 *
 * @return NULL if the tuple_fifo has been closed. A page otherwise.
 */
void tuple_fifo::_flush_write_page(bool done_writing) {

    // after the call to send_eof() the write page is NULL
    assert(!is_done_writing());
    
    // * * * BEGIN CRITICAL SECTION * * *
    critical_section_t cs(_lock);
    _termination_check();


    switch(_state.current()) {
    case tuple_fifo_state_t::IN_MEMORY: {
        
            
        /* Wait for space to free up if we are using a "no flush"
           policy. */
        if (!FLUSH_TO_DISK_ON_FULL) {
            /* tuple_fifo stays in memory */
            /* If the buffer is currently full, we must wait for space to
               open up. Once we start waiting we continue waiting until
               space for '_threshold' pages is available. */
            for(size_t threshold=1;
                _available_in_memory_writes() < threshold; threshold = _threshold) {
                /* wait until something important changes */
                wait_for_reader();
                _termination_check();
            }
        }


        /* At this point, we don't have to wait for space anymore. If
           we still don't have enough space, it must be because we are
           using a disk flush policy. Check whether we can proceed
           without flushing to disk. */
        if (_available_in_memory_writes() >= 1) {
            
            /* Add _write_page to other tuple_fifo pages unless
               empty. */
            if(!_write_page->empty()) {
                _pages.push_back(_write_page.release());
                _pages_in_memory++;
                _pages_in_fifo++;
            }
            
            /* Allocate a new _write_page if necessary. */
            if(done_writing) {
                /* Allocation of a new _write_page is not necessary
                   (because we are done writing). Just do state
                   transition. */
                _state.transition(tuple_fifo_state_t::IN_MEMORY_DONE_WRITING);
                _write_page.done();
            }
            else
                _write_page = _alloc_page();
            
            /* wake the reader if necessary */
            if(_available_in_memory_reads() >= _threshold || is_done_writing())
                ensure_reader_running();
            
            break;
        }


        /* If we are here, we need to flush to disk. */
        /* Create on disk file. */
        c_str filepath = tuple_fifo_directory_t::generate_filepath(_fifo_id);
        _page_file = fopen(filepath.data(), "w+");
        assert(_page_file != NULL);
        if (_page_file == NULL)
            THROW2(FileException,
                   "fopen(%s) failed", filepath.data());
        TRACE(TRACE_ALWAYS, "Created tuple_fifo file %s\n",
              filepath.data());
        
        /* Append this page to _pages and flush the entire
           page_list to disk. */
        if(!_write_page->empty()) {
            _pages.push_back(_write_page.release());
            _pages_in_memory++;
            _pages_in_fifo++;
        }
        for (page_list::iterator it = _pages.begin(); it != _pages.end(); ) {
            qpipe::page* p = *it;
            p->fwrite_full_page(_page_file);

            /* done with page */
            p->clear();
            _free_pages.push_back(p);
            it = _pages.erase(it);

            assert(_pages_in_memory > 0);
            _pages_in_memory--;
        }
        fflush(_page_file);
        
        /* update _file_head_page */
        assert(_file_head_page == 0);
        _file_head_page = _next_page;

        _state.transition(tuple_fifo_state_t::ON_DISK);
        if (done_writing) {
            /* transition again! */
            _state.transition(tuple_fifo_state_t::ON_DISK_DONE_WRITING);
            _write_page.done();
        }
        else {
            /* allocate from free list */
            assert(!_free_pages.empty());
            _write_page = _alloc_page();

            /* TODO It's clear whether we want to replace the
               SENTINAL_PAGE here. On the one hand, if we can replace
               it, we can free the rest of the pages in the free
               list. On the other, we still need to check for the
               SENTINAL_PAGE in _get_read_page since pages may be
               removed using get_page (instead of tuples removed with
               get_tuple). */
            if (_read_page == SENTINEL_PAGE) {
                _set_read_page(_alloc_page());
                /* After this point, we should not release either of
                   these pages. */
            }
        }

        /* wake the reader if necessary */
        if(_available_fifo_reads() >= _threshold || is_done_writing())
            ensure_reader_running();

        break;

    } /* endof case: IN_MEMORY */
        
    case tuple_fifo_state_t::ON_DISK: {

        int fseek_ret = fseek(_page_file, 0, SEEK_END);
        assert(!fseek_ret);
        if (fseek_ret)
            THROW1(FileException, "fseek to EOF");
        _write_page->fwrite_full_page(_page_file);
        fflush(_page_file);
        _pages_in_fifo++;

        if (done_writing) {
            _state.transition(tuple_fifo_state_t::ON_DISK_DONE_WRITING);
            _write_page.done();
        }
        else {
            /* simply reuse write page */
            _write_page->clear();
        }
        
        /* wake the reader if necessary */
        if(_available_fifo_reads() >= _threshold || is_done_writing())
            ensure_reader_running();

        break;
    }

    default:
        unreachable();

    } /* endof switch statement */


    // * * * END CRITICAL SECTION * * *
}



/**
 * @brief Use a timeout_ms value of 0 to wait until a page appears or
 * the tuple_fifo is closed (normal behavior). Use a negative
 * timeout_ms value to avoid waiting. If the tuple_fifo contains no
 * pages, we return immediately with a value of 0. Use a positive
 * timeout_ms value to wait for a max length of time.
 *
 * @return 1 if we got a page. -1 if the tuple_fifo has been
 * closed. If 'timeout_ms' is negative and this method returns 0, it
 * means the tuple_fifo is empty. If the timeout_ms value is positive
 * and we return 0, it means we timed out.
 */
int tuple_fifo::_get_read_page(int timeout_ms) {

    // * * * BEGIN CRITICAL SECTION * * *
    critical_section_t cs(_lock);
    _termination_check();


    /* Free the page so the writer can use it. */
    if (is_in_memory() && (_read_page != SENTINEL_PAGE)) {
        /* We are still maintaining an in-memory page list from which
           we are pulling pages. We release them to _free_pages as we
           are done with them. */
        _read_page->clear();
        _free_pages.push_back(_read_page.release());
        _set_read_page(SENTINEL_PAGE);
    }


    /* If 'wait_on_empty' and the buffer is currently empty, we must
       wait for space to open up. Once we start waiting we continue
       waiting until either space for '_threshold' pages is available
       OR the writer has invoked send_eof() or terminate(). */
    for(size_t t=1;
        (timeout_ms >= 0) && !is_done_writing() && (_available_fifo_reads() < t);
        t = _threshold) {
        /* We are to either wait for a page or wait for timeout_ms. */
        if(!wait_for_writer(timeout_ms))
            /* Timed out! */
            break;
        _termination_check();
    }


    TRACE(TRACE_ALWAYS&TRACE_MASK_DISK,
          "available reads = %d\n", (int)_available_fifo_reads());
    if(_available_fifo_reads() == 0) {
        /* If we are here, we exited the loop above because one of the
           other conditions failed. We either noticed that the
           tuple_fifo has been closed or we've timed out. */
        if(is_done_writing()) {
            /* notify caller that the tuple_fifo is closed */
            TRACE(TRACE_ALWAYS&TRACE_MASK_DISK, "Returning -1\n");
	    return -1;
        }
	if(timeout_ms != 0)
            /* notify caller that we timed out */
	    return 0;
	unreachable();
    }


    switch(_state.current()) {
    case tuple_fifo_state_t::IN_MEMORY:
    case tuple_fifo_state_t::IN_MEMORY_DONE_WRITING: {
        /* pull the page from page_list */
        assert(!_pages.empty());
        _set_read_page(_pages.front());
        _pages.pop_front();
        assert(_pages_in_memory > 0);
        _pages_in_memory--;
        break;
    }
    case tuple_fifo_state_t::ON_DISK:
    case tuple_fifo_state_t::ON_DISK_DONE_WRITING: {

        /* We are on disk. We should not be releasing _read_page after
           iterating over its entries. However, we still need to be
           prepared against code which extracts pages from the
           tuple_fifo using get_page(). get_page() sets _read_page to
           the SENTINEL_PAGE. */
        if (_read_page == SENTINEL_PAGE)
            _set_read_page(_alloc_page());
        else {
            /* We are reusing the same read page... do a reset */
            _read_page->clear();
            _set_read_page(_read_page.release());
        }


        /* Make sure that at this point, we are not dealing with the
           SENTINAL_PAGE. */
        assert(_read_page != SENTINEL_PAGE);
        assert(_read_page->page_size() == malloc_page_pool::instance()->page_size());


        /* read page from disk file */
        _read_page->clear();
        TRACE(TRACE_ALWAYS&TRACE_MASK_DISK, "_next_page = %d\n", (int)_next_page);
        TRACE(TRACE_ALWAYS&TRACE_MASK_DISK, "_file_head_page = %d\n", (int)_file_head_page);
        unsigned long seek_pos =
            (_next_page - _file_head_page) * get_default_page_size();
        TRACE(TRACE_ALWAYS&TRACE_MASK_DISK, "fseek to %lu\n", seek_pos);
        int fseek_ret = fseek(_page_file, seek_pos, SEEK_SET);
        assert(!fseek_ret);
        if (fseek_ret)
            THROW2(FileException, "fseek to %lu", seek_pos);
        int fread_ret = _read_page->fread_full_page(_page_file);
        assert(fread_ret);
        _set_read_page(_read_page.release());


        size_t page_size = _read_page->page_size();
        if (TRACE_ALWAYS&TRACE_MASK_DISK) {
            page* pg = _read_page.release();
            unsigned char* pg_bytes = (unsigned char*)pg;
            for (size_t i = 0; i < page_size; i++) {
                printf("%02x", pg_bytes[i]);
                if (i % 2 == 0)
                    printf("\t");
                if (i % 16 == 0)
                    printf("\n");
            } 
            _set_read_page(pg);
        }
        
        TRACE(TRACE_ALWAYS&TRACE_MASK_DISK, "Read %d %d-byte tuples\n",
              (int)_read_page->tuple_count(),
              (int)_read_page->tuple_size());
        break;
    }

    default:
        unreachable();
    } /* endof switch statement */

    
    assert(_pages_in_fifo > 0);
    _pages_in_fifo--;
    _next_page++;
    
    
    /* wake the writer if necessary */
    if(!FLUSH_TO_DISK_ON_FULL
       && (_available_in_memory_writes() >= _threshold)
       && !is_done_writing())
        ensure_writer_running();


    // * * * END CRITICAL SECTION * * *
    return 1;
}



int tuple_fifo_generate_id() {
    static acounter_t next_fifo_id;
    return next_fifo_id.fetch_and_inc();
}



void tuple_fifo::tuple_fifo_state_t::transition(const _tuple_fifo_state_t next) {
    if (_transition_ok(next))
        _value = next;
    else {
        TRACE(TRACE_ALWAYS, "Invalid transition from %s to %s\n",
              to_string().data(),
              state_to_string(next).data());
        assert(0);
    }
}



bool tuple_fifo::tuple_fifo_state_t::_transition_ok(const _tuple_fifo_state_t next) {
    switch(_value) {
    case INVALID:
        /* Only allowed transition should be to IN_MEMORY since we are
           only INVALID in the constructor until we invoke init(). */
        return next == IN_MEMORY;
    case IN_MEMORY:
        return
            (next == IN_MEMORY_DONE_WRITING)
            || (next == ON_DISK)
            || (next == TERMINATED);
    case ON_DISK:
        return
            (next == ON_DISK_DONE_WRITING)
            || (next == TERMINATED);
    default:
        return false;
    }
}



EXIT_NAMESPACE(qpipe);
