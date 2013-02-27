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

/** @file:   tuple.h
 *
 *  @brief:  Representation of tuples in QPipe
 *
 *  @author: Ippokratis Pandis
 *  @author: Ryan Johnson
 */

#ifndef __QPIPE_TUPLE_H
#define __QPIPE_TUPLE_H


#include <cassert>
#include <new>

#include "util.h"



ENTER_NAMESPACE(qpipe);



// exported constants

static const int DEFAULT_BUFFER_PAGES = 100;



// exported datatypes
extern void set_default_page_size(size_t page_size);
extern size_t get_default_page_size();



/**
 *  @brief QPIPE tuple. An initialized tuple stores a char* into some
 *  tuple_page_t as well as the size of the data.
 *
 * "IMPORTANT: tuple objects do *NOT* own the data array they point
 * to; pages do.  They are only convenient wrappers. Code must not
 * assume that tuples passed between functions -- or the data they
 * contain -- will remain unchanged over time. The general contract is
 * that any code that wishes to retain a tuple passed between
 * functions must make a deep copy to a page it owns, or obtain
 * ownership of the page the tuple belongs to.
 */

class tuple_t 
{
public:
    char* data;
    size_t size;

    tuple_t() {
	// no data
        init(NULL, 0);
    }
    
    tuple_t(char* d, size_t s) {
        init(d, s);
    }

    /**
     *  @brief Copy the specified tuple. This function performs a deep
     *  copy (copy of data bytes) from the src tuple to this
     *  tuple. The two tuples must already be initialized with the
     *  same lengths. The behavior of this function is undefined if
     *  this tuple does not point into a tuple page with at least
     *  src.size bytes available.
     *
     *  @param src The tuple we are creating a copy of.
     */

    void assign(const tuple_t &src) {
        assert(size == src.size);
        memcpy(data, src.data, size);
    }

private:

    /**
     *  @brief Initialize data and size.
     *
     *  @param d this.data will be set to this value.
     *
     *  @param s this.size will be set to this value.
     */

    void init(char* d, size_t s) {
        data = d;
        size = s;
    }
};



class page_pool 
{
    size_t _page_size;
    
public:
    page_pool(size_t page_size)
        : _page_size(page_size)
    {
    }
    
    size_t page_size() const {
        return _page_size;
    }

    /**
     * @brief Allocates a new page.
     */
    virtual void* alloc()=0;
    
    /**
     * @brief releases a page. After this method returns it is no
     * longer safe to access the page.
     */
    virtual void free(void* page)=0;

    
    virtual ~page_pool() { }
};



/**
 * @brief A page pool implementation backed by the freestore (i.e.
 * heap).
 */
struct malloc_page_pool : page_pool 
{
private:
    static malloc_page_pool _instance;
    
public:
    static malloc_page_pool *instance() {
        return &_instance;
    }
        
    malloc_page_pool(size_t page_size = get_default_page_size())
        : page_pool(page_size)
    {
    }
    virtual void* alloc() {
        void* ptr = malloc(page_size());
        if(!ptr)
            throw std::bad_alloc();

        return ptr;
    }
    virtual void free(void* ptr) {
        ::free(ptr);
    }
};


class tuple_fifo;


/**
 *  @brief Wapper class for a page header that stores the page's
 *  size. The constructor is private to prevent stray headers from
 *  being created in the code. We instead export a static alloc()
 *  function that allocates a new page and places a header at the
 *  base.
 */
class page 
{
    page_pool* _pool;
    size_t _tuple_size;
    size_t _padded_size;
    size_t _free_count;
    size_t _end_offset;
public:
    page* next;
    
private:
    // jumping-off point for the data on this page
#ifdef __GCC
    char   _data_array[0];
    char* _data() { return _data_array; }
#else
    char* _data() { return sizeof(page)+(char*)this; }
#endif
 public:

    /**
     * @brief Calculates the capacity of a page given the (page_size)
     * and (tuple_size).
     */
    static size_t capacity(size_t page_size, size_t tuple_size) {
        return (page_size - sizeof(page))/tuple_size;
    }
    
    static page* alloc(size_t tuple_size, page_pool* pool=malloc_page_pool::instance()) {
        return new (pool->alloc()) page(pool, tuple_size);
    }
    
    void free() {
        /* Do not call the destructor before releasing the memory. */
        _pool->free(this);
    }
    
    size_t page_size() const {
        return _pool->page_size();
    }

    size_t tuple_size() const {
        return _tuple_size;
    }


    /**
     *  @brief Returns the total number of tuples that fit into this
     *  page.
     *
     *  @return See description.
     */
    size_t capacity() const {
        return capacity(page_size(), tuple_size());
    }

    size_t tuple_count() const {
        return _end_offset/tuple_size();
    }


    /**
     *  @brief Empty this page of its tuples.
     */
    void clear() {
        _free_count = capacity();
        _end_offset = 0;
    }
    

    /**
     *  @brief Returns true if and only if this page currently
     *  contains zero tuples.
     */
    bool empty() const {
        return _end_offset == 0;
    }
    

    /**
     *  @brief Returns true if and only if this page currently
     *  contains the maximum number of tuples it can fit.
     */
    bool full() const {
        return _free_count == 0;
    }

    /**
     *  @brief Get a tuple_t to the tuple stored at the specified
     *  index.
     *
     *  @param index The tuple index. Tuples are zero-indexed.
     */
    tuple_t get_tuple(size_t index) {
        return tuple_t(&_data()[index*tuple_size()], tuple_size());
    }


    /**
     *  @brief Fill this page with tuples read from the specified
     *  file. If this page already contains tuples, we will overwrite
     *  them.
     *
     *  @param file The file to read from.
     *
     *  @return true if a page was successfully read. false if there
     *  thare no more pages to read in the file.
     *
     *  @throw FileException if a read error occurs.
     */
    bool read_full_page(int fd);


    /**
     *  @brief Drain this page to the specified file. If the page is
     *  not full, we drain padding so a full page is written.
     *
     *  @param file The file to write to.
     *
     *  @return void
     *
     *  @throw FileException if a write error occurs.
     */
    void write_full_page(int fd);
    

    /**
     *  @brief Fill this page with tuples read from the specified
     *  file. If this page already contains tuples, we will overwrite
     *  them.
     *
     *  @param file The file to read from.
     *
     *  @return true if a page was successfully read. false if there
     *  thare no more pages to read in the file.
     *
     *  @throw FileException if a read error occurs.
     */
    bool fread_full_page(FILE* file);


    /**
     *  @brief Drain this page to the specified file. If the page is
     *  not full, we drain padding so a full page is written.
     *
     *  @param file The file to write to.
     *
     *  @return void
     *
     *  @throw FileException if a write error occurs.
     */
    void fwrite_full_page(FILE *file);
    

    /**
     *  @brief Try to allocate space for a new tuple.
     *
     *  @return NULL if the page is full. Otherwise, the address of
     *  the newly allocated tuple.
     */
    char* allocate() {
        assert(!full());
        char *result = &_data()[_end_offset];
        _end_offset += tuple_size();
        _free_count--;
        return result;
    }
    
    /**
     *  @brief Allocate a new tuple on this page and initializes
     *  'tuple' to point to it. This is a pre-assembly strategy; use
     *  it to obtain tuples in order to assemble data "in place"
     *  without extra memory copies.
     *
     *  @param tuple On success, this tuple will be set to point to
     *  the newly allocated space.
     *
     *  @return true on successful allocate. false on failure (if the
     *  page is full).
     */
    tuple_t allocate_tuple() {
        return tuple_t(allocate(), tuple_size());
    }
    

    /**
     *  @brief Allocate a new tuple on this page and copy the contents
     *  of 'tuple' into it. This is a post-assembly strategy. Use it
     *  to efficiently copy existing tuples.
     *
     *  @param tuple On success, the tuple copied to this page. The
     *  size of 'tuple' must match the tuple page's internal tuple
     *  size.
     *
     *  @return true on successful allocate and copy. False if the
     *  page is full.
     */
    void append_tuple(const tuple_t &tuple) {
        // use tuple::assign() instead of a naked memcpy()
        allocate_tuple().assign(tuple);
    }
    

    /**
     *  @brief Iterator over the tuples in this page. Each dereference
     *  returns a tuple_t.
     */
    class iterator {

    private:
        tuple_t _current;
        
    public:

        iterator()
            : _current(NULL, 0)
        {
        }

        iterator(page* page, size_t offset)
            : _current(page->_data() + offset,
                       page->tuple_size())
        {
        }

        iterator(size_t size, char* data)
            : _current(data, size)
        {
        }

        bool operator ==(const iterator& other) const {
            return _current.data == other._current.data;
        }

        bool operator !=(const iterator& other) const {
            return !(*this == other);
        }

        tuple_t operator *() {
            return _current;
        }

        tuple_t *operator ->() {
            return &_current;
        }

        /**
         * @brief Returns the current value and advances the iterator
         * in a single operation
         */
        tuple_t advance() {
            tuple_t result = **this;
            ++*this;
            return result;
        }

        iterator &operator ++() {
            _current.data += _current.size;
            return *this;
        }

        iterator operator ++(int) {
            iterator old = *this;
            ++*this;
            return old;
        }
    };
    
    iterator begin() {
        return iterator(this, 0);
    }
    
    iterator end() {
        return iterator(this, _end_offset);
    }
    
    
private:
    page(page_pool* pool, size_t tuple_size)
        : _pool(pool),
          _tuple_size(tuple_size),
          next(NULL)
    {
	// must be 8-byte aligned!
	test_alignment(_data(), sizeof(double));
        clear();
    }
    

    // Prevent attempts to copy pages. Allowing shallow copies would be
    // dangerous; deep copies are expensive and not necessary so far.
    page(const page &other);

    page &operator =(const page &other);    

    // Prevent the user from stack allocating pages or calling delete
    // on page pointers. Use page::alloc() and page::free() instead
    ~page();
};



EXIT_NAMESPACE(qpipe);



/**
 * Specialize the guard template to release pages back into their pool
 * instead of calling 'delete' operator (which would fail to compile
 * in any case)
 */
template <>
inline void guard<qpipe::page>::action(qpipe::page* ptr) {
    ptr->free();
}



ENTER_NAMESPACE(qpipe);


/**
 * @brief Stores a list of pages that should all be freed at the same time.
 *
 * This class is useful for keeping a set of pages from leaking -- or
 * being free prematurely -- while their tuples are being processed in
 * a secondary data structure (eg sorting and hashing)
 */
class page_trash_stack {

    guard<qpipe::page> _head;
    int _size;
public:
    page_trash_stack()
	: _size(0)
    {
    }
    void add(qpipe::page* p) {
        p->next = _head.release();
        _head = p;
	++_size;
    }
    void clear() {
        // use the page guard free the pages one by one
        for( ; _head; _head = _head->next);
	_size = 0;
    }
    int size() { return _size; }
    ~page_trash_stack() {
        clear();
    }
};


EXIT_NAMESPACE(qpipe);

#endif
