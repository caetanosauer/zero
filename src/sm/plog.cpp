#include "plog.h"

#include "logrec.h"

plog_t::plog_t() :
    _used_size(0),
    state(UNCOMMITTED)
{
    pthread_mutex_init(&mutex, NULL);
    data = smlevel_0::allocator.allocate<char>((size_t) INITIAL_SIZE);
    _alloc_size = INITIAL_SIZE;
    w_assert1(data);
}

plog_t::~plog_t()
{
    smlevel_0::allocator.release<char>(data);
}

    /*
     * Insertion of log records is handled by plog_xct_t. All we have to do
     * here is make sure that enough space is available for one log record
     * with maximum size. Ideally, we would know the size beforehand, so that
     * no space is wasted, but unfortunately that's not how Shore was designed.
     * Note that we don't explicitly support insertion by multiple threads,
     * but it should work due to the mutex. However, if the mutex is eliminated
     * in the future as a consequence of disk-to-disk propagation (i.e., no
     * single-page rollback and thus no iteration on uncommitted plogs), than
     * some other concurrency mechanism is required for multi-threaded xct's.
     */
char* plog_t::get()
{
    lock();
    if (space_available() < sizeof(logrec_t)) {
        // grow allocated extent with factor 1.5
        _alloc_size *= 3 / 2;
        char* data_new =
            smlevel_0::allocator.allocate<char>(_alloc_size);
        memcpy(data_new, data, _used_size);
        smlevel_0::allocator.release<char>(data);
        data = data_new;
    }
    return data + _used_size;
}

void plog_t::give(logrec_t* lr)
{
    w_assert1((char*) lr == data + _used_size);
    w_assert1(_used_size + lr->length() <= _alloc_size);
    // TODO: assert that I hold the lock
    // set LSN as offset within extent
    lr->set_lsn_ck(lsn_t(0, _used_size));
    _used_size += lr->length();
    unlock();
}

bool plog_t::iter_t::next(logrec_t*& lr)
{
    if (finished) {
        return false;
    }

    if (!forward && pos == plog->used_size()) {
        // backward iterator starts ater last logrec
        move_pos_backwards(pos);
    }

    lr = (logrec_t*) (plog->data + pos);
    if (forward) {
        pos += lr->length();
        if (pos >= plog->used_size()) {
            finished = true;
        }
    }
    else if (pos > 0) {
        w_assert0(pos >= lr->length());
        move_pos_backwards(pos);
    }
    else {
        finished = true;
    }
    // TODO merge my Shore-MT code
    //w_assert1(lr->valid_header());
    return true;
}

void plog_t::iter_t::move_pos_backwards(uint32_t& pos)
{
    w_assert1(pos > sizeof(lsn_t));
    pos = *((uint32_t*) (plog->data + pos - sizeof(lsn_t)));
}

// CS: Implementation of non-specialized templates must be included
// after their instantiation to <char> above. They cannot be in the
// same header file as the declaration though (allocator.h). Therefore,
// the only solution is to include the cpp file after the template usage.
// This solution is even shown in Stroustroup's book (Section 23.7 of 4th
// edition). What's really bad about it is that we need to include the 
// file in every translation unit.
// One option may be to use external linkage of templates (TODO)
#include "allocator.cpp"
