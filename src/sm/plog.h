#ifndef PLOG_H
#define PLOG_H

#include "sm_base.h"
#include "allocator.h"

class plog_t
{
    friend class plog_iter_t;
public:
    enum {
        INITIAL_SIZE = 16384
    };

    enum state_t {
        UNCOMMITTED = 0x01,
        COMMITTED = 0x02,
        ABORTED = 0x04
    };

    typedef pthread_mutex_t mutex_t;

private:
    uint32_t    _alloc_size;
    uint32_t    _used_size;
    state_t     state;
    mutex_t     mutex;
    char*       data;

public:
    plog_t();
    ~plog_t();

    uint32_t alloc_size() { return _alloc_size; }
    uint32_t used_size() { return _used_size; }
    uint32_t space_available() { return alloc_size() - used_size(); }

    void set_state(state_t s) { state = s; }

    char* get();
    void give(logrec_t*);

    class plog_iter_t {
    private:
        plog_t* plog;
        uint32_t pos;
        bool forward;
        bool finished;

    public:
        plog_iter_t(plog_t* plog, bool forward) :
            plog(plog), pos(0), forward(forward), finished(false)
        {
            plog->lock();
            if (!forward) {
                pos = plog->used_size();
            }
        }

        ~plog_iter_t()
        {
            plog->unlock();
        }

        bool next(logrec_t*& lr);

    private:
        inline void move_pos_backwards(uint32_t& pos);
    };

    plog_iter_t* iterate_backwards()
    {
        return new plog_iter_t(this, false);
    }

    plog_iter_t* iterate_forwards()
    {
        return new plog_iter_t(this, true);
    }

private:
    void lock()
    {
        pthread_mutex_lock(&mutex);
    }

    void unlock()
    {
        pthread_mutex_unlock(&mutex);
    }
};

#endif
