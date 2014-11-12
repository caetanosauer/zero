#ifndef PLOG_H
#define PLOG_H

#include "sm_base.h"

class plog_ext_m
{
public:
    enum {
        EXTENT_SIZE = 16384
    };

    struct extent_t
    {
        typedef pthread_mutex_t mutex_t;

        extent_t*   next;
        extent_t*   prev;
        uint16_t    size;
        bool        committed;
        bool        aborted;
        mutex_t     mutex;
        char        data[EXTENT_SIZE];

        extent_t() :
            next(NULL),
            prev(NULL),
            size(0),
            committed(false)
        {
            pthread_mutex_init(&mutex, NULL);
        }

        uint16_t space_available() { return EXTENT_SIZE - size; }
        bool is_first() { return prev == NULL; }
        bool is_last() { return next == NULL; }
    };

    struct ext_iter_t {
        extent_t* ext;
        uint16_t pos;
        bool forward;

        ext_iter_t(extent_t* ext, bool forward) :
            ext(ext), pos(0), forward(forward)
        {}

        bool next(logrec_t*& lr)
        {
            if (forward && pos >= ext->size) {
                if (ext->is_last()) return false;
                ext = ext->next;
                pos = 0;
                return next(lr);
            }
            if (!forward && pos == 0) {
                if (ext->is_first()) return false;
                ext = ext->prev;
                // lsn of logrecs in private logs contain offset inside extent
                pos = *((uint16_t*) (ext->data + ext->size - sizeof(lsn_t)));
                return next(lr);
            }
            lr = (logrec_t*) (ext->data + pos);
            if (forward) {
                pos += lr->length();
            }
            else {
                w_assert0(pos >= lr->length());
                pos = *((uint16_t*) (ext->data + pos - sizeof(lsn_t)));
            }
            return true;
        }
    };

    static ext_iter_t* iterate_backwards(extent_t* ext)
    {
        return new ext_iter_t(ext, false);
    }

    virtual extent_t*   alloc_extent() = 0;
    virtual void        free_extent(extent_t* ext) = 0;
};

class plog_ext_naive : public plog_ext_m
{
public:
    virtual extent_t* alloc_extent() { return new extent_t(); }

    virtual void free_extent(extent_t* ext) { delete ext; }
};


#endif
