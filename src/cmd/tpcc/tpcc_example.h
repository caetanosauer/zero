/*
 * (c) Copyright 2013-2014, Hewlett-Packard Development Company, LP
 */

#ifndef TPCC_EXAMPLE_H
#define TPCC_EXAMPLE_H

// An example experiment to show how you can add your own experiment with tpcc.h.
#include "tpcc.h"

namespace tpcc {
    /** This implements individual transactions. */
    class example_worker_thread_t : public worker_thread_t {
    public:
        example_worker_thread_t(int32_t worker_id_arg, driver_thread_t *parent)
            : worker_thread_t(worker_id_arg, parent) {}
        virtual ~example_worker_thread_t() {}

    protected:
        // program parameters
        uint32_t wid;
        uint32_t did;
        
        // whatever thread-local status
        
        rc_t init_worker();
        rc_t run_worker();
    private:
        rc_t do_the_work();
    };

    /** Usually no code here except you need special initialization.*/
    class tpcc_example_thread_t : public driver_thread_t {
    public:
        tpcc_example_thread_t();
        virtual ~tpcc_example_thread_t() {}

    protected:
        worker_thread_t* new_worker_thread (int32_t worker_id) {
            return new example_worker_thread_t(worker_id, this);
        }
    };
}

#endif // TPCC_EXAMPLE_H
