#ifndef THREAD_WRAPPER_H
#define THREAD_WRAPPER_H

#include <thread>
#include <memory>

#include "w_rc.h"
#include "tls.h"
#include "latch.h"

/*
 * The sole purpose of this class is to replace sthread_t with as little code impact as
 * possible -- new code should use the C++11 thread library directly (as soon as the TODO
 * below about tls_manager is fixed...)
 */
class thread_wrapper_t
{
public:

    thread_wrapper_t()
    {
    }

    virtual ~thread_wrapper_t()
    {
        thread_ptr.reset();
    }

    /*
     * Virtual methods to be overridden by sub-classes.
     */
    virtual void run() = 0;
    virtual void before_run() {};
    virtual void after_run() {};

    void spawn()
    {
        // CS TODO: these are required for the old shore TLS allocator, which is still used.
        // With C++11 and the thread_local specifier, it should be much easier to perform this
        // type of static initialization;
        tls_tricks::tls_manager::thread_init();
        smthread_t::add_me_to_thread_list();

        before_run();
        run();
        after_run();

        smthread_t::remove_me_from_thread_list();
        tls_tricks::tls_manager::thread_fini();

        // latch_t maintains some static data structures that must be deleted manually
        latch_t::on_thread_destroy();
    }

    w_rc_t fork()
    {
        thread_ptr.reset(new std::thread ([this] { spawn(); }));
        return RCOK;
    }

    w_rc_t join()
    {
        thread_ptr->join();
        return RCOK;
    }

private:
    std::unique_ptr<std::thread> thread_ptr;
};

#endif
