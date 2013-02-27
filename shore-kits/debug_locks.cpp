/***
    A debug helper for use within gdb when things go wron in the lock manager.

    To use:

    0. Compile this file by issuing `make debug_locks.so' -- it will
       compile using the same flags as the rest of the kits. This step
       can really be done at any time, and can also be repeated as
       necessary (that's the whole point of the helper .so!)

    1. When a deadlock or other problem is observed, issue the
       following command from gdb to load the library:

       p dlopen("./debug_locks.so", 2)

       If you get a non-zero return, the call succeeded; a zero return
       can be diagnosed by calling:

       p (char*)dlerror()

    2. All symbols from this file should be available, so you can,
       e.g. do the following:

       set scheduler-lock on
       p lock_waiters(false)
       set scheduler-lock off

       The scheduler-lock calls ensure that no other threads run while
       the helper function does its work. Also note that if you call
       the function from a context that doesn't know what 'false' is,
       you will get errors about gdb not finding an overloaded
       instance to call; if so, just call lock_waiters(0) instead.

    3. If you decide to change the helper library, just edit the code,
       recompile the .so, and then issue this command from gdb:

       p dlclose($number)

       ... where $number is the numbered convenience variable that gdb
       used to store the result of your dlopen() call. It should
       return zero. WARNING: never call dlclose with scheduler-lock
       on, it leads to seg faults in the loader on my machine.

    That's basically it! Feel free to add new functions or modify
    existing ones as needed, of course.

    One final warning: this file is not compiled as part of the normal
    build process, so code rot is a very real possibility. Feel free
    to check in updated versions that fix any such problems you happen
    to encounter while debugging, but there's absolutely no need to
    test this file before every commit.
 ***/

#define private public
#define protected public
//#define class struct

#include "w_defines.h"
#include "st_error_enum_gen.h"
#include "sm_int_4.h"
#include "kvl_t.h"
#include "pmap.h"
#include "sm_s.h"
#include "page_s.h"
#include "bf.h"
#include "page.h"
#include "sm_io.h"
#include "extent.h"
#include "lock.h"
#include "lock_s.h"
#include "lock_x.h"
#include "lock_core.h"
#include "tls.h"

// from sm/lock_core.cpp:
class bucket_t {
public:

    DEF_LOCK_X_TYPE(3);              // declare/define lock_x type
    // lock_x: see lock_x.h
    lock_x                        mutex;   // serialize access to lock_info_t
    // lock_x's mutex is of type queue_based_lock_t
    lock_core_m::chain_list_t     chain;

    NORET                         bucket_t() :
                                    chain(W_LIST_ARG(lock_head_t, chain),
                                            &mutex.mutex) {
                                  }

    private:
    // disabled
    NORET                         bucket_t(const bucket_t&);
    bucket_t&                     operator=(const bucket_t&);
};



#include <map>
#include <sstream>

extern "C"
void scan_lock_pool() {
  typedef std::map<int, int> lmap;
  lmap lengths;
  lock_core_m* core = smlevel_0::lm->_core;
  for(int i=0; i < core->_htabsz; i++)
    lengths[core->_htab[i].chain.num_members()]++;

  printf("Bucket density histogram:\n");
  for(lmap::iterator it=lengths.begin(); it != lengths.end(); ++it)
    printf("\t%d: %d\n", it->first, it->second);
  printf("\n");
}

extern "C"
void lock_waiters(bool show_all) {
    typedef std::map<int, int> lmap;
    lmap lengths;
    lock_core_m* core = smlevel_0::lm->_core;
    for(int i=0; i < core->_htabsz; i++) {
        lock_core_m::chain_list_i  it(core->_htab[i].chain);
        while(lock_head_t *lock=it.next() ) {
            if (not show_all and not lock->waiting)
                continue;
            
            std::ostringstream os;
            os << lock->name << std::ends << std::flush;
            char const* lm = lock_base_t::mode_str[lock->granted_mode];
            printf("Lock %p: %s %s:\n", lock, os.str().c_str(), lm);
            lock_head_t::unsafe_queue_iterator_t it(*lock);
            while(lock_request_t *req=it.next()) {
                char const* st = "";
                char const* cm = "";
                switch(req->_state) {
                case lock_m::t_waiting:
                    st = "*";
                    break;
                case lock_m::t_converting:
                    st = "/";
                    cm = lock_base_t::mode_str[req->convert_mode()];
                    break;
                default:
                    break;
                }
                tid_t tid = req->get_lock_info()->tid();
                char const* m = lock_base_t::mode_str[req->mode()];
                printf("\t%p: %d.%d %s%s%s\n", req, tid.get_hi(), tid.get_lo(), m, st, cm);
            }
          
        }
    }
}

extern "C"
int debug_test() {
    return fprintf(stderr, "Hi! The address of the lock manager is: %p\n", &smlevel_4::lm);
}
