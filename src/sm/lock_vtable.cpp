#include "w_defines.h"

#define LOCK_CORE_C
#define SM_SOURCE

#include "sm_int_1.h"

#include "vtable.h"
#include "sm_vtable_enum.h"

#include "lock_s.h"
#include "lock_x.h"
#include "lock_core.h"
#include "lock_bucket.h"

/** virtual tables for lock_m, lock_core. */

int                 
lock_m::collect( vtable_t & res, bool names_too) 
{
    return _core->collect(res, names_too);
}



enum {
    /* for locks */
    lock_name_attr,
    lock_mode_attr,
    lock_tid_attr,
    lock_status_attr,

    /* last number! */
    lock_last 
};

const char *lock_vtable_attr_names[] =
{
    "Name",
    "Lock mode",
    "Children",
    "Tid",
    "Status"
};

static vtable_names_init_t names_init(lock_last, lock_vtable_attr_names);

int
lock_core_m::collect( vtable_t& /*v*/, bool /*names_too*/)
{
    /*
    // NOTE: This does not have to be atomic or thread-safe.
    // It yields approximate statistics and is used by ss_m.
    int n = _requests_allocated;
    w_assert1(n>=0);
    int found = 0;
    int per_bucket = 0;

    if(names_too) n++;

    // n = # lock request = # rows
    // Number of attributes = lock_last.
    // names_init.max_size() is max attribute length.
    int max_size = names_init.max_size();
    max_size = max_size < 100 ? 100 : max_size; // for lock names
    if(v.init(n, lock_last, max_size)) return -1;

    vtable_func<lock_request_t> f(v);
    if(names_too) f.insert_names();

    if (n > 0) {
        for (uint h = 0; h < _htabsz; h++)  {
            w_assert9(v.size() == n);
            per_bucket=0;
            lock_head_t* lock;
            ACQUIRE_BUCKET_MUTEX(h);
            chain_list_i i(_htab[h].chain);
            lock = i.next();
            while (lock)  {
                ACQUIRE_HEAD_MUTEX(lock); //collect vtable 
                lock_request_t* request;
                lock_head_t::safe_queue_iterator_t r(*lock); 
                while ((request = r.next()))  {
                    if(found <= n)  {
                        f(*request);
                        per_bucket++;
                        found++;
                    } else {
                        // back out of this bucket
                        f.back_out(per_bucket);
                        break;
                    }
                }
                RELEASE_HEAD_MUTEX(lock); // acquired here NOT through find_lock_head
                if(found <= n)  {
                    lock = i.next();
                } else {
                    lock = 0;
                }
            }
            RELEASE_BUCKET_MUTEX(h);
            if(found > n)  {
                // realloc and re-start with same bucket
                if(f.realloc()<0) return -1;
                h--;
                found -= per_bucket;
                n = v.size(); // get new size
            }
        }
    }
    w_assert9(found <= n);
    */
    return 0;
}

