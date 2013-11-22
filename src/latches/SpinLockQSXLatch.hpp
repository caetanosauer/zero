#include <inttypes.h>
#include <pthread.h>

class SimpleLatch {
private:
    pthread_spinlock_t lock;
    int64_t num_readers;
    int64_t writer_count;
    bool write_held;
    
public:
    typedef int64_t TicketQ;
    typedef int64_t TicketS;
    typedef int64_t TicketX;

    static bool isFailure(const TicketQ & t) {
        return t==-1;
    }
    static bool isFailure(const TicketS & t) {
        return t==-1;
    }
    static bool isFailure(const TicketX & t) {
        return t==-1;
    }

    
    TicketQ acquireQ() {
        TicketQ ret;
        do {
            pthread_spin_lock(&lock);
            if (write_held) {
                pthread_spin_unlock(&lock);
                continue;
            } else {
                ret = writer_count;
                pthread_spin_unlock(&lock);
                return ret;
            }            
        } while(true);
    }
    TicketS acquireS() {
        TicketS ret = 1;
        do {
            pthread_spin_lock(&lock);
            if (write_held) {
                pthread_spin_unlock(&lock);
                continue;
            } else {
                num_readers++;
                pthread_spin_unlock(&lock);
                return ret;
            }
        } while(true);        
    }
    TicketX acquireX() {
        TicketX ret = 1;
        do {
            pthread_spin_lock(&lock);
            if (write_held || num_readers > 0) {
                pthread_spin_unlock(&lock);
                continue;
            } else {
                write_held = true;
                writer_count++;;
                pthread_spin_unlock(&lock);
                return ret;
            }
        } while(true);                
    }

    bool tryAcquireQ(TicketQ &t) { 
        pthread_spin_lock(&lock);
        if (write_held) {
            pthread_spin_unlock(&lock);
            return false; 
        } else {
            t = writer_count;
            pthread_spin_unlock(&lock);
            return true;
        }
    }
    bool tryAcquireS(TicketS &t) { 
        pthread_spin_lock(&lock);
        if (write_held) {
            pthread_spin_unlock(&lock);
            return false;
        } else {
            num_readers++;
            t = 1;
            pthread_spin_unlock(&lock);
            return true;
        }
    }
    bool tryAcquireX(TicketX &t) { 
        pthread_spin_lock(&lock);
        if (write_held || num_readers > 0) {
            pthread_spin_unlock(&lock);
            return false;
        } else {
            write_held = true;
            writer_count++;
            t = 1;
            pthread_spin_unlock(&lock);
            return true;
        }
    }

    bool releaseQ(const TicketQ &t) {
        pthread_spin_lock(&lock);
        if (t == writer_count && ! write_held) {
            pthread_spin_unlock(&lock);
            return true;                
        } else {
            pthread_spin_unlock(&lock);
            return false;                
        }
    }
    bool releaseS(const TicketS &t) {  // Will likely return only true.
        pthread_spin_lock(&lock);
        num_readers--;
        pthread_spin_unlock(&lock);
        return true;
    }

    bool releaseX(const TicketX &t) { // Will likely return only true.
        pthread_spin_lock(&lock);
        write_held = false;
        pthread_spin_unlock(&lock);
        return true;
    }

    // Upgrade to higher mode -- Functions in form of tryUpgradeAB.  Caller promises they have
    // rights in mode A, and they want to get rights in mode B.  If the return value is true, they
    // will have successfully relinquished the previous rights in exchange for the new without any
    // other threads having taken an incompatable latch mode to the starting mode inbetween (e.g.,
    // S to X guarantees no other thread gets X inbetween).  If false, the caller still has their
    // original rights; it is likely that subsequent calls will still fail.  Note that this can
    // return false spurriously for subtle memory-order related reasons.  Callers must deal with an
    // implementation which is simply "return false".
    //
    // This call may block, but if it blocks it will have the same deadlock properties as the
    // matching acquire call (e.g., acquireX will block forever is someone else calls acquireS
    // without a matching releaseS).
    //
    // Only one of the two tickets will be valid after return, and newTicket's validity is the same
    // as the return value.  (TBD -- maybe the old ticket could still be used for a reaquire?)
    //
    // Users must be able to deal with implementations which always return false.
    bool tryUpgradeQS(const TicketQ &t, TicketS & newTicket) {
        pthread_spin_lock(&lock);
        if (t == writer_count && ! write_held) {
            num_readers++;
            newTicket = 1;
            pthread_spin_unlock(&lock);
            return true;
        } else {
            pthread_spin_unlock(&lock);
            return false;
        }
    }

    bool tryUpgradeQX(const TicketQ &t, TicketX & newTicket) {
        pthread_spin_lock(&lock);
        if (t == writer_count && ! write_held && num_readers==0) {
            write_held = true;
            writer_count++;
            newTicket = 1;
            pthread_spin_unlock(&lock);
            return true;
        } else {
            pthread_spin_unlock(&lock);
            return false;
        }
    }

    bool tryUpgradeSX(const TicketS &t, TicketX & newTicket) {
        pthread_spin_lock(&lock);
        if (num_readers==1) {
            write_held = true;
            writer_count++;
            num_readers--;
            newTicket = 1;
            pthread_spin_unlock(&lock);
            return true;
        } else {
            pthread_spin_unlock(&lock);
            return false;
        }        
    }

    // Downgrade to lower mode -- Functions in form of tryDowngradeAB.  Caller promises that they
    // have rights in mode A, and they want to instead have rights in mode B.  If the return value
    // is true, they will have successfully relinquished the previous rights in exchange for the
    // new without any other threads having taken an incompatable latch mode to the starting mode
    // inbetween (e.g., X to S guarantees no other thread gets X inbetween, or S for that matter).
    // If false, the caller still has their original rights.  This may return false spurriously,
    // and callers must also deal with an implementation which is simply "return false".
    //
    // This call may block, and it has the same deadlock properties as aquiring the matching
    // acquire call.
    //
    // Only one of the two tickets will be valid after the return, and newTicket's validity is the
    // same as the return value.
    //
    // Users must be able to deal with implementations which always return false.
    bool tryDownGradeXS(const TicketX &t, TicketS & newTicket) {
        pthread_spin_lock(&lock);
        num_readers++;
        write_held = false;
        newTicket = 1;
        pthread_spin_unlock(&lock);
        return true;        
    }
    bool tryDownGradeXQ(const TicketX &t, TicketQ & newTicket) {
        pthread_spin_lock(&lock);
        newTicket = writer_count;
        write_held = false;
        pthread_spin_unlock(&lock);
        return true;            
    }
    bool tryDownGradeSQ(const TicketS &t, TicketQ & newTicket) {
        pthread_spin_lock(&lock);
        newTicket = writer_count;
        num_readers--;
        pthread_spin_unlock(&lock);
        return true;
    }

    // Reacquire rights which had been released -- If you give a previously valid ticket for a
    // particular mode, the internal implementation will try to figure out if any other threads had
    // acquired the latch in an incompatible mode since the ticket was issued.  Upon a return of
    // true, the caller will hold the latch in the indicated mode, and can treat it as if they had
    // held that mode continuously.
    //
    // Callers should not use these methods unless they have called release using that ticket.
    //
    // Users must be able to deal with implementations which always return false.
    bool reaquireQ(TicketQ &t) {
        pthread_spin_lock(&lock);
        if (t == writer_count) {
            pthread_spin_unlock(&lock);
            return true;
        } else {
            pthread_spin_unlock(&lock);
            return false;
        }
    }
    bool reaquireS(TicketS &t) {
        // Currently, ticket doesn't have enough info, so we just give up.
        return false;
    }
    bool reaquireX(TicketX &t) {
        // Currently, ticket doesn't have enough info, so we just give up.
        return false;
    }
    
    // TODO:
    //
    // API to allow statically asserting if an implementation has a "useful" implementation of
    // any particular method or mode.
    
    InternalLatch() {
        pthread_spin_init(&lock, PTHREAD_PROCESS_SHARED);
        num_readers = writer_count = 0;
        write_held = false;
    }
    ~InternalLatch() {
        pthread_spin_destroy(&lock);
    }
};

