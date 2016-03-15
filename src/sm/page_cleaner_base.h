#ifndef PAGE_CLEANER_BASE_H
#define PAGE_CLEANER_BASE_H

#include "smthread.h"

class page_cleaner_base : public smthread_t {
public:
    virtual void run() = 0;
    virtual w_rc_t wakeup_cleaner() = 0;
    virtual w_rc_t force_volume() = 0;
    virtual w_rc_t shutdown() = 0;
};

#endif