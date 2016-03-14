#ifndef PAGE_CLEANER_BASE_H
#define PAGE_CLEANER_BASE_H

#include "smthread.h"

class page_cleaner_base : public smthread_t {
public:
    virtual void wakeup(bool wait = false) = 0;
    virtual void shutdown() = 0;
};

#endif
