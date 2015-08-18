#ifndef PAGE_CLEANER_H
#define PAGE_CLEANER_H

#include "w_defines.h"
#include "sm_base.h"
#include "smthread.h"
#include "vid_t.h"
#include "vol.h"
#include "generic_page.h"
#include "logarchiver.h"

#include <vector>

class page_cleaner : public smthread_t {
public:
    page_cleaner (vol_t* _volume, LogArchiver::ArchiveDirectory* _archive);
    ~page_cleaner ();
    void run();

private:
    vol_t* volume;
    LogArchiver::ArchiveDirectory* archive;
    lsn_t current_lsn;
    vector<generic_page> write_buffer;

    void flush_write_buffer();
};


#endif // PAGE_CLEANER_H