#include "w_defines.h"

#define SM_SOURCE

#include "restore.h"
#include "logarchiver.h"
#include "vol.h"
#include "sm_options.h"

RestoreBitmap::RestoreBitmap(size_t size)
    : bits(size, false) // initialize all bits to false
{
}

RestoreBitmap::~RestoreBitmap()
{
}

bool RestoreBitmap::get(size_t i)
{
    spinlock_read_critical_section cs(&mutex);
    return bits.at(i);
}

void RestoreBitmap::set(size_t i)
{
    spinlock_read_critical_section cs(&mutex);
    bits.at(i) = true;
}

RestoreMgr::RestoreMgr(const sm_options& options, ArchiveDirectory* archive,
        vol_t* volume)
    : archive(archive), volume(volume), numRestoredPages(0)
{
    w_assert0(archive);
    w_assert0(volume);

    segmentSize = options.get_int_option("sm_restore_segsize", 1024);
    if (segmentSize <= 0) {
        W_FATAL_MSG(fcINTERNAL,
                << "Restore segment size must be a positive number");
    }

    /**
     * We assume that the given vol_t contains the valid metadata of the
     * volume. If the device is lost in/with a system failure -- meaning that
     * it cannot be properly mounted --, it should contain the metadata of the
     * backup volume. By "metadata", we mean at least the number of pages in
     * the volume, which is required to control restore progress. Note that
     * the number of "used" pages is of no value for restore, because pages
     * may get allocated and deallocated (possibly multiple times) during log
     * replay.
     */
    numPages = volume->num_pages();
}

RestoreMgr::~RestoreMgr()
{
    delete bitmap;
}

size_t RestoreMgr::getSegmentForPid(const shpid_t& pid)
{
    return (size_t) pid / segmentSize;
}

shpid_t RestoreMgr::getPidForSegment(size_t segment)
{
    return shpid_t(segment * segmentSize);
}

bool RestoreMgr::isRestored(const shpid_t& pid)
{
    size_t seg = getSegmentForPid(pid);
    return bitmap->get(seg);
}

bool RestoreMgr::waitUntilRestored(const shpid_t& pid, size_t timeout_in_ms)
{
    // TODO implement
    (void) timeout_in_ms;
    return isRestored(pid);
}

void RestoreMgr::requestRestore(const shpid_t& pid)
{
    // TODO implement
    (void) pid;
}
