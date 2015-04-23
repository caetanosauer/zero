#include "w_defines.h"

#ifndef RESTORE_H
#define RESTORE_H

#include "sm_int_1.h"

class sm_options;
class RestoreBitmap;
class ArchiveDirectory;

/** \brief Class that controls the process of restoring a failed volume
 *
 * \author Caetano Sauer
 */
class RestoreMgr : public smthread_t {
public:
    RestoreMgr(const sm_options&, ArchiveDirectory*, vol_t*);
    virtual ~RestoreMgr();

    /** \brief Returns true if given page is already restored.
     *
     * This method is used to check if a page has already been restored, i.e.,
     * if it can be read from the volume already.
     */
    bool isRestored(const shpid_t& pid);

    /** \brief Request restoration of a given page
     *
     * This method is used by on-demand restore to signal the intention of
     * reading a specific page which is not yet restored. This method simply
     * generates a request with the restore scheduler -- no guarantees are
     * provided w.r.t. when page will be restored.
     */
    void requestRestore(const shpid_t& pid);

    /** \brief Blocks until given page is restored
     *
     * This method will block until the given page ID is restored or the given
     * timeout is reached. It returns false in case of timeout and true if the
     * page is restored. When this method returns true, the caller is allowed
     * to read the page from the volume. This is basically equivalent to
     * polling on the isRestored() method.
     */
    bool waitUntilRestored(const shpid_t& pid, size_t timeout_in_ms);

protected:
    RestoreBitmap* bitmap;
    ArchiveDirectory* archive;
    vol_t* volume;

    /** \brief Number of pages restored so far
     * (must be a multiple of segmentSize)
     */
    size_t numRestoredPages;

    /** \brief Total number of pages in the failed volume
     */
    size_t numPages;

    /** \brief Size of a segment in pages
     *
     * The segment is the unit of restore, i.e., one segment is restored at a
     * time. The bitmap keeps track of segments already restored, i.e., one bit
     * per segment.
     */
    int segmentSize;

    /** \brief Gives the segment number of a certain page ID.
     */
    size_t getSegmentForPid(const shpid_t& pid);

    /** \brief Gives the first page ID of a given segment number.
     */
    shpid_t getPidForSegment(size_t segment);
};

/** \brief Bitmap data structure that controls the progress of restore
 *
 * The bitmap contains one bit for each segment of the failed volume.  All bits
 * are initially "false", and a bit is set to "true" when the corresponding
 * segment has been restored. This class is completely oblivious to pages
 * inside a segment -- it is the callers resposibility to interpret what a
 * segment consists of.
 */
class RestoreBitmap {
public:
    RestoreBitmap(size_t size);
    virtual ~RestoreBitmap();

    bool get(size_t i);
    void set(size_t i);
protected:
    std::vector<bool> bits;
    mcs_rwlock mutex;
};

#endif
