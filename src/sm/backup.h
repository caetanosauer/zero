/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */

#ifndef BACKUP_H
#define BACKUP_H

/**
 * \defgroup SSMBCK Backup Module
 * \brief \b Backup \b Manager manipulates backup files that can be used for recovery from
 * failures.
 * \ingroup SSMAPI
 * \details
 * \section SUMMARY Overview
 * Backup module provides the following functionalities:
 * \li Take a snapshot of the current database.
 * \li Provides the snapshot image of the specified data page.
 * \li Answers whether the specified data page is contained in the backup.
 *
 * \section LIMIT Limitations
 * We so far support a very limited functionalities.
 * \li To take a snapshot, the user has to do manual file copy of the entire volume while
 * database is not running. No API for taking backups, no incremental backup, no whatsover.
 * \li We assume only one version of backup per volume. When the page image in the backup
 * is corrupted, another backup would be useful, but we do not support it so far.
 * \li No serious performance optimization for anything.
 *
 * \section FUTURE Planned Extension
 * Addressing the limitations above would be high-priority.
 */

#include "vid_t.h"
#include "basics.h"
#include <string>


// forward declarations
class generic_page;
class AlignedMemory;
class BackupFile;
class BackupManager;


/**
 * \brief The API class of Backup Manager.
 * \ingroup SSMBCK
 * \details
 * Main class of \ref SSMBCK.
 * We assume the user has created backup files under the backup folder with the following
 * filename: "/backup_" + vid.
 * This means we can manipulate only one backup file per volume.
 * So far, the backup file is a simple file copy of the volume data file.
 * The current implementations simply read the alloc_p area and btree_p area for each
 * API call.
 */
class BackupManager {

public:
    BackupManager(const std::string &backup_folder);
    ~BackupManager();

    /**
     * \brief Tells whether there is a backup file for the volume.
     * @param[in] vid volume ID
     * @return true if the given volume exists as a backup file
     */
    bool    volume_exists(vid_t vid);

    /**
     * \brief Tells whether the given page exists in this backup.
     * This method works only for fixable pages, not stnode_p or alloc_p.
     * @param[in] vid volume ID
     * @param[in] shpid page ID
     * @return true if the given page exists in this backup
     */
    bool    page_exists(vid_t vid, shpid_t shpid);

    /**
     * \brief Retrieve page with shpid from vid.
     * @param[out] page The page image in backup
     * @param[in] vid volume ID
     * @param[in] shpid page ID
     * @pre page_exists(vid, shpid) == true
     */
    w_rc_t  retrieve_page(generic_page &page, vid_t vid, shpid_t shpid);

    /**
     * \brief Returns the expected file path of the backup file for the
     * given volume.
     * @param[in] vid volume ID
     * @return path of the backup file.
     */
    std::string get_backup_path(vid_t vid) const;

    /** Returns path of the backup folder. */
    const std::string& get_backup_folder() const;

private:
    /**
     * \brief This one simply retrieves the page as a byte array, no content checking,
     * so it can be used for any type of data region.
     * @pre file.is_opened() == true
     */
    w_rc_t  _retrieve_page(BackupFile &file, generic_page &page, shpid_t shpid);

    /**
     * path of the backup folder, absolute or relative from working directory.
     */
    std::string _backup_folder;
};

/**
 * \brief helper class to automatically deallocate a page allocated by posix_memalign().
 * \details
 * Maybe become a common class later..
 */
class AlignedMemory {
public:
    AlignedMemory(size_t size);
    ~AlignedMemory();

    size_t  get_size() const;
    char*   get_buffer();
    void    release();

    enum ConstantValue {
        /**
        * Linux memory alignment for posix_memalign() in bytes.
        */
        POSIX_MEM_ALIGNMENT = 4096,
    };

private:
    size_t  _size;
    char*   _buffer;
};

/**
 * \brief Represents a backup file for one volume.
 * \ingroup SSMBCK
 * \details
 * This class uses O_DIRECT to bypass OS bufferpool.
 */
class BackupFile {
public:
    /**
     * Empty constructor that doesn't open the file yet.
     * @param[in] vid volume ID
     * @param[in] path File path of the backup file
     */
    BackupFile(vid_t vid, const std::string &path);
    /** Automatically closes the file if it is opened. */
    ~BackupFile();

    /** Tries to open the backup file for the specified volume. */
    void    open();

    /** Close the backup file if not yet closed. */
    void    close();

    /**
     * \brief Read the given page ID from the backup file.
     * @param[out] buffer Buffer to receive the page
     * @param[in] shpid page ID
     * @pre buffer.get_size() >= sizeof(generic_page)
     */
    w_rc_t  read_page(AlignedMemory& buffer, shpid_t shpid);

    /** Returns if the file exists and is correctly opened. */
    bool    is_opened() const;
    vid_t get_vid() const;

private:
    /** File path of the backup file, relative to the working directory. */
    std::string _path;
    /** Volume ID. */
    vid_t _vid;
    /** Return value of the POSIX open() semantics (e.g., -1 is "invalid"). */
    int     _fd;
};

inline BackupManager::BackupManager(const std::string &backup_folder)
    : _backup_folder(backup_folder) {}
inline BackupManager::~BackupManager() {}
inline const string& BackupManager::get_backup_folder() const { return _backup_folder; }


inline AlignedMemory::~AlignedMemory() { release(); }
inline size_t AlignedMemory::get_size() const { return _size; }
inline char* AlignedMemory::get_buffer() { return _buffer; }

inline BackupFile::BackupFile(vid_t vid, const std::string &path)
    : _path(path), _vid(vid), _fd(-1) {}
inline BackupFile::~BackupFile() { close(); }
inline vid_t BackupFile::get_vid() const { return _vid; }
inline bool BackupFile::is_opened() const { return _fd != -1; }

#endif // BACKUP_H
