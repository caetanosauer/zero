// -*- mode:c++; c-basic-offset:4 -*-
/*<std-header orig-src='shore' incl-file-exclusion='INTERNAL_H'>

 $Id: internal.h,v 1.12 2010/12/08 17:37:33 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

/*  -- do not edit anything above this line --   </std-header>*/

/* This file contains doxygen documentation only */

/**\page IMPLNOTES Implementation Notes
 *
 * \section MODULES Storage Manager Modules
 * The storage manager code contains the following modules (with related C++ classes):
 *
 * - \ref SSMAPI (ss_m) 
 *   Most of the programming interface to the storage manager is encapsulated
 *   in the ss_m class. 
 * - \ref VOL_M (vol_m) and \ref DIR_M (dir_m)
 *   These managers handle volumes, page allocation and stores, which are the
 *   structures underlying files of records, B+-Tree indexes.
 * - \ref BTREE_M (btree_m)
 *   handle the storage structures available to servers.
 * - \ref LOCK_M (lock_m) 
 *   The lock manager is quasi-stand-alone.
 * - \ref XCT_M (xct_t) and * \ref LOG_M (log_m) handle transactions,
 *   logging, and recovery.
 * - \ref BF_M (bf_m)
 *   The buffer manager works closely with \ref XCT_M and \ref LOG_M.
 *
 * \attention
 * \anchor ATSIGN 
 * \htmlonly
 * <b><font color=#B2222>
 * In the storage manager, fixing a page in the buffer pool acquires
 a latch
 and the verbs "fix" and "latch" are used interchangeably here.
 For searching convenience, where latching/fixing occurs the symbol @
 has been inserted.
 * </b></font> 
 * \endhtmlonly
 *
 * \section VOL_M I/O Manager and Volume Manager
 * The I/O manager was, in the early days of SHORE, expected to
 * have more responsibility than it now has; now it is little more
 * than a wrapper for the \ref VOL_M.   
 * For the purpose of this discussion, 
 * the I/O Manager and the volume manager are the same entity.
 * There is a single read-write lock associated 
 * with the I/O-Volume manager to serialize access.  
 * Read-only functions acquire the lock in read mode; updating 
 * functions acquire the lock in write mode.
 *
 * \note
 * Much of the page- and extent-allocation code relies on the fact that
 * access to the manager is serialized, and this lock is a major source of
 * contention.
 *
 * The volume manager handles formatting of volumes,
 * allocation and deallocation of pages and extents in stores.
 * Within a page, allocation of space is up to the manager of the
 * storage structure (btree or file).
 *
 * The following sections describe the \ref VOL_M:
 * - \ref EXTENTSTORE 
 * - \ref STORENODE 
 * - \ref OBJIDS 
 * - \ref STONUMS 
 * - \ref ALLOCEXT 
 * - \ref XAFTERXCT 
 * - \ref IMPLICAT 
 * - \ref ALLOCST 
 * - \ref SAFTERXCT 
 * - \ref ALLOCPG 
 * - \ref VOLCACHES 
 * - \ref PAGES 
 * - \ref RSVD_MODE 
 *
 * \subsection EXTENTSTORE Extents and Stores
 *
 * Files and indexes are types of \e stores. A store is a persistent
 * data structure to which pages are allocated and deallocated, but which
 * is independent of the purpose for which it is used (index or file).
 *
 * Pages are reserved and allocated for a store in units of ss_m::ext_sz 
 * (enumeration value smlevel_0::ext_sz, found in sm_base.h), 
 * a compile-time constant that indicates the size of an extent.
 *
 * An extent is a set of contiguous pages, represented 
 * by a persistent data structure \ref extlink_t.  Extents are
 * linked together to form the entire structure of a store.  
 * The head of this list has a reference to it from a store-node
 * (\ref stnode_t), described below.
 * Extents (extlink_t) are co-located on extent-map pages at 
 * the beginning of the volume. 
 *
 * Each extent has an owner, 
 * which is the store id (\ref snum_t) of the store to which it belongs.
 * Free extents are not linked together; 
 * they simply have no owner (signified by an  \ref extlink_t::owner == 0).
 *
 * An extent id is a number of type \ref extnum_t.  It is arithmetically
 * determined from a page number, and the pages in an extent are arithmetically derived from an extent number. 
 * The \ref extnum_t is used in acquiring locks on
 * extents and it is used for locating the associated \ref extlink_t and the
 * extent-map page on which the \ref extlink_t resides.
 * Scanning the pages in a store can be accomplished by scanning the 
 * list of \ref extlink_t.   
 * 
 * The entire allocation metadata for a page are in its extent, which contains a
 * bitmap indicating which of its pages are allocated.
 * One cannot determine the allocation status of a page from the page 
 * itself: the extent map page must be inspected.
 *
 * \subsection STORENODE Store Nodes
 * A \ref stnode_t holds metadata for a store, including a reference to
 * the first extent in the store. 
 * A store \e always contains at least one allocated extent, even if
 * no pages in that extent are allocated.
 * Scanning the pages in a store can be accomplished by scanning the 
 * list of \ref extlink_t.   
 *
 * Store nodes are co-located on store-map pages at the beginning of a volume, 
 * after the extent maps. The volume is formatted to allow
 * as many store nodes as there are extents.
 *
 * \subsection OBJIDS Object Identifiers, Object Location, and Locks
 *
 * There is a close interaction among various object identifiers,
 * the data structures in which the 
 * objects reside, and the locks acquired on the objects.
 * 
 * Simply put:
 * - a volume identifier (ID) consists of an 
 *   integral number, e.g., 1, represented in an output stream as v(1).
 * - a store identifier consists of a volume ID and a store number, e.g., 3,
 *   represented s(1.3).
 * - an index ID and a file ID are merely store IDs.
 * - a page ID contains a store ID and a page number, e.g., 48, represented
 *   p(1.3.48).
 * - a record ID for a record in a file contains a 
 *   page ID and a slot number, e.g., 2, represented r(1.3.48.2).
 *
 *  Clearly, from a record ID, its page and slot can 
 *  be derived without consulting any indices.  It is 
 *  also clear that records cannot move, which has 
 *  ramifications for \ref RSVD_MODE, described below.
 *  The \ref LOCK_M understands these identifiers as well as extent
 *  IDs, and generates locks from identifiers.  
 *
 * \subsection STONUMS Predefined Stores
 *
 * A volume contains these pre-defined structures:
 * - Header: page 0 (the first page) of the volume; contains :
 *   - a format version #
 *   - the long volume id
 *   - extent size
 *   - number of extents
 *   - number of extents used for store 0 (see below)
 *   - number of pages used for store 0 (see below)
 *   - the first page of the extent map
 *   - the first page of the store map
 *   - page size
 * - store #0 : a "pseudo-store" containing the extent-map and store-map pages. This
 *   starts with page 1 (the second page) of the volume.
 * - store #1 :  directory of the stores (used by the storage manager): this is
 *   a btree index mapping store-number to metadata about the store, 
 *   including (but not limited to) the store's use (btree/file-small-object-pages/file-large-object-pages), 
 *   and, in the case of indices, the root page of the index,
 *   and, in the case of files, the store number of the associated large-object-page store. 
 * - store #2 :  root index (for use by the server)
 *
 * \subsection ALLOCEXT Allocation and Deallocation of Extents 
 *
 * \anchor ALLOCEXTA 
 * Finding an extent to allocate to a store requires
 * searching through the 
 *   extent-map pages for an extent that is both 
 *   unallocated (owner is zero) and not locked. 
 *  - The storage 
 *   manager caches the minimum free extent number with which to start a
 *   search; this number is reset to its static lower bound when the
 *   volume is mounted, meaning that the first extent operation after a
 *   mount starts its search at the head of the volume.
 * - Subsequent searches start the search with the lowest free extent
 *   number.
 * - Extent-map pages are latched as needed for this linear search 
 *   \ref ATSIGN "\@".
 * - The first appropriate extent found is IX-locked.
 *   These locks are explicitly acquired by the lock manager; 
 *   extent locks are not in the lock hierarchy;
 *
 * \anchor ALLOCEXTA2 
 * Allocating a set of extents to a store is a matter of linking
 * them together and then appending the list to the 
 * tail of the store's linked list:
 *  - Locks are \e not acquired for previous and next extents in the list; 
 *    EX-latches protect these structures;
 *  - New extents are always linked in at the tail of the list.
 *  - One extent-map page is fixed at a time 
 *   \ref ATSIGN "\@".  Entire portions of the
 *   extent list that reside on the same extent-map page are 
 *   linked while holding the page latched, and 
 *   logged in a single log record. This is useful only for creating
 *   large objects; all other page-allocations result in allocation of
 *   one or zero extents.
 *
 * Allocation is handled slightly differently in the two contexts 
 * in which it is performed: 
 * - creating a store (see \ref ALLOCST), and 
 * - inserting new pages into an existing store (see \ref ALLOCPG ).
 * These two cases are described in more detail below.
 *
 * Extents are freed:
 * - when a transaction deletes a store and commits, and
 * - when a transaction has deleted the last page in the extent; this involves acquiring an IX lock on the extent, but does not
 * preclude other transactions from allocating pages in the same extent. 
 * Also, since the transaction might abort, the extent must not be re-used 
 * for another store by another transaction. Furthermore, the page-deleting 
 * transaction could re-use the pages.  For these reasons, extents are left
 * in a store until the transaction commits (see \ref XCT_M and \ref XAFTERXCT,
 * and \ref SAFTERXCT). 
 * \anchor ALLOCEXTD1 
 * Thus, deallocating an extent \e before a transaction commits comprises:
 *  - clearing the extent-has-allocated-page bit in the already-held
 *   extent IX lock, and does not involve any page-fixes.
 *
 * \subsection XAFTERXCT Commit-Time Handling of Extent-Deallocation
 * At commit time, the transaction deallocates extents in two contexts:
 * - When destroying stores that were marked for deletion by the transaction,and
 * - While freeing extents marked for freeing by the transaction as a result
 *   of (incremental) page-freeing.
 *
 * For the latter case, the transaction asks the transaction
 * manager to 
 * identify all extents on which it has locks (lock manager's job).  
 * If 
 * - the lock manager can upgrade the extent's lock to EX mode \e and 
 * - the extent still contains no allocated pages, 
 * the lock manager frees the extent.
 * (An optimization avoids excessive page-fixing here: the
 * extent lock contains a bit indicating whether the extent contains any
 * allocated pages.)
 *
 * \anchor ALLOCEXTD2 
 * Deallocating an extent from a store (at transaction-commit) comprises:
 * - Identifying the previous- and next- extent numbers;
 * - Identifying the pages containing the \ref extlink_t structures for the
 *   extent to be freed and for the previous- and next- extent structures,
 *   which may mean as many as three pages;
 * - Sorting the page numbers and EX-latching 
 *   \ref ATSIGN "\@"
 *   the extent-map pages in 
 *   ascending order to avoid latch-latch deadlocks;
 * - Ensuring that the previous- and next- extent numbers on the 
 *   to-be-freed extent have not changed (so that we know that we have 
 *   fixed the right pages);
 *   There should be no opportunity for these links to change since
 *   the volume manager is a monitor (protected by a read-write lock).
 * - Updating the extents and physically logging each of the updates.
 * - Updating caches related to the store.
 *
 * If the extent is in a still-allocated store, the entity freeing
 * the extent (the lock manager) will have acquired an EX lock on
 * the extent for the transaction. If the extent is part of a
 * destroyed store, the store will have an EX lock on it and this will
 * prevent any other transaction from trying to deallocate the extent.
 *
 * \subsection IMPLICAT Implications of This Design
 * This extent-based design has the following implications:
 * - Before it can be used, a volume must be formatted for a given size 
 *   so that the number of extent map pages and store map pages can 
 *   be established;
 * - Extending the volume requires reformatting, so the server is forced to
 *   perform database reorganization in this case;
 * - Location of an extlink_t can be determined arithmetically from a page
 *   number (thus also from a record ID), 
 *   which is cheaper than looking in an index of any sort; however,
 *   this means that
 *   extra work is done to validate the record ID (that is, its 
 *   page's store-membership);
 * - Because the store-membership of a page is immaterial in locating the page,
 *   the buffer-pool manager need not pay any attention to stores; in fact,
 *   it reduces I/O costs by sorting pages by {volume,page number} and writing 
 *   contiguous pages in one system call;
 * - Because the store-membership of an extent is immaterial in locating the
 *   extent, extent locks do not contain a store number, and their locks
 *   can be aquired regardless of their allocation state.  One can
 *   test the "locked" status of an extent prior to allocating it.
 * - Extent-map pages tend to be hot (remain in the buffer pool), which 
 *   minimizes I/O;
 * - Extent-map pages could be a source of latch contention, however
 *   they are only latched in the volume manager, which redirects the
 *   contention to the volume mutex;
 * - The number of page fixes required for finding free extents is bounded 
 *   by the number of extent-map pages on the volume, and in some cases
 *   employs O(n) (linear) searches, as described in the item below;
 * - Pages may be reserved for allocation in a file without being allocated, 
 *   so optimal use of the volume requires that the allocated extents 
 *   be searched before new extents are allocated; 
 * - Deallocating a page and changing store flags (logging attributes)
 *   of a page or store does not require touching the page itself; entire
 *   stores are deallocated by latching and updating only the required
 *   extent-map pages;
 * - The high fan-out of extent-map pages to pages ensures that deallocating
 *   stores is cheap;
 * - Clustering of pages is achieved, which is useful for large objects and
 *   can be helpful for file scans;
 * - Prefetching of file pages can be achieved by inspection of extent maps;
 * - Files need not impose their own structure on top of stores: store
 *   order is file order; the fact that the storage manager avoids 
 *   superimposing a file structure has \ref FILERAMIFICATIONS "ramifications of its own".
 *
 *
 * The volume layer does not contain any means of spreading out or clustering
 * extents over extent-map pages for clustering (or for latch-contention 
 * mitigation).
 *
 * \subsection ALLOCST Creating and Destroying Stores
 *
 * For each store  the storage manager keeps certain metadata about the store
 * in a \e directory, which is an index maintained by the \ref DIR_M.
 * 
 * \anchor STORECREATE
 * Creating a store comprises:
 * - Finding an unused store number. This is a linear search through the
 *   store node map pages for a stnode_t (one with no associated extent list)
 *   that is not locked. The linear search starts at a revolving location.
 *   In the worst case, it will search all the stnode_t and therefore
 *   fix 
 *   \ref ATSIGN "\@"
 *   all the store map pages;
 * - Aquiring a store lock in EX mode, long-duration;
 * - Finding an extent for the store (details \ref ALLOCEXTA "here");
 * - Updating the stnode_t 
 *   \ref ATSIGN "\@"
 *   to reserve it (legitimize the store number);
 * - Logging the creation store operation without the first extent;
 * - Allocating the extent to the store (details \ref ALLOCEXTA2 "here")
 *   (we cannot allocate an extent without a legitimate owning store to which
 *   to allocate it);
 * - Updating the stnode_t to add the first extent 
 *   \ref ATSIGN "\@";
 * - Logging the store operation to add the first exent to the store.
 *
 * Destroying a store before transaction-commit comprises these steps:
 * - Verify that the store number is a valid number (no latches required);
 * - Mark the store as deleted:
 *   - Latch the store map page (that holds the stnode_t) for the store 
 *   \ref ATSIGN "\@";
 *   - EX-lock the store (long duration);
 *   - Mark the stnode_t as "t_deleting_store" 
 *   \ref ATSIGN "\@"
 *   (meaning it is to be
 *   deleted at end-of-transaction, see \ref XCT_M);
 *   - Log this marking store operation;
 *   - Add the store to a list of stores to free when the transaction 
 *   commits (See \ref XCT_M);
 * - Clear caches related to the store.
 *
 * \subsection SAFTERXCT Commit-Time Handling of Store-Destruction
 * Removing a store (at commit time) marked for deletion comprises these steps:
 * - Verify that the store is still marked for deletion (partial rollback
 *   does not inspect the list of stores to delete, and in any case, this
 *   has to be one on restart because the list is transient) 
 *   \ref ATSIGN "\@",
 * - Update the stnode_t to indicate that the store's extents are about to
 *   be deallocated 
 *   \ref ATSIGN "\@";
 * - Log the above store operation for crash recovery;
 * - Free (really) the extents in the store 
 *   \ref ATSIGN "\@";
 * - Update the stnode_t to clear its first extent 
 *   \ref ATSIGN "\@";
 * - Log the store as having been deleted in toto;
 * - Clear cached information for this store.
 *
 * \subsection ALLOCPG Allocation and Deallocation of Pages
 *
 * Allocating an extent to a store does not make its pages "visible" to the
 * server. They are considered "reserved".
 * Pages within the extent have to be allocated 
 * (their bits in the extent's bitmap must be set).  
 *
 * When the store is used for an index, the page is not 
 * visible until it has been formatted
 * and inserted (linked) into the index.  
 * In the case of files, however,
 * the situation is complicated by the lack of linkage of file pages by
 * another means.  Pages used for large objects are referenced through an
 * index in the object's metadata, but pages used 
 * for small objects become part of the
 * file when the 
 * page's extent bitmap indicates that it is
 * allocated. 
 *
 * \anchor ALLOCPGTOSTORE
 * Allocating a page to a store comprises these steps
 * (in fact, the code is
 * written to allocate a number of pages at once; this description is 
 * simplified):
 * - Locate a reserved page in the store
 *   - if the page must be \e appended to the store, special precautions are
 *   needed to ensure that the reserved page is the next unallocated page 
 *   in the last extent of the store 
 *   \ref ATSIGN "\@";
 *   - if the page need not be appended, any reserved page will do
 *      - look in the cache for extents with reserved pages
 *      - if none found \e and we are not in append-only context,
 *      search the file's extents for reserved pages 
 *   \ref ATSIGN "\@";
 *      (This can be disabled by changing the value 
 *      of the constant \e never_search in sm_io.cpp.)
 * - If no reserved pages are found, find and allocate an extent 
 *   (details \ref ALLOCEXTA "here");
 * - Acquire an IX lock on the extent in which we found reserved page(s)
 * - Find a reserved page in the given extent that has \e no \e lock on it
 *   (if no such thing exists, skip this extent and find another
 *   \ref ATSIGN "\@");
 * - Acquire a lock on the available page 
 *   (mode IX or EX, and duration depend on the context)
 *   - btree index manager uses EX mode, instant duration , meaning that
 *   deallocated pages can be re-used;
 * - Call back to (file or index) manager to accept or reject this page:
 *   - file manager allocating a small-record file page
 *   fixes 
 *   \ref ATSIGN "\@"
 *   the page (for formatting) and returns "accept";
 *   - file manager allocating a large-record page just returns "accept";
 *   - btree index managers just return "accept";
 * - Log the page allocation, 
 *   set "has-page-allocated" indicator in the extent lock.
 *
 * As mentioned above, there are times when the volume manager is told to
 * allocate new pages at the end of the store (append).  This happens 
 * when the file manager allocates small-object file pages unless the
 * caller passes in the policy t_compact, indicating that it should search
 * the file for available pages.
 * The server can choose its policy when calling \ref ss_m::create_rec
 * (see \ref pg_policy_t).
 * When the server uses \ref append_file_i, only the policy t_append
 * is used, which enforces append-only page allocation.
 *
 * Deallocating a page in a store comprises these steps:
 * - Acquire a long-duration EX lock on the page;
 * - Verify the store-membership of the page if required to do so (by
 *   the file manager in cases in which it was forced to  unfix
 *   and  fix 
 *   \ref ATSIGN "\@"
 *   the page);
 * - Acquire a long-duration IX lock on the page's extent;
 * - Fix 
 *   \ref ATSIGN "\@"
 *   the extent-map page and update the extent's bitmap, log the update. 
 * - If the extent now contains only reserved pages, mark the extent as
 *   removable (clear the extent-has-allocated-page bit in the already-held
 *   IX lock).
 *
 *
 * \subsection PAGES Page Types
 * Pages in a volume come in a variety of page types, all the same size.
 * The size of a page is a compile-time constant.  It is controlled by
 * a build-time configuration option (see 
 * \ref CONFIGOPT). the default page size is 8192 bytes.
 *
 * All pages are \e slotted (those that don't need the slot structure  FIXME!
 * may use only one slot) and have the following layout:
 * - header, including
 *   - lsn_t log sequence number of last page update
 *   - page id
 *   - links to next and previous pages (used by some storage structures)
 *   - page tag (indicates type of page)
 *   - store flags (logging level metadata)
 * - slots (grow down)
 * - slot table array of pointers to the slots (grows up)
 * - footer (copy of log sequence number of last page update)
 *
 * Each page type is a C++ class that derives from the base class
 * generic_page_header.  generic_page_header contains headers that are
 * common to all page types. The types are as follows:
 *
 * - generic_page : 
 * - alloc_page   : free-page allocation pages, used by vol_m
 * - stnode_page  : store-node pages, used by vol_m
 * - btree_page   :
 *
 * Issues specific to the page types will be dealt with in the descriptions of the modules that use them.
 * 
 * \subsection FREESPACE Summary of Free-Space Management
 *
 * Free space is managed in several ways:
 * - Free extents have no owner (persistent datum in the extlink_t).
 * - Allocated extents with free pages are cached by the 
 *   volume manager (transient data).
 * - Allocated extents contain a map of the free-space buckets to which its
 *   pages belong for file pages (persistent, in the extlink_t).
 * - The volume manager keeps caches the last page in a file (transient).
 * - The volume manager keeps a cache of extents in a store that
 *   contain reserved pages (transient).
 * - The volume manager caches the lowest unallocated extent in a volume (transient).
 *
 * \section BTREE_M B+-Tree Manager
 *
 * The values associated with the keys are opaque to the storage
 * manager, except when IM (Index Management locking protocol) is used, 
 * in which case the value is
 * treated as a record ID, but no integrity checks are done.  
 * It is the responsibility of the server to see that the value is
 * legitimate in this case.
 *
 * The implementation of B-trees is straight from the Mohan ARIES/IM
 * and ARIES/KVL papers. See \ref MOH1, which covers both topics.
 *
 * Those two papers give a thorough explanation of the arcane algorithms,
 * including logging considerations.  
 * Anyone considering changing the B-tree code is strongly encouraged 
 * to read these papers carefully.  
 * Some of the performance tricks described in these papers are 
 * not implemented here.  
 * For example, the ARIES/IM paper describes performance of logical 
 * undo of insert operations if and only if physical undo 
 * is not possible.  
 * The storage manager always undoes inserts logically.
 *
 * \bug GNATS 137 Latches can now be downgraded; btree code should use this.
 *
 * \section DIR_M Directory Manager
 * All storage structures created by a server
 * have entries in a B+-Tree index called the 
 * \e store \e directory or just \e directory.
 * This index is not exposed to the server.
 *
 * The storage manager maintains  some transient and some persistent data
 * for each store.  The directory's key is the store ID, and the value it
 * returns from a lookup is a
 * sdesc_t ("store descriptor") structure, which
 * contains both the persistent and transient information. 
 *
 * The persistent information is in a sinfo_s structure; the 
 * transient information is resident only in the cache of sdesc_t 
 * structures that the directory manager
 * maintains.
 *
 * The metadata include:
 * - what kind of storage structure uses this store  (btree, file)
 * - if a B-tree, is it unique and what kind of locking protocol does it use?
 * - what stores compose this storage structure (e.g., if file, what is the
 *   large-object store and what is the small-record store?)
 * - what is the root page of the structure (if an index)
 * - what is the key type if this is an index
 *
 * \section LOCK_M Lock Manager
 *
 * The lock manager understands the folling kind of locks
 * - volume
 * - store
 * - keyrange
 * - none
 *
 * Lock requests are issued with a lock ID (lockid_t), which
 * encodes the identity of the entity being locked, the kind of
 * lock, and, by inference,  a lock hierarchy for a subset of the
 * kinds of locks above.
 * The lock manager does not insist that lock identifiers 
 * refer to any existing object.  
 *
 * \subsection LOCK_M_SM Lock Acquisition and Release by Storage Manager
 * Locks are acquired by storage manager operations as appropriate to the
 * use of the data (read/write). (Update locks are not acquired by the
 * storage manager.)
 *
 * The storage manager's API allows explicit acquisition 
 * of locks by a server.    User modes user1, user2, user3 and user4 are provided for that purpose.
 *
 * Freeing locks is automatic at transaction commit and rollback.  
 *
 * There is limited support for freeing locks in the middle of 
 * a transaction:
 * - locks of duration less than t_long can be unlocked with unlock()
 *
 * \subsection LCACHE Lock Cache
 * To avoid expensive lock manager queries, each transaction 
 * keeps a cache of the last \<N\> locks acquired (the number
 * \<N\> is a compile-time constant).
 * This close association between the transaction manager and
 * the lock manager is encapsulated in several classes in the file lock_x.
 *
 * \subsection DLD Deadlock Detection
 * The lock manager uses a statistical deadlock-detection scheme
 * known as "Dreadlocks" [KH1].
 * Each storage manager thread (smthread_t) has a unique fingerprint, which is
 * a set of bits; the deadlock detector ORs together the bits of the
 * elements in a waits-for-dependency-list; each thread, when 
 * blocking, holds a  digest (the ORed bitmap).  
 * It is therefore cheap for a thread to detect a cycle when it needs to 
 * block awaiting a lock: look at the holders
 * of the lock and if it finds itself in any of their digests, a
 * cycle will result.
 * This works well when the total number of threads relative to the bitmap
 * size is such that it is possible to assign a unique bitmap to each
 * thread.   
 * If you cannot do so, you will have false-positive deadlocks
 * "detected".
 * The storage manager counts, in its statistics, the number of times
 * it could not assign a unique fingerprint to a thread.  
 * If you notice excessive transaction-aborts due to false-positive
 * deadlocks,
 * you can compile the storage manager to use a larger
 * number bits in the 
 * \code sm_thread_map_t \endcode
 * found in 
 * \code smthread.h \endcode.
 *
 * \section XCT_M Transaction Manager
 * When a transaction commits, these steps are taken to
 * manage stores:
 * - Stores that were given sm_store_property_t t_load_file or 
 *   t_insert_file are turned * into t_regular stores;
 * - The transaction enters a state called "freeing space" so that
 *   stores marked for deletion can be handled properly in event of
 *   a crash/restart before the transaction logs it commit-completion;
 * - Stores marked for deletion are removed (see \ref SAFTERXCT);
 * - Extents marked for freeing are freed. These are extents marked for
 *   freeing in stores that were not marked for deletion; rather,
 *   these are extents that are marked for deletion due to 
 *   incremental freeing of their pages.
 *
 * Because these are logged actions, and they occur if and only if the 
 * transaction commits, the storage manager guarantees that the ending
 * of the transaction and re-marking and deletion of stores is atomic.
 * This is accomplished by putting the transaction into a state
 * xct_freeing_space, and writing a log record to that effect.
 * The space is freed, the stores are converted, and a final log record is written before the transaction is truly ended.
 * In the vent of a carash while a transaction is freeing space, 
 * recovery searches all the 
 * store metadata for stores marked for deleteion
 * and deletes those that would otherwise have been missed in redo.
 *
 * \section LOG_M Log Manager
 *
 * \subsection LOG_M_USAGE How the Server Uses the Log Manager
 *
 * Log records for redoable-undoable operations contain both the 
 * redo- and undo- data, hence an operation never causes two 
 * different log records to be written for redo and for undo.  
 * This, too, controls logging overhead.  
 *
 * The protocol for applying an operation to an object is as follows:
 * - Lock the object.
 * - Fix the page(s) affected in exclusive mode.
 * - Apply the operation.
 * - Write the log record(s) for the operation.
 * - Unfix the page(s).
 *
 * The protocol for writing log records is as follows:
 * - Grab the transaction's log buffer in which the last log record is to be 
 *   cached by calling xct_t::get_logbuf()
 *   - Ensure that we have reserved enough log space for this transaction 
 *   to insert the desired log record an to undo it. This is done by
 *   by passing in 
 *   the type of the log record we are about to insert, and by using a
 *   "fudge factor" (multiplier) associated with the given log record type.
 *   The fudge factor indicates on average, how many bytes tend to be needed to undo the action being logged.
 * - Write the log record into the buffer (the idiom is to construct it
 *      there using C++ placement-new).
 * - Release the buffer with xct_t::give_logbuf(),
 *    passing in as an argument the fixed page that was affected
 *    by the update being logged.  This does several things: 
 *    - writes the transaction ID, previous LSN for this transaction 
 *      into the log record
 *    - inserts the record into the log and remembers this record's LSN
 *    - marks the given page dirty.
 *
 * Between the time the xct log buffer is grabbed and the time it is
 * released, the buffer is held exclusively by the one thread that
 * grabbed it, and updates to the xct log buffer can be made freely.
 * (Note that this per-transaction log buffer is unrelated to the log buffer
 * internal to the log manager.)
 *
 * During recovery, no logging is done in analysis or redo phases; only during
 * the undo phase are log records inserted.  Log-space reservation is not
 * needed until recovery is complete; the assumption is that if the
 * transaction had enough log space prior to recovery, it has enough space
 * during recovery.
 * Prepared transactions pose a challenge, in that they are not resolved until
 * after recovery is complete. Thus, when a transaction-prepare is logged,
 * the log-space-reservations of that transaction are logged along with the rest of the transaction state (locks, coordinator, etc.) and before 
 * recovery is complete, these transactions acquire their prior log-space
 * reservations.
 *
 * The above protocol is enforced by the storage manager in helper
 * functions that create log records; these functions are generated
 * by Perl scripts from the source file logdef.dat.  (See \ref LOGRECS.)
 *
 * The file logdef.dat also contains the fudge factors for log-space
 * reservation. These factors were experimentally determined.
 * There are corner cases involving btree page SMOs (structure-modification operations), in which the
 * fudge factors will fail.  [An example is when a transaction aborts after
 * having removed entries, and after other transactions have inserted
 * entries; the aborting transaction needs to re-insert its entries, which 
 * now require splits.]
 * The storage manager has no resolution for this.
 * The fudge factors handle the majority of cases without reserving excessive
 * log-space.
 * \bug GNATS 156  Btree SMOs during rollback can cause problems.
 *
 *\subsection LOGRECS Log Record Types
 * The input to the above-mentioned Perl script is the source of all
 * log record types.  Each log record type is listed in the  file
 * \code logdef.dat \endcode
 * which is fairly self-explanatory, reproduced here:
 * \include logdef.dat
 *
 * The bodies of the methods of the class \<log-rec-name\>_log
 * are hand-written and reside in \code logrec.cpp \endcode.
 *
 * Adding a new log record type consists in adding a line to
 * \code logdef.dat, \endcode
 * adding method definitions to 
 * \code logrec.cpp, \endcode
 * and adding the calls to the free function log_<log-rec-name\>(args)
 * in the storage manager.
 * The base class for every log record is logrec_t, which is worth study
 * but is not documented here.
 *
 * Some logging records are \e compensated, meaning that the 
 * log records are skipped during rollback. 
 * Compensations may be needed because some operation simply cannot
 * be undone.  The protocol for compensating actions is as follows:
 * - Fix the needed pages.
 * - Grab an \e anchor in the log.  
 *   This is an LSN for the last log record written for this transaction.
 * - Update the pages and log the updates as usual.
 * - Write a compensation log record (or piggy-back the compensation on
 *   the last-written log record for this transaction to reduce 
 *   logging overhead) and free the anchor.
 *
 * \note Grabbing an anchor prevents all other threads in a multi-threaded
 * transaction from gaining access to the transaction manager.  Be careful
 * with this, as it can cause mutex-latch deadlocks where multi-threaded
 * transactions are concerned.  In other words, two threads cannot concurrently
 * update in the same transaction.
 *
 * In some cases, the following protocol is used to avoid excessive
 * logging by general update functions that, if logging were turned
 * on, would generate log records of their own.
 * - Fix the pages needed in exclusive mode.
 * - Turn off logging for the transaction.
 * - Perform the updates by calling some general functions.  If an error occurs, undo the updates explicitly.
 * - Turn on logging for the transaction.
 * - Log the operation.  If an error occurs, undo the updates with logging turned off..
 * - Unfix the pages.
 *
 * The mechanism for turning off logging for a transaction is to
 * construct an instance of xct_log_switch_t.
 *
 * When the instance is destroyed, the original logging state
 * is restored.  The switch applies only to the transaction that is 
 * attached to the thread at the time the switch instance is constructed, 
 * and it prevents other threads of the transaction from using 
 * the log (or doing much else in the transaction manager) 
 * while the switch exists.
 *
 * \subsection LOG_M_INTERNAL Log Manager Internals
 *
 * The log is a collection of files, all in the same directory, whose
 * path is determined by a run-time option.
 * Each file in the directory is called a "log file" and represents a
 * "partition" of the log. The log is partitioned into files to make it 
 * possible to archive portions of the log to free up disk space.
 * A log file has the name \e log.\<n\> where \<n\> is a positive integer.
 * The log file name indicates the set of logical sequence numbers (lsn_t)
 * of log records (logrec_t) that are contained in the file.  An
 * lsn_t has a \e high part and a \e low part, and the
 * \e high part (a.k.a., \e file part) is the \<n\> in the log file name.
 *
 * The user-settable run-time option sm_logsize indicates the maximum 
 * number of KB that may be opened at once; this, in turn, determines the
 * size of a partition file, since the number of partition files is
 * a compile-time constant.
 * The storage manager computes partition sizes based on the user-provided
 * log size, such that partitions sizes are a convenient multiple of blocks
 * (more about which, below).
 *
 * A new partition is opened when the tail of the log approaches the end
 * of a partition, that is, when the next insertion into the log
 * is at an offset larger than the maximum partition size.  (There is a
 * fudge factor of BLOCK_SIZE in here for convenience in implementation.)
 * 
 * The \e low part of an lsn_t represents the byte-offset into the log file
 * at which the log record with that lsn_t sits.
 *
 * Thus, the total file size of a log file \e log.\<n\>
 * is the size of all log records in the file, 
 * and the lsn_t of each log record in the file is
 * lsn_t(\<n\>, <byte-offset>) of the log record within the file.
 *
 * The log is, conceptually, a forever-expanding set of files. The log 
 * manager will open at most PARTITION_COUNT log files at any one time.
 * - PARTITION_COUNT = smlevel_0::max_openlog
 * - smlevel_0::max_openlog (sm_base.h) = SM_LOG_PARTITIONS
 * - SM_LOG_PARTITIONS a compile-time constant (which can be overridden in 
 *   config/shore.def).
 *
 * The log is considered to have run out of space if logging requires that
 * more than smlevel_0::max_openlog partitions are needed.
 * Partitions are needed only as long as they contain log records 
 * needed for recovery, which means:
 * - log records for pages not yet made durable (min recovery lsn)
 * - log records for uncommitted transactions (min xct lsn)
 * - log records belonging to the last complete checkpoint
 *
 * Afer a checkpoint is taken and its log records are durable,
 * the storage manager tries to scavenge all partitions that do not
 * contain necessary log records.  The buffer manager provides the
 * min recovery lsn; the transaction manager provides the min xct lsn,
 * and the log manager keeps track of the location of the last 
 * completed checkpoint in its master_lsn.  Thus the minimum of the
 * 
 * \e file part of the minmum of these lsns indicates the lowest partition 
 * that cannot be scavenged; all the rest are removed.
 *
 * When the log is in danger of runing out of space 
 * (because there are long-running  transactions, for example) 
 * the server may be called via the
 * LOG_WARN_CALLBACK_FUNC argument to ss_m::ss_m.  This callback may
 * abort a transaction to free up log space, but the act of aborting
 * consumes log space. It may also archive a log file and remove it.
 * If the server provided a
 * LOG_ARCHIVED_CALLBACK_FUNC argument to ss_m::ss_m, this callback
 * can be used to retrieve archived log files when needed for
 * rollback.
 * \warning This functionality is not complete and has not been
 * well-tested.
 *
 * Log files (partitions) are written in fixed-sized blocks.  The log
 * manager pads writes, if necessary, to make them BLOCK_SIZE. 
 * - BLOCK_SIZE = 8192, a compile-time constant.
 *
 * A skip_log record indicates the logical end of a partition.
 * The log manager ensures that the last log record in a file 
 * is always a skip_log record. 
 *
 * Log files (partitions) are composed of segments. A segment is
 * an integral number of blocks.
 * - SEGMENT_SIZE = 128*BLOCK_SIZE, a compile-time constant.
 *
 * The smallest partition is one segment plus one block, 
 * but may be many segments plus one block. The last block enables
 * the log manager to write the skip_log record to indicate the
 * end of the file.
 *
 * The partition size is determined by the storage manager run-time option,
 * sm_logsize, which determines how much log can be open at any time,
 * i.e., the combined sizes of the PARTITION_COUNT partitions.
 *
 * The maximum size of a log record (logrec_t) is 3 storage manager pages.
 * A page happens to match the block size but the two compile-time
 * constants are not inter-dependent. 
 * A segment is substantially larger than a block, so it can hold at least
 * several maximum-sized log records, preferably many.
 * 
 * Inserting a log record consists of copying it into the log manager's
 * log buffer (1 segment in size).  The buffer wraps so long as there
 * is room in the partition.  Meanwhile, a log-flush daemon thread
 * writes out unflushed portions of the log buffer. 
 * The log daemon can lag behind insertions, so each insertion checks for
 * space in the log buffer before it performs the insert. If there isn't
 * enough space, it waits until the log flush daemon has made room.
 *
 * When insertion of a log record would wrap around the buffer and the
 * partition has no room for more segments, a new partition is opened,
 * and the entire newly-inserted log record will go into that new partition.
 * Meanwhile, the log-flush daemon will see that the rest of the log
 * buffer is written to the old partition, and the next time the
 * log flush daemon performs a flush, it will be flushing to the
 * new partition.
 *
 * The bookkeeping of the log buffer's free and used space is handled
 * by the notion of \e epochs.
 * An epoch keeps track of the start and end of the unflushed portion
 * of the segment (log buffer).  Thus, an epoch refers to only one
 * segment (logically, log buffer copy within a partition).
 * When an insertion fills the log buffer and causes it to wrap, a new
 * epoch is created for the portion of the log buffer representing
 * the new segment, and the old epoch keeps track of the portion of the 
 * log buffer representing the old segment.  The inserted log record
 * usually spans the two segements, as the segments are written contiguously
 * to the same log file (partition).
 *
 * When an insertion causes a wrap and there is no more room in the
 * partition to hold the new segment, a new 
 * epoch is created for the portion of the log buffer representing
 * the new segment, and the old epoch keeps track of the portion of the 
 * log buffer representing the old segment, as before.  
 * Now, however, the inserted log record is inserted, in its entirety,
 * in the new segment.  Thus, no log record spans partitions.
 *
 * Meanwhile, the log-flush buffer knows about the possible existence of
 * two epochs.  When an old epoch is valid, it flushes that epoch.
 * When a new epoch is also valid, it flushes that new one as well.
 * If the two epochs have the same target partition, the two flushes are
 * done with a single write.
 *
 * The act of flushing an epoch to a partition consists in a single
 * write of a size that is an even multiple of BLOCK_SIZE.  The
 * flush appends a skip_log record, and zeroes as needed, to round out the
 * size of the write.  Writes re-write portions of the log already
 * written, in order to overwrite the skip_log record at the tail of the
 * log (and put a new one at the new tail).
 *
 *
 *\subsection RECOV Recovery
 * The storage manager performs ARIES-style logging and recovery.
 * This means the logging and recovery system has these characteristics:
 * - uses write-ahead logging (WAL)
 * - repeats history on restart before doing any rollback 
 * - all updates are logged, including those performed during rollback
 * - compensation records are used in the log to bound the amount
 *   of logging done for rollback 
 *   and guarantee progress in the case of repeated 
 *   failures and restarts.
 *
 * Each time a storage manager (ss_m class) is constructed, the logs
 * are inspected, the last checkpoint is located, and its lsn is
 * remembered as the master_lsn, then recovery is performed.
 * Recovery consists of three phases: analysis, redo and undo.
 *
 *\subsubsection RECOVANAL Analysis
 * This pass analyzes the log starting at the master_lsn, and
 *   reading log records written thereafter.  Reading the log records for the
 *   last completed checkpoint, it reconstructs the transaction table, the
 *   buffer-pool's dirty page table, and mounts the devices and
 *   volumes that were mounted at the time of the checkpoint.
 *   From the dirty page table, it determines the \e redo_lsn, 
 *   the lowest recovery lsn of the dirty pages, which is 
 *   where the next phase of recovery must begin.
 *
 *\subsubsection RECOVREDO Redo
 * This pass starts reading the log at the redo_lsn, and, for each
 *   log record thereafter, decides whether that log record's 
 *   work needs to be redone.  The general protocol is:
 *   - if the log record is not redoable, it is ignored
 *   - if the log record is redoable and contains a page ID, the
 *   page is inspected and its lsn is compared to that of the log
 *   record. If the page lsn is later than the log record's sequence number,
 *   the page does not need to be updated per this log record, and the
 *   action is not redone.
 *
 *\subsubsection RECOVUNDO Undo
 *  After redo,  the state of the database matches that at the time 
 *  of the crash.  Now the storage manager rolls back the transactions that 
 *  remain active.  
 *  Care is taken to undo the log records in reverse chronological order, 
 *  rather than allowing several transactions to roll back 
 *  at their own paces.  This is necessary because some operations 
 *  use page-fixing for concurrency-control (pages are protected 
 *  only with latches if there is no page lock in
 *  the lock hierarchy -- this occurs when 
 *  logical logging and high-concurrency locking are used, 
 *  in the B-trees, for example.  A crash in the middle of 
 * a compensated action such as a page split must result in 
 * the split being undone before any other operations on the 
 * tree are undone.). 
 * \bug GNATS 49 (performance) There is no concurrent undo.
 *
 * After the storage manager has recovered, control returns from its
 * constructor method to the caller (the server).
 * There might be transactions left in prepared state.  
 * The server is now free to resolve these transactions by 
 * communicating with its coordinator. 
 *
 *\subsection LSNS Log Sequence Numbers
 *
 * Write-ahead logging requires a close interaction between the
 * log manager and the buffer manager: before a page can be flushed
 * from the buffer pool, the log might have to be flushed.
 *
 * This also requires a close interaction between the transaction
 * manager and the log manager.
 * 
 * All three managers understand a log sequence number (lsn_t).
 * Log sequence numbers serve to identify and locate log records
 * in the log, to timestamp pages, identify timestamp the last
 * update performed by a transaction, and the last log record written
 * by a transaction.  Since every update is logged, every update
 * can be identified by a log sequence number.  Each page bears
 * the log sequence number of the last update that affected that
 * page.
 *
 * A page cannot be written to disk until  the log record with that
 * page's lsn has been written to the log (and is on stable storage).
 * A log sequence number is a 64-bit structure,  with part identifying
 * a log partition (file) number and the rest identifying an offset within the file. 
 *
 * \subsection LOGPART Log Partitions
 *
 * The log is partitioned to simplify archiving to tape (not implemented)
 * The log comprises 8 partitions, where each partition's
 * size is limited to approximately 1/8 the maximum log size given
 * in the run-time configuration option sm_logsize.
 * A partition resides in a file named \e log.\<n\>, where \e n
 * is the partition number.
 * The configuration option sm_logdir names a directory 
 * (which must exist before the storage manager is started) 
 * in which the storage manager may create and destroy log files.
 *
 *  The storage manger may have at most 8 active partitions at any one time.  
 *  An active partition is one that is needed because it 
 *  contains log records for running transactions.  Such partitions 
 *  could (if it were supported) be streamed to tape and their disk 
 *  space reclaimed.  Space is reclaimed when the oldest transaction 
 *  ends and the new oldest transaction's first log record is 
 *  in a newer partition than that in which the old oldest 
 *  transaction's first log record resided.  
 *  Until tape archiving is implemented, the storage 
 *  manager issues an error (eOUTOFLOGSPACE) 
 *  if it consumes sufficient log space to be unable to 
 *  abort running transactions and perform all resulting necessary logging 
 *  within the 8 partitions available. 
 * \note Determining the point at which there is insufficient space to
 * abort all running transactions is a heuristic matter and it
 * is not reliable.  The transaction "reserves" log space for rollback, meaning
 * that no other transaction can consume that space until the transaction ends.'
 * A transaction has to reserve significantly more space to roll back than it
 * needs for forward processing B-tree deletions; this is because the log overhead
 * for the insertions is considerably larger than that for deletion.
 * The (compile-time) page size is also a factor in this heuristic.
 *
 * Log records are buffered by the log manager until forced to stable 
 * storage to reduce I/O costs.  
 * The log manager keeps a buffer of a size that is determined by 
 * a run-time configuration option.  
 * The buffer is flushed to stable storage when necessary.  
 * The last log in the buffer is always a skip log record, 
 * which indicates the end of the log partition.
 *
 * Ultimately, archiving to tape is necessary.  The storage manager
 * does not perform write-aside or any other work in support of
 * long-running transactions.
 *
 * The checkpoint manager chkpt_m sleeps until kicked into action
 * by the log manager, and when it is kicked, it takes a checkpoint, 
 * then sleeps again.  Taking a checkpoint amounts to these steps:
 * - Write a chkpt_begin log record.
 * - Write a series of log records recording the mounted devices and volumes..
 * - Write a series of log records recording the mounted devices.
 * - Write a series of log records recording the buffer pool's dirty pages.
 *    For each dirty page in the buffer pool, the page id and its recovery lsn 
 *    is logged.  
 *    \anchor RECLSN
 *    A page's  recovery lsn is metadata stored in the buffer 
 *    manager's control block, but is not written on the page. 
 *    It represents an lsn prior to or equal to the log's current lsn at 
 *    the time the page was first marked dirty.  Hence, it
 *    is less than or equal to the LSN of the log record for the first
 *    update to that page after the page was read into the buffer
 *    pool (and remained there until this checkpoint).  The minimum
 *    of all the  recovery lsn written in this checkpoint 
 *    will be a starting point for crash-recovery, if this is 
 *    the last checkpoint completed before a crash.
 * - Write a series of log records recording the states of the known 
 *    transactions, including the prepared transactions.  
 * - Write a chkpt_end log record.
 * - Tell the log manage where this checkpoint is: the lsn of the chkpt_begin
 *   log record becomes the new master_lsn of the log. The master_lsn is
 *   written in a special place in the log so that it can always be 
 *   discovered on restart.
 *
 *   These checkpoint log records may interleave with other log records, making
 *   the checkpoint "fuzzy"; this way the world doesn't have to grind to
 *   a halt while a checkpoint is taken, but there are a few operations that
 *   must be serialized with all or portions of a checkpoint. Those operations
 *   use mutex locks to synchronize.  Synchronization of operations is
 *   as follows:
 *   - Checkpoints cannot happen simultaneously - they are serialized with
 *   respect to each other.
 *   - A checkpoint and the following are serialized:
 *      - mount or dismount a volume
 *      - prepare a transaction
 *      - commit or abort a transaction (a certain portion of this must
 *        wait until a checkpoint is not happening)
 *      - heriocs to cope with shortage of log space
 *   - The portion of a checkpoint that logs the transaction table is
 *     serialized with the following:
 *      - operations that can run only with one thread attached to
 *        a transaction (including the code that enforces this)
 *      - transaction begin, end
 *      - determining the number of active transactions
 *      - constructing a virtual table from the transaction table
 *
 * \section BF_M Buffer Manager
 * The buffer manager is the means by which all other modules (except
 * the log manager) read and write pages.  
 * A page is read by calling bf_m::fix.
 * If the page requested cannot be found in the buffer pool, 
 * the requesting thread reads the page and blocks waiting for the 
 * read to complete.
 *
 * All frames in the buffer pool are the same size, and 
 * they cannot be coalesced, 
 * so the buffer manager manages a set of pages of fixed size.
 *
 * \subsection BFHASHTAB Hash Table
 * The buffer manager maintains a hash table mapping page IDs to
 * buffer control blocks.  A control block points to its frame, and
 * from a frame one can arithmetically locate its control block (in
 * bf_m::get_cb(const generic_page *)).
 * The hash table for the buffer pool uses cuckoo hashing 
 * (see \ref P1) with multiple hash functions and multiple slots per bucket.  
 * These are compile-time constants and can be modified (bf_htab.h).
 *
 * Cuckoo hashing is subject to cycles, in which making room on one 
 * table bucket A would require moving something else into A.
 * Using at least two slots per bucket reduces the chance of a cycle.
 *
 * The implementation contains a limit on the number of times it looks for
 * an empty slot or moves that it has to perform to make room.  It does
 * If cycles are present, the limit will be hit, but hitting the limit
 * does not necessarily indicate a cycle.  If the limit is hit,
 * the insert will fail.
 * The "normal" solution in this case is to rebuild the table with
 * different hash functions. The storage manager does not handle this case.
 * \bug  GNATS 47 
 * In event of insertion failure, the hash table will have to be rebuilt with
 * different hash functions, or will have to be modified in some way.
 *
 * \bug GNATS 35 The buffer manager hash table implementation contains a race.
 * While a thread performs a hash-table
 * lookup, an item could move from one bucket to another (but not
 * from one slot to another within a bucket).
 * The implementation contains a temporary work-around for
 * this, until the problem is more gracefully fixed: if lookup fails to
 * find the target of the lookup, it performs an expensive lookup and
 * the statistics record these as bf_harsh_lookups. This is expensive.
 *
 * \subsection REPLACEMENT Page Replacement
 * When a page is fixed, the buffer manager looks for a free buffer-pool frame,
 * and if one is not available, it has to choose a victim to replace. 
 * It uses a clock-based algorithm to determine where in the buffer pool
 * to start looking for an unlatched frame:
 * On the first pass of the buffer pool it considers only clean frames. 
 * On the second pass it will consider dirty pages,
 * and on the third or subsequent pass it will consider any frame.
 *
 * The buffer manager forks background threads to flush dirty pages. 
 * The buffer manager makes an attempt to avoid hot pages and to minimize 
 * the cost of I/O by sorting and coalescing requests for contiguous pages. 
 * Statistics kept by the buffer manager tell the number of resulting write 
 * requests of each size.
 *
 * There is one bf_cleaner_t thread for each volume, and it flushes pages for that
 * volume; this is done so that it can combine contiguous pages into
 * single write requests to minimize I/O.  Each bf_cleaner_t is a master thread with
 * multiple page-writer slave threads.  The number of slave threads per master
 * thread is controlled by a run-time option.
 * The master thread can be disabled (thereby disabling all background
 * flushing of dirty pages) with a run-time option. 
 *
 * The buffer manager writes dirty pages even if the transaction
 * that dirtied the page is still active (steal policy). Pages
 * stay in the buffer pool as long as they are needed, except when
 * chosen as a victim for replacement (no force policy).
 *
 * The replacement algorithm is clock-based (it sweeps the buffer
 * pool, noting and clearing reference counts). This is a cheap
 * way to achieve something close to LRU; it avoids much of the
 * overhead and mutex bottlenecks associated with LRU.
 *
 * The buffer manager maintains a hash table that maps page IDs to buffer 
 * frame  control blocks (bfcb_t), which in turn point to frames
 * in the buffer pool.  The bfcb_t keeps track of the page in the frame, 
 * the page ID of the previously-held page, 
 * and whether it is in transit, the dirty/clean state of the page, 
 * the number of page fixes (pins) held on the page (i.e., reference counts), 
 * the \ref RECLSN "recovery lsn" of the page, etc.  
 * The control block also contains a latch.  A page, when fixed,
 * is always fixed in a latch mode, either LATCH_SH or LATCH_EX.
 * \bug GNATS 40 bf_m::upgrade_latch() drops the latch and re-acquires in
 * the new mode, if it cannot perform the upgrade without blocking. 
 * This is an issue inherited from the original SHORE storage manager.
 * To block in this case
 * would enable a deadlock in which two threads hold the latch in SH mode
 * and both want to upgrade to EX mode.  When this happens, the statistics 
 * counter \c bf_upgrade_latch_race is incremented.
 *
 * Page fixes are expensive (in CPU time, even if the page is resident).
 *
 * Each page type defines a set of fix methods that are virtual in 
 * the base class for all pages: The rest of the storage manager 
 * interacts with the buffer manager primarily through these methods 
 * of the page classes.  
 * The macros MAKEPAGECODE are used for each page subtype; they 
 * define all the fix methods on the page in such a way that bf_m::fix() 
 * is properly called in each case. 
 *
 * A page frame may be latched for a page without the page being 
 * read from disk; this
 * is done when a page is about to be formatted. 
 *
 * The buffer manager is responsible for maintaining WAL; this means it may not
 * flush to disk dirty pages whose log records have not reached stable storage yet.
 * Temporary pages (see sm_store_property_t) do not get logged, so they do not
 * have page lsns to assist in determining their clean/dirty status, and since pages
 * may change from temporary (unlogged) to logged, they require special handling, described
 * below.
 *
 * When a page is unfixed, sometimes it has been updated and must be marked dirty.
 * The protocol used in the storage manager is as follows:
 *
 * - Fixing with latch mode EX signals intent to dirty the page. If the page
 *   is not already dirty, the buffer control block for the page is given a
 *   recovery lsn of the page's lsn. This means that any dirtying of the page
 *   will be done with a log record whose lsn is larger than this recovery lsn.
 *   Fixing with EX mode of an already-dirty page does not change 
 *   the recovery lsn  for the page.
 *
 * - Clean pages have a recovery lsn of lsn_t::null.
 *
 * - A thread updates a page in the buffer pool only when it has the
 *   page EX-fixed(latched).
 *
 * - After the update to the page, the thread writes a log record to 
 *   record the update.  The log functions (generated by Perl) 
 *   determine if a log record should be written (not if a tmp 
 *   page, or if logging turned off, for example),
 *   and if not, they call page.set_dirty() so that any subsequent
 *   unfix notices that the page is dirty.
 *   If the log record is written, the modified page is unfixed with
 *   unfix_dirty() (in xct_impl::give_logbuf).
 *
 * - Before unfixing a page, if it was written, it must be marked dirty first
 *   with 
 *   - set_dirty followed by unfix, or
 *   - unfix_dirty (which is set_dirty + unfix).
 *
 * - Before unfixing a page, if it was NOT written, unfix it with bf_m::unfix
 *   so its recovery lsn gets cleared.  This happens only if this is the
 *   last thread to unfix the page.  The page could have multiple fixers 
 *   (latch holders) only if it were fixed in SH mode.  If fixed (latched)
 *   in EX mode,  this will be the only thread to hold the latch and the
 *   unfix will clear the recovery lsn.
 *
 *  It is possible that a page is fixed in EX mode, marked dirty but never
 *  updated after all,  then unfixed.  The buffer manager attempts to recognize
 *  this situation and clean the control block "dirty" bit and recovery lsn.
 *
 * Things get a little complicated where the buffer-manager's 
 * page-writer threads are
 * concerned.  The  page-writer threads acquire a share latches and copy
 * dirty pages; this being faster than holding the latch for the duration of the
 * write to disk
 * When the write is finished,  the page-writer re-latches the page with the
 * intention of marking it clean if no intervening updates have occurred. This
 * means changing the \e dirty bit and updating the recovery lsn in the buffer 
 * control block. The difficulty lies in determining if the page is indeed clean,
 * that is, matches the latest durable copy.
 * In the absence of unlogged (t_temporary) pages, this would not be terribly
 * difficult but would still have to cope with the case that the page was
 * (updated and) written by another thread between the copy and the re-fix.
 * It might have been cleaned, or that other thread might be operating in
 * lock-step with this thread.
 * The conservative handling would be not to change the recovery lsn in the
 * control block if the page's lsn is changed, however this has 
 * serious consequences
 * for hot pages: their recovery lsns might never be moved toward the tail of
 * the log (the recovery lsns remain artificially low) and 
 * thus the hot pages can prevent scavenging of log partitions. If log
 * partitions cannot be scavenged, the server runs out of log space.
 * For this reason, the buffer manager goes to some lengths to update the
 * recovery lsn if at all possible.
 * To further complicate matters, the page could have changed stores, 
 * and thus its page type or store (logging) property could differ.
 * The details of this problem are handled in a function called determine_rec_lsn().
 *
 * \subsection PAGEWRITERMUTEX Page Writer Mutexes
 *
 * The buffer manager keeps a set of \e N mutexes to sychronizing the various
 * threads that can write pages to disk.  Each of these mutexes covers a
 * run of pages of size smlevel_0::max_many_pages. N is substantially smaller
 * than the number of "runs" in the buffer pool (size of 
 * the buffer pool/max_many_pages), so each of the N mutexes actually covers
 * several runs:  
 * \code
 * page-writer-mutex = page / max_many_pages % N
 * \endcode
 *
 * \subsection BFSCAN Foreground Page Writes and Discarding Pages
 * Pages can be written to disk by "foreground" threads under several
 * circumstances.
 * All foreground page-writing goes through the method bf_m::_scan.
 * This is called for:
 * - discarding all pages from the buffer pool (bf_m::_discard_all)
 * - discarding all pages belonging to a given store from the buffer pool 
 *   (bf_m::_discard_store), e.g., when a store is destroyed.
 * - discarding all pages belonging to a given volume from the buffer pool 
 *   (bf_m::_discard_volume), e.g., when a volume is destroyed.
 * - forcing all pages to disk (bf_m::_force_all) with or without invalidating
 *   their frames, e.g., during clean shutdown.
 * - forcing all pages of a store to disk (bf_m::_force_store) with 
 *   or without invalidating
 *   their frames, e.g., when changing a store's property from unlogged to
 *   logged.
 * - forcing all pages of a volume to disk (bf_m::_force_store) with 
 *   without invalidating the frames, e.g., when dismounting a volume.
 * - forcing all pages whose recovery lsn is less than or equal to a given
 *   lsn_t, e.g.,  for a clean shutdown, after restart.
 */
/**\page Logging 
 *
 * See \ref LOG_M.
 * */

/**\page DEBUGAID Debugging Aids
  *\section SSMDEBUGAPI Storage Manager Methods for Debugging
 *
 * The storage manager contains a few methods that are useful for
 * debugging purposes. Some of these should be used for not other
 * purpose, as they are not thread-safe, or might be very expensive.
 * See \ref SSMAPIDEBUG.
 * 
 *\section SSMDEBUG Build-time Debugging Options
 *
 * At configure time, you can control which debugger-related options
 * (symbols, inlining, etc) with the debug-level options. See \ref CONFIGOPT.
 * \section SSMTRACE Tracing (--enable-trace)
 * When this build option is used, additional code is included in the build to
 * enable some limited tracing.  These C Preprocessor macros apply:
 * -W_TRACE
 *  --enable-trace defines this.
 * -FUNC
 *  Outputs the function name when the function is entered.
 * -DBG 
 *  Outputs the arguments.
 * -DBGTHRD 
 *  Outputs the arguments.
 *
 *  The tracing is controlled by these environment variables:
 *  -DEBUG_FLAGS: a list of file names to trace, e.g. "smfile.cpp log.cpp"
 *  -DEBUG_FILE: name of destination for the output. If not defined, the output
 *    is sent to cerr/stderr.
 *
 * See \ref CONFIGOPT.
 *  \note This tracing is not thread-safe, as it uses streams output.
 * \section SSMENABLERC Return Code Checking (--enable-checkrc)
 * If a w_rc_t is set but not checked with method is_error(), upon destruction the
 * w_rc_t will print a message to the effect "error not checked".
 * See \ref CONFIGOPT.
 *
 */
