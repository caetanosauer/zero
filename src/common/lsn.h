/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager

                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne

                         All Rights Reserved.

   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.

   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

/*<std-header orig-src='shore' incl-file-exclusion='SM_S_H'>

 $Id: lsn.h,v 1.5 2010/12/08 17:37:34 nhall Exp $

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

#ifndef LSN_H
#define LSN_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include "w_base.h"

/* FRJ: Major changes to lsn_t
 * Once the database runs long enough we will run out of
 * partition numbers (only 64k possible).
 * Fortunately, this is a log, so lsn_t don't last forever.
 * Eventually things become durable and the log partition
 * file gets reclaimed (deleted).
 * As long as the first partition is gone before
 * the last one fills, we can simply wrap and change the
 * sense of lsn_t comparisions.
 * as follows:
 *
   Suppose we use unsigned 8 bit partition numbers. If the current
   global lsn has pnum >= 128 (MSB set), any lsn we encounter with
   pnum < 128 (MSB clear) must be older:

   0        1        ...        126        127        128        129        ...         254        255


   On the other hand, if the current global lsn has pnum < 128
   (MSB clear), any lsn we encounter with pnum >= 128 (MSB set)
   must be *older* (ie, the pnum recently wrapped from 255 back to
   0):

   128        129        ...         254        255        0        1        ...        126        127

   The first situation is easy: regular comparisons provide the
   ordering we want; The second is trickier due to the wrapping of
   partition numbers. Fortunately, there's an easy way around the
   problem!  Since the MSB of the pnum is also the MSB of the
   64-bit value holding the lsn, if comparisons interpret the
   64-bit value as signed we get the proper ordering:

   -128        -127        ...        -2        -1        0        1        ...        126        127

   We assume there are enough lsn_t that less than half are in use at
   a time. So, we divide the universe of lsn's into four regions,
   marked by the most significant two bits of the file.

   00+01 - don't care
   01+10 - unsigned comparisons
   10+11 - unsigned comparisons
   11+00 - signed comparisons

   For now, though, we'll just assume overflow doesn't happen ;)
*/

typedef int64_t sm_diskaddr_t;

/**
 * \defgroup LSNS Log Sequence Numbers (LSN)
 * \brief How Log Sequence Numbers are Used
 * \ingroup SSMLOG
 * \details
 * \section LLR Locates Log Records
 * A log sequence number generally points to a record in the log.
 * It consists of two parts:
 * - hi(), a.k.a., file(). This is a number that matches a log partition
 *                         file, e.g., "log.<file>"
 * - lo(), a.k.a., rba(). This is byte-offset into the log partition, and is
 *                        the first byte of a log record, or is the first
 *                        byte after the last log record in the file (where
 *                        the next log record could be written).
 *
 * \note Once the database runs long enough we will run out of
 * partition numbers (only 64k possible).
 * Fortunately, this is a log, so lsn_t don't last forever.
 * Eventually things become durable and the log partition
 * file gets reclaimed (deleted).
 * As long as the first partition is gone before
 * the last one fills, we can simply wrap and change the
 * sense of lsn_t comparisions.
 *
 * \subsection WORK Identifying Limit of Partial Rollback
 *
 * A savepoint (sm_save_point_t) is an lsn_t. It tells how far back
 * a transaction should undo its actions when ss_m::rollback_work is called.
 *
 * \section PTS Page Timestamps
 * Each page has an lsn_t that acts as a timestamp; it is the
 * lsn of the last log record that describes an update to the page.
 *
 * In recovery, when a page is read in, all log records with sequence
 * numbers less than the page lsn have been applied, and redo of these
 * log records is not necessary.
 *
 * The storage manager has other special cases: lsn_t(0,1) -- this is
 * in page.cpp, fixable_page_h::_format(), and indicates a freshly formatted page <<<>>>
 * with no further updates.
 * \subsection NPCD Nominal Page Corruption-Detection
 * Pages have two copies of their page lsn; one at the head and one at the
 * end of the page. Presumably if the two match, the page is uncorrupted,
 * though that is no guarantee.  Certainly if they do not match,
 * something is wrong.
 *
 * \section BPFS Buffer Pool Frame Status
 * The buffer pool's control blocks (bfcb_t) contain an lsn_t,
 * the "recovery lsn" or rec_lsn.  This is a timestamp that can
 * be compared with the page lsns to determine if the copy in the
 * buffer pool is up-to-date or not.
 *
 * A recovery lsn is a lower bound on the lsn of a log record
 * that updated the page in the frame.
 * A clean page in the buffer pool has a rec_lsn of lsn_t::null.
 * Each time a page is fixed in EX mode, the buffer control block
 * ensures that the rec_lsn is not lsn_t::null, thereby indicating that
 * this page is probably dirty and needs flushing or, possibly,
 * is being flushed.
 * The rec_lsn is set to the tail of the log at the time the fix is done; this
 * ensures that any log record written for an update to the page has at
 * least the rec_lsn sequence number. There might be several updates to
 * the page before the page is cleaned, so the rec_lsn is indeed a lower
 * bound, and that's all we can know about it.
 *
 * Special cases of log sequence numbers:
 * - null: not a valid lsn_t
 * - max:  soon to overflow
 * - lsn(0,1) : used when some pages are formatted.
 *
 */

typedef uint64_t lsndata_t;
const lsndata_t lsndata_null = 0;
const lsndata_t lsndata_max = 0xFFFFFFFFFFFFFFFF;

/**\brief Log Sequence Number. See \ref LSNS.
 *
 * \ingroup LSNS
 * \details
 *
 * A log sequence number points to a record in the log.
 * It consists of two parts:
 * - hi(), a.k.a., file(). This is a number that matches a log partition
 *                         file, e.g., "log.<file>"
 * - lo(), a.k.a., rba(). This is byte-offset into the log partition, and is
 *                        the first byte of a log record, or is the first
 *                        byte after the last log record in the file (where
 *                        the next log record could be written).
 *
 * All state is  stored in a single 64-bit value.
 * This reading or setting is atomic on
 * 64-bit platforms (though updates still need protection).
 * \warning This is NOT atomic on 32-bit platforms.
 *
 * Because all state fits in 64 bits,
 * there is a trade-off between maximum supported log partition size
 * and number of partitions. Two reasonable choices are:
 *
 * - 16-bit partition numbers, up to 256TB per partition
 * - 32-bit partition numbers, up to 4GB per partition
 *
 * 48-bit offsets are larger, but (slightly) more expensive and likely
 * to wrap sooner.
 * 32-bit offsets are still pretty big, and the chance of wrapping
 * is *much* smaller (though a production system could theoretically
 * hit the limit, since the count persists as long as the database
 * exists.
 * For now we go with the 32-32 split.
 *
 * lsn_t no longer cares whether the disk can handle the full range
 * it supports.
 * If you support 48-bit partition sizes and the disk can
 * only handle 32-bit offsets, the largest file will just happen to be
 * smaller than lsn_t technically supports.
 *
 * lsn_t does not cater to unaligned accesses.
 * Log writes, in particular,
 * are expected to be 8-byte aligned.
 * The extra wasted bytes just aren't worth the performance hit of allowing
 * misalignment.
 *
 * \note Once the database runs long enough we will run out of
 * partition numbers (only 64k possible).
 * Fortunately, this is a log, so lsn_t don't last forever.
 * Eventually things become durable and the log partition
 * file gets reclaimed (deleted).
 * As long as the first partition is gone before
 * the last one fills, we can simply wrap and change the
 * sense of lsn_t comparisions.
 *
 */
class lsn_t {
    enum { file_hwm  =    0xffff };
public:
    enum { PARTITION_BITS=16 };
    enum { PARTITION_SHIFT=(64-PARTITION_BITS) };

    static uint64_t mask() {
        static uint64_t const ONE = 1;
        return (ONE << PARTITION_SHIFT)-1;
    }

    lsn_t() : _data(0) { }
    lsn_t(lsndata_t data) : _data(data) { }

    lsn_t(uint32_t f, sm_diskaddr_t r) :
                _data(from_file(f) | from_rba(r)) { }

    // copy operator
    lsn_t(const lsn_t & other) : _data(other._data) { }

    lsndata_t data()         const { return _data; }
    void set (lsndata_t data) {_data = data;}

    bool valid()             const {
                                    // valid is essentially iff file != 0
#if W_DEBUG_LEVEL > 1
                                    uint64_t  copy_of_data =  _data;
                                    uint64_t  m =  mask();
                                    bool first = copy_of_data > m;
                                    uint64_t  f =
                                                    to_file(copy_of_data);
                                    bool second = (f != 0);
                                    w_assert2(first == second);
#endif
                                   return (_data > mask());
                            }

    uint32_t hi()   const  { return file(); }
    uint32_t file() const { return to_file(_data); }

    sm_diskaddr_t     lo()   const  { return rba(); }
    sm_diskaddr_t     rba()  const { return to_rba(_data); }

    // WARNING: non-atomic read-modify-write operations!
    void copy_rba(const lsn_t &other) {
                _data = get_file(_data) | get_rba(other._data); }
    void set_rba(sm_diskaddr_t &other) {
                _data = get_file(_data) | get_rba(other); }

    // WARNING: non-atomic read-modify-write operations!
    lsn_t& advance(int amt) { _data += amt; return *this; }

    lsn_t &operator+=(long delta) { return advance(delta); }
    lsn_t operator+(long delta) const { return lsn_t(*this).advance(delta); }

    bool operator>(const lsn_t& l) const { return l < *this; }
    bool operator<(const lsn_t& l) const { return _data < l._data; }
    bool operator>=(const lsn_t& l) const { return !(*this < l); }
    bool operator<=(const lsn_t& l) const { return !(*this > l); }
    bool operator==(const lsn_t& l) const { return _data == l._data; }
    bool operator!=(const lsn_t& l) const { return !(*this == l); }

    std::string str();

    bool is_null() const { return _data == 0; }


/*
 * This is the SM's idea of on-disk and in-structure
 * things that reflect the size of a "disk".  It
 * is different from fileoff_t because the two are
 * orthogonal.  A system may be constructed that
 * has big addresses, but is only running on
 * a "small file" environment.
 */
    static const int sm_diskaddr_max;

    static const lsn_t null;
    static const lsn_t max;

private:
    lsndata_t        _data;

    static uint32_t to_file(uint64_t f) {
                return (uint32_t) (f >> PARTITION_SHIFT); }

    static uint64_t get_file(uint64_t data) {
                return data &~ mask(); }

    static uint64_t from_file(uint32_t data) {
                return ((uint64_t) data) << PARTITION_SHIFT; }

    static sm_diskaddr_t to_rba(uint64_t r) {
                return (sm_diskaddr_t) (r & mask()); }

    static uint64_t get_rba(uint64_t data) {
                return to_rba(data); }

    static uint64_t from_rba(sm_diskaddr_t data) {
                return to_rba(data); }
};

inline ostream& operator<<(ostream& o, const lsn_t& l)
{
    return o << l.file() << '.' << l.rba();
}

inline istream& operator>>(istream& i, lsn_t& l)
{
    sm_diskaddr_t d;
    char c;
    uint64_t f;
    i >> f >> c >> d;
    l = lsn_t(f, d);
    return i;
}
#endif
