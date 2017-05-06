#ifndef LOGREC_SUPPORT_H
#define LOGREC_SUPPORT_H

#include "lock.h"
#include "btree_page.h"
#include "alloc_page.h"
#include "stnode_page.h"

/**
 * Classes used to store structured log record data used by certain
 * log record types.
 *
 * CS TODO: this is a temporary and transitional header -- we should get rid of
 * these classes in the log manager clean-up
 */

/**
 * \brief Log content of page_evict to maintain EMLSN in parent page.
 * \ingroup Single-Page-Recovery
 * \details
 * This is the log of the system transaction to maintain EMLSN.
 * The log is generated whenever we evict a page from bufferpool to maintain EMLSN
 * in the parent page.
 * @see log_page_evict()
 */
struct page_evict_t {
    lsn_t                   _child_lsn;
    general_recordid_t      _child_slot;
    page_evict_t(const lsn_t &child_lsn, general_recordid_t child_slot)
        : _child_lsn (child_lsn), _child_slot(child_slot) {}
};

/**
 * This is a special way of logging the creation of a new page.
 * New page creation is usually a page split, so the new page has many
 * records in it. To simplify and to avoid many log entries in that case,
 * we log ALL bytes from the beginning to the end of slot vector,
 * and from the record_head8 to the end of page.
 * We can assume totally defragmented page image because this is page creation.
 * We don't need UNDO (again, this is page creation!), REDO is just two memcpy().
 */
template<class PagePtr>
struct page_img_format_t {
    size_t      beginning_bytes;
    size_t      ending_bytes;
    char        data[logrec_t::max_data_sz - 2 * sizeof(size_t)];

    int size()        { return 2 * sizeof(size_t) + beginning_bytes + ending_bytes; }

    page_img_format_t (const PagePtr p)
    {
        /*
         * The mid-section of a btree page is usually not used, since head
         * entries are stored on the beginning of the page and variable-sized
         * "bodies" (i.e., key-value data) at the end of the page. This method
         * returns a pointer to the beginning of the unused part and its length.
         * The loc record then just contains the parts before and after the
         * unused section. For pages other than btree ones, the unused part
         * is either at the beginning or at the end of the page, and it must
         * be set to zero when replaying the log record.
         */

        size_t unused_length;
        char* unused;
        switch (p->tag()) {
            case t_alloc_p: {
                auto page = reinterpret_cast<alloc_page*>(p->get_generic_page());
                unused = page->unused_part(unused_length);
                break;
            }
            case t_stnode_p: {
                auto page = reinterpret_cast<stnode_page*>(p->get_generic_page());
                unused = page->unused_part(unused_length);
                break;
            }
            case t_btree_p: {
                auto page = reinterpret_cast<btree_page*>(p->get_generic_page());
                unused = page->unused_part(unused_length);
                break;
            }
            default:
                W_FATAL(eNOTIMPLEMENTED);
        }

        const char *pp_bin = (const char *) p->get_generic_page();
        beginning_bytes = unused - pp_bin;
        ending_bytes    = sizeof(generic_page) - (beginning_bytes + unused_length);

        ::memcpy (data, pp_bin, beginning_bytes);
        ::memcpy (data + beginning_bytes, unused + unused_length, ending_bytes);
        // w_assert1(beginning_bytes >= btree_page::hdr_sz);
        w_assert1(beginning_bytes + ending_bytes <= sizeof(generic_page));
    }

    void apply(PagePtr page)
    {
        // w_assert1(beginning_bytes >= btree_page::hdr_sz);
        w_assert1(beginning_bytes + ending_bytes <= sizeof(generic_page));
        char *pp_bin = (char *) page->get_generic_page();
        ::memcpy (pp_bin, data, beginning_bytes);
        ::memcpy (pp_bin + sizeof(generic_page) - ending_bytes,
                data + beginning_bytes, ending_bytes);
    }
};

/*************************************************************************
 *
 * OLD CHKPT LOG RECORDS -- DEPRECATED
 *
 ************************************************************************/

struct chkpt_bf_tab_t {
    struct brec_t {
    PageID    pid;      // +8 -> 8
    /*
     *  CS: store is required to mark as in-doubt on buffer pool.
     *  Perhaps we can remove the store number from buffer control blocks
     *  (bf_tree_cb_t), provided that they are not required. (TODO)
     */
    lsn_t    rec_lsn;   // +8 -> 16, this is the minimum (earliest) LSN
    lsn_t    page_lsn;  // +8 -> 24, this is the latest (page) LSN
    };

    // max is set to make chkpt_bf_tab_t fit in logrec_t::data_sz
    enum { max = (logrec_t::max_data_sz - 2 * sizeof(uint32_t)) / sizeof(brec_t) };
    uint32_t              count;
    fill4              filler;
    brec_t             brec[max];

    chkpt_bf_tab_t(
        int                 cnt,        // I-  # elements in pids[] and rlsns[]
        const PageID*         pids,        // I-  id of of dirty pages
        const lsn_t*         rlsns,        // I-  rlsns[i] is recovery lsn of pids[i], the oldest
        const lsn_t*         plsns)        // I-  plsns[i] is page lsn lsn of pids[i], the latest
        : count(cnt)
    {
        w_assert1( sizeof(*this) <= logrec_t::max_data_sz );
        w_assert1(count <= max);
        for (uint i = 0; i < count; i++) {
            brec[i].pid = pids[i];
            brec[i].rec_lsn = rlsns[i];
            brec[i].page_lsn = plsns[i];
        }
    }

    int                size() const
    {
        return (char*) &brec[count] - (char*) this;
    }
};

struct chkpt_xct_tab_t {
    struct xrec_t {
    tid_t                 tid;
    lsn_t                last_lsn;
    lsn_t                first_lsn;
    smlevel_0::xct_state_t        state;
    };

    // max is set to make chkpt_xct_tab_t fit in logrec_t::data_sz
    enum {     max = ((logrec_t::max_data_sz - sizeof(tid_t) -
            2 * sizeof(uint32_t)) / sizeof(xrec_t))
    };
    tid_t            youngest;    // maximum tid in session
    uint32_t            count;
    fill4            filler;
    xrec_t             xrec[max];

    chkpt_xct_tab_t(
        const tid_t&                         _youngest,
        int                                 cnt,
        const tid_t*                         tid,
        const smlevel_0::xct_state_t*         state,
        const lsn_t*                         last_lsn,
        const lsn_t*                         first_lsn)
        : youngest(_youngest), count(cnt)
    {
        w_assert1(count <= max);
        for (uint i = 0; i < count; i++)  {
            xrec[i].tid = tid[i];
            xrec[i].state = state[i];
            xrec[i].last_lsn = last_lsn[i];
            xrec[i].first_lsn = first_lsn[i];
        }
    }

    int             size() const
    {
        return (char*) &xrec[count] - (char*) this;
    }
};

struct chkpt_xct_lock_t {
    struct lockrec_t {
    okvl_mode            lock_mode;
    uint32_t             lock_hash;
    };

    // max is set to make chkpt_xct_lock_t fit in logrec_t::data_sz
    enum {     max = ((logrec_t::max_data_sz - sizeof(tid_t) -
            2 * sizeof(uint32_t)) / sizeof(lockrec_t))
    };

    tid_t            tid;    // owning transaction tid
    uint32_t         count;
    fill4            filler;
    lockrec_t        xrec[max];

    chkpt_xct_lock_t(
        const tid_t&                        _tid,
        int                                 cnt,
        const okvl_mode*                    lock_mode,
        const uint32_t*                     lock_hash)
        : tid(_tid), count(cnt)
    {
        w_assert1(count <= max);
        for (uint i = 0; i < count; i++)  {
            xrec[i].lock_mode = lock_mode[i];
            xrec[i].lock_hash = lock_hash[i];
        }
    }

    int             size() const
    {
        return (char*) &xrec[count] - (char*) this;
    }
};

struct chkpt_backup_tab_t
{
    uint32_t count;
    uint32_t data_size;
    char     data[logrec_t::max_data_sz];

    enum {
        max = (logrec_t::max_data_sz - 2 * sizeof(uint32_t))
                / (smlevel_0::max_devname)
    };


    chkpt_backup_tab_t(
            int cnt,
            const string* paths)
        : count(cnt)
    {
        std::stringstream ss;
        for (uint i = 0; i < count; i++) {
            ss << paths[i] << endl;
        }
        data_size = ss.tellp();
        w_assert0(data_size <= logrec_t::max_data_sz);
        ss.read(data, data_size);
    }

    chkpt_backup_tab_t(
        const std::vector<string>& paths);

    int size() const {
        return data_size + sizeof(uint32_t) * 2;
    }

    void read(std::vector<string>& paths)
    {
        std::string s;
        std::stringstream ss;
        ss.write(data, data_size);

        for (uint i = 0; i < count; i++) {
            ss >> s;
            paths.push_back(s);
        }
    }
};

struct chkpt_restore_tab_t
{
    enum {
        maxBitmapSize = logrec_t::max_data_sz - 2*sizeof(PageID)
            - sizeof(uint32_t),
        // one segment for each bit in the bitmap
        maxSegments = maxBitmapSize * 8
    };

    PageID firstNotRestored;
    uint32_t bitmapSize;
    char bitmap[maxBitmapSize];

    chkpt_restore_tab_t()
        : firstNotRestored(0), bitmapSize(0)
    {}

    size_t length()
    {
        return sizeof(PageID)
            + sizeof(uint32_t)
            + bitmapSize;
    }
};

struct xct_list_t {
    struct xrec_t {
        tid_t                 tid;
    };

    // max is set to make chkpt_xct_tab_t fit in logrec_t::data_sz
    enum {     max = ((logrec_t::max_data_sz - sizeof(tid_t) -
            2 * sizeof(uint32_t)) / sizeof(xrec_t))
    };
    uint32_t            count;
    fill4              filler;
    xrec_t             xrec[max];

    xct_list_t(const xct_t* list[], int count)
    {
        w_assert1(count <= max);
        for (int i = 0; i < count; i++)  {
            xrec[i].tid = list[i]->tid();
        }
    }

    int               size() const
    {
        return (char*) &xrec[count] - (char*) this;
    }
};


#endif
