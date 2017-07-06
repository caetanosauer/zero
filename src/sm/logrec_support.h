#ifndef LOGREC_SUPPORT_H
#define LOGREC_SUPPORT_H

#include "lock.h"
#include "btree_page.h"
#include "alloc_page.h"
#include "stnode_page.h"

/**
 * This is a special way of logging the creation of a new page.
 * New page creation is usually a page split, so the new page has many
 * records in it. To simplify and to avoid many log entries in that case,
 * we log ALL bytes from the beginning to the end of slot vector,
 * and from the record_head8 to the end of page.
 * We can assume totally defragmented page image because this is page creation.
 * We don't need UNDO (again, this is page creation!), REDO is just two memcpy().
 */
struct page_img_format_t {
    size_t      beginning_bytes;
    size_t      ending_bytes;
    char        data[logrec_t::max_data_sz - 2 * sizeof(size_t)];

    int size()        { return 2 * sizeof(size_t) + beginning_bytes + ending_bytes; }

    page_img_format_t (const generic_page* p)
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
        const char* unused;
        switch (p->tag) {
            case t_alloc_p: {
                auto page = reinterpret_cast<const alloc_page*>(p);
                unused = page->unused_part(unused_length);
                break;
            }
            case t_stnode_p: {
                auto page = reinterpret_cast<const stnode_page*>(p);
                unused = page->unused_part(unused_length);
                break;
            }
            case t_btree_p: {
                auto page = reinterpret_cast<const btree_page*>(p);
                unused = page->unused_part(unused_length);
                break;
            }
            default:
                W_FATAL(eNOTIMPLEMENTED);
        }

        const char *pp_bin = reinterpret_cast<const char *>(p);
        beginning_bytes = unused - pp_bin;
        ending_bytes    = sizeof(generic_page) - (beginning_bytes + unused_length);

        ::memcpy (data, pp_bin, beginning_bytes);
        ::memcpy (data + beginning_bytes, unused + unused_length, ending_bytes);
        // w_assert1(beginning_bytes >= btree_page::hdr_sz);
        w_assert1(beginning_bytes + ending_bytes <= sizeof(generic_page));
    }

    void apply(generic_page* page)
    {
        // w_assert1(beginning_bytes >= btree_page::hdr_sz);
        w_assert1(beginning_bytes + ending_bytes <= sizeof(generic_page));
        char *pp_bin = reinterpret_cast<char *>(page);
        ::memcpy (pp_bin, data, beginning_bytes);
        ::memcpy (pp_bin + sizeof(generic_page) - ending_bytes,
                data + beginning_bytes, ending_bytes);
    }
};

#endif

