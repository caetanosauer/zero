/*
 * (c) Copyright 2013-2014, Hewlett-Packard Development Company, LP
 */

#include "tpcc_full.h"
#include <set>

namespace tpcc {
    rc_t full_worker_thread_t::do_stock_level() {
        const uint32_t wid = get_random_warehouse_id();
        const uint32_t did = get_random_district_id();
        const uint32_t threshold = rnd.uniform_within(10, 20);

        // SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID=wid AND D_ID=did
        district_data d_data;
        W_DO(lookup_district(wid, did, d_data));
        const uint32_t next_oid = d_data.D_NEXT_O_ID;
        
        // SELECT COUNT(DISTINCT(s_i_id))
        // FROM ORDERLINE INNER JOIN STOCK ON (WID,IID)
        // WHERE WID=wid AND DID=did AND OID BETWEEN next_oid-20 AND next_oid
        // AND QUANTITY<threshold
        size_t result = 0;
        stock_data s_data;
        std::set<uint32_t> item_ids;
        W_DO(collect_items_from_orderline(wid, did, next_oid - 20, next_oid, item_ids));
        for (std::set<uint32_t>::const_iterator it = item_ids.begin(); it != item_ids.end(); ++it) {
            W_DO(lookup_stock(wid, *it, s_data));
            if (s_data.S_QUANTITY < threshold) {
                ++result;
            }
        }
        
        if (driver->get_verbose_level() >= VERBOSE_DETAIL) {
            std::cout << "Stock-Level: result=" << result << std::endl;
        }
        return RCOK;
    }

    rc_t full_worker_thread_t::collect_items_from_orderline(uint32_t wid, uint32_t did,
        uint32_t oid_from, uint32_t oid_to, std::set<uint32_t> &item_ids) {
        w_keystr_t lower(to_keystr<orderline_pkey>(wid, did, oid_from, 0));
        w_keystr_t upper(to_keystr<orderline_pkey>(wid, did, oid_to + 1, 0));// +1 as it's exclusive
        item_ids.clear();
        bt_cursor_t cursor (get_our_volume_id(), STNUM_ORDERLINE_PRIMARY,
            lower, true, upper, false, true);
        do {
            W_DO(cursor.next());
            if (cursor.eof()) {
                break;
            }
            uint32_t iid = reinterpret_cast<const orderline_data*>(cursor.elem())->OL_I_ID;
            item_ids.insert(iid);
        } while (true);
        return RCOK;
    }
}
