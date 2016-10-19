/*
 * (c) Copyright 2013-2014, Hewlett-Packard Development Company, LP
 */

#include "tpcc_full.h"

namespace tpcc {

    rc_t full_worker_thread_t::do_order_status() {
        const uint32_t wid = get_random_warehouse_id();
        const uint32_t did = get_random_district_id();

        uint32_t cid;
        customer_data c_data;
        W_DO(lookup_customer_by_id_or_name(wid, did, c_data, cid));
        
        // identify the last order by this customer
        uint32_t oid;
        rc_t ret = get_last_orderid_by_customer(wid, did, cid, oid);
        if (ret.err_num() == eNOTFOUND) {
            if (driver->get_verbose_level() >= VERBOSE_DETAIL) {
                std::cout << "OrderStatus: no order" << std::endl;
            }
            return RCOK; // this is a correct result
        } else if (ret.is_error()) {
            return ret;
        }
        
        // SELECT IID,SUPPLY_WID,QUANTITY,AMOUNT,DELIVERY_D FROM ORDERLINE
        // WHERE WID/DID/OID=..
        w_keystr_t lower(to_keystr<orderline_pkey>(wid, did, oid, 0));
        w_keystr_t upper(to_keystr<orderline_pkey>(wid, did, oid + 1, 0));
        bt_cursor_t cursor (get_our_volume_id(), STNUM_ORDERLINE_PRIMARY,
            lower, true, upper, false, true);
        uint32_t cnt = 0;
        do {
            W_DO(cursor.next());
            if (cursor.eof()) {
                break;
            }
            const orderline_data *ol_data = reinterpret_cast<const orderline_data*>(cursor.elem());
            if (driver->get_verbose_level() >= VERBOSE_TRACE) {
                std::cout << "Order-status[" << cnt << "]:"
                    << "IID=" << ol_data->OL_I_ID
                    << ", SUPPLY_WID=" << ol_data->OL_SUPPLY_W_ID
                    << ", QUANTITY=" << ol_data->OL_QUANTITY
                    << ", AMOUNT=" << ol_data->OL_AMOUNT
                    << ", DELIVERY_D=" << ol_data->OL_DELIVERY_D
                    << std::endl;
            }
            ++cnt;
        } while (true);
        if (driver->get_verbose_level() >= VERBOSE_DETAIL) {
            std::cout << "Order-status:" << cnt << " records. wid=" << wid
            << ", did=" << did << ", cid=" << cid << ", oid=" << oid << std::endl;
        }
        return RCOK;
    }

    rc_t full_worker_thread_t::get_last_orderid_by_customer(uint32_t wid, uint32_t did,
        uint32_t cid, uint32_t &result) {
        // SELECT TOP 1 ... FROM ORDERS WHERE WID/DID/CID=.. ORDER BY OID DESC
        // Use the secondary index for this query.
        w_keystr_t lower(to_keystr<order_skey>(wid, did, cid, 0));
        w_keystr_t upper(to_keystr<order_skey>(wid, did, cid + 1, 0));
        bt_cursor_t cursor (get_our_volume_id(), STNUM_ORDER_SECONDARY,
            lower, true, upper, false, false);

        W_DO(cursor.next());
        if (cursor.eof()) {
            return RC(eNOTFOUND);
        }
        order_skey fetched;
        from_keystr(cursor.key(), fetched);
        w_assert1(fetched.O_W_ID == wid);
        w_assert1(fetched.O_D_ID == did);
        w_assert1(fetched.O_C_ID == cid);
        result = fetched.O_ID;
        return RCOK;
    }
}
