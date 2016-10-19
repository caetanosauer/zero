/*
 * (c) Copyright 2013-2014, Hewlett-Packard Development Company, LP
 */

#include "tpcc_full.h"
#include <string.h>
#include <sstream>

namespace tpcc {
    const uint32_t MIN_OL_CNT = 5;
    const uint32_t MAX_OL_CNT = 15;

    rc_t full_worker_thread_t::do_neworder() {
        const uint32_t wid = get_random_warehouse_id();
        const uint32_t did = get_random_district_id();
        const uint32_t cid = rnd.non_uniform_within(1023, 1, 3000);
        const uint32_t ol_cnt = rnd.uniform_within(MIN_OL_CNT, MAX_OL_CNT);

        // New-order transaction has the "1% random rollback" rule.
        const bool will_rollback = (rnd.uniform_within(1, 100) == 1);

        // these are for showing results on stdout (part of the spec, kind of)
        char        output_bg[MAX_OL_CNT];
        uint32_t    output_prices[MAX_OL_CNT];
        char        output_item_names[MAX_OL_CNT][25];
        uint32_t    output_quantities[MAX_OL_CNT];
        double      output_amounts[MAX_OL_CNT];
        double      output_total = 0.0;

        // SELECT ... from WAREHOUSE and CUSTOMER
        // SELECT ... FROM DISTRICT and also
        // UPDATE DISTRICT SET next_o_id=next_o_id+1
        // so, take X lock to avoid lock upgrade.
        warehouse_data w_data;
        W_DO(lookup_warehouse(wid, w_data));

        district_data d_data;
        W_DO(lookup_district(wid, did, d_data, true));
        const uint32_t oid = d_data.D_NEXT_O_ID;
        ++d_data.D_NEXT_O_ID;
        W_DO(OVERWRITE_COLUMN(STNUM_DISTRICT_PRIMARY, to_keystr<district_pkey>(wid, did),
                       d_data, D_NEXT_O_ID));

        customer_data c_data;
        W_DO(lookup_customer(wid, did, cid, c_data));

        // INSERT INTO ORDERLINE with random item.
        bool all_local_warehouse = true;
        for (uint32_t ol = 1; ol <= ol_cnt; ++ol) {
            const uint32_t quantity = rnd.uniform_within(1, 10);
            uint32_t iid = rnd.non_uniform_within(8191, 1, 100000);
            if (will_rollback) {
                // it should be some unused value.
                iid = 12345678;
            }

            // only 1% has different wid for supplier.
            bool remote_warehouse = (rnd.uniform_within(1, 100) == 1);
            uint32_t supply_wid;
            if (remote_warehouse && driver->get_max_warehouse_id() > 1) {
                supply_wid = rnd.uniform_within_except(1, driver->get_max_warehouse_id(), wid);
                all_local_warehouse = false;
            } else {
                supply_wid = wid;
            }

            // SELECT ... FROM ITEM WHERE IID=iid
            item_data i_data;
            rc_t ret = lookup_item(iid, i_data);
            if (ret.is_error()) {
                if (ret.err_num() == eNOTFOUND) {
                    // This is by design.
                    w_assert1(will_rollback);
                    if (driver->get_verbose_level() >= VERBOSE_DETAIL) {
                        std::cout << "NewOrder: 1% random rollback happened. Dummy IID=" << iid << std::endl;
                    }
                    return RC(eUSERABORT);
                } else {
                    return ret; // otherwise, real error.
                }
            }

            // SELECT ... FROM STOCK WHERE WID=supply_wid AND IID=iid
            // then UPDATE quantity and remote count, so take X lock first.
            stock_data s_data;
            W_DO(lookup_stock(supply_wid, iid, s_data, true));
            if (s_data.S_QUANTITY > quantity) {
                s_data.S_QUANTITY -= quantity;
            } else {
                s_data.S_QUANTITY = s_data.S_QUANTITY - quantity + 91;
            }
            if (remote_warehouse) {
                ++s_data.S_REMOTE_CNT;
                // in this case we are overwriting two columns next to each other
                w_assert1(IS_SUCCESSIVE_2COLUMNS(s_data, S_QUANTITY, S_REMOTE_CNT));
                W_DO(OVERWRITE_2COLUMNS(STNUM_STOCK_PRIMARY,
                            to_keystr<stock_pkey>(supply_wid, iid),
                            s_data, S_QUANTITY, S_REMOTE_CNT));
            } else {
                W_DO(OVERWRITE_COLUMN(STNUM_STOCK_PRIMARY,
                                      to_keystr<stock_pkey>(supply_wid, iid),
                                      s_data, S_QUANTITY));
            }

            orderline_data ol_data;
            zero_clear(ol_data);
            ol_data.OL_AMOUNT = quantity * i_data.I_PRICE * (1.0 + w_data.W_TAX + d_data.D_TAX) * (1.0 - c_data.C_DISCOUNT);
            ::memcpy(ol_data.OL_DIST_INFO, s_data.pick_dist(did), 25);
            ol_data.OL_I_ID = iid;
            ol_data.OL_QUANTITY = quantity;
            ol_data.OL_SUPPLY_W_ID = supply_wid;
            W_DO(insert(STNUM_ORDERLINE_PRIMARY, to_keystr<orderline_pkey>(wid, did, oid, ol), ol_data));

            // output variables
            output_bg[ol - 1] = ::strstr(i_data.I_DATA, "original") != NULL
                && ::strstr(s_data.S_DATA, "original") != NULL ? 'B' : 'G';
            output_prices[ol - 1] = i_data.I_PRICE;
            ::memcpy(output_item_names[ol - 1], i_data.I_NAME, 25);
            output_quantities[ol - 1] = quantity;
            output_amounts[ol - 1] = ol_data.OL_AMOUNT;
            output_total += ol_data.OL_AMOUNT;
        }

        // INSERT INTO ORDERS and NEW_ORDERS
        std::string time_str(get_current_time_string());
        order_data o_data;
        o_data.O_ALL_LOCAL = all_local_warehouse ? 1 : 0;
        o_data.O_C_ID = cid;
        o_data.O_CARRIER_ID = 0;
        ::memcpy(o_data.O_ENTRY_D, time_str.data(), time_str.size());
        o_data.O_ENTRY_D[time_str.size()] = '\0';
        o_data.O_OL_CNT = ol_cnt;
        W_DO(insert(STNUM_ORDER_SECONDARY, to_keystr<order_skey>(wid, did, cid, oid)));
        W_DO(insert(STNUM_ORDER_PRIMARY, to_keystr<order_pkey>(wid, did, oid), o_data));
        W_DO(insert(STNUM_NEWORDER_PRIMARY, to_keystr<neworder_pkey>(wid, did, oid)));

        // show output on console
        if (driver->get_verbose_level() >= VERBOSE_DETAIL) {
            std::stringstream str;
            str << "Neworder: : wid=" << wid << ", did=" << did << ", oid=" << oid
                << ", cid=" << cid << ", ol_cnt=" << ol_cnt << ", total=" << output_total;
            for (uint32_t i = 0; i < ol_cnt; ++i) {
                str << ". ol[" << (i + 1) << "]=" << output_bg[i]
                    << "." << output_item_names[i] << ".$" << output_prices[i]
                    << "*" << output_quantities[i] << "." << output_amounts[i];
            }
            str << std::endl;
            std::cout << str.str();
        }
        return RCOK;
    }
}
