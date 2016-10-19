/*
 * (c) Copyright 2013-2014, Hewlett-Packard Development Company, LP
 */

#include "tpcc_full.h"
#include <cstdio>

namespace tpcc {
    rc_t full_worker_thread_t::do_payment() {
        // these are the customer's home wid/did
        const uint32_t c_wid = get_random_warehouse_id();
        const uint32_t c_did = get_random_district_id();
        const double amount = ((double) rnd.uniform_within(100, 500000)) / 100.0f;

        // 85% accesses the home wid/did. 15% other wid/did (wid must be !=c_wid).
        uint32_t wid, did;
        const bool remote_warehouse = rnd.uniform_within(1, 100) > 85;
        if (remote_warehouse) {
            wid = rnd.uniform_within_except(1, parent->get_max_warehouse_id(), c_wid);
            did = get_random_district_id(); // re-draw did.
        } else {
            wid = c_wid;
            did = c_did;
        }

        // all of the following Take X lock because we are updating them.

        // UPDATE WAREHOUSE SET YTD=YTD+amount
        warehouse_data w_data;
        W_DO(lookup_warehouse(wid, w_data, true));
        w_data.W_YTD += amount;
        W_DO(OVERWRITE_COLUMN(STNUM_WAREHOUSE_PRIMARY, to_keystr<warehouse_pkey>(wid),
                              w_data, W_YTD));

        // UPDATE DISTRICT SET YTD=YTD+amount
        district_data d_data;
        W_DO(lookup_district(wid, did, d_data, true));
        d_data.D_YTD += amount;
        W_DO(OVERWRITE_COLUMN(STNUM_DISTRICT_PRIMARY, to_keystr<district_pkey>(wid, did),
                              d_data, D_YTD));

        // get customer record. 
        uint32_t cid;
        customer_data c_data;
        W_DO(lookup_customer_by_id_or_name(c_wid, c_did, c_data, cid, true));
        
        std::string time_str(get_current_time_string());

        // UPDATE CUSTOMER SET BALANCE=BALANCE+amount,
        // (if C_CREDID="BC") C_DATA=...
        c_data.C_BALANCE += amount;
        if (c_data.C_CREDIT[0] == 'B' && c_data.C_CREDIT[0] == 'C') {
            const size_t DATA_SIZE = sizeof(c_data.C_DATA);
            char c_new_data[DATA_SIZE];
            std::sprintf(c_new_data, "| %4d %2d %4d %2d %4d $%7.2f %s",
                cid, did, wid, did, wid, c_data.C_BALANCE, time_str.c_str());
            ::strncat(c_new_data, c_data.C_DATA, DATA_SIZE - 1 - ::strlen(c_new_data));
            ::memcpy(c_data.C_DATA, c_new_data, DATA_SIZE);
            // in this case we are overwriting two columns next to each other
            w_assert1(IS_SUCCESSIVE_2COLUMNS(c_data, C_BALANCE, C_DATA));
            W_DO(OVERWRITE_2COLUMNS(STNUM_CUSTOMER_PRIMARY,
                           to_keystr<customer_pkey>(c_wid, c_did, cid),
                            c_data, C_BALANCE, C_DATA));
        } else {
            W_DO(OVERWRITE_COLUMN(STNUM_CUSTOMER_PRIMARY,
                                  to_keystr<customer_pkey>(c_wid, c_did, cid),
                                  c_data, C_BALANCE));
        }

        // INSERT INTO HISTORY
        history_data h_data;
        zero_clear(h_data);
        h_data.H_AMOUNT = amount;
        h_data.H_C_D_ID = c_did;
        h_data.H_C_ID = cid;
        h_data.H_C_W_ID = c_wid;
        h_data.H_D_ID = did;
        h_data.H_W_ID = wid;

        std::string h_new_data(w_data.W_NAME);
        h_new_data.append(std::string(4, ' '));
        h_new_data.append(d_data.D_NAME);
        ::memcpy(h_data.H_DATA, h_new_data.data(), h_new_data.size());
        h_data.H_DATA[h_new_data.size()] = '\0';

        ::memcpy(h_data.H_DATE, time_str.data(), time_str.size());
        h_data.H_DATE[time_str.size()] = '\0';

        uint64_t hid = parent->issue_next_history_id();
        W_DO(insert(STNUM_HISTORY_PRIMARY, to_keystr<history_pkey>(hid), h_data));

        if (driver->get_verbose_level() >= VERBOSE_DETAIL) {
            std::cout << "Payment: wid=" << wid << ", did=" << did
                << ", cid=" << cid << ", c_wid=" << c_wid << ", c_did=" << c_did
                << ", time=" << time_str << std::endl;
        }
        return RCOK;
    }
}
