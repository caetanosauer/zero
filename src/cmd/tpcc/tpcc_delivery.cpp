/*
 * (c) Copyright 2013-2014, Hewlett-Packard Development Company, LP
 */

#include "tpcc_full.h"
#include <set>

namespace tpcc {
    rc_t full_worker_thread_t::do_delivery() {
        const uint32_t wid = get_random_warehouse_id();
        const uint32_t carrier_id = rnd.uniform_within(1, 10);
        std::string delivery_time(get_current_time_string());
        for (uint32_t did = 1; did <= parent->max_district_id; ++did) {
            uint32_t oid;
            rc_t ret = pop_neworder(wid, did, oid);
            if (ret.err_num() == eNOTFOUND) {
                if (driver->get_verbose_level() >= VERBOSE_DETAIL) {
                    std::cout << "Delivery: no neworder" << std::endl;
                }
                continue;
            } else if (ret.is_error()) {
                return ret;
            }

            // SELECT CID FROM ORDER WHERE wid/did/oid=..
            // Take X lock because we are updating it
            order_data o_data;
            W_DO(lookup_order(wid, did, oid, o_data, true));
            uint32_t cid = o_data.O_C_ID;

            // UPDATE ORDER SET O_CARRIER_ID=carrier_id WHERE wid/did/oid=..
            // Note that we don't have to update the secondary index
            // as O_CARRIER_ID is not included in it.
            o_data.O_CARRIER_ID = carrier_id;
            W_DO(OVERWRITE_COLUMN(STNUM_ORDER_PRIMARY, to_keystr<order_pkey>(wid, did, oid),
                                  o_data, O_CARRIER_ID));

            // SELECT SUM(ol_amount) FROM ORDERLINE WHERE wid/did/oid=..
            // UPDATE ORDERLINE SET DELIVERY_D=delivery_time WHERE wid/did/oid=..
            // again, take X lock because we are updating.
            uint64_t amount_total = 0;
            std::vector<uint32_t> ol_numbers;
            W_DO(collect_ol_from_orderline(wid, did, oid, ol_numbers));
            orderline_data ol_data;
            for (size_t i = 0; i < ol_numbers.size(); ++i) {
                W_DO(lookup_orderline(wid, did, oid, ol_numbers[i], ol_data));
                amount_total += ol_data.OL_AMOUNT;
                ::memcpy(ol_data.OL_DELIVERY_D, delivery_time.data(),
                            sizeof(ol_data.OL_DELIVERY_D));
                W_DO(OVERWRITE_COLUMN(STNUM_ORDERLINE_PRIMARY,
                                      to_keystr<orderline_pkey>(wid, did, oid, ol_numbers[i]),
                                      ol_data, OL_DELIVERY_D));
            }

            // UPDATE CUSTOMER SET balance+=amount_total WHERE WID/DID/CID=..
            // No need to update secondary index as balance is not a key.
            customer_data c_data;
            W_DO(lookup_customer(wid, did, cid, c_data, true));
            c_data.C_BALANCE += amount_total;
            W_DO(OVERWRITE_COLUMN(STNUM_CUSTOMER_PRIMARY,
                                  to_keystr<customer_pkey>(wid, did, cid),
                                  c_data, C_BALANCE));

            if (driver->get_verbose_level() >= VERBOSE_DETAIL) {
                std::cout << "Delivery: updated: oid=" << oid << ", #ol=" << ol_numbers.size() << std::endl;
            }
        }
        return RCOK;
    }

    rc_t full_worker_thread_t::pop_neworder(uint32_t wid, uint32_t did, uint32_t &result_oid) {
        w_keystr_t lower(to_keystr<neworder_pkey>(wid, did, 0));
        w_keystr_t upper(to_keystr<neworder_pkey>(wid, did + 1, 0));

        // we are deleting it, so always better to take X lock at the first place
        g_xct()->set_query_exlock_for_select(true);
        bt_cursor_t cursor (get_our_volume_id(), STNUM_NEWORDER_PRIMARY,
            lower, true, upper, false, true);
        W_DO(cursor.next());

        if (cursor.eof()) {
            return RC(eNOTFOUND); // no NEWORDER record
        }

#if W_DEBUG_LEVEL >=1
        if(cursor.key().compare(lower) < 0) {
            neworder_pkey cpkey;
            from_keystr<neworder_pkey>(cursor.key(), cpkey);
            neworder_pkey lpkey;
            from_keystr<neworder_pkey>(lower, lpkey);
            DBGOUT1(<< "COMPARISON FAILED: " << endl
                << "cursor key = " << cpkey.NO_W_ID
                << "." << cpkey.NO_D_ID
                << "." << cpkey.NO_O_ID
                << "lower key = " << lpkey.NO_W_ID
                << "." << lpkey.NO_D_ID
                << "." << lpkey.NO_O_ID
            );
        }
        // CS: BUG -- these assertions fail very often. I am guessing
        // that not found should be returned in these cases
        // See ticket on BitBucket #11
        //w_assert1(cursor.key().compare(lower) >= 0);
        //w_assert1(cursor.key().compare(upper) < 0);
        if (cursor.key().compare(upper) < 0
                || cursor.key().compare(lower) >= 0)
        {
            return RC(eNOTFOUND);
        }
#endif
        neworder_pkey fetched;
        from_keystr(cursor.key(), fetched);
        w_assert1(fetched.NO_W_ID == wid);
        w_assert1(fetched.NO_D_ID == did);
        result_oid = fetched.NO_O_ID;

        g_xct()->set_query_exlock_for_select(false);

        // delete the fetched record
        w_keystr_t foster_fetched(to_keystr<neworder_pkey>(wid, did, result_oid));
        W_DO(ss_m::destroy_assoc(get_stid(STNUM_NEWORDER_PRIMARY), foster_fetched));
        return RCOK;
    }

    rc_t full_worker_thread_t::collect_ol_from_orderline(uint32_t wid, uint32_t did,
            uint32_t oid, std::vector<uint32_t> &ol_numbers) {
        w_keystr_t lower(to_keystr<orderline_pkey>(wid, did, oid, 0));
        w_keystr_t upper(to_keystr<orderline_pkey>(wid, did, oid + 1, 0));
        ol_numbers.clear();

        // sp far only use of this method is to update these records. so, take X lock
        g_xct()->set_query_exlock_for_select(true);
        bt_cursor_t cursor (get_our_volume_id(), STNUM_ORDERLINE_PRIMARY,
            lower, true, upper, false, true);
        do {
            W_DO(cursor.next());
            if (cursor.eof()) {
                break;
            }
            orderline_pkey fetched;
            from_keystr(cursor.key(), fetched);
            w_assert1(fetched.OL_W_ID == wid);
            w_assert1(fetched.OL_D_ID == did);
            w_assert1(fetched.OL_O_ID == oid);
            ol_numbers.push_back(fetched.OL_NUMBER);
        } while (true);
        g_xct()->set_query_exlock_for_select(false);
        return RCOK;
    }
}
