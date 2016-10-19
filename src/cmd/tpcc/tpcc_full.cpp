/*
 * (c) Copyright 2013-2014, Hewlett-Packard Development Company, LP
 */

#include "tpcc_full.h"
#include <Lintel/ProgramOptions.hpp>
#include <Lintel/AtomicCounter.hpp>

/** Number of transactions to run per thread. */
uint32_t transactions_per_thread;
lintel::ProgramOption<uint32_t> po_transactions ( "transactions", "Number of transactions to run per thread", 10000);

int main ( int argc, char ** argv ) {
    lintel::parseCommandLine ( argc, argv, false );
    tpcc::full_driver_thread_t thread;
    return thread.fire_experiments();
}

namespace tpcc {
    full_driver_thread_t::full_driver_thread_t() : driver_thread_t (), next_history_id(0) {
        preload_tables = true; // preload all tables!
    }

    worker_thread_t* full_driver_thread_t::new_worker_thread(int32_t worker_id_arg) {
        return new full_worker_thread_t(worker_id_arg, this);
    }
    
    rc_t full_driver_thread_t::do_more_init() {
        transactions_per_thread = po_transactions.get();
        next_history_id = max_history_id + 1;
        return RCOK;
    }

    uint64_t full_driver_thread_t::issue_next_history_id() {
        return lintel::unsafe::atomic_fetch_add<uint64_t>(&next_history_id, 1);
    }
    
    full_worker_thread_t::full_worker_thread_t(int32_t worker_id_arg, full_driver_thread_t *parent_arg)
        : worker_thread_t(worker_id_arg, parent_arg),
            parent(parent_arg) {
    }
    
    rc_t full_worker_thread_t::init_worker() {
        return RCOK;
    }
    
    rc_t full_worker_thread_t::run_worker() {
        for (uint32_t rep = 0; rep < transactions_per_thread; ++rep) {
            uint16_t transaction_type = rnd.uniform_within(1, 100);
            // remember the random seed to repeat the same transaction on abort/retry.
            uint64_t rnd_seed = rnd.get_current_seed();

            if (rep % 1000 == 0) {
                std::cout << "Worker-" << worker_id << " xct: "
                    << rep << "/" << transactions_per_thread << "..." << std::endl;
            }
            
            // abort-retry loop
            while (true) {
                rnd.set_current_seed(rnd_seed);
                W_DO(open_xct());
                g_xct()->set_query_exlock_for_select(false); // in a few places we set it to true

                if (transaction_type <= XCT_NEWORDER_PERCENT) {
                    last_rc = do_neworder();
                } else if (transaction_type <= XCT_PAYMENT_PERCENT) {
                    last_rc = do_payment();
                } else if (transaction_type <= XCT_ORDER_STATUS_PERCENT) {
                    last_rc = do_order_status();
                } else if (transaction_type <= XCT_DELIVERY_PERCENT) {
                    last_rc = do_delivery();
                } else {
                    last_rc = do_stock_level();
                }

                W_DO(close_xct(last_rc, 0, true));
                if (!last_rc.is_error()) {
                    break;
                } else if (last_rc.err_num() == eUSERABORT) {
                    // if random rollback happens, we must abort the xct and
                    // not retry (see the comment in Sec 2.4.1.4).
                    break; // this is counted as "done"
                } else if (last_rc.err_num() == eDUPLICATE) {
                    // In no-lock setting, insertion/deletion errors are expected
                    // retry, but change the seed to avoid the same error.
                    rnd_seed = rnd.get_current_seed();
                    continue;
                } else if (last_rc.err_num() != eDEADLOCK
                        && last_rc.err_num() != eTOOMANYRETRY) {
                    // unexpected error!
                    std::cerr << "Worker-" << worker_id << " exit due to unexpected error " << last_rc << std::endl;
                    return last_rc;
                }
                // otherwise, retry
            }
        }
       
        std::cout << "Worker-" << worker_id << " all done" << std::endl;
        return RCOK;
    }
    
    template <class KEY_TYPE, class DATA_TYPE>
    rc_t lookup_general(KEY_TYPE &key, DATA_TYPE &result, stnum_enum stnum,
                        bool for_write, bool might_not_exist = false) {
        if (for_write) {
            g_xct()->set_query_exlock_for_select(true);
        }
        w_keystr_t foster_key(_to_keystr(key));
        smsize_t len = sizeof(result);
        bool found;
        W_DO(ss_m::find_assoc(get_stid(stnum), foster_key, &result, len, found));
        if (for_write) {
            g_xct()->set_query_exlock_for_select(false);
        }
        if (!found) {
            if (!might_not_exist) { // suppress it as it's expected for items
                std::cerr << "Unexpected error: record not found." << std::endl;
            }
            return RC(eNOTFOUND);
        }
        return RCOK;
    }

    rc_t full_worker_thread_t::lookup_customer(uint32_t wid, uint32_t did, uint32_t cid, customer_data &result, bool for_write) {
        customer_pkey key(wid, did, cid);
        W_DO(lookup_general(key, result, STNUM_CUSTOMER_PRIMARY, for_write));
        return RCOK;
    }
    rc_t full_worker_thread_t::lookup_district(uint32_t wid, uint32_t did, district_data &result, bool for_write) {
        district_pkey key(wid, did);
        W_DO(lookup_general(key, result, STNUM_DISTRICT_PRIMARY, for_write));
        return RCOK;
    }
    rc_t full_worker_thread_t::lookup_item(uint32_t iid, item_data &result, bool for_write) {
        item_pkey key(iid);
        W_DO(lookup_general(key, result, STNUM_ITEM_PRIMARY, for_write, true));
        return RCOK;
    }
    rc_t full_worker_thread_t::lookup_order(uint32_t wid, uint32_t did, uint32_t oid, order_data &result, bool for_write) {
        order_pkey key(wid, did, oid);
        W_DO(lookup_general(key, result, STNUM_ORDER_PRIMARY, for_write));
        return RCOK;
    }
    rc_t full_worker_thread_t::lookup_orderline(uint32_t wid, uint32_t did, uint32_t oid, uint32_t ol, orderline_data &result, bool for_write) {
        orderline_pkey key(wid, did, oid, ol);
        W_DO(lookup_general(key, result, STNUM_ORDERLINE_PRIMARY, for_write));
        return RCOK;
    }
    rc_t full_worker_thread_t::lookup_stock(uint32_t wid, uint32_t iid, stock_data &result, bool for_write) {
        stock_pkey key(wid, iid);
        W_DO(lookup_general(key, result, STNUM_STOCK_PRIMARY, for_write));
        return RCOK;
    }
    rc_t full_worker_thread_t::lookup_warehouse(uint32_t wid, warehouse_data &result, bool for_write) {
        warehouse_pkey key(wid);
        W_DO(lookup_general(key, result, STNUM_WAREHOUSE_PRIMARY, for_write));
        return RCOK;
    }

    rc_t full_worker_thread_t::lookup_customer_by_id_or_name(uint32_t wid, uint32_t did,
        customer_data &result, uint32_t &result_cid, bool for_write) {
        // 60% by name, 40% by ID
        bool by_name = rnd.uniform_within(1, 100) <= 60;
        if (by_name) {
            char lastname[sizeof(result.C_LAST)];
            generate_lastname(rnd.non_uniform_within(255, 0, 999), lastname);
            std::string lname(lastname, sizeof(result.C_LAST));
            rc_t ret = lookup_customer_by_name(wid, did, lname, result, result_cid, for_write);
            if (ret.is_error()) {
                if (ret.err_num() == eNOTFOUND) {
                    std::cout << "Customer-lookup: Unexpected error. lastname no matches:" << lname << std::endl;
                }
                return ret;
            }
        } else {
            result_cid = rnd.non_uniform_within(1023, 1, 3000);
            W_DO(lookup_customer(wid, did, result_cid, result, for_write));
        }
        return RCOK;
    }

    rc_t full_worker_thread_t::lookup_customer_by_name(uint32_t wid, uint32_t did,
        const std::string &lname, customer_data &result, uint32_t &result_cid, bool for_write) {

        std::string empty_str(lname.size(), 0);
        std::string lname_incremented(lname);
        // this increment never overflows because it's NULL character (see tpcc_schema.h).
        ++lname_incremented[lname.size() - 1];
        
        if (for_write) {
            g_xct()->set_query_exlock_for_select(true);
        }
        w_keystr_t lower_bound(to_keystr<customer_skey>(wid, did, lname.data(), empty_str.data(), 0));
        w_keystr_t upper_bound(to_keystr<customer_skey>(wid, did, lname_incremented.data(), empty_str.data(), 0));
        bt_cursor_t cursor (get_our_volume_id(), STNUM_CUSTOMER_SECONDARY,
            lower_bound, true, upper_bound, false, true);
        customer_skey fetched_key;
        std::vector<uint32_t> result_cids;
        do {
            W_DO(cursor.next());
            if (cursor.eof()) {
                break;
            }
            from_keystr(cursor.key(), fetched_key);
            result_cids.push_back(fetched_key.C_ID);
        } while (true);
        
        if (for_write) {
            g_xct()->set_query_exlock_for_select(false);
        }
        if (result_cids.size() == 0) {
            return RC(eNOTFOUND);
        }
        
        // take midpoint
        result_cid = result_cids[result_cids.size() / 2];
        W_DO(lookup_customer(wid, did, result_cid, result));
        return RCOK;
    }
}
