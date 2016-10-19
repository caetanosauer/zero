/*
 * (c) Copyright 2013-2014, Hewlett-Packard Development Company, LP
 */

#ifndef TPCC_FULL_H
#define TPCC_FULL_H

/**
* \file tpcc_full.h
* \brief Implements the full TPCC query workload.
* \details
* This is the canonical TPCC workload which use as the default experiment.
* We also have various focused/modified workload to evaluate specific aspects.
*
* NOTE we don't have the notion of "terminal" in TPCC spec,
* so all threads emulate one individual user that issues one transaction.
* Instead, we have an optional skewed randomness to choose WID/DID in the benchmark.
* In the original TPCC, WID/DID are uniformly picked, so the access skews are low.
* We (when specified) emulate high-skew workloads like what Shore-MT VLDB'12 paper
* did.
*/

#include "tpcc.h"
#include <set>

namespace tpcc {
    class full_driver_thread_t;
    class full_worker_thread_t;
    
    // See Sec 5.2.2 of the TPCC spec
    const uint16_t XCT_NEWORDER_PERCENT = 45;
    const uint16_t XCT_PAYMENT_PERCENT = 43 + XCT_NEWORDER_PERCENT;
    const uint16_t XCT_ORDER_STATUS_PERCENT = 4 + XCT_PAYMENT_PERCENT;
    const uint16_t XCT_DELIVERY_PERCENT = 4 + XCT_ORDER_STATUS_PERCENT;
    // remainings are stock-level xct.
    
    class full_worker_thread_t : public worker_thread_t {
    public:
        full_worker_thread_t(int32_t worker_id_arg, full_driver_thread_t *parent_arg);
        virtual ~full_worker_thread_t() {}

    protected:
        rc_t init_worker();
        rc_t run_worker();
    private:
        /** Run the TPCC Neworder transaction. Implemented in tpcc_neworder.cpp. */
        rc_t do_neworder();

        /** Run the TPCC Payment transaction. Implemented in tpcc_payment.cpp. */
        rc_t do_payment();

        /** Run the TPCC Neworder transaction. Implemented in tpcc_order_status.cpp. */
        rc_t do_order_status();
        rc_t get_last_orderid_by_customer(uint32_t wid, uint32_t did,
            uint32_t cid, uint32_t &result);

        /** Run the TPCC Neworder transaction. Implemented in tpcc_delivery.cpp. */
        rc_t do_delivery();

        /** Run the TPCC Neworder transaction. Implemented in tpcc_stock_level.cpp. */
        rc_t do_stock_level();

        /**
         * Typical lookup (SELECT .. FROM .. WHERE primary_key=x) methods.
         * They assume the record exists (writes an error message otherwise).
         */
        rc_t lookup_customer(uint32_t wid, uint32_t did, uint32_t cid, customer_data &result, bool for_write = false);
        rc_t lookup_district(uint32_t wid, uint32_t did, district_data &result, bool for_write = false);
        rc_t lookup_item(uint32_t iid, item_data &result, bool for_write = false);
        rc_t lookup_order(uint32_t wid, uint32_t did, uint32_t oid, order_data &result, bool for_write = false);
        rc_t lookup_orderline(uint32_t wid, uint32_t did, uint32_t oid, uint32_t ol, orderline_data &result, bool for_write = false);
        rc_t lookup_stock(uint32_t wid, uint32_t iid, stock_data &result, bool for_write = false);
        rc_t lookup_warehouse(uint32_t wid, warehouse_data &result, bool for_write = false);

        /** slightly special. Search 60% by last name (take midpoint), 40% by ID. */
        rc_t lookup_customer_by_id_or_name(uint32_t wid, uint32_t did,
            customer_data &result, uint32_t &result_cid, bool for_write = false);
        rc_t lookup_customer_by_name(uint32_t wid, uint32_t did, const std::string &lname,
            customer_data &result, uint32_t &result_cid, bool for_write = false);

        /**
         * SELECT OL_I_ID FROM ORDERLINE
         * WHERE WID=wid AND DID=did AND OID BETWEEN oid_from AND oid_to.
         * Implemented in tpcc_stock_level.cpp.
         */
        rc_t collect_items_from_orderline(uint32_t wid, uint32_t did,
            uint32_t oid_from, uint32_t oid_to, std::set<uint32_t> &item_ids);

        /**
         * SELECT OL_NUMBER FROM ORDERLINE
         * WHERE WID=wid AND DID=did AND OID=oid.
         * Implemented in tpcc_delivery.cpp.
         */
        rc_t collect_ol_from_orderline(uint32_t wid, uint32_t did,
            uint32_t oid, std::vector<uint32_t> &ol_numbers);

        /**
         * SELECT TOP 1 OID FROM NEWORDER WHERE WID=wid AND DID=did ORDER BY OID
         * then delete it from NEWORDER, returning the OID (eNOTFOUND if no record exists).
         * Implemented in tpcc_delivery.cpp.
         */
        rc_t pop_neworder(uint32_t wid, uint32_t did, uint32_t &result_oid);

        full_driver_thread_t *parent;
    };

    class full_driver_thread_t : public driver_thread_t {
        friend class full_worker_thread_t;

        public:
        full_driver_thread_t();
        virtual ~full_driver_thread_t() {
        }
        
        /** this method is assured to be thread-safe. */
        uint64_t issue_next_history_id();
        
        protected:
        virtual rc_t do_more_init();
        worker_thread_t* new_worker_thread(int32_t worker_id_arg);
        
        private:
        /** This must be accessed with atomic fetch_add. */
        uint64_t next_history_id;
    };
}


#endif // TPCC_FULL_H
