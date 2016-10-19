/*
 * (c) Copyright 2013-2014, Hewlett-Packard Development Company, LP
 */

#include "tpcc_example.h"
#include "btcursor.h"
#include <Lintel/ProgramOptions.hpp>

lintel::ProgramOption<uint32_t> po_wid ( "wid", "Warehouse ID", 1);
lintel::ProgramOption<uint32_t> po_did ( "did", "District ID", 1);

int main ( int argc, char ** argv ) {
    lintel::parseCommandLine ( argc, argv, false );
    tpcc::tpcc_example_thread_t thread;
    return thread.fire_experiments();
}

namespace tpcc {
    tpcc_example_thread_t::tpcc_example_thread_t() : driver_thread_t () {
        preload_tables = false; // true if you want to warm-up buffer pool.
    }

    rc_t example_worker_thread_t::init_worker() {
        wid = po_wid.get();
        did = po_did.get();
        return RCOK;
    }
    rc_t example_worker_thread_t::run_worker() {
        std::cout << "Hey, I'm worker-" << worker_id << "."
            << " Reading customers in WID=" << wid << " (--wid <ID> to specify)"
                << ", DID=" << did << " (--did <ID> to specify)..." << std::endl;

        // Probably you would have a loop here to run many xcts per thread.
        W_DO(open_xct());
        last_rc = do_the_work();
        W_DO(close_xct(last_rc));
        return last_rc;
    }
    
    rc_t example_worker_thread_t::do_the_work() {
        // Let's scan records in CUSTOMER's secondary index
        // (C_W_ID, C_D_ID, C_LAST, C_FIRST, C_ID)
        // where C_W_ID, C_D_ID are the given values.
        std::string empty_str(17, '\0');
        w_keystr_t lower_bound(to_keystr<customer_skey>(wid, did, empty_str.data(), empty_str.data(), 0));
        w_keystr_t upper_bound(to_keystr<customer_skey>(wid, did + 1, empty_str.data(), empty_str.data(), 0));
        bt_cursor_t cursor (get_our_volume_id(), STNUM_CUSTOMER_SECONDARY,
            lower_bound, true, upper_bound, false, true);
        customer_skey fetched_key;
        int32_t cnt = 0;
        do {
            W_DO(cursor.next());
            if (cursor.eof()) {
                break;
            }
            from_keystr(cursor.key(), fetched_key);
            w_assert0(fetched_key.C_W_ID == wid);
            w_assert0(fetched_key.C_D_ID == did);
            w_assert0(fetched_key.C_ID != 0);
            
            ++cnt;
            if (cnt < 30) {
                std::cout << "Worker-" << worker_id << ". Last-name='" << fetched_key.C_LAST
                    << "', first_name='" << fetched_key.C_FIRST
                    << "', C_ID=" << fetched_key.C_ID << std::endl;
            }
        } while (true);
        
        std::cout << "Worker-" << worker_id << ". Read " << cnt << " records" << std::endl;
        return RCOK;
    }
}
