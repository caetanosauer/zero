/*
 * (c) Copyright 2013-2014, Hewlett-Packard Development Company, LP
 */

#include "tpcc.h"
#include "tpcc_load.h"
#include "../experiments_env.h"

#include <Lintel/ProgramOptions.hpp>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <string>

using namespace tpcc;

verbose_enum verbose_level;

/** Number of warehouses. */
uint32_t warehouses;
lintel::ProgramOption<uint32_t>  po_warehouses ( "warehouses", "Number of warehouses", 4 );

/** Number of items in total. */
uint32_t items;
lintel::ProgramOption<uint32_t>  po_items ( "items", "Number of items", 100000 );

/** Number of customers per district. */
uint32_t customers_per_district;
lintel::ProgramOption<uint32_t>  po_customers ( "customers", "Number of customers per district", 3000 );

/** Number of districts per warehouse. */
uint32_t districts_per_warehouse;
lintel::ProgramOption<uint32_t>  po_districts ( "districts", "Number of districts per warehouse", 10 );

/** Number of orders per district. */
uint32_t orders_per_district;
lintel::ProgramOption<uint32_t>  po_orders ( "orders", "Number of orders per district", 3000 );

/** Number of variations of last names. */
uint32_t lnames;
lintel::ProgramOption<uint32_t> po_lnames ( "lnames", "Last name variations", 1000 );

/** Which table(s) to load. default is all. */
bool load_tables[tpcc::STNUM_COUNT];
lintel::ProgramOption<std::string>  po_load_tables ( "tables", "Specify tables to load data. "
        "c:Customer+History, d:District, i: Item, "
        "o:Order+Orderline+Neworder, s:Stock, w:Warehouse (default all)"
        " e.g., --tables cdw" );

int main ( int argc, char ** argv ) {
    lintel::parseCommandLine ( argc, argv, false );

    verbose_level           = get_verbose_level();
    warehouses              = po_warehouses.get();
    districts_per_warehouse = po_districts.get();
    customers_per_district  = po_customers.get();
    orders_per_district     = po_orders.get();
    items                   = po_items.get();
    lnames                  = po_lnames.get();

    ::memset ( load_tables, 1, sizeof ( load_tables ) );
    std::string tables_str = po_load_tables.get();
    if ( tables_str.size() > 0 ) {
        if (verbose_level >= VERBOSE_STANDARD) {
            std::cout << "Load specified TPCC tables:" << tables_str << std::endl;
        }
        ::memset ( load_tables, 0, sizeof ( load_tables ) );
        for ( size_t i = 0; i < tables_str.size(); ++i ) {
            switch ( tables_str[i] ) {
                // secondary indexes are implicit.
            case 'c':
                load_tables[tpcc::STNUM_CUSTOMER_PRIMARY] = true;
                break;
            case 'd':
                load_tables[tpcc::STNUM_DISTRICT_PRIMARY] = true;
                break;
            case 'i':
                load_tables[tpcc::STNUM_ITEM_PRIMARY] = true;
                break;
            case 'o':
                load_tables[tpcc::STNUM_ORDER_PRIMARY] = true;
                break;
            case 's':
                load_tables[tpcc::STNUM_STOCK_PRIMARY] = true;
                break;
            case 'w':
                load_tables[tpcc::STNUM_WAREHOUSE_PRIMARY] = true;
                break;
            default:
                std::cerr << "Unknown table flag. Re-run with -h for help." << std::endl;
                ::exit ( 1 );
            }
        }
    } else {
        if (verbose_level >= VERBOSE_STANDARD) {
            std::cout << "Load all TPCC tables." << std::endl;
        }
    }

    if (verbose_level >= VERBOSE_STANDARD) {
        std::cout << "#Warehouses = " << warehouses
                  << ", #Districts/warehouse=" << districts_per_warehouse
                  << ", #Customers/warehouse=" << customers_per_district
                  << ", #Orders/warehouse=" << orders_per_district
                  << ", #Items=" << items
                  << ", #Last-names=" << lnames
                  << std::endl;
    }

    tpcc::load_driver_thread_t thread;
    return thread.fire_experiments();
}

namespace tpcc {
    /** timestamp for date fields. */
    char       *timestamp;
    load_driver_thread_t::load_driver_thread_t()
        : driver_thread_t (), cid_array(NULL), rnd(LOAD_RANDOM_SEED) {
        data_load = true;
        preload_tables = false;
    }
    load_driver_thread_t::~load_driver_thread_t() {
        delete[] cid_array;
    }

    rc_t load_driver_thread_t::do_init() {
        sm_config_info_t config_info;
        W_DO ( ss_m::config_info ( config_info ) );

        devid_t        devid;
        u_int        vol_cnt;
        W_DO ( ss_m::format_dev ( data_device, disk_quota_in_kb, true ) );
        W_DO ( ss_m::mount_dev ( data_device, vol_cnt, devid ) );
        // generate a volume ID for the new volume we are about to
        // create on the device
        W_DO ( ss_m::generate_new_lvid ( lvid ) );
        // create the new volume
        W_DO ( ss_m::create_vol ( data_device, lvid, disk_quota_in_kb, false, vid ) );

        // create tables
        W_DO ( create_tpcc_tables() );
        return RCOK;
    }

    rc_t load_driver_thread_t::create_tpcc_tables() {
        // Initialize timestamp (for date columns)
        time_t t_clock;
        ::time ( &t_clock );
        timestamp = ::ctime ( &t_clock );
        assert ( timestamp != NULL );

        if (verbose_level >= VERBOSE_STANDARD) {
            std::cout << "creating TPCC tables..." << endl;
        }
        timeval start,stop,result;
        ::gettimeofday ( &start,NULL );

        for ( uint stnum = get_first_store_id(); stnum < STNUM_COUNT; ++stnum ) {
            create_table_expect_stnum ( stnum_names[stnum], stids[stnum], stnum );
        }

        if (verbose_level >= VERBOSE_STANDARD) {
            std::cout << "loading TPCC tables..." << endl;
        }

        // Make sure the following order matches the order in stnum_enum
        W_DO ( ss_m::begin_xct() );
        g_xct()->set_query_concurrency ( smlevel_0::t_cc_none ); // to speed up table creation
        W_DO (LoadItems());
        W_DO (LoadWare());
        W_DO (LoadStock());
        W_DO (LoadDistrict());
        W_DO (LoadCust());
        W_DO (LoadOrd());
        W_DO ( ss_m::commit_xct() );

        if (verbose_level >= VERBOSE_STANDARD) {
            cout << "done! making checkpoint..." << endl;
        }
        W_DO ( ss_m::force_buffers() );
        W_DO ( ss_m::checkpoint() );
        if (verbose_level >= VERBOSE_STANDARD) {
            cout << "made checkpoint." << endl;
        }

        ::gettimeofday ( &stop,NULL );
        timersub ( &stop, &start,&result );
        if (verbose_level >= VERBOSE_STANDARD) {
            cout << "initial load time=" << ( result.tv_sec + result.tv_usec/1000000.0 ) << " sec" << endl;
        }

        ::usleep ( 10000000 ); // to make sure no background thread is doing anything while experiments

        return RCOK;
    }

    void load_driver_thread_t::random_orig ( bool *orig ) {
        ::memset ( orig, 0, items * sizeof ( bool ) );
        for ( uint32_t i=0; i<items/10; ++i ) {
            int32_t pos;
            do {
                pos=rnd.uniform_within ( 0, items );
            } while ( orig[pos] );
            orig[pos] = true;
        }
    }

    rc_t load_driver_thread_t::LoadItems () {
        if ( !load_tables[STNUM_ITEM_PRIMARY] ) {
            return RCOK;
        }
        if (verbose_level >= VERBOSE_STANDARD) {
            std::cout << "Loading Item " << std::endl;
        }

        bool orig[items];
        random_orig ( orig );

        item_data i_data;
        for ( uint32_t i_id=1; i_id<=items; i_id++ ) {
            zero_clear ( i_data );

            /* Generate Item Data */
            MakeAlphaString ( 14, 24, i_data.I_NAME );
            i_data.I_PRICE = ( ( float ) rnd.uniform_within ( 100L,10000L ) ) /100.0;
            int32_t idatasiz = MakeAlphaString ( 26,50, i_data.I_DATA );

            if ( orig[i_id-1] ) {
                int32_t pos = rnd.uniform_within ( 0,idatasiz-8 );
                ::memcpy ( i_data.I_DATA + pos, "original", 8 );
            }

            if (verbose_level >= VERBOSE_TRACE) {
                std::cout << "IID = " << i_id << ", Name= " << i_data.I_NAME << ", Price = " << i_data.I_PRICE << std::endl;
            }

            i_data.I_IM_ID = 0;
            W_DO ( insert (STNUM_ITEM_PRIMARY, to_keystr<item_pkey>(i_id), i_data) );
            if (verbose_level >= VERBOSE_STANDARD) {
                if ( ! ( i_id % 500 ) ) {
                    std::cout << ".";
                    if ( ! ( i_id % 20000 ) ) std::cout << i_id << std::endl;
                }
            }
        }

        if (verbose_level >= VERBOSE_STANDARD) {
            std::cout << "Item Done." << std::endl;
        }
        return RCOK;
    }

    rc_t load_driver_thread_t::LoadWare () {
        if ( !load_tables[STNUM_WAREHOUSE_PRIMARY] ) {
            return RCOK;
        }
        if (verbose_level >= VERBOSE_STANDARD) {
            std::cout << "Loading Warehouse" << std::endl;
        }
        warehouse_data w_data;
        for ( uint32_t w_id=1; w_id<=warehouses; w_id++ ) {
            zero_clear ( w_data );

            // Generate Warehouse Data
            MakeAlphaString ( 6, 10, w_data.W_NAME );
            MakeAddress ( w_data.W_STREET_1, w_data.W_STREET_2, w_data.W_CITY, w_data.W_STATE, w_data.W_ZIP );
            w_data.W_TAX = ( ( float ) rnd.uniform_within ( 10L,20L ) ) /100.0;
            w_data.W_YTD = 3000000.00;
            if (verbose_level >= VERBOSE_STANDARD) {
                std::cout << "WID = " << w_id << ", Name= " << w_data.W_NAME << ", Tax = " << w_data.W_TAX << std::endl;
            }
            W_DO ( insert(STNUM_WAREHOUSE_PRIMARY, to_keystr<warehouse_pkey>(w_id), w_data) );
        }
        if (verbose_level >= VERBOSE_STANDARD) {
            std::cout << "Loaded Warehouse" << std::endl;
        }
        return RCOK;
    }

    rc_t load_driver_thread_t::LoadStock () {
        if ( !load_tables[STNUM_STOCK_PRIMARY] ) {
            return RCOK;
        }
        stock_data s_data;
        for ( uint32_t w_id=1; w_id<=warehouses; ++w_id ) {
            if (verbose_level >= VERBOSE_STANDARD) {
                std::cout << "Loading Stock Wid=" << w_id << std::endl;
            }
            bool orig[items];
            random_orig ( orig );

            for ( uint32_t s_i_id=1; s_i_id<=items; s_i_id++ ) {
                zero_clear ( s_data );

                // Generate Stock Data
                for (uint32_t d_id = 1; d_id <= districts_per_warehouse; ++d_id) {
                    MakeAlphaString ( 24,24,s_data.pick_dist(d_id) );
                }
                int32_t sdatasiz = MakeAlphaString ( 26,50, s_data.S_DATA );
                if ( orig[s_i_id-1] ) {
                    int32_t pos=rnd.uniform_within ( 0,sdatasiz-8 );
                    ::memcpy ( s_data.S_DATA + pos, "original", 8 );
                }

                s_data.S_QUANTITY = rnd.uniform_within (10, 100);
                s_data.S_YTD = 0;
                s_data.S_ORDER_CNT = 0;
                s_data.S_REMOTE_CNT = 0;
                W_DO ( insert (STNUM_STOCK_PRIMARY, to_keystr<stock_pkey>(w_id, s_i_id), s_data) );
                if ( verbose_level >= VERBOSE_TRACE ) {
                    std::cout << "SID = " << s_i_id << ", WID = " << w_id << ", Quan = " << s_data.S_QUANTITY << std::endl;
                }
                if (verbose_level >= VERBOSE_STANDARD) {
                    if ( ! ( s_i_id % 500 ) ) {
                        std::cout << ".";
                        if ( ! ( s_i_id % 20000 ) ) std::cout << " " << s_i_id << std::endl;
                    }
                }
            }
        }
        if (verbose_level >= VERBOSE_STANDARD) {
            std::cout << " Stock Done." << std::endl;
        }
        return RCOK;
    }

    rc_t load_driver_thread_t::LoadDistrict () {
        if ( !load_tables[STNUM_DISTRICT_PRIMARY] ) {
            return RCOK;
        }
        district_data d_data;
        for ( uint32_t w_id=1; w_id<=warehouses; ++w_id ) {
            if (verbose_level >= VERBOSE_STANDARD) {
                std::cout << "Loading District Wid=" << w_id << std::endl;
            }
            for ( uint32_t d_id=1; d_id<=districts_per_warehouse; d_id++ ) {
                zero_clear ( d_data );
                d_data.D_YTD = 30000;
                d_data.D_NEXT_O_ID = 3001;
                MakeAlphaString ( 6, 10,d_data.D_NAME );
                MakeAddress ( d_data.D_STREET_1, d_data.D_STREET_2, d_data.D_CITY, d_data.D_STATE, d_data.D_ZIP );
                d_data.D_TAX = ( ( float ) rnd.uniform_within ( 10, 20) ) /100.0;
                W_DO ( insert(STNUM_DISTRICT_PRIMARY, to_keystr<district_pkey>(w_id, d_id), d_data) );
                if ( verbose_level >= tpcc::VERBOSE_DETAIL ) {
                    std::cout << "DID = " << d_id << ", WID = " << w_id << ", Name = " << d_data.D_NAME << ", Tax = " << d_data.D_TAX << std::endl;
                }
            }
        }
        if (verbose_level >= VERBOSE_STANDARD) {
            std::cout << "District Done" << std::endl;
        }
        return RCOK;
    }

    rc_t load_driver_thread_t::LoadCust () {
        if ( !load_tables[STNUM_CUSTOMER_PRIMARY] ) {
            return RCOK;
        }
        for ( uint32_t w_id=1; w_id<=warehouses; w_id++ ) {
            for ( uint32_t d_id=1; d_id<=districts_per_warehouse; d_id++ ) {
                W_DO ( Customer ( d_id,w_id ) );
            }
        }
        return RCOK;
    }

    rc_t load_driver_thread_t::LoadOrd () {
        if ( !load_tables[STNUM_ORDER_PRIMARY] ) {
            return RCOK;
        }
        for ( uint32_t w_id=1; w_id<=warehouses; w_id++ ) {
            for ( uint32_t d_id=1; d_id<=districts_per_warehouse; d_id++ ) {
                W_DO ( Orders ( d_id, w_id ) );
            }
        }
        return RCOK;
    }

    uint64_t next_history_id = 1;

    rc_t load_driver_thread_t::Customer ( uint32_t d_id, uint32_t w_id ) {
        if (verbose_level >= VERBOSE_STANDARD) {
            std::cout << "Loading Customer for DID=" << d_id << ", WID=" << w_id;
        }
        customer_data c_data;
        history_data  h_data;
        for ( uint32_t c_id = 1; c_id <= customers_per_district; ++c_id) {
            zero_clear ( c_data );
            zero_clear ( h_data );

            // Generate Customer Data
            MakeAlphaString ( 8, 16, c_data.C_FIRST );
            c_data.C_MIDDLE[0] = 'O';
            c_data.C_MIDDLE[1] = 'E';
            c_data.C_MIDDLE[2] = '\0';

            if ( c_id <= lnames ) {
                generate_lastname( c_id-1,c_data.C_LAST );
            } else {
                generate_lastname( rnd.non_uniform_within ( 255,0,lnames - 1 ),c_data.C_LAST );
            }

            MakeAddress ( c_data.C_STREET_1, c_data.C_STREET_2, c_data.C_CITY, c_data.C_STATE, c_data.C_ZIP );
            MakeNumberString ( 16, 16, c_data.C_PHONE );
            c_data.C_CREDIT[0]= (rnd.uniform_within (0, 1) == 0 ? 'G' : 'B');
            c_data.C_CREDIT[1]='C';
            c_data.C_CREDIT[2]='\0';
            MakeAlphaString ( 300,500,c_data.C_DATA );

            // Prepare for putting into the database
            c_data.C_DISCOUNT = ( ( float ) rnd.uniform_within ( 0, 50 ) ) /100.0;
            c_data.C_BALANCE = -10.0;
            c_data.C_CREDIT_LIM = 50000;

            W_DO ( insert(STNUM_CUSTOMER_PRIMARY, to_keystr<customer_pkey>( w_id, d_id, c_id ), c_data));
            W_DO ( insert(STNUM_CUSTOMER_SECONDARY, to_keystr<customer_skey>( w_id, d_id, c_data.C_LAST, c_data.C_FIRST, c_id)));
            if ( verbose_level >= VERBOSE_TRACE ) {
                std::cout << "CID = " << c_id << ", LST = " << c_data.C_LAST << ", P# = " << c_data.C_PHONE << std::endl;
            }

            MakeAlphaString ( 12,24, h_data.H_DATA );
            h_data.H_C_ID = c_id;
            h_data.H_C_D_ID = d_id;
            h_data.H_C_W_ID = w_id;
            h_data.H_W_ID = w_id;
            h_data.H_D_ID = d_id;
            h_data.H_AMOUNT = 10.0;
            ::memcpy ( h_data.H_DATE, timestamp, 26 );
            W_DO ( insert (STNUM_HISTORY_PRIMARY, to_keystr<history_pkey>(next_history_id++), h_data));

            if (verbose_level >= VERBOSE_STANDARD) {
                if ( ! ( c_id % 100 ) ) {
                    std::cout << ".";
                    if ( ! ( c_id % 1000 ) ) std::cout << c_id;
                }
            }
        }
        if (verbose_level >= VERBOSE_STANDARD) {
            std::cout << std::endl;
        }

        return RCOK;
    }

    rc_t load_driver_thread_t::Orders ( uint32_t d_id, uint32_t w_id ) {
        if (verbose_level >= VERBOSE_STANDARD) {
            std::cout << "Loading Orders for D=" << d_id << ", W= " << w_id << "...";
        }
        InitPermutation();           /* initialize permutation of customer numbers */

        order_data     o_data;
        orderline_data ol_data;
        for ( uint32_t o_id=1; o_id<=orders_per_district; o_id++ ) {
            zero_clear ( o_data );

            // Generate Order Data
            uint32_t o_c_id = GetPermutation();
            uint32_t o_carrier_id = rnd.uniform_within ( 1,10 );
            uint32_t o_ol_cnt=rnd.uniform_within ( 5,15 );

            o_data.O_C_ID = o_c_id;
            o_data.O_ALL_LOCAL = 1;
            o_data.O_OL_CNT = o_ol_cnt;
            ::memcpy ( o_data.O_ENTRY_D, timestamp, 26 );

            if ( o_id > 2100 ) {     /* the last 900 orders have not been delivered) */
                o_data.O_CARRIER_ID = 0;
                W_DO ( insert (STNUM_NEWORDER_PRIMARY, to_keystr<neworder_pkey>(w_id, d_id, o_id)) );
            } else {
                o_data.O_CARRIER_ID = o_carrier_id;
            }

            W_DO ( insert (STNUM_ORDER_PRIMARY, to_keystr<order_pkey>(w_id, d_id, o_id), o_data) );
            W_DO ( insert (STNUM_ORDER_SECONDARY, to_keystr<order_skey>(w_id, d_id, o_c_id, o_id)) );

            if ( verbose_level >= VERBOSE_TRACE ) {
                std::cout << "OID = " << o_id << ", CID = " << o_c_id << ", DID = " << d_id << ", WID = " << w_id << std::endl;
            }

            for ( uint32_t ol=1; ol<=o_ol_cnt; ol++ ) {
                zero_clear ( ol_data );

                // Generate Order Line Data
                MakeAlphaString ( 24,24,  ol_data.OL_DIST_INFO );
                ol_data.OL_I_ID = rnd.uniform_within ( 1,items );
                ol_data.OL_SUPPLY_W_ID = w_id;
                ol_data.OL_QUANTITY = 5;
                if ( o_id > 2100 ) {
                    ol_data.OL_AMOUNT = 0;
                } else {
                    std::string time_str(get_current_time_string());
                    ol_data.OL_AMOUNT = ( float ) ( rnd.uniform_within ( 10L, 10000L ) ) /100.0;
                    ::memcpy ( ol_data.OL_DELIVERY_D, time_str.data(), time_str.size());
                }

                W_DO ( insert(STNUM_ORDERLINE_PRIMARY, to_keystr<orderline_pkey>(w_id, d_id, o_id, ol), ol_data) );
                if ( verbose_level >= VERBOSE_TRACE ) {
                    std::cout << "OL = " << ol << ", IID = " << ol_data.OL_I_ID << ", QUAN = " << ol_data.OL_QUANTITY << ", AMT = " << ol_data.OL_AMOUNT << std::endl;
                }
            }

            if (verbose_level >= VERBOSE_STANDARD) {
                if ( ! ( o_id % 100 ) ) {
                    std::cout << ".";
                    if ( ! ( o_id % 1000 ) ) std::cout << o_id;
                }
            }
        }

        if (verbose_level >= VERBOSE_STANDARD) {
            std::cout << std::endl;
        }
        return RCOK;
    }

    int32_t load_driver_thread_t::MakeAlphaString ( int32_t min, int32_t max, char *str ) {
        const char *character =
            /***  "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"; */
            "abcedfghijklmnopqrstuvwxyz";
        int32_t length = rnd.uniform_within ( min, max );

        for ( int32_t  i=0;  i<length;  i++ ) {
            str[i] = character[rnd.uniform_within ( 0, sizeof ( character )-2 )];
        }
        // to make sure, fill out _all_ remaining part with NULL character.
        ::memset ( str + length, 0, max - length + 1 );

        return length;
    }

    int32_t load_driver_thread_t::MakeNumberString ( int32_t min, int32_t max, char *str ) {
        const char *character =
            /***  "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"; */
            "1234567890";
        int32_t length = rnd.uniform_within ( min, max );
        for ( int32_t  i=0;  i<length;  i++ ) {
            str[i] = character[rnd.uniform_within ( 0, sizeof ( character )-2 )];
        }
        // to make sure, fill out _all_ remaining part with NULL character.
        ::memset ( str + length, 0, max - length + 1 );

        return length;
    }

    void load_driver_thread_t::MakeAddress ( char *str1, char *str2, char *city, char *state, char *zip ) {
        MakeAlphaString ( 10,20,str1 ); /* Street 1*/
        MakeAlphaString ( 10,20,str2 ); /* Street 2*/
        MakeAlphaString ( 10,20,city ); /* City */
        MakeAlphaString ( 2,2,state ); /* State */
        MakeNumberString ( 9,9,zip ); /* Zip */
    }

    void load_driver_thread_t::InitPermutation() {
        if ( cid_array == NULL ) {
            cid_array = new bool[customers_per_district + 1];
        }
        ::memset(cid_array, 0, sizeof(bool) * (customers_per_district + 1));
    }

    uint32_t load_driver_thread_t::GetPermutation() {
        while ( true ) {
            uint32_t r = rnd.uniform_within (1, customers_per_district);
            if ( cid_array[r] ) {           /* This number already taken */
                continue;
            }
            cid_array[r] = true;               /* mark taken */
            return ( r );
        }
    }
}
