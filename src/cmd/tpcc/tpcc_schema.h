/*
 * (c) Copyright 2013-2014, Hewlett-Packard Development Company, LP
 */

#ifndef TPCC_SCHEMA_H
#define TPCC_SCHEMA_H

#include "w_endian.h"
#include "w_key.h"
#include <string.h>
#include <ostream>

/**
 * \file tpcc_schema.h
 * \brief Definition of TPC-C schema.
 * \details
* Acknowledgement:
* Soem of the following source came from TpccOverBkDB by A. Fedorova:
*   http://www.cs.sfu.ca/~fedorova/Teaching/CMPT886/Spring2007/benchtools.html
*
* All character arrays are 1 char longer than required to
* account for the null character at the end of the string.
* A few things have been changed to be more platform independent.
*
* Also, all key classes have "convert_from_host_to_big_endian()"
* and "convert_from_big_endian_to_host()" methods to store
* all multi-byte integer types (e.g., uint32_t) in big endian
* so that memcmp is enough to sort.
* Before calling w_keystr_t::construct_regularkey(),
* call convert_from_host_to_big_endian().
* After retrieving a key from Foster B-tree,
* call convert_from_big_endian_to_host().
* 
* Finally, in key classes we have only unsigned integer types.
* This makes the comparison easier (if it's signed, memcmp doesn't work
* even in big-endian).
*/
namespace tpcc {
#define ENDIAN_CONVERTERS(X) \
    void convert_from_host_to_big_endian() {\
        serialize_be(&( X ), ( X ));\
    }\
    void convert_from_big_endian_to_host() {\
        deserialize_ho( X );\
    }

// I know what you will say. Variadic macro? I wouldn't rely too much on new features.
#define ENDIAN_CONVERTERS2(X, Y) \
    void convert_from_host_to_big_endian() {\
        serialize_be(&( X ), ( X ));\
        serialize_be(&( Y ), ( Y ));\
    }\
    void convert_from_big_endian_to_host() {\
        deserialize_ho( X );\
        deserialize_ho( Y );\
    }

#define ENDIAN_CONVERTERS3(X, Y, Z) \
    void convert_from_host_to_big_endian() {\
        serialize_be(&( X ), ( X ));\
        serialize_be(&( Y ), ( Y ));\
        serialize_be(&( Z ), ( Z ));\
    }\
    void convert_from_big_endian_to_host() {\
        deserialize_ho( X );\
        deserialize_ho( Y );\
        deserialize_ho( Z );\
    }

#define ENDIAN_CONVERTERS4(X, Y, Z, ZZ) \
    void convert_from_host_to_big_endian() {\
        serialize_be(&( X ), ( X ));\
        serialize_be(&( Y ), ( Y ));\
        serialize_be(&( Z ), ( Z ));\
        serialize_be(&( ZZ ), ( ZZ ));\
    }\
    void convert_from_big_endian_to_host() {\
        deserialize_ho( X );\
        deserialize_ho( Y );\
        deserialize_ho( Z );\
        deserialize_ho( ZZ );\
    }
    
    /** (wid). */
    struct warehouse_pkey {
        warehouse_pkey() {}
        warehouse_pkey(uint32_t wid) : W_ID(wid){}
        uint32_t W_ID;
        ENDIAN_CONVERTERS(W_ID);
    };

    struct warehouse_data {
        char   W_NAME[11];
        char   W_STREET_1[21];
        char   W_STREET_2[21];
        char   W_CITY[21];
        char   W_STATE[3];
        char   W_ZIP[10];
        double W_TAX;
        double W_YTD;
    };

    /** (wid, did). */
    struct district_pkey {
        district_pkey() {}
        district_pkey(uint32_t wid, uint32_t did) : D_W_ID(wid), D_ID(did){}
        uint32_t D_W_ID;
        uint32_t D_ID;
        ENDIAN_CONVERTERS2(D_W_ID, D_ID);
    };

    struct district_data {
        char   D_NAME[11];
        char   D_STREET_1[21];
        char   D_STREET_2[21];
        char   D_CITY[21];
        char   D_STATE[3];
        char   D_ZIP[10];
        double D_TAX;
        uint64_t D_YTD;
        uint32_t D_NEXT_O_ID;
    };

    /** (wid, did, cid). */
    struct customer_pkey {
        customer_pkey() {}
        customer_pkey(uint32_t wid, uint32_t did, uint32_t cid)
            : C_W_ID(wid), C_D_ID(did), C_ID(cid){}
        uint32_t C_W_ID;
        uint32_t C_D_ID;
        uint32_t C_ID;
        ENDIAN_CONVERTERS3(C_W_ID, C_D_ID, C_ID);
    };

    struct customer_data {
        char   C_FIRST[17];
        char   C_MIDDLE[3];
        char   C_LAST[17];
        char   C_STREET_1[21];
        char   C_STREET_2[21];
        char   C_CITY[21];
        char   C_STATE[3];
        char   C_ZIP[10];
        char   C_PHONE[16];
        char   C_SINCE[26];
        char   C_CREDIT[3];
        double C_CREDIT_LIM;
        double C_DISCOUNT;
        uint64_t C_YTD_PAYMENT;
        uint32_t C_PAYMENT_CNT;
        uint32_t C_DELIVERY_CNT;
        double C_BALANCE;
        char   C_DATA[501];
    };

    /**
     * (wid, did, last, first, cid).
     * Secondary index to allow lookup by last name.
     */
    struct customer_skey {
        customer_skey() {}
        customer_skey(uint32_t wid, uint32_t did,
            const char* last, const char* first, uint32_t cid)
            : C_W_ID(wid), C_D_ID(did), C_ID(cid){
            ::memcpy(C_LAST, last, sizeof(C_LAST));
            ::memcpy(C_FIRST, first, sizeof(C_FIRST));
        }
        uint32_t C_W_ID;
        uint32_t C_D_ID;
        char C_LAST[17];
        char C_FIRST[17];
        uint32_t C_ID;
        ENDIAN_CONVERTERS3(C_W_ID, C_D_ID, C_ID);
    };

    /** (hid). */
    struct history_pkey {
        history_pkey() {}
        history_pkey(uint64_t hid) : H_ID(hid){}
        uint64_t H_ID;
        ENDIAN_CONVERTERS(H_ID);
    };

    /**
     * NOTE unlike the original implementation, these are data, not key.
     * Key is a sequentially issued integer above.
     * TPC-C spec says this table has no key, but Foster B-tree needs a key (we don't support heap).
     */
    struct history_data {
        uint32_t H_C_ID;
        uint32_t H_C_D_ID;
        uint32_t H_C_W_ID;
        uint32_t H_D_ID;
        uint32_t H_W_ID;
        char    H_DATE[26];
        double  H_AMOUNT;
        char    H_DATA[25];
    };

    /** (wid, did, oid). */
    struct neworder_pkey {
        neworder_pkey() {}
        neworder_pkey(uint32_t wid, uint32_t did, uint32_t oid)
            : NO_W_ID(wid), NO_D_ID(did), NO_O_ID(oid){}
        uint32_t NO_W_ID;
        uint32_t NO_D_ID;
        uint32_t NO_O_ID;
        ENDIAN_CONVERTERS3(NO_W_ID, NO_D_ID, NO_O_ID);
    };

    /** (wid, did, oid). */
    struct order_pkey {
        order_pkey() {}
        order_pkey(uint32_t wid, uint32_t did, uint32_t oid)
            : O_W_ID(wid), O_D_ID(did), O_ID(oid){}
        uint32_t O_W_ID;
        uint32_t O_D_ID;
        uint32_t O_ID;
        ENDIAN_CONVERTERS3(O_W_ID, O_D_ID, O_ID);
    };

    struct order_data {
        uint32_t O_C_ID;
        char O_ENTRY_D[26];
        uint32_t O_CARRIER_ID;
        char O_OL_CNT;
        char O_ALL_LOCAL;
    };

    /**
     * (wid, did, cid, oid).
     * Secondary index to allow look-ups by O_W_ID, O_D_ID, O_C_ID.
     * The last O_ID is uniquefier (different from original implementation).
     */
    struct order_skey {
        order_skey() {}
        order_skey(uint32_t wid, uint32_t did, uint32_t cid, uint32_t oid)
            : O_W_ID(wid), O_D_ID(did), O_C_ID(cid), O_ID(oid){}
        uint32_t O_W_ID;
        uint32_t O_D_ID;
        uint32_t O_C_ID;
        uint32_t O_ID;
        ENDIAN_CONVERTERS4(O_W_ID, O_D_ID, O_C_ID, O_ID);
    };

    /** (wid, did, oid, ol). */
    struct orderline_pkey {
        orderline_pkey() {}
        orderline_pkey(uint32_t wid, uint32_t did, uint32_t oid, uint32_t ol)
            : OL_W_ID(wid), OL_D_ID(did), OL_O_ID(oid), OL_NUMBER(ol){}
        uint32_t OL_W_ID;
        uint32_t OL_D_ID;
        uint32_t OL_O_ID;
        uint32_t OL_NUMBER;
        ENDIAN_CONVERTERS4(OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER);
    };

    struct orderline_data {
        uint32_t OL_I_ID;
        uint32_t OL_SUPPLY_W_ID;
        char   OL_DELIVERY_D[26];
        char   OL_QUANTITY;
        double OL_AMOUNT;
        char   OL_DIST_INFO[25];
    };

    /** (iid). */
    struct item_pkey {
        item_pkey() {}
        item_pkey(uint32_t iid) : I_ID(iid){}
        uint32_t I_ID;
        ENDIAN_CONVERTERS(I_ID);
    };

    struct item_data {
        uint32_t I_IM_ID;
        char I_NAME[25];
        uint32_t I_PRICE;
        char I_DATA[51];
    };

    /** (wid, iid). */
    struct stock_pkey {
        stock_pkey() {}
        stock_pkey(uint32_t wid, uint32_t iid) : S_W_ID(wid), S_I_ID(iid){}
        uint32_t S_W_ID;
        uint32_t S_I_ID;
        ENDIAN_CONVERTERS2(S_W_ID, S_I_ID);
    };

    struct stock_data {
        char S_DIST_01[25];
        char S_DIST_02[25];
        char S_DIST_03[25];
        char S_DIST_04[25];
        char S_DIST_05[25];
        char S_DIST_06[25];
        char S_DIST_07[25];
        char S_DIST_08[25];
        char S_DIST_09[25];
        char S_DIST_10[25];
        uint64_t S_YTD;
        uint32_t S_ORDER_CNT;
        uint32_t S_QUANTITY;
        uint32_t S_REMOTE_CNT;
        char S_DATA[51];

        char* pick_dist(uint32_t did) {
            switch (did) {
                case 1: return S_DIST_01;
                case 2: return S_DIST_02;
                case 3: return S_DIST_03;
                case 4: return S_DIST_04;
                case 5: return S_DIST_05;
                case 6: return S_DIST_06;
                case 7: return S_DIST_07;
                case 8: return S_DIST_08;
                case 9: return S_DIST_09;
                case 10: return S_DIST_10;
                default: return NULL;
            }
        }
    };

    // Make sure this order matches the order in tpcc_load_thread_t::create_tpcc_tables().
    // We don't have metadata store for now, so we have to rely on hard-coded order.
    enum stnum_enum {
        STNUM_CUSTOMER_PRIMARY = 1,// FIRST_STORE_ID
        STNUM_CUSTOMER_SECONDARY,
        STNUM_DISTRICT_PRIMARY,
        STNUM_HISTORY_PRIMARY,
        STNUM_ITEM_PRIMARY,
        STNUM_NEWORDER_PRIMARY,
        STNUM_ORDER_PRIMARY,
        STNUM_ORDER_SECONDARY,
        STNUM_ORDERLINE_PRIMARY,
        STNUM_STOCK_PRIMARY,
        STNUM_WAREHOUSE_PRIMARY,
        STNUM_COUNT,
    };
    
    const char* const stnum_names[] = {
        "",
        "CUSTOMER_PRIMARY",
        "CUSTOMER_SECONDARY",
        "DISTRICT_PRIMARY",
        "HISTORY_PRIMARY",
        "ITEM_PRIMARY",
        "NEWORDER_PRIMARY",
        "ORDER_PRIMARY",
        "ORDER_SECONDARY",
        "ORDERLINE_PRIMARY",
        "STOCK_PRIMARY",
        "WAREHOUSE_PRIMARY",
        "",
    };
}

namespace tpcc {
    /**
     * Use it like:
     * neworder_pkey fetched;
     * from_keystr(cursor.key(), fetched);
     * NOTE this method doesn't work if the keys are infimum/supremum.
     * Beawawe of the cases (see w_keystr_t's comments).
     */
    template<class KEY>
    void from_keystr(const w_keystr_t& keystr, KEY &result) {
        keystr.serialize_as_nonkeystr(&result);

        // Do NOT forget to call convert_from_big_endian_to_host().
        // Otherwise the returned integers are in big endians.
        result.convert_from_host_to_big_endian();
    }
    
    
    /**
     * Use them like:
     * w_keystr_t key(to_keystr<neworder_pkey>(wid, did, 0));
     * Don't bother variadic template (C++11). Not worth it.
     */
    template<class KEY>
    w_keystr_t _to_keystr(KEY &key) {
        key.convert_from_host_to_big_endian();
        w_keystr_t ret;
        ret.construct_regularkey(&key, sizeof(KEY));
        return ret;
    }
    template<class KEY, typename V1>
    w_keystr_t to_keystr(V1 v1) {
        KEY key(v1);
        return _to_keystr(key);
    }
    template<class KEY, typename V1, typename V2>
    w_keystr_t to_keystr(V1 v1, V2 v2) {
        KEY key(v1, v2);
        return _to_keystr(key);
    }
    template<class KEY, typename V1, typename V2, typename V3>
    w_keystr_t to_keystr(V1 v1, V2 v2, V3 v3) {
        KEY key(v1, v2, v3);
        return _to_keystr(key);
    }
    template<class KEY, typename V1, typename V2, typename V3, typename V4>
    w_keystr_t to_keystr(V1 v1, V2 v2, V3 v3, V4 v4) {
        KEY key(v1, v2, v3, v4);
        return _to_keystr(key);
    }
    template<class KEY, typename V1, typename V2, typename V3, typename V4, typename V5>
    w_keystr_t to_keystr(V1 v1, V2 v2, V3 v3, V4 v4, V5 v5) {
        KEY key(v1, v2, v3, v4, v5);
        return _to_keystr(key);
    }
}

// ostream << overloading. these are in global namespace to be used conveniently.
inline std::ostream& operator<<(std::ostream& o, const tpcc::warehouse_pkey& v) {
    o << "(" << v.W_ID << ")";
    return o;
}
inline std::ostream& operator<<(std::ostream& o, const tpcc::district_pkey& v) {
    o << "(" << v.D_W_ID << "," << v.D_ID << ")";
    return o;
}
inline std::ostream& operator<<(std::ostream& o, const tpcc::customer_pkey& v) {
    o << "(" << v.C_W_ID << "," << v.C_D_ID << "," << v.C_ID << ")";
    return o;
}
inline std::ostream& operator<<(std::ostream& o, const tpcc::customer_skey& v) {
    o << "(" << v.C_W_ID << "," << v.C_D_ID << "," << v.C_LAST << "," << v.C_FIRST << "," << v.C_ID << ")";
    return o;
}
inline std::ostream& operator<<(std::ostream& o, const tpcc::order_pkey& v) {
    o << "(" << v.O_W_ID << "," << v.O_D_ID << "," << v.O_ID << ")";
    return o;
}
inline std::ostream& operator<<(std::ostream& o, const tpcc::order_skey& v) {
    o << "(" << v.O_W_ID << "," << v.O_D_ID << "," << v.O_C_ID << "," << v.O_ID << ")";
    return o;
}
inline std::ostream& operator<<(std::ostream& o, const tpcc::orderline_pkey& v) {
    o << "(" << v.OL_W_ID << "," << v.OL_W_ID << "," << v.OL_O_ID << "," << v.OL_NUMBER << ")";
    return o;
}
inline std::ostream& operator<<(std::ostream& o, const tpcc::neworder_pkey& v) {
    o << "(" << v.NO_W_ID << "," << v.NO_D_ID << "," << v.NO_O_ID << ")";
    return o;
}
inline std::ostream& operator<<(std::ostream& o, const tpcc::history_pkey& v) {
    o << "(" << v.H_ID << ")";
    return o;
}
inline std::ostream& operator<<(std::ostream& o, const tpcc::item_pkey& v) {
    o << "(" << v.I_ID << ")";
    return o;
}
inline std::ostream& operator<<(std::ostream& o, const tpcc::stock_pkey& v) {
    o << "(" << v.S_W_ID << "," << v.S_I_ID << ")";
    return o;
}

#endif // TPCC_SCHEMA_H
