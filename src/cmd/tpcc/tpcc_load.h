/*
 * (c) Copyright 2013-2014, Hewlett-Packard Development Company, LP
 */

#ifndef TPCC_LOAD_H
#define TPCC_LOAD_H

#include "tpcc.h"

namespace tpcc {
    // TODO should be a program param
    const uint64_t LOAD_RANDOM_SEED = 0x1123467889ab;
    
    /**
    * \brief Data load program for TPC-C schema.
    * \details
    * Acknowledgement:
    * Some of the following source came from TpccOverBkDB by A. Fedorova:
    *   http://www.cs.sfu.ca/~fedorova/Teaching/CMPT886/Spring2007/benchtools.html
    * Several things have been changed to adjust it for C++ and Foster-Btree.
    */
    class load_driver_thread_t : public driver_thread_t {
        public:
        load_driver_thread_t();
        virtual ~load_driver_thread_t();
        protected:
        virtual rc_t do_init();
        worker_thread_t* new_worker_thread ( int32_t) {
            return NULL; // this is just data loading. no worker thread.
        }

        private:
        stid_t stids[STNUM_COUNT];

        /** Whether the customer id for the current order is already taken. */
        bool* cid_array;

        tlr_t rnd;

        rc_t create_tpcc_tables();
    
        void         random_orig(bool *orig);

        void         InitPermutation();
        uint32_t     GetPermutation();

        /** Loads the Item table. */
        rc_t         LoadItems();

        /** Loads the Warehouse table. */
        rc_t         LoadWare();

        /** Loads the Customer Table */
        rc_t         LoadCust();

        /** Loads the Orders and Order_Line Tables */
        rc_t         LoadOrd();

        rc_t         LoadNewOrd();

        /** Loads the Stock table. */
        rc_t         LoadStock();

        /** Loads the District table. */
        rc_t         LoadDistrict();

        /**
        * Loads Customer Table.
        * Also inserts corresponding history record.
        * @param[in] d_id district id
        * @param[in] w_id warehouse id
        */
        rc_t         Customer (uint32_t d_id,uint32_t w_id );

        /**
        *  Loads the Orders table.
        *  Also loads the Order_Line table on the fly.
        *
        *  @param[in] w_id warehouse id
        *  @param[in] d_id district id
        */
        rc_t         Orders ( uint32_t d_id, uint32_t w_id );

        void         MakeAddress ( char *str1, char *str2, char *city, char *state, char *zip );

        /** Make a string of letter */
        int32_t      MakeAlphaString ( int32_t min, int32_t max, char *str );

        /** Make a string of letter */
        int32_t      MakeNumberString ( int32_t min, int32_t max, char *str );
    };
}


#endif // TPCC_LOAD_H
