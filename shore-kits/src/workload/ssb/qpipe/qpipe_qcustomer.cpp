/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-kits -- Benchmark implementations for Shore-MT
   
                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne
   
                         All Rights Reserved.
   
   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in custorting
   documentation.
   
   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

/** @file:   qpipe_qcustomer.cpp
 *
 *  @brief:  Implementation of simple QPIPE SSB tablescans over Shore-MT
 *
 *  @author: Manos Athanassoulis
 *  @date:   July 2010
 */

#include "workload/ssb/shore_ssb_env.h"
#include "qpipe.h"

using namespace shore;
using namespace qpipe;



ENTER_NAMESPACE(ssb);


/******************************************************************** 
 *
 * QPIPE qcustomer - Structures needed by operators 
 *
 ********************************************************************/

// We encapsulate the query's helper classes and structures
// inside the class ssb_qcustomer. The purpose of this class
// is just to contain the helper classes and not be instantiated.

class ssb_qcustomer {
    
public:
    
    typedef ssb_customer_tuple customer_tuple;

    struct count_tuple {
        int COUNT;
    };

    typedef struct ssb_qcustomer::count_tuple projected_tuple;

    class customer_tscan_filter_t : public tuple_filter_t {
    private:
        ShoreSSBEnv* _ssbdb;
        table_row_t* _prcust;
        rep_row_t _rr;

        ssb_customer_tuple _customer;

    public:

        customer_tscan_filter_t(ShoreSSBEnv* ssbdb, qcustomer_input_t &in)
        : tuple_filter_t(ssbdb->customer_desc()->maxsize()), _ssbdb(ssbdb) {

            // Get a customer tupple from the tuple cache and allocate space
            _prcust = _ssbdb->customer_man()->get_tuple();
            _rr.set_ts(_ssbdb->customer_man()->ts(),
                    _ssbdb->customer_desc()->maxsize());
            _prcust->_rep = &_rr;

        }

        ~customer_tscan_filter_t() {
            // Give back the customer tuple 
            _ssbdb->customer_man()->give_tuple(_prcust);
        }


        // Predication

        bool select(const tuple_t &input) {

            // Get next customer and read its shipdate
            if (!_ssbdb->customer_man()->load(_prcust, input.data)) {
                assert(false); // RC(se_WRONG_DISK_DATA)
            }

            return (true);
        }


        // Projection

        void project(tuple_t &d, const tuple_t &s) {
            customer_tuple *dest;
            dest = aligned_cast<customer_tuple > (d.data);

            _prcust->get_value(0, _customer.C_CUSTKEY);
            _prcust->get_value(1, _customer.C_NAME, STRSIZE(25));
            _prcust->get_value(2, _customer.C_ADDRESS, STRSIZE(25));
            _prcust->get_value(3, _customer.C_CITY, STRSIZE(10));
            _prcust->get_value(4, _customer.C_NATION, STRSIZE(15));
            _prcust->get_value(5, _customer.C_REGION, STRSIZE(12));
            _prcust->get_value(6, _customer.C_PHONE, STRSIZE(15));
            _prcust->get_value(7, _customer.C_MKTSEGMENT, STRSIZE(10));


            TRACE(TRACE_RECORD_FLOW, "CUSTOMER: %s --d\n",
                    _customer.C_NATION);

            memcpy(dest, &_customer, sizeof (_customer));
        }

        customer_tscan_filter_t* clone() const {
            return new customer_tscan_filter_t(*this);
        }

        c_str to_string() const {
            return c_str("customer_tscan_filter_t()");
        }
    };
    
    struct qcustomer_count_aggregate_t : public tuple_aggregate_t {

        class count_key_extractor_t : public key_extractor_t {
        public:

            count_key_extractor_t()
            : key_extractor_t(0, 0) {
                // We don't need to form groups in the count,
                // so the key size is 0. As such, No hints or comparisons 
                // should be made by the aggregator.
            }
            
            virtual key_extractor_t* clone() const {
                return new count_key_extractor_t(*this);
            }
        };

        count_key_extractor_t _extractor;

        qcustomer_count_aggregate_t()
        : tuple_aggregate_t(sizeof (count_tuple)) {

        }

        virtual key_extractor_t * key_extractor() {
            return &_extractor;
        }

        virtual void aggregate(char* agg_data, const tuple_t &) {
            count_tuple* agg = aligned_cast<count_tuple > (agg_data);
            agg->COUNT++;
        }

        virtual void finish(tuple_t &d, const char* agg_data) {
            count_tuple* agg = aligned_cast<count_tuple > (agg_data);
            count_tuple* output = aligned_cast<count_tuple > (d.data);
            output->COUNT = agg->COUNT;
        }

        virtual qcustomer_count_aggregate_t * clone() const {
            return new qcustomer_count_aggregate_t(*this);
        }

        virtual c_str to_string() const {
            return "qcustomer_count_aggregate_t";
        }
    };

    static const c_str* dump_tuple(tuple_t* tup) {
        ssb_qcustomer::customer_tuple *dest;
        dest = aligned_cast<ssb_qcustomer::customer_tuple> (tup->data);
        return new c_str("%d|%s|%s|%s|%s|%s|%s|%s|\n",
                dest->C_CUSTKEY,
                dest->C_NAME,
                dest->C_ADDRESS,
                dest->C_CITY,
                dest->C_NATION,
                dest->C_REGION,
                dest->C_PHONE,
                dest->C_MKTSEGMENT);
    }
    
};





class ssb_qcustomer_process_tuple_t : public process_tuple_t 
{    
public:
        
    void begin() {
        TRACE(TRACE_QUERY_RESULTS, "*** qcustomer ANSWER ...\n");
        TRACE(TRACE_QUERY_RESULTS, "*** qcustomer COUNT...\n");
    }
    
    virtual void process(const tuple_t& output) {
        ssb_qcustomer::projected_tuple *tuple;
        tuple = aligned_cast<ssb_qcustomer::projected_tuple>(output.data);
        TRACE(TRACE_QUERY_RESULTS, "%d\n", tuple->COUNT);
    }
};



/******************************************************************** 
 *
 * QPIPE qcustomer - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_qpipe_qcustomer(const int xct_id, 
                                  qcustomer_input_t& in)
{
    TRACE( TRACE_ALWAYS, "********** qcustomer *********\n");

   
    policy_t* dp = this->get_sched_policy();
    xct_t* pxct = smthread_t::me()->xct();
    

    // TSCAN PACKET
    tuple_fifo* tscan_out_buffer =
        new tuple_fifo(sizeof(ssb_qcustomer::customer_tuple));
        tscan_packet_t* customer_tscan_packet =
        new tscan_packet_t("TSCAN CUSTOMER",
                           tscan_out_buffer,
                           new ssb_qcustomer::customer_tscan_filter_t(this,in),
                           this->db(),
                           _pcustomer_desc.get(),
                           pxct
                           //, SH 
                           );

    tuple_fifo* fdump_output = new tuple_fifo(sizeof(ssb_qcustomer::customer_tuple));
    fdump_packet_t* fdump_packet =
            new fdump_packet_t(c_str("FDUMP"),
            fdump_output,
            new trivial_filter_t(fdump_output->tuple_size()),
            NULL,
            c_str("%s/customers.tbl", getenv("HOME")),
            NULL,
            customer_tscan_packet,
            ssb_qcustomer::dump_tuple);

    tuple_fifo* agg_output = new tuple_fifo(sizeof (ssb_qcustomer::projected_tuple));
    aggregate_packet_t* agg_packet =
            new aggregate_packet_t(c_str("COUNT_AGGREGATE"),
            agg_output,
            new trivial_filter_t(agg_output->tuple_size()),
            new ssb_qcustomer::qcustomer_count_aggregate_t(),
            new ssb_qcustomer::qcustomer_count_aggregate_t::count_key_extractor_t(),
            fdump_packet);
        
    qpipe::query_state_t* qs = dp->query_state_create();
    customer_tscan_packet->assign_query_state(qs);
    fdump_packet->assign_query_state(qs);
    agg_packet->assign_query_state(qs);
        
    // Dispatch packet
    ssb_qcustomer_process_tuple_t pt;
    process_query(agg_packet, pt);
    dp->query_state_destroy(qs);

    return (RCOK); 
}


EXIT_NAMESPACE(ssb);

