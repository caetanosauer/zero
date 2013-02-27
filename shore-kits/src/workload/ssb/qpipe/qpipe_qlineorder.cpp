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
   portions thereof, and that both notices appear in supporting
   documentation.
   
   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

/** @file:   qpipe_qlineorder.cpp
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
 * QPIPE qlineorder - Structures needed by operators 
 *
 ********************************************************************/

// We encapsulate the query's helper classes and structures
// inside the class ssb_qsupplier. The purpose of this class
// is just to contain the helper classes and not be instantiated.

class ssb_qlineorder {
    
public:

    typedef ssb_lineorder_tuple lineorder_tuple;
    
//    struct lineorder_tuple {
//        int LO_ORDERKEY;
//        int LO_LINENUMBER;
//    };
    
    struct count_tuple {
        int COUNT;
    };

    typedef struct count_tuple projected_tuple;

    class lineorder_tscan_filter_t : public tuple_filter_t {
    private:
        ShoreSSBEnv* _ssbdb;
        table_row_t* _prline;
        rep_row_t _rr;

        ssb_lineorder_tuple _lineorder;

    public:

        lineorder_tscan_filter_t(ShoreSSBEnv* ssbdb, qlineorder_input_t &in)
        : tuple_filter_t(ssbdb->lineorder_desc()->maxsize()), _ssbdb(ssbdb) {

            // Get a lineorder tupple from the tuple cache and allocate space
            _prline = _ssbdb->lineorder_man()->get_tuple();
            _rr.set_ts(_ssbdb->lineorder_man()->ts(),
                    _ssbdb->lineorder_desc()->maxsize());
            _prline->_rep = &_rr;

        }

        ~lineorder_tscan_filter_t() {
            // Give back the lineorder tuple 
            _ssbdb->lineorder_man()->give_tuple(_prline);
        }


        // Predication

        bool select(const tuple_t &input) {

            // Get next lineorder and read its shipdate
            if (!_ssbdb->lineorder_man()->load(_prline, input.data)) {
                assert(false); // RC(se_WRONG_DISK_DATA)
            }

            return (true);
        }


        // Projection

        void project(tuple_t &d, const tuple_t &s) {

            ssb_qlineorder::lineorder_tuple *dest;
            dest = aligned_cast<ssb_qlineorder::lineorder_tuple> (d.data);

            _prline->get_value(0, _lineorder.LO_ORDERKEY);
            _prline->get_value(1, _lineorder.LO_LINENUMBER);
            _prline->get_value(2, _lineorder.LO_CUSTKEY);
            _prline->get_value(3, _lineorder.LO_PARTKEY);
            _prline->get_value(4, _lineorder.LO_SUPPKEY);
            _prline->get_value(5, _lineorder.LO_ORDERDATE);
            _prline->get_value(6, _lineorder.LO_ORDERPRIORITY, STRSIZE(15));
            _prline->get_value(7, _lineorder.LO_SHIPPRIORITY);
            _prline->get_value(8, _lineorder.LO_QUANTITY);
            _prline->get_value(9, _lineorder.LO_EXTENDEDPRICE);
            _prline->get_value(10, _lineorder.LO_ORDTOTALPRICE);
            _prline->get_value(11, _lineorder.LO_DISCOUNT);
            _prline->get_value(12, _lineorder.LO_REVENUE);
            _prline->get_value(13, _lineorder.LO_SUPPLYCOST);
            _prline->get_value(14, _lineorder.LO_TAX);
            _prline->get_value(15, _lineorder.LO_COMMIDATE);
            _prline->get_value(16, _lineorder.LO_SHIPMODE, STRSIZE(10));

            TRACE(TRACE_RECORD_FLOW, "%d|%d --d\n",
                    _lineorder.LO_ORDERKEY,
                    _lineorder.LO_LINENUMBER);
//
//            dest->LO_ORDERKEY = _lineorder.LO_ORDERKEY;
//            dest->LO_LINENUMBER = _lineorder.LO_LINENUMBER;
            memcpy(dest, &_lineorder, sizeof (_lineorder));
        }

        lineorder_tscan_filter_t* clone() const {
            return new lineorder_tscan_filter_t(*this);
        }

        c_str to_string() const {
            return c_str("lineorder_tscan_filter_t()");
        }
    };

    struct qlineorder_count_aggregate_t : public tuple_aggregate_t {

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

        qlineorder_count_aggregate_t()
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

        virtual qlineorder_count_aggregate_t * clone() const {
            return new qlineorder_count_aggregate_t(*this);
        }

        virtual c_str to_string() const {
            return "qlineorder_count_aggregate_t";
        }
    };

    static const c_str* dump_tuple(tuple_t* tup) {
        ssb_qlineorder::lineorder_tuple *dest;
        dest = aligned_cast<ssb_qlineorder::lineorder_tuple> (tup->data);
        return new c_str("%d|%d|%d|%d|%d|%d|%s|%d|%d|%d|%d|%d|%d|%d|%d|%d|%s|\n",
                dest->LO_ORDERKEY,
                dest->LO_LINENUMBER,
                dest->LO_CUSTKEY,
                dest->LO_PARTKEY,
                dest->LO_SUPPKEY,
                dest->LO_ORDERDATE,
                dest->LO_ORDERPRIORITY,
                dest->LO_SHIPPRIORITY,
                dest->LO_QUANTITY,
                dest->LO_EXTENDEDPRICE,
                dest->LO_ORDTOTALPRICE,
                dest->LO_DISCOUNT,
                dest->LO_REVENUE,
                dest->LO_SUPPLYCOST,
                dest->LO_TAX,
                dest->LO_COMMIDATE,
                dest->LO_SHIPMODE);
    }
    
};

class ssb_qlineorder_process_tuple_t : public process_tuple_t 
{    
public:
        
    void begin() {
        TRACE(TRACE_QUERY_RESULTS, "*** qlineorder ANSWER ...\n");
        TRACE(TRACE_QUERY_RESULTS, "*** qlineorder COUNT...\n");
    }
    
    virtual void process(const tuple_t& output) {
        ssb_qlineorder::projected_tuple *tuple;
        tuple = aligned_cast<ssb_qlineorder::projected_tuple>(output.data);
        TRACE(TRACE_QUERY_RESULTS, "%d\n", tuple->COUNT);
    }
};



/******************************************************************** 
 *
 * QPIPE qlineorder - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_qpipe_qlineorder(const int xct_id, 
                                  qlineorder_input_t& in)
{
    TRACE( TRACE_ALWAYS, "********** qlineorder *********\n");

   
    policy_t* dp = this->get_sched_policy();
    xct_t* pxct = smthread_t::me()->xct();
    

    // TSCAN PACKET
    tuple_fifo* tscan_out_buffer =
        new tuple_fifo(sizeof (ssb_qlineorder::lineorder_tuple));
        tscan_packet_t* lineorder_tscan_packet =
        new tscan_packet_t("TSCAN LINEORDER",
                           tscan_out_buffer,
                           new ssb_qlineorder::lineorder_tscan_filter_t(this,in),
                           this->db(),
                           _plineorder_desc.get(),
                           pxct
                           //, SH 
                           );
        
    tuple_fifo* fdump_output = new tuple_fifo(sizeof (ssb_qlineorder::lineorder_tuple));
    fdump_packet_t* fdump_packet =
            new fdump_packet_t(c_str("FDUMP"),
            fdump_output,
            new trivial_filter_t(fdump_output->tuple_size()),
            NULL,
            c_str("%s/lineorders.tbl", getenv("HOME")),
            NULL,
            lineorder_tscan_packet,
            ssb_qlineorder::dump_tuple);
        
    tuple_fifo* agg_output = new tuple_fifo(sizeof(ssb_qlineorder::count_tuple));
    aggregate_packet_t* agg_packet =
            new aggregate_packet_t(c_str("COUNT_AGGREGATE"),
            agg_output,
            new trivial_filter_t(agg_output->tuple_size()),
            new ssb_qlineorder::qlineorder_count_aggregate_t(),
            new ssb_qlineorder::qlineorder_count_aggregate_t::count_key_extractor_t(),
            fdump_packet);
        
    qpipe::query_state_t* qs = dp->query_state_create();
    lineorder_tscan_packet->assign_query_state(qs);
    fdump_packet->assign_query_state(qs);
    agg_packet->assign_query_state(qs);
        
    // Dispatch packet
    ssb_qlineorder_process_tuple_t pt;
    process_query(agg_packet, pt);
    dp->query_state_destroy(qs);

    return (RCOK); 
}


EXIT_NAMESPACE(ssb);
