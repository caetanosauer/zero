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

/** @file:   qpipe_qorders.cpp
 *
 *  @brief:  Implementation of a orders tablescan over QPipe over Shore-MT
 *
 *  @author: Manos Athanassoulis
 *  @date:   Nov 2011
 */

#include "workload/tpch/shore_tpch_env.h"
#include "qpipe.h"

using namespace shore;
using namespace qpipe;



ENTER_NAMESPACE(tpch);


/********************************************************************
 *
 * QPIPE Scan Orders - Structures needed by operators
 *
 ********************************************************************/

// count
class tpch_qorders{

public:


struct count_tuple {
    int COUNT;
};




class tscan_filter_t : public tuple_filter_t
{
private:
    ShoreTPCHEnv* _tpchdb;
    table_row_t* _prord;
    rep_row_t _rr;

    tpch_orders_tuple _orders;
public:

    tscan_filter_t(ShoreTPCHEnv* tpchdb, qorders_input_t &in)
        : tuple_filter_t(tpchdb->orders_desc()->maxsize()), _tpchdb(tpchdb)
          //: tuple_filter_t(sizeof(tpch_lineitem_tuple)), _tpchdb(tpchdb)
    {

    	// Get a lineitem tupple from the tuple cache and allocate space
        _prord = _tpchdb->orders_man()->get_tuple();
        _rr.set_ts(_tpchdb->orders_man()->ts(),
                   _tpchdb->orders_desc()->maxsize());
        _prord->_rep = &_rr;

    }

    ~tscan_filter_t()
    {
        // Give back the lineitem tuple
        _tpchdb->orders_man()->give_tuple(_prord);
    }


    // Predication
    bool select(const tuple_t &input) {

        // Get next lineitem and read its shipdate
        if (!_tpchdb->orders_man()->load(_prord, input.data)) {
            assert(false); // RC(se_WRONG_DISK_DATA)
        }

	return (true);
    }


    // Projection
    void project(tuple_t &d, const tuple_t &s) {

        tpch_orders_tuple *dest;
        dest = aligned_cast<tpch_orders_tuple>(d.data);

        _prord->get_value(0,  _orders.O_ORDERKEY);
        _prord->get_value(1,  _orders.O_CUSTKEY);
        _prord->get_value(2,  _orders.O_ORDERSTATUS);
        _prord->get_value(3,  _orders.O_TOTALPRICE);
        _prord->get_value(4,  _orders.O_ORDERDATE,15);
        _prord->get_value(5,  _orders.O_ORDERPRIORITY,15);
        _prord->get_value(6,  _orders.O_CLERK,15);
        _prord->get_value(7,  _orders.O_SHIPPRIORITY);
        _prord->get_value(8,  _orders.O_COMMENT,79);
	
	memcpy(dest,&_orders,sizeof(tpch_orders_tuple));

	if (dest->O_ORDERKEY<100000)
	{

          TRACE(TRACE_ALWAYS, "%d|\n",
		dest->O_ORDERKEY);

		/*TRACE(TRACE_RECORD_FLOW, "%d|%d|%c|%lf|%s|%s|%s|%d|%s|\n",
 	  dest->O_ORDERKEY,
 	  dest->O_CUSTKEY,
	  dest->O_ORDERSTATUS,
	  dest->O_TOTALPRICE,
	  dest->O_ORDERDATE,
	  dest->O_ORDERPRIORITY,
  	  dest->O_CLERK,
	  dest->O_SHIPPRIORITY,
 	  dest->O_COMMENT);*/

	  }
        //dest->C_CUSTKEY = _orders.C_CUSTKEY;
    }

    tscan_filter_t* clone() const {
        return new tscan_filter_t(*this);
    }

    c_str to_string() const {
        return c_str("tscan_filter_t");
    }
};



// aggregate
class count_aggregate_t : public tuple_aggregate_t
{
public:

    class count_key_extractor_t : public key_extractor_t {
    public:

        count_key_extractor_t()
        : key_extractor_t(0, 0) {
            //Every key in the same group. Useful for count aggregates
        }

        virtual key_extractor_t* clone() const {
            return new count_key_extractor_t(*this);
        }

    };
private:
    count_key_extractor_t _extractor;

public:

    count_aggregate_t()
        : tuple_aggregate_t(sizeof(count_tuple))
    {
    }

    key_extractor_t* key_extractor() { return &_extractor; }



    void aggregate(char* agg_data, const tuple_t &s) {
        count_tuple* tuple = aligned_cast<count_tuple>(agg_data);

        // update count
        tuple->COUNT++;

    }

    void finish(tuple_t &d, const char* agg_data) {
        count_tuple *dest;
        dest = aligned_cast<count_tuple>(d.data);
        count_tuple* tuple = aligned_cast<count_tuple>(agg_data);

	dest->COUNT=tuple->COUNT;
    }

    count_aggregate_t* clone() const {
        return new count_aggregate_t(*this);
    }

    c_str to_string() const {
        return "count_aggregate_t";
    }
};


class tpch_orders_process_tuple_t : public process_tuple_t
{
public:

    void begin() {
        TRACE(TRACE_QUERY_RESULTS, "*** SCAN ORDERS ANSWER ...\n");
        TRACE(TRACE_QUERY_RESULTS, "*** COUNT ...\n");
    }

    virtual void process(const tuple_t& output) {
        count_tuple *tuple;
        tuple = aligned_cast<count_tuple>(output.data);
        TRACE(TRACE_QUERY_RESULTS, "*** %d\n",
              tuple->COUNT);
    }
};


static const c_str* dump_o_tuple(tuple_t* tup) {
    tpch_orders_tuple *dest;
    dest = aligned_cast<tpch_orders_tuple> (tup->data);
    return new c_str("%d|%d|%c|%lf|%s|%s|%s|%d|%s|\n",
		     dest->O_ORDERKEY,
		     dest->O_CUSTKEY,
		     dest->O_ORDERSTATUS,
		     dest->O_TOTALPRICE.to_double(),
		     dest->O_ORDERDATE,
		     dest->O_ORDERPRIORITY,
		     dest->O_CLERK,
		     dest->O_SHIPPRIORITY,
		     dest->O_COMMENT);
}


};

/********************************************************************
 *
 * QPIPE Scan Orders - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_qpipe_qorders(const int xct_id, qorders_input_t& in)
{
    TRACE( TRACE_ALWAYS, "********** SCAN ORDERS *********\n");

    policy_t* dp = this->get_sched_policy();
    xct_t* pxct = smthread_t::me()->xct();


    // TSCAN PACKET
    tuple_fifo* tscan_out_buffer =
        new tuple_fifo(sizeof(tpch_orders_tuple));
    tscan_packet_t* tscan_packet =
        new tscan_packet_t("TSCAN ORDERS",
                           tscan_out_buffer,
                           new tpch_qorders::tscan_filter_t(this,in),
                           this->db(),
                           _porders_desc.get(),
                           pxct
                           /*, SH */
                           );


        tuple_fifo* fdump_output = new tuple_fifo(sizeof(tpch_orders_tuple));
    fdump_packet_t* fdump_packet =
            new fdump_packet_t(c_str("FDUMP"),
            fdump_output,
            new trivial_filter_t(fdump_output->tuple_size()),
            NULL,
            c_str("%s/orders.tbl", getenv("HOME")),
            NULL,
            tscan_packet,
	    tpch_qorders::dump_o_tuple);
   


    // AGG PACKET CREATION
    tuple_fifo* count_output_buffer =
        new tuple_fifo(sizeof(tpch_qorders::count_tuple));
    packet_t* count_packet =
        new partial_aggregate_packet_t("COUNT",
                                       count_output_buffer,
                                       new trivial_filter_t(count_output_buffer->tuple_size()),
                                       fdump_packet,
				       //tscan_packet,
                                       new tpch_qorders::count_aggregate_t(),
                                       new tpch_qorders::count_aggregate_t::count_key_extractor_t(),
                                       new int_key_compare_t());

    qpipe::query_state_t* qs = dp->query_state_create();
    tscan_packet->assign_query_state(qs);
    fdump_packet->assign_query_state(qs);
    count_packet->assign_query_state(qs);

    // Dispatch packet
    tpch_qorders::tpch_orders_process_tuple_t pt;
    process_query(count_packet, pt);
    dp->query_state_destroy(qs);

    return (RCOK);
}


EXIT_NAMESPACE(tpch);
