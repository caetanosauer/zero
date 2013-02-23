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

/** @file:   qpipe_qpartsupp.cpp
 *
 *  @brief:  Implementation of a partsupp tablescan over QPipe over Shore-MT
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
 * QPIPE Scan Partsupp - Structures needed by operators
 *
 ********************************************************************/

// count
class tpch_qpartsupp{

public:


struct count_tuple {
    int COUNT;
};




class tscan_filter_t : public tuple_filter_t
{
private:
    ShoreTPCHEnv* _tpchdb;
    table_row_t* _prpartsupp;
    rep_row_t _rr;

    tpch_partsupp_tuple _partsupp;
public:

    tscan_filter_t(ShoreTPCHEnv* tpchdb, qpartsupp_input_t &in)
        : tuple_filter_t(tpchdb->partsupp_desc()->maxsize()), _tpchdb(tpchdb)
          //: tuple_filter_t(sizeof(tpch_lineitem_tuple)), _tpchdb(tpchdb)
    {

    	// Get a lineitem tupple from the tuple cache and allocate space
        _prpartsupp = _tpchdb->partsupp_man()->get_tuple();
        _rr.set_ts(_tpchdb->partsupp_man()->ts(),
                   _tpchdb->partsupp_desc()->maxsize());
        _prpartsupp->_rep = &_rr;

    }

    ~tscan_filter_t()
    {
        // Give back the lineitem tuple
        _tpchdb->partsupp_man()->give_tuple(_prpartsupp);
    }


    // Predication
    bool select(const tuple_t &input) {

        // Get next lineitem and read its shipdate
        if (!_tpchdb->partsupp_man()->load(_prpartsupp, input.data)) {
            assert(false); // RC(se_WRONG_DISK_DATA)
        }

	return (true);
    }


    // Projection
    void project(tuple_t &d, const tuple_t &s) {

        tpch_partsupp_tuple *dest;
        dest = aligned_cast<tpch_partsupp_tuple>(d.data);

        _prpartsupp->get_value(0,  _partsupp.PS_PARTKEY);
        _prpartsupp->get_value(1,  _partsupp.PS_SUPPKEY);
        _prpartsupp->get_value(2,  _partsupp.PS_AVAILQTY);
        _prpartsupp->get_value(3,  _partsupp.PS_SUPPLYCOST);
        _prpartsupp->get_value(4,  _partsupp.PS_COMMENT,199);


        memcpy(dest,&_partsupp,sizeof(tpch_partsupp_tuple));
        if (dest->PS_PARTKEY%100000==0)
            TRACE(TRACE_RECORD_FLOW, "%d %d\n", dest->PS_PARTKEY, dest->PS_SUPPKEY);
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


class tpch_partsupp_process_tuple_t : public process_tuple_t
{
public:

    void begin() {
        TRACE(TRACE_QUERY_RESULTS, "*** SCAN PARTSUPPS ANSWER ...\n");
        TRACE(TRACE_QUERY_RESULTS, "*** COUNT ...\n");
    }

    virtual void process(const tuple_t& output) {
        count_tuple *tuple;
        tuple = aligned_cast<count_tuple>(output.data);
        TRACE(TRACE_QUERY_RESULTS, "*** %d\n",
              tuple->COUNT);
    }
};

    static const c_str* dump_tuple(tuple_t* tup) {
        tpch_partsupp_tuple *dest;
        dest = aligned_cast<tpch_partsupp_tuple> (tup->data);
        return new c_str("%d|%d|%d|%lf|%s|\n",
			 dest->PS_PARTKEY,
			 dest->PS_SUPPKEY,
			 dest->PS_AVAILQTY,
			 dest->PS_SUPPLYCOST.to_double(),
			 dest->PS_COMMENT);
    }


};

/********************************************************************
 *
 * QPIPE Scan Partsupp - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_qpipe_qpartsupp(const int xct_id, qpartsupp_input_t& in)
{
    TRACE( TRACE_ALWAYS, "********** SCAN PARTSUPPS *********\n");

    policy_t* dp = this->get_sched_policy();
    xct_t* pxct = smthread_t::me()->xct();


    // TSCAN PACKET
    tuple_fifo* tscan_out_buffer =
        new tuple_fifo(sizeof(tpch_partsupp_tuple));
    tscan_packet_t* tscan_packet =
        new tscan_packet_t("TSCAN PARTSUPPS",
                           tscan_out_buffer,
                           new tpch_qpartsupp::tscan_filter_t(this,in),
                           this->db(),
                           _ppartsupp_desc.get(),
                           pxct
                           /*, SH */
                           );


    tuple_fifo* fdump_output = new tuple_fifo(sizeof(tpch_partsupp_tuple));
    fdump_packet_t* fdump_packet =
            new fdump_packet_t(c_str("FDUMP"),
            fdump_output,
            new trivial_filter_t(fdump_output->tuple_size()),
            NULL,
            c_str("%s/partsupp.tbl", getenv("HOME")),
            NULL,
            tscan_packet,
	    tpch_qpartsupp::dump_tuple);


    // AGG PACKET CREATION
    tuple_fifo* count_output_buffer =
        new tuple_fifo(sizeof(tpch_qpartsupp::count_tuple));
    packet_t* count_packet =
        new partial_aggregate_packet_t("COUNT",
                                       count_output_buffer,
                                       new trivial_filter_t(count_output_buffer->tuple_size()),
                                       fdump_packet,
                                       new tpch_qpartsupp::count_aggregate_t(),
                                       new tpch_qpartsupp::count_aggregate_t::count_key_extractor_t(),
                                       new int_key_compare_t());

    qpipe::query_state_t* qs = dp->query_state_create();
    count_packet->assign_query_state(qs);
    fdump_packet->assign_query_state(qs);
    tscan_packet->assign_query_state(qs);

    // Dispatch packet
    tpch_qpartsupp::tpch_partsupp_process_tuple_t pt;
    process_query(count_packet, pt);
    dp->query_state_destroy(qs);

    return (RCOK);
}


EXIT_NAMESPACE(tpch);
