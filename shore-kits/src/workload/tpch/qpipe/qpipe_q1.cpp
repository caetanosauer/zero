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

/** @file:   qpipe_q1.cpp
 *
 *  @brief:  Implementation of QPIPE TPCH Q1 over Shore-MT
 *
 *  @author: Ippokratis Pandis
 *  @date:   Apr 2010
 */

#include "workload/tpch/shore_tpch_env.h"
#include "qpipe.h"

using namespace shore;
using namespace qpipe;



ENTER_NAMESPACE(tpch);


/******************************************************************** 
 *
 * QPIPE Q1 - Structures needed by operators 
 *
 ********************************************************************/

// the tuples after tablescan projection
struct q1_projected_lineitem_tuple {
	decimal L_QUANTITY;
	decimal L_EXTENDEDPRICE;
	decimal L_DISCOUNT;
	decimal L_TAX;
	char L_RETURNFLAG;
	char L_LINESTATUS;
};


// the final aggregated tuples
struct q1_aggregate_tuple {
	decimal L_SUM_QTY;
	decimal L_SUM_BASE_PRICE;
	decimal L_SUM_DISC_PRICE;
	decimal L_SUM_CHARGE;
	decimal L_AVG_QTY;
	decimal L_AVG_PRICE;
	decimal L_AVG_DISC;
	decimal L_COUNT_ORDER;
	char L_RETURNFLAG ;
	char L_LINESTATUS;
};




class q1_tscan_filter_t : public tuple_filter_t 
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prline;
	rep_row_t _rr;

	tpch_lineitem_tuple _lineitem;
	time_t _shipdate;

	/* Random Predicates */
	/* TPC-H Specification 2.3.0 */
	/* DELTA random within [60 .. 120] */
	/*Random predicates computed in src/workload/tpch/tpch_input.cpp*/
	q1_input_t* q1_input;
public:

	q1_tscan_filter_t(ShoreTPCHEnv* tpchdb, q1_input_t &in)
	: tuple_filter_t(tpchdb->lineitem_desc()->maxsize()), _tpchdb(tpchdb)
	//: tuple_filter_t(sizeof(tpch_lineitem_tuple)), _tpchdb(tpchdb)
	{

		// Get a lineitem tupple from the tuple cache and allocate space
		_prline = _tpchdb->lineitem_man()->get_tuple();
		_rr.set_ts(_tpchdb->lineitem_man()->ts(),
				_tpchdb->lineitem_desc()->maxsize());
		_prline->_rep = &_rr;

		/* Random predicates read from input
	   L_SHIPDATE <= 1998-12-01 - DELTA DAYS
		 */
		q1_input=&in;

		char date[15];
		timet_to_str(date,q1_input->l_shipdate);
		TRACE (TRACE_ALWAYS,"Random predicates: %s\n", date);
	}

	virtual ~q1_tscan_filter_t()
	{
		// Give back the lineitem tuple
		_tpchdb->lineitem_man()->give_tuple(_prline);
	}


	// Predication
	bool select(const tuple_t &input) {

		// Get next lineitem and read its shipdate
		if (!_tpchdb->lineitem_man()->load(_prline, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}


		_prline->get_value(10, _lineitem.L_SHIPDATE, 15);
		_shipdate = str_to_timet(_lineitem.L_SHIPDATE);

		// Return true if it passes the filter
		if  ( _shipdate <= q1_input->l_shipdate ) {
			//TRACE(TRACE_RECORD_FLOW, "+ %s\n", _lineitem.L_SHIPDATE);
			return (true);
		}
		else {
			//TRACE(TRACE_RECORD_FLOW, ". %s\n", _lineitem.L_SHIPDATE);
			return (false);
		}
	}


	// Projection
	void project(tuple_t &d, const tuple_t &s) {

		q1_projected_lineitem_tuple *dest;
		dest = aligned_cast<q1_projected_lineitem_tuple>(d.data);

		_prline->get_value(4, _lineitem.L_QUANTITY);
		_prline->get_value(5, _lineitem.L_EXTENDEDPRICE);
		_prline->get_value(6, _lineitem.L_DISCOUNT);
		_prline->get_value(7, _lineitem.L_TAX);
		_prline->get_value(8, _lineitem.L_RETURNFLAG);
		_prline->get_value(9, _lineitem.L_LINESTATUS);

		/*TRACE( TRACE_RECORD_FLOW, "%.2f|%.2f|%.2f|%.2f|%c|%c\n",
				_lineitem.L_QUANTITY,
				_lineitem.L_EXTENDEDPRICE,
				_lineitem.L_DISCOUNT,
				_lineitem.L_TAX,
				_lineitem.L_RETURNFLAG,
				_lineitem.L_LINESTATUS);*/

		dest->L_QUANTITY = _lineitem.L_QUANTITY;
		dest->L_EXTENDEDPRICE = _lineitem.L_EXTENDEDPRICE / 100.0;
		dest->L_DISCOUNT = _lineitem.L_DISCOUNT / 100.0;
		dest->L_TAX = _lineitem.L_TAX / 100.0;
		dest->L_RETURNFLAG = _lineitem.L_RETURNFLAG;
		dest->L_LINESTATUS = _lineitem.L_LINESTATUS;

	}

	q1_tscan_filter_t* clone() const {
		return new q1_tscan_filter_t(*this);
	}

	c_str to_string() const {
		char date[15];
		timet_to_str(date,q1_input->l_shipdate);
		return c_str("q1_tscan_filter_t(%s)",date);
	}
};



// "order by L_RETURNFLAG, L_LINESTATUS"
struct q1_key_extract_t : public key_extractor_t 
{
	q1_key_extract_t()
	: key_extractor_t(sizeof(char)*2, offsetof(q1_projected_lineitem_tuple, L_RETURNFLAG))
	{
		assert(sizeof(char) == 1);
	}

	int extract_hint(const char* key_data) const {
		// store the return flag and line status in the
		char *key;
		key = aligned_cast<char>(key_data);

		int result = (*key << 8) + *(key + sizeof(char));

		return result;
	}

	q1_key_extract_t* clone() const {
		return new q1_key_extract_t(*this);
	}
};



// aggregate
class q1_count_aggregate_t : public tuple_aggregate_t 
{
private:
	q1_key_extract_t _extractor;

public:

	q1_count_aggregate_t()
	: tuple_aggregate_t(sizeof(q1_aggregate_tuple))
	{
	}

	key_extractor_t* key_extractor() { return &_extractor; }

	void aggregate(char* agg_data, const tuple_t &s) {
		q1_projected_lineitem_tuple *src;
		src = aligned_cast<q1_projected_lineitem_tuple>(s.data);
		q1_aggregate_tuple* tuple = aligned_cast<q1_aggregate_tuple>(agg_data);

		// cache resused values for convenience
		decimal L_EXTENDEDPRICE = src->L_EXTENDEDPRICE;
		decimal L_DISCOUNT = src->L_DISCOUNT;
		decimal L_QUANTITY = src->L_QUANTITY;
		decimal L_DISC_PRICE = L_EXTENDEDPRICE * (1 - L_DISCOUNT);

		// update count
		tuple->L_COUNT_ORDER++;

		// update sums
		tuple->L_SUM_QTY += L_QUANTITY;
		tuple->L_SUM_BASE_PRICE += L_EXTENDEDPRICE;
		tuple->L_SUM_DISC_PRICE += L_DISC_PRICE;
		tuple->L_SUM_CHARGE += L_DISC_PRICE * (1 + src->L_TAX);
		tuple->L_AVG_QTY += L_QUANTITY;
		tuple->L_AVG_PRICE += L_EXTENDEDPRICE;
		tuple->L_AVG_DISC += L_DISCOUNT;
		tuple->L_RETURNFLAG = src->L_RETURNFLAG;
		tuple->L_LINESTATUS = src->L_LINESTATUS;

		static const uint OUTPUT_RATE = 10; // 100
		if ((tuple->L_COUNT_ORDER).to_int() % OUTPUT_RATE == 0) {
			//TRACE(TRACE_RECORD_FLOW, "%.2f\n", tuple->L_COUNT_ORDER.to_double());
			fflush(stdout);
		}
	}

	void finish(tuple_t &d, const char* agg_data) {
		q1_aggregate_tuple *dest;
		dest = aligned_cast<q1_aggregate_tuple>(d.data);
		q1_aggregate_tuple* tuple = aligned_cast<q1_aggregate_tuple>(agg_data);

		*dest = *tuple;
		// compute averages
		dest->L_AVG_QTY /= dest->L_COUNT_ORDER;
		dest->L_AVG_PRICE /= dest->L_COUNT_ORDER;
		dest->L_AVG_DISC /= dest->L_COUNT_ORDER;
	}

	q1_count_aggregate_t* clone() const {
		return new q1_count_aggregate_t(*this);
	}

	c_str to_string() const {
		return "q1_count_aggregate_t";
	}
};


class tpch_q1_process_tuple_t : public process_tuple_t 
{    
public:

	void begin() {
		TRACE(TRACE_QUERY_RESULTS, "*** Q1 ANSWER ...\n");
		TRACE(TRACE_QUERY_RESULTS, "*** L_RETURNFLAG\tL_LINESTATUS\tSUM_QTY\tSUM_BASE_PRICE\tSUM_DISC_PRICE\tSUM_CHARGE\tAVG_QTY\tAVG_PRICE\tAVG_DISC\tCOUNT_ORDER\n");
	}

	virtual void process(const tuple_t& output) {
		q1_aggregate_tuple *tuple;
		tuple = aligned_cast<q1_aggregate_tuple>(output.data);
		TRACE(TRACE_QUERY_RESULTS, "*** %c\t%c\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\n",
				tuple->L_RETURNFLAG,
				tuple->L_LINESTATUS,
				tuple->L_SUM_QTY.to_double(),
				tuple->L_SUM_BASE_PRICE.to_double(),
				tuple->L_SUM_DISC_PRICE.to_double(),
				tuple->L_SUM_CHARGE.to_double(),
				tuple->L_AVG_QTY.to_double(),
				tuple->L_AVG_PRICE.to_double(),
				tuple->L_AVG_DISC.to_double(),
				tuple->L_COUNT_ORDER.to_double());
	}
};



/******************************************************************** 
 *
 * QPIPE Q1 - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_qpipe_q1(const int xct_id, 
		q1_input_t& in)
{
	TRACE( TRACE_ALWAYS, "********** Q1 *********\n");

	/*
select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date '1998-12-01' - interval '72' day 
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus
;
	 */


	policy_t* dp = this->get_sched_policy();
	xct_t* pxct = smthread_t::me()->xct();


	// TSCAN PACKET
	tuple_fifo* tscan_out_buffer =
			new tuple_fifo(sizeof(q1_projected_lineitem_tuple));
	tscan_packet_t* q1_tscan_packet =
			new tscan_packet_t("TSCAN LINEITEM",
					tscan_out_buffer,
					new q1_tscan_filter_t(this,in),
					this->db(),
					_plineitem_desc.get(),
					pxct
					/*, SH */
			);


	// AGG PACKET CREATION
	tuple_fifo* agg_output_buffer =
			new tuple_fifo(sizeof(q1_aggregate_tuple));
	packet_t* q1_agg_packet =
			new partial_aggregate_packet_t("AGG Q1",
					agg_output_buffer,
					new trivial_filter_t(agg_output_buffer->tuple_size()),
					q1_tscan_packet,
					new q1_count_aggregate_t(),
					new q1_key_extract_t(),
					new int_key_compare_t());

	qpipe::query_state_t* qs = dp->query_state_create();
	q1_agg_packet->assign_query_state(qs);
	q1_tscan_packet->assign_query_state(qs);

	// Dispatch packet
	tpch_q1_process_tuple_t pt;
	process_query(q1_agg_packet, pt);
	dp->query_state_destroy(qs);

	return (RCOK);
}


EXIT_NAMESPACE(tpch);
