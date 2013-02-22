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

/** @file:   qpipe_q12.cpp
 *
 *  @brief:  Implementation of QPIPE TPCH Q12 over Shore-MT
 *
 *  @author: 
 *  @date:   
 */

#include "workload/tpch/shore_tpch_env.h"
#include "workload/tpch/tpch_struct.h"
#include "workload/tpch/tpch_util.h"
#include "qpipe.h"

using namespace shore;
using namespace qpipe;



ENTER_NAMESPACE(tpch);

/*
 * select
 *     l_shipmode,
 *     sum(case
 *         when o_orderpriority ='1-URGENT'
 *             or o_orderpriority ='2-HIGH'
 *         then 1
 *         else 0
 *     end) as high_line_count,
 *     sum(case
 *         when o_orderpriority <> '1-URGENT'
 *             and o_orderpriority <> '2-HIGH'
 *         then 1
 *         else 0
 *         end) as low_line_count
 * from
 *     tpcd.orders,
 *     tpcd.lineitem
 * where
 *     o_orderkey = l_orderkey
 *     and l_shipmode in ('MAIL', 'SHIP')
 *     and l_commitdate < l_receiptdate
 *     and l_shipdate < l_commitdate
 *     and l_receiptdate >= date('1994-01-01')
 *     and l_receiptdate < date('1994-01-01') + 1 year
 * group by
 *     l_shipmode
 * order by
 *     l_shipmode;
 */

struct q12_lineitem_scan_tuple {
	int L_ORDERKEY;
	tpch_l_shipmode L_SHIPMODE;
};

struct q12_orders_scan_tuple {
	int O_ORDERKEY;
	int O_ORDERPRIORITY;
};

struct q12_join_tuple {
	tpch_l_shipmode L_SHIPMODE;
	int O_ORDERPRIORITY;
};

struct q12_tuple {
	tpch_l_shipmode L_SHIPMODE;
	int HIGH_LINE_COUNT;
	int LOW_LINE_COUNT;
};

struct q12_final_tuple {
	char L_SHIPMODE[STRSIZE(10)];
	int HIGH_LINE_COUNT;
	int LOW_LINE_COUNT;
};


/**
 * @brief select L_ORDERKEY, L_SHIPMODE from LINEITEM where L_SHIPMODE
 * in ('MAIL', 'SHIP') and L_COMMITDATE < L_RECEIPTDATE and L_SHIPDATE
 * < L_COMMITDATE and L_RECEIPTDATE >= [date] and L_RECEIPTDATE <
 * [date] + 1 year
 */
class q12_lineitem_tscan_filter_t : public tuple_filter_t 
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prline;
	rep_row_t _rr;

	tpch_lineitem_tuple _lineitem;
	time_t _commitdate;
	time_t _receiptdate;
	time_t _shipdate;
	int _shipmode;

	/* Random Predicates */
	/* TPC-H Specification 2.5.13 */
	/* YEAR random within [1993, 1997]
	 * SHIPMODE1 random within [0, 6]
	 * SHIPMODE2 random within [0, 6] and different than SHIPMODE1
	 */
	q12_input_t* q12_input;
	time_t _last_l_receiptdate;
public:

	q12_lineitem_tscan_filter_t(ShoreTPCHEnv* tpchdb, q12_input_t &in)
	: tuple_filter_t(tpchdb->lineitem_desc()->maxsize()), _tpchdb(tpchdb)
	//: tuple_filter_t(sizeof(tpch_lineitem_tuple)), _tpchdb(tpchdb)
	{

		// Get a lineitem tupple from the tuple cache and allocate space
		_prline = _tpchdb->lineitem_man()->get_tuple();
		_rr.set_ts(_tpchdb->lineitem_man()->ts(),
				_tpchdb->lineitem_desc()->maxsize());
		_prline->_rep = &_rr;


		// Generate the random predicates
		q12_input=&in;
		struct tm date;
		gmtime_r(&(q12_input->l_receiptdate), &date);
		date.tm_year ++;
		_last_l_receiptdate=mktime(&date);

		char shipmode1[11];
		char shipmode2[11];
		char fshipdate[15];
		char lshipdate[15];

		shipmode_to_str(shipmode1, (tpch_l_shipmode)(q12_input->l_shipmode1));
		shipmode_to_str(shipmode2, (tpch_l_shipmode)(q12_input->l_shipmode2));
		timet_to_str(fshipdate, q12_input->l_receiptdate);
		timet_to_str(lshipdate, _last_l_receiptdate);

		TRACE(TRACE_ALWAYS, "Random Predicates:\nL_SHIPMODE in (%s, %s); %s <= L_RECEIPTDATE < %s\n", shipmode1, shipmode2, fshipdate, lshipdate);
	}

	~q12_lineitem_tscan_filter_t()
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
		_prline->get_value(11, _lineitem.L_COMMITDATE, 15);
		_commitdate = str_to_timet(_lineitem.L_COMMITDATE);
		_prline->get_value(12, _lineitem.L_RECEIPTDATE, 15);
		_receiptdate = str_to_timet(_lineitem.L_RECEIPTDATE);
		_prline->get_value(14, _lineitem.L_SHIPMODE, 15);
		_shipmode=str_to_shipmode(_lineitem.L_SHIPMODE);

		//TODO implement it with _and_predicate

		// Return true if it passes the filter
		if  ( (_shipmode==q12_input->l_shipmode1 || _shipmode==q12_input->l_shipmode2) && _commitdate<_receiptdate && _shipdate<_commitdate && _receiptdate >= q12_input->l_receiptdate && _receiptdate < _last_l_receiptdate ) {
			//TRACE(TRACE_RECORD_FLOW, "+ %d %s\n", _lineitem.L_ORDERKEY, _lineitem.L_SHIPMODE);
			return (true);
		}
		else {
			//TRACE(TRACE_RECORD_FLOW, ". %d %s\n", _lineitem.L_ORDERKEY, _lineitem.L_SHIPMODE);
			return (false);
		}
	}


	// Projection
	void project(tuple_t &d, const tuple_t &s) {

		q12_lineitem_scan_tuple *dest;
		dest = aligned_cast<q12_lineitem_scan_tuple>(d.data);

		_prline->get_value(0, _lineitem.L_ORDERKEY);
		_prline->get_value(14, _lineitem.L_SHIPMODE, 10);

		/*TRACE( TRACE_RECORD_FLOW, "%d|%s\n",
               _lineitem.L_ORDERKEY,
               _lineitem.L_SHIPMODE);*/

		dest->L_ORDERKEY = _lineitem.L_ORDERKEY;
		dest->L_SHIPMODE = str_to_shipmode(_lineitem.L_SHIPMODE);

	}

	q12_lineitem_tscan_filter_t* clone() const {
		return new q12_lineitem_tscan_filter_t(*this);
	}

	c_str to_string() const {
		char date[15];
		timet_to_str(date, q12_input->l_receiptdate);
		return c_str("q12_lineitem_tscan_filter_t(%s, %d, %d)", date, q12_input->l_shipmode1, q12_input->l_shipmode2);
	}
};


//Q12 orders scan filter
class q12_orders_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prorder;
	rep_row_t _rr;

	/*One lineitem tuple*/
	tpch_orders_tuple _orders;
public:

	q12_orders_tscan_filter_t(ShoreTPCHEnv* tpchdb)
	: tuple_filter_t(tpchdb->orders_desc()->maxsize()), _tpchdb(tpchdb)
	{
		// Get an orders tupple from the tuple cache and allocate space
		_prorder = _tpchdb->orders_man()->get_tuple();
		_rr.set_ts(_tpchdb->orders_man()->ts(),
				_tpchdb->orders_desc()->maxsize());
		_prorder->_rep = &_rr;

	}

	~q12_orders_tscan_filter_t()
	{
		// Give back the orders tuple
		_tpchdb->orders_man()->give_tuple(_prorder);
	}

	bool select(const tuple_t &input) {
		// Get next order
		/*Needed. Eventhough we do not apply any selection criteria we need this
		 *statement to bring the data from the storage manager */
		if (!_tpchdb->orders_man()->load(_prorder, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		return(true);
	}

	// Projection
	void project(tuple_t &d, const tuple_t &s) {

		q12_orders_scan_tuple *dest;
		dest = aligned_cast<q12_orders_scan_tuple>(d.data);

		_prorder->get_value(0, _orders.O_ORDERKEY);
		_prorder->get_value(5, _orders.O_ORDERPRIORITY, 15);

		char number[2];
		strncpy(number,_orders.O_ORDERPRIORITY,1);
		number[1] = '\0';



		/*TRACE( TRACE_RECORD_FLOW, "%d, %s\n",
        		_orders.O_ORDERKEY,
        		_orders.O_ORDERPRIORITY);*/
		dest->O_ORDERKEY = _orders.O_ORDERKEY;
		dest->O_ORDERPRIORITY = atoi(number);
	}

	q12_orders_tscan_filter_t* clone() const {
		return new q12_orders_tscan_filter_t(*this);
	}

	c_str to_string() const {
		c_str result("select O_ORDERKEY, O_ORDERPRIORITY from ORDERS");
		return result;
	}
};


//JOIN ORDERS - LINEITEM on ORDERKEY return L_SHIPMODE, O_ORDERPRIORITY
struct q12_join_t : tuple_join_t {

	q12_join_t()
	: tuple_join_t(sizeof(q12_orders_scan_tuple),
			offsetof(q12_orders_scan_tuple,O_ORDERKEY),
			sizeof(q12_lineitem_scan_tuple),
			offsetof(q12_lineitem_scan_tuple,L_ORDERKEY),
			sizeof(int),
			sizeof(q12_join_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {

		q12_join_tuple* dest = aligned_cast<q12_join_tuple>(d.data);
		q12_orders_scan_tuple* left =  aligned_cast<q12_orders_scan_tuple>(l.data);
		q12_lineitem_scan_tuple* right =  aligned_cast<q12_lineitem_scan_tuple>(r.data);

		dest->L_SHIPMODE=right->L_SHIPMODE;
		dest->O_ORDERPRIORITY=left->O_ORDERPRIORITY;

		//TRACE (TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %d\n",left->O_ORDERKEY,right->L_ORDERKEY,dest->L_SHIPMODE,dest->O_ORDERPRIORITY);
	}

	virtual q12_join_t* clone() const {
		return new q12_join_t(*this);
	}

	virtual c_str to_string() const {
		return "join LINEITEM, ORDERS, select L_SHIPMODE, O_ORDERPRIORITY";
	}
};

/**
 * @brief sum(case
 *         when o_orderpriority ='1-URGENT'
 *             or o_orderpriority ='2-HIGH'
 *         then 1
 *         else 0
 *     end) as high_line_count,
 *     sum(case
 *         when o_orderpriority <> '1-URGENT'
 *             and o_orderpriority <> '2-HIGH'
 *         then 1
 *         else 0
 *         end) as low_line_count
 */
struct q12_aggregate_t : tuple_aggregate_t {
	default_key_extractor_t _extractor;

	q12_aggregate_t()
	: tuple_aggregate_t(sizeof(q12_tuple)),
	  _extractor(sizeof(tpch_l_shipmode),
			  offsetof(q12_join_tuple, L_SHIPMODE))
	{
	}

	virtual key_extractor_t* key_extractor() {
		return &_extractor;
	}
	virtual void aggregate(char* agg_data, const tuple_t &t) {
		q12_join_tuple* inp = aligned_cast<q12_join_tuple>(t.data);
		q12_tuple* agg = aligned_cast<q12_tuple>(agg_data);

		if (inp->O_ORDERPRIORITY==1 || inp->O_ORDERPRIORITY==2)
			agg->HIGH_LINE_COUNT++;
		else
			agg->LOW_LINE_COUNT++;
	}
	virtual void finish(tuple_t &dest, const char* agg_data) {
		memcpy(dest.data, agg_data, dest.size);
	}
	virtual q12_aggregate_t* clone() const {
		return new q12_aggregate_t(*this);
	}
	virtual c_str to_string() const {
		return "q12_aggregate_t";
	}
};

struct q12_agg_filter_t : public tuple_filter_t {

	q12_agg_filter_t()
	: tuple_filter_t(sizeof(q12_final_tuple))
	{
	}

	bool select(const tuple_t &input) {
		return true;
	}

	void project(tuple_t &d, const tuple_t &s) {

		q12_final_tuple *out = aligned_cast<q12_final_tuple>(d.data);
		q12_tuple *in = aligned_cast<q12_tuple>(s.data);

		shipmode_to_str(out->L_SHIPMODE, (in->L_SHIPMODE));
		out->HIGH_LINE_COUNT = in->HIGH_LINE_COUNT;
		out->LOW_LINE_COUNT = in->LOW_LINE_COUNT;
	}

	virtual q12_agg_filter_t* clone() const {
		return new q12_agg_filter_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("q12_agg_filter_t");
	}
};

struct q12_sort_key_extractor_t : public key_extractor_t {

	q12_sort_key_extractor_t()
	: key_extractor_t(STRSIZE(10) * sizeof(char), offsetof(q12_final_tuple, L_SHIPMODE))
	{
	}

	virtual int extract_hint(const char* key) const {
		char *k;
		k = aligned_cast<char>(key);

		int result = (*k << 8) + *(k + sizeof(char));

		return result;
	}

	virtual q12_sort_key_extractor_t* clone() const {
		return new q12_sort_key_extractor_t(*this);
	}
};

struct q12_sort_key_compare_t : public key_compare_t {

	virtual int operator()(const void* key1, const void* key2) const {
		char* shipmode1 = aligned_cast<char>(key1);
		char* shipmode2 = aligned_cast<char>(key2);

		return strcmp(shipmode1, shipmode2);
	}

	virtual q12_sort_key_compare_t* clone() const {
		return new q12_sort_key_compare_t(*this);
	}
};



class tpch_q12_process_tuple_t : public process_tuple_t {

public:

	virtual void begin() {
		TRACE(TRACE_QUERY_RESULTS, "*** Q12 %10s %10s %10s\n",
				"Shipmode", "High_Count", "Low_Count");
	}

	virtual void process(const tuple_t& output) {
		q12_final_tuple* r = aligned_cast<q12_final_tuple>(output.data);
		TRACE(TRACE_QUERY_RESULTS, "*** Q12 %10s %10d %10d\n",
				r->L_SHIPMODE,
				r->HIGH_LINE_COUNT,
				r->LOW_LINE_COUNT);
	}

};



/******************************************************************** 
 *
 * QPIPE Q12 - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_qpipe_q12(const int xct_id, 
		q12_input_t& in)
{
	TRACE( TRACE_ALWAYS, "********** Q12 *********\n");

	policy_t* dp = this->get_sched_policy();
	xct_t* pxct = smthread_t::me()->xct();


	//TSCAN LINEITEM
	tuple_fifo* q12_lineitem_buffer = new tuple_fifo(sizeof(q12_lineitem_scan_tuple));
	packet_t* q12_lineitem_tscan_packet =
			new tscan_packet_t("lineitem TSCAN",
					q12_lineitem_buffer,
					new q12_lineitem_tscan_filter_t(this,in),
					this->db(),
					_plineitem_desc.get(),
					pxct
					/*, SH */
			);



	//TSCAN ORDERS
	tuple_fifo* q12_orders_buffer = new tuple_fifo(sizeof(q12_orders_scan_tuple));
	packet_t* q12_orders_tscan_packet =
			new tscan_packet_t("orders TSCAN",
					q12_orders_buffer,
					new q12_orders_tscan_filter_t(this),
					this->db(),
					_porders_desc.get(),
					pxct
					/*, SH */
			);

	//JOIN
	tuple_fifo* q12_join_buffer = new tuple_fifo(sizeof(q12_join_tuple));
	packet_t* q12_join_packet =
			new hash_join_packet_t("orders-lineitem HJOIN",
					q12_join_buffer,
					new trivial_filter_t(sizeof(q12_join_tuple)),
					q12_orders_tscan_packet,
					q12_lineitem_tscan_packet,
					new q12_join_t());



	//AGGREGATE
	tuple_fifo* q12_agg_buffer = new tuple_fifo(sizeof(q12_final_tuple));
	tuple_aggregate_t *q12_aggregate = new q12_aggregate_t();
	packet_t* q12_agg_packet;
	q12_agg_packet = new partial_aggregate_packet_t("SUM AGG",
			q12_agg_buffer,
			new q12_agg_filter_t(),
			q12_join_packet,
			q12_aggregate,
			new default_key_extractor_t(sizeof(tpch_l_shipmode), offsetof(q12_join_tuple, L_SHIPMODE)),//sizeof(int),offsetof(q12_join_tuple,L_SHIPMODE)),
			new int_key_compare_t());

	tuple_fifo* q12_sort_buffer = new tuple_fifo(sizeof(q12_final_tuple));
	packet_t* q12_sort_packet =
			new sort_packet_t("SORT BY SHIPMODE",
					q12_sort_buffer,
					new trivial_filter_t(sizeof(q12_final_tuple)),
					new q12_sort_key_extractor_t(),
					new q12_sort_key_compare_t(),
					q12_agg_packet);




	qpipe::query_state_t* qs = dp->query_state_create();
	//q12_*****->assign_query_state(qs);
	q12_lineitem_tscan_packet->assign_query_state(qs);
	q12_orders_tscan_packet->assign_query_state(qs);
	q12_join_packet->assign_query_state(qs);
	q12_agg_packet->assign_query_state(qs);
	q12_sort_packet->assign_query_state(qs);

	// Dispatch packet
	tpch_q12_process_tuple_t pt;
	//LAST PACKET
	//process_query(q12_join_packet, pt);//TODO
	process_query(q12_sort_packet, pt);//TODO

	dp->query_state_destroy(qs);


	return (RCOK);
}


EXIT_NAMESPACE(qpipe);
