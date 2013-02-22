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

/** @file:   qpipe_q3.cpp
 *
 *  @brief:  Implementation of QPIPE TPCH Q3 over Shore-MT
 *
 *  @author:    Andreas Schädeli
 *  @date:      2011-10-24
 */

#include "workload/tpch/shore_tpch_env.h"
#include "workload/tpch/tpch_struct.h"
#include "workload/tpch/tpch_util.h"
#include "qpipe.h"

using namespace shore;
using namespace qpipe;



ENTER_NAMESPACE(tpch);

/********************************************************************
 *
 * QPIPE Q3 - Structures needed by operators
 *
 ********************************************************************/

/*
 *  select
 *      top 10
 *      l_orderkey,
 *      sum(l_extendedprice*(1-l_discount)) as revenue,
 *      o_orderdate,
 *      o_shippriority
 *  from
 *      customer,
 *      orders,
 *      lineitem
 *  where
 *      c_mktsegment = '[SEGMENT]'
 *      and c_custkey = o_custkey
 *      and l_orderkey = o_orderkey
 *      and o_orderdate < '[DATE]'
 *      and l_shipdate > '[DATE]'
 *  group by
 *      l_orderkey,
 *      o_orderdate,
 *      o_shippriority
 *  order by
 *      revenue desc,
 *      o_orderdate
 */


struct q3_projected_customer_tuple {
	long C_CUSTKEY;
};

struct q3_projected_orders_tuple {
	int O_ORDERKEY;
	int O_CUSTKEY;
	time_t O_ORDERDATE;
	int O_SHIPPRIORITY;
};

struct q3_projected_lineitem_tuple {
	int L_ORDERKEY;
	decimal L_EXTENDEDPRICE;
	decimal L_DISCOUNT;
};


struct q3_o_join_c_tuple {
	int O_ORDERKEY;
	time_t O_ORDERDATE;
	int O_SHIPPRIORITY;
};

struct q3_aggregated_lineitem_tuple {
	int L_ORDERKEY;
	decimal REVENUE;
};

struct q3_aggregated_tuple {
	int L_ORDERKEY;
	time_t O_ORDERDATE;
	int O_SHIPPRIORITY;
	decimal REVENUE;
};

struct q3_agg_key {
	int L_ORDERKEY;
	time_t O_ORDERDATE;
	int O_SHIPPRIORITY;
};


class q3_customer_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prcust;
	rep_row_t _rr;

	tpch_customer_tuple _customer;

	q3_input_t* q3_input;
	char _mktsegment[STRSIZE(10)];

public:
	q3_customer_tscan_filter_t(ShoreTPCHEnv* tpchdb, q3_input_t &in)
	: tuple_filter_t(tpchdb->customer_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prcust = _tpchdb->customer_man()->get_tuple();
		_rr.set_ts(_tpchdb->customer_man()->ts(),
				_tpchdb->customer_desc()->maxsize());
		_prcust->_rep = &_rr;

		// Generate the random predicates
		q3_input = &in;
		segment_to_str(_mktsegment, q3_input->c_segment);

		TRACE(TRACE_ALWAYS, "Random predicate:\nCUSTOMER.C_MKTSEGMENT = '%s'\n", _mktsegment);
	}

	virtual ~q3_customer_tscan_filter_t()
	{
		// Give back the customer tuple
		_tpchdb->customer_man()->give_tuple(_prcust);
	}

	bool select(const tuple_t &input) {
		// Get next customer tuple and read its marketsegment
		if (!_tpchdb->customer_man()->load(_prcust, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		_prcust->get_value(6, _customer.C_MKTSEGMENT, STRSIZE(10));

		return (strcmp(_customer.C_MKTSEGMENT, _mktsegment) == 0);
	}

	void project(tuple_t &d, const tuple_t &s) {

		q3_projected_customer_tuple *dest = aligned_cast<q3_projected_customer_tuple>(d.data);

		_prcust->get_value(0, _customer.C_CUSTKEY);

		//TRACE(TRACE_RECORD_FLOW, "%d\n", _customer.C_CUSTKEY);

		dest->C_CUSTKEY = _customer.C_CUSTKEY;
	}

	q3_customer_tscan_filter_t* clone() const {
		return new q3_customer_tscan_filter_t(*this);
	}

	c_str to_string() const {
		//return c_str("q3_customer_tscan_filter_t(%s)", _mktsegment);
		return c_str("q3_customer_tscan_filter_t");
	}
};


class q3_orders_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prorders;
	rep_row_t _rr;

	tpch_orders_tuple _orders;
	time_t _orderdate;

	q3_input_t* q3_input;

public:
	q3_orders_tscan_filter_t(ShoreTPCHEnv* tpchdb, q3_input_t &in)
	: tuple_filter_t(tpchdb->orders_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prorders = _tpchdb->orders_man()->get_tuple();
		_rr.set_ts(_tpchdb->orders_man()->ts(),
				_tpchdb->orders_desc()->maxsize());
		_prorders->_rep = &_rr;

		// Generate the random predicates
		q3_input = &in;

		char time[15];
		timet_to_str(time, q3_input->current_date);
		TRACE(TRACE_ALWAYS, "Random predicate:\nORDERS.O_ORDERDATE < %s\n", time);
	}

	virtual ~q3_orders_tscan_filter_t()
	{
		// Give back the orders tuple
		_tpchdb->orders_man()->give_tuple(_prorders);
	}

	bool select(const tuple_t &input) {
		// Get next orders tuple and read its orderdate
		if (!_tpchdb->orders_man()->load(_prorders, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		_prorders->get_value(4, _orders.O_ORDERDATE, 15);
		_orderdate = str_to_timet(_orders.O_ORDERDATE);

		return _orderdate < q3_input->current_date;
	}

	void project(tuple_t &d, const tuple_t &s) {

		q3_projected_orders_tuple *dest = aligned_cast<q3_projected_orders_tuple>(d.data);

		_prorders->get_value(0, _orders.O_ORDERKEY);
		_prorders->get_value(1, _orders.O_CUSTKEY);
		_prorders->get_value(4, _orders.O_ORDERDATE, 15);
		_prorders->get_value(7, _orders.O_SHIPPRIORITY);

		//TRACE(TRACE_RECORD_FLOW, "%d|%d|%s|%d\n", _orders.O_ORDERKEY, _orders.O_CUSTKEY, _orders.O_ORDERDATE, _orders.O_SHIPPRIORITY);

		dest->O_ORDERKEY = _orders.O_ORDERKEY;
		dest->O_CUSTKEY = _orders.O_CUSTKEY;
		dest->O_ORDERDATE = str_to_timet(_orders.O_ORDERDATE);
		dest->O_SHIPPRIORITY = _orders.O_SHIPPRIORITY;

	}

	q3_orders_tscan_filter_t* clone() const {
		return new q3_orders_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q3_orders_tscan_filter_t(%s)", ctime(&(q3_input->current_date)));
	}
};

class q3_lineitem_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prline;
	rep_row_t _rr;

	tpch_lineitem_tuple _lineitem;
	time_t _shipdate;

	q3_input_t* q3_input;

public:
	q3_lineitem_tscan_filter_t(ShoreTPCHEnv* tpchdb, q3_input_t &in)
	: tuple_filter_t(tpchdb->lineitem_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prline = _tpchdb->lineitem_man()->get_tuple();
		_rr.set_ts(_tpchdb->lineitem_man()->ts(),
				_tpchdb->lineitem_desc()->maxsize());
		_prline->_rep = &_rr;

		// Generate the random predicates
		q3_input = &in;

		char time[15];
		timet_to_str(time, q3_input->current_date);
		TRACE(TRACE_ALWAYS, "Random predicate:\nLINEITEM.L_SHIPDATE > %s\n", time);
	}

	virtual ~q3_lineitem_tscan_filter_t()
	{
		// Give back the lineitem tuple
		_tpchdb->lineitem_man()->give_tuple(_prline);
	}

	bool select(const tuple_t &input) {
		// Get next lineitem tuple and read its shipdate
		if (!_tpchdb->lineitem_man()->load(_prline, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		_prline->get_value(10, _lineitem.L_SHIPDATE, 15);
		_shipdate = str_to_timet(_lineitem.L_SHIPDATE);

		return _shipdate > q3_input->current_date;
	}

	void project(tuple_t &d, const tuple_t &s) {

		q3_projected_lineitem_tuple *dest = aligned_cast<q3_projected_lineitem_tuple>(d.data);

		_prline->get_value(0, _lineitem.L_ORDERKEY);
		_prline->get_value(5, _lineitem.L_EXTENDEDPRICE);
		_prline->get_value(6, _lineitem.L_DISCOUNT);

		dest->L_ORDERKEY = _lineitem.L_ORDERKEY;
		dest->L_EXTENDEDPRICE = _lineitem.L_EXTENDEDPRICE / 100.0;
		dest->L_DISCOUNT = _lineitem.L_DISCOUNT / 100.0;
	}

	q3_lineitem_tscan_filter_t* clone() const {
		return new q3_lineitem_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q3_lineitem_tscan_filter_t(%s)", ctime(&(q3_input->current_date)));
	}
};



struct q3_o_join_c_t : public tuple_join_t {

	q3_o_join_c_t()
	: tuple_join_t(sizeof(q3_projected_orders_tuple),
			offsetof(q3_projected_orders_tuple, O_CUSTKEY),
			sizeof(q3_projected_customer_tuple),
			offsetof(q3_projected_customer_tuple, C_CUSTKEY),
			sizeof(int),
			sizeof(q3_o_join_c_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q3_o_join_c_tuple* dest = aligned_cast<q3_o_join_c_tuple>(d.data);
		q3_projected_orders_tuple* left = aligned_cast<q3_projected_orders_tuple>(l.data);
		q3_projected_customer_tuple* right = aligned_cast<q3_projected_customer_tuple>(r.data);

		dest->O_ORDERKEY = left->O_ORDERKEY;
		dest->O_ORDERDATE = left->O_ORDERDATE;
		dest->O_SHIPPRIORITY = left->O_SHIPPRIORITY;

		char date[STRSIZE(10)];
		timet_to_str(date, left->O_ORDERDATE);
		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %s %d\n", left->O_CUSTKEY, right->C_CUSTKEY, left->O_ORDERKEY, date, left->O_SHIPPRIORITY);
	}

	virtual q3_o_join_c_t* clone() const {
		return new q3_o_join_c_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join ORDERS, CUSTOMER, select O_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY");
	}
};


struct q3_lineitem_sieve_t : public tuple_sieve_t {

	q3_lineitem_sieve_t()
	:tuple_sieve_t(sizeof(q3_aggregated_lineitem_tuple))
	{
	}

	virtual bool pass(tuple_t& dest, const tuple_t& src) {
		q3_aggregated_lineitem_tuple* out = aligned_cast<q3_aggregated_lineitem_tuple>(dest.data);
		q3_projected_lineitem_tuple* in = aligned_cast<q3_projected_lineitem_tuple>(src.data);

		out->L_ORDERKEY = in->L_ORDERKEY;
		out->REVENUE = in->L_EXTENDEDPRICE * (1 - in->L_DISCOUNT);
		return true;
	}

	virtual q3_lineitem_sieve_t* clone() const {
		return new q3_lineitem_sieve_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("q3_lineitem_sieve_t");
	}
};


struct q3_l_join_oc_t : public tuple_join_t {

	q3_l_join_oc_t()
	:tuple_join_t(sizeof(q3_aggregated_lineitem_tuple),
			offsetof(q3_aggregated_lineitem_tuple, L_ORDERKEY),
			sizeof(q3_o_join_c_tuple),
			offsetof(q3_o_join_c_tuple, O_ORDERKEY),
			sizeof(int),
			sizeof(q3_aggregated_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {

		q3_aggregated_tuple* dest = aligned_cast<q3_aggregated_tuple>(d.data);
		q3_aggregated_lineitem_tuple* left = aligned_cast<q3_aggregated_lineitem_tuple>(l.data);
		q3_o_join_c_tuple* right = aligned_cast<q3_o_join_c_tuple>(r.data);

		dest->L_ORDERKEY = left->L_ORDERKEY;
		dest->REVENUE = left->REVENUE;
		dest->O_ORDERDATE = right->O_ORDERDATE;
		dest->O_SHIPPRIORITY = right->O_SHIPPRIORITY;

		char date[STRSIZE(10)];
		timet_to_str(date, right->O_ORDERDATE);
		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %.2f %s %d\n", left->L_ORDERKEY, right->O_ORDERKEY, right->O_ORDERKEY, left->REVENUE.to_double(), date, right->O_SHIPPRIORITY);
	}

	virtual q3_l_join_oc_t* clone() const {
		return new q3_l_join_oc_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join LINEITEM, ORDERS, CUSTOMER, select L_ORDERKEY, REVENUE, O_ORDERDATE, O_SHIPPRIORITY");
	}
};


struct q3_agg_key_compare_t : public key_compare_t {

	virtual int operator()(const void* key1, const void* key2) const {
		q3_agg_key* k1 = aligned_cast<q3_agg_key>(key1);
		q3_agg_key* k2 = aligned_cast<q3_agg_key>(key2);
		int diff_orderkey = k1->L_ORDERKEY - k2->L_ORDERKEY;
		int diff_orderdate = k1->O_ORDERDATE - k2->O_ORDERDATE;
		int diff_shipprio = k1->O_SHIPPRIORITY - k2->O_SHIPPRIORITY;

		return (diff_orderkey != 0 ? diff_orderkey : (diff_orderdate != 0 ? diff_orderdate : diff_shipprio));
	}

	virtual q3_agg_key_compare_t* clone() const {
		return new q3_agg_key_compare_t(*this);
	}
};


struct q3_aggregate_t : tuple_aggregate_t {
	default_key_extractor_t _extractor;

	q3_aggregate_t()
	:tuple_aggregate_t(sizeof(q3_aggregated_tuple)),
	 _extractor(sizeof(q3_agg_key), offsetof(q3_agg_key, L_ORDERKEY))
	{
	}

	virtual key_extractor_t* key_extractor() {
		return &_extractor;
	}

	virtual void aggregate(char* agg_data, const tuple_t &t) {
		q3_aggregated_tuple* dest = aligned_cast<q3_aggregated_tuple>(agg_data);
		q3_aggregated_tuple* src = aligned_cast<q3_aggregated_tuple>(t.data);
		dest->REVENUE += src->REVENUE;
		if(dest->L_ORDERKEY == 3987331) TRACE(TRACE_RECORD_FLOW, "%.2f|%.2f\n", src->REVENUE.to_double(), dest->REVENUE.to_double());
	}

	virtual q3_aggregate_t* clone() const {
		return new q3_aggregate_t(*this);
	}

	virtual void finish(tuple_t& d, const char* agg_data) {
		memcpy(d.data, agg_data, tuple_size());
	}

	virtual c_str to_string() const {
		return c_str("q3_aggregate_t");
	}

	virtual void init(char* agg_data) {
		memset(agg_data, 0, tuple_size());
	}
};

struct q3_top10_t : tuple_aggregate_t {
	default_key_extractor_t _extractor;

	q3_top10_t()
	: tuple_aggregate_t(sizeof(q3_aggregated_tuple)),
	  _extractor(0,0)
	{
	}

	virtual key_extractor_t* key_extractor() {
		return &_extractor;
	}

	virtual void aggregate(char* agg_data, const tuple_t &t) {
		memcpy(agg_data, t.data, tuple_size());
	}

	virtual q3_top10_t* clone() const {
		return new q3_top10_t(*this);
	}

	virtual void finish(tuple_t& d, const char* agg_data) {
		memcpy(d.data, agg_data, tuple_size());
	}

	virtual c_str to_string() const {
		return c_str("q3_top10_t");
	}
};

struct q3_sort_key_extractor_t : public key_extractor_t {

	q3_sort_key_extractor_t() : key_extractor_t(sizeof(q3_aggregated_tuple))
	{
	}

	virtual int extract_hint(const char* tuple_key) {
		q3_aggregated_tuple* tuple = aligned_cast<q3_aggregated_tuple>(tuple_key);
		return -((tuple->REVENUE).to_int());
	}

	virtual q3_sort_key_extractor_t* clone() const {
		return new q3_sort_key_extractor_t(*this);
	}
};

struct q3_sort_key_compare_t : public key_compare_t {

	virtual int operator()(const void* key1, const void* key2) const {
		q3_aggregated_tuple* t1 = aligned_cast<q3_aggregated_tuple>(key1);
		q3_aggregated_tuple* t2 = aligned_cast<q3_aggregated_tuple>(key2);
		int diff_rev = (t2->REVENUE - t1->REVENUE).to_int();
		int diff_time = t2->O_ORDERDATE - t1->O_ORDERDATE;

		return (diff_rev != 0 ? diff_rev : diff_time);
	}

	virtual q3_sort_key_compare_t* clone() const {
		return new q3_sort_key_compare_t(*this);
	}
};

class q3_top10_filter_t : public tuple_filter_t {

private:
	int _count;

public:

	q3_top10_filter_t() : tuple_filter_t(sizeof(q3_aggregated_tuple)), _count(0)
	{
	}

	virtual bool select(const tuple_t &tuple) {
		if(++_count <= 10) {
			return true;
		}
		else {
			return false;
		}
	}

	virtual q3_top10_filter_t* clone() const {
		return new q3_top10_filter_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("q3_top10_filter_t");
	}
};



class tpch_q3_process_tuple_t : public process_tuple_t {

public:

	virtual void begin() {
		TRACE(TRACE_QUERY_RESULTS, "*** Q3 %14s %14s %14s %14s\n",
				"L_ORDERKEY", "REVENUE", "O_ORDERDATE", "O_SHIPPRIORITY");
	}

	virtual void process(const tuple_t& output) {
		q3_aggregated_tuple* r = aligned_cast<q3_aggregated_tuple>(output.data);

		char date[STRSIZE(10)];
		timet_to_str(date, r->O_ORDERDATE);
		TRACE(TRACE_QUERY_RESULTS, "*** Q3 %14d %14.4f %14s %14d\n",
				r->L_ORDERKEY, r->REVENUE.to_double(), date, r->O_SHIPPRIORITY);
	}

};



/********************************************************************
 *
 * QPIPE Q3 - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_qpipe_q3(const int xct_id,
		q3_input_t& in)
{
	TRACE( TRACE_ALWAYS, "********** Q3 *********\n");

	policy_t* dp = this->get_sched_policy();
	xct_t* pxct = smthread_t::me()->xct();


	//TSCAN CUSTOMER
	tuple_fifo* q3_customer_buffer = new tuple_fifo(sizeof(q3_projected_customer_tuple));
	packet_t* q3_customer_tscan_packet =
			new tscan_packet_t("customer TSCAN",
					q3_customer_buffer,
					new q3_customer_tscan_filter_t(this, in),
					this->db(),
					_pcustomer_desc.get(),
					pxct);

	//TSCAN ORDERS
	tuple_fifo* q3_orders_buffer = new tuple_fifo(sizeof(q3_projected_orders_tuple));
	packet_t* q3_orders_tscan_packet =
			new tscan_packet_t("orders TSCAN",
					q3_orders_buffer,
					new q3_orders_tscan_filter_t(this, in),
					this->db(),
					_porders_desc.get(),
					pxct);

	//ORDERS JOIN CUSTOMERS
	tuple_fifo* q3_o_join_c_buffer = new tuple_fifo(sizeof(q3_o_join_c_tuple));
	packet_t* q3_o_join_c_packet =
			new hash_join_packet_t("orders-customer HJOIN",
					q3_o_join_c_buffer,
					new trivial_filter_t(sizeof(q3_o_join_c_tuple)),
					q3_orders_tscan_packet,
					q3_customer_tscan_packet,
					new q3_o_join_c_t());

	//TSCAN LINEITEM
	tuple_fifo* q3_lineitem_buffer = new tuple_fifo(sizeof(q3_projected_lineitem_tuple));
	packet_t* q3_lineitem_tscan_packet =
			new tscan_packet_t("lineitem TSCAN",
					q3_lineitem_buffer,
					new q3_lineitem_tscan_filter_t(this, in),
					this->db(),
					_plineitem_desc.get(),
					pxct);

	//LINEITEM SIEVE
	tuple_fifo* q3_aggregated_lineitem_buffer = new tuple_fifo(sizeof(q3_aggregated_lineitem_tuple));
	sieve_packet_t* q3_aggregated_lineitem_packet =
			new sieve_packet_t("lineitem AGGREGATE",
					q3_aggregated_lineitem_buffer,
					new trivial_filter_t(sizeof(q3_aggregated_lineitem_tuple)),
					new q3_lineitem_sieve_t(),
					q3_lineitem_tscan_packet);

	//LINEITEM JOIN O_C
	tuple_fifo* q3_l_join_oc_buffer = new tuple_fifo(sizeof(q3_aggregated_tuple));
	packet_t* q3_l_join_oc_packet =
			new hash_join_packet_t("lineitem-orders_customer HJOIN",
					q3_l_join_oc_buffer,
					new trivial_filter_t(sizeof(q3_aggregated_tuple)),
					q3_aggregated_lineitem_packet,
					q3_o_join_c_packet,
					new q3_l_join_oc_t());

	//AGGREGATE
	tuple_fifo* q3_agg_buffer = new tuple_fifo(sizeof(q3_aggregated_tuple));
	packet_t* q3_agg_packet =
			new partial_aggregate_packet_t("SUM AGG",
					q3_agg_buffer,
					new trivial_filter_t(sizeof(q3_aggregated_tuple)),
					q3_l_join_oc_packet,
					new q3_aggregate_t(),
					new default_key_extractor_t(sizeof(q3_agg_key)),
					new q3_agg_key_compare_t());

	//SORT + TOP10 FILTER
	tuple_fifo* q3_final_buffer = new tuple_fifo(sizeof(q3_aggregated_tuple));
	packet_t* q3_final_packet =
			new sort_packet_t("SORT",
					q3_final_buffer,
					new q3_top10_filter_t(),
					new q3_sort_key_extractor_t(),
					new q3_sort_key_compare_t(),
					q3_agg_packet);

	tuple_fifo* q3_ffinal_buffer = new tuple_fifo(sizeof(q3_aggregated_tuple));
	packet_t* q3_ffinal_packet =
			new partial_aggregate_packet_t("Top10 Agg",
					q3_ffinal_buffer,
					new q3_top10_filter_t(),
					q3_final_packet,
					new q3_top10_t(),
					new default_key_extractor_t(sizeof(q3_aggregated_tuple)),
					new int_key_compare_t());



	qpipe::query_state_t* qs = dp->query_state_create();
	q3_customer_tscan_packet->assign_query_state(qs);
	q3_orders_tscan_packet->assign_query_state(qs);
	q3_o_join_c_packet->assign_query_state(qs);
	q3_lineitem_tscan_packet->assign_query_state(qs);
	q3_aggregated_lineitem_packet->assign_query_state(qs);
	q3_l_join_oc_packet->assign_query_state(qs);
	q3_agg_packet->assign_query_state(qs);
	q3_final_packet->assign_query_state(qs);
	q3_ffinal_packet->assign_query_state(qs);

	// Dispatch packet
	tpch_q3_process_tuple_t pt;
	//LAST PACKET
	process_query(q3_final_packet, pt);//TODO

	dp->query_state_destroy(qs);


	return (RCOK);
}


EXIT_NAMESPACE(qpipe);
