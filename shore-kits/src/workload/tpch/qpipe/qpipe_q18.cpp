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

/** @file:   qpipe_q18.cpp
 *
 *  @brief:  Implementation of QPIPE TPCH Q18 over Shore-MT
 *
 *  @author:    Andreas Schädeli
 *  @date:      2011-11-21
 */


#include "workload/tpch/shore_tpch_env.h"
#include "workload/tpch/tpch_util.h"
#include "qpipe.h"

using namespace shore;
using namespace qpipe;



ENTER_NAMESPACE(tpch);


/********************************************************************
 *
 * QPIPE Q18 - Structures needed by operators
 *
 ********************************************************************/

/*
select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	sum(l_quantity)
from
	customer,
	orders,
	lineitem
where
	o_orderkey in (
		select
			l_orderkey
		from
			lineitem
		group by
			l_orderkey having
				sum(l_quantity) > [QUANTITY] )
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey
group by
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice
order by
	o_totalprice desc,
	o_orderdate;
 */


struct q18_projected_lineitem_tuple {
	int L_ORDERKEY;
	decimal L_QUANTITY;
};

struct q18_projected_orders_tuple {
	int O_ORDERKEY;
	int O_CUSTKEY;
	char O_ORDERDATE[STRSIZE(10)];
	decimal O_TOTALPRICE;
};

struct q18_projected_customer_tuple {
	int C_CUSTKEY;
	char C_NAME[STRSIZE(25)];
};


struct q18_l_join_o_tuple {
	int O_ORDERKEY;
	int O_CUSTKEY;
	char O_ORDERDATE[STRSIZE(10)];
	decimal O_TOTALPRICE;
	decimal L_QUANTITY;
};

struct q18_final_tuple {
	char O_ORDERDATE[STRSIZE(10)];
	decimal O_TOTALPRICE;
	char C_NAME[STRSIZE(25)];
	int C_CUSTKEY;
	int O_ORDERKEY;
	decimal L_QUANTITY;
};

struct q18_sort_key {
	char O_ORDERDATE[STRSIZE(10)];
	decimal O_TOTALPRICE;
};


class q18_lineitem_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prline;
	rep_row_t _rr;

	tpch_lineitem_tuple _lineitem;

public:
	q18_lineitem_tscan_filter_t(ShoreTPCHEnv* tpchdb, q18_input_t &in)
	: tuple_filter_t(tpchdb->lineitem_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prline = _tpchdb->lineitem_man()->get_tuple();
		_rr.set_ts(_tpchdb->lineitem_man()->ts(),
				_tpchdb->lineitem_desc()->maxsize());
		_prline->_rep = &_rr;
	}

	virtual ~q18_lineitem_tscan_filter_t()
	{
		// Give back the lineitem tuple
		_tpchdb->lineitem_man()->give_tuple(_prline);
	}

	bool select(const tuple_t &input) {
		// Get next lineitem tuple and read its shipdate
		if (!_tpchdb->lineitem_man()->load(_prline, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		return true;
	}

	void project(tuple_t &d, const tuple_t &s) {

		q18_projected_lineitem_tuple *dest = aligned_cast<q18_projected_lineitem_tuple>(d.data);

		_prline->get_value(0, _lineitem.L_ORDERKEY);
		_prline->get_value(4, _lineitem.L_QUANTITY);

		//TRACE(TRACE_RECORD_FLOW, "%d|%.2f\n", _lineitem.L_ORDERKEY, _lineitem.L_QUANTITY);

		dest->L_ORDERKEY = _lineitem.L_ORDERKEY;
		dest->L_QUANTITY = _lineitem.L_QUANTITY;
	}

	q18_lineitem_tscan_filter_t* clone() const {
		return new q18_lineitem_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q18_lineitem_tscan_filter_t()");
	}
};

class q18_orders_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prorders;
	rep_row_t _rr;

	tpch_orders_tuple _orders;

public:
	q18_orders_tscan_filter_t(ShoreTPCHEnv* tpchdb, q18_input_t &in)
	: tuple_filter_t(tpchdb->orders_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prorders = _tpchdb->orders_man()->get_tuple();
		_rr.set_ts(_tpchdb->orders_man()->ts(),
				_tpchdb->orders_desc()->maxsize());
		_prorders->_rep = &_rr;
	}

	virtual ~q18_orders_tscan_filter_t()
	{
		// Give back the orders tuple
		_tpchdb->orders_man()->give_tuple(_prorders);
	}

	bool select(const tuple_t &input) {
		// Get next orders tuple and read its orderdate
		if (!_tpchdb->orders_man()->load(_prorders, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		return true;
	}

	void project(tuple_t &d, const tuple_t &s) {

		q18_projected_orders_tuple *dest = aligned_cast<q18_projected_orders_tuple>(d.data);

		_prorders->get_value(0, _orders.O_ORDERKEY);
		_prorders->get_value(1, _orders.O_CUSTKEY);
		_prorders->get_value(3, _orders.O_TOTALPRICE);
		_prorders->get_value(4, _orders.O_ORDERDATE, sizeof(_orders.O_ORDERDATE));

		//TRACE(TRACE_RECORD_FLOW, "%d|%d|%.2f|%s\n", _orders.O_ORDERKEY, _orders.O_CUSTKEY, _orders.O_TOTALPRICE.to_double(), _orders.O_ORDERDATE);

		dest->O_ORDERKEY = _orders.O_ORDERKEY;
		dest->O_CUSTKEY = _orders.O_CUSTKEY;
		dest->O_TOTALPRICE = _orders.O_TOTALPRICE;
		memcpy(dest->O_ORDERDATE, _orders.O_ORDERDATE, sizeof(dest->O_ORDERDATE));
	}

	q18_orders_tscan_filter_t* clone() const {
		return new q18_orders_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q18_orders_tscan_filter_t()");
	}
};

class q18_customer_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prcust;
	rep_row_t _rr;

	tpch_customer_tuple _customer;

public:
	q18_customer_tscan_filter_t(ShoreTPCHEnv* tpchdb, q18_input_t &in)
	: tuple_filter_t(tpchdb->customer_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prcust = _tpchdb->customer_man()->get_tuple();
		_rr.set_ts(_tpchdb->customer_man()->ts(),
				_tpchdb->customer_desc()->maxsize());
		_prcust->_rep = &_rr;
	}

	virtual ~q18_customer_tscan_filter_t()
	{
		// Give back the customer tuple
		_tpchdb->customer_man()->give_tuple(_prcust);
	}

	bool select(const tuple_t &input) {
		// Get next customer tuple
		if (!_tpchdb->customer_man()->load(_prcust, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		return true;
	}

	void project(tuple_t &d, const tuple_t &s) {

		q18_projected_customer_tuple *dest = aligned_cast<q18_projected_customer_tuple>(d.data);

		_prcust->get_value(0, _customer.C_CUSTKEY);
		_prcust->get_value(1, _customer.C_NAME, sizeof(_customer.C_NAME));

		//TRACE(TRACE_RECORD_FLOW, "%d|%s\n", _customer.C_CUSTKEY, _customer.C_NAME);

		dest->C_CUSTKEY = _customer.C_CUSTKEY;
		memcpy(dest->C_NAME, _customer.C_NAME, sizeof(dest->C_NAME));
	}

	q18_customer_tscan_filter_t* clone() const {
		return new q18_customer_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q18_customer_tscan_filter_t");
	}
};


struct q18_l_join_o_t : public tuple_join_t {

	q18_l_join_o_t()
	: tuple_join_t(sizeof(q18_projected_lineitem_tuple),
			offsetof(q18_projected_lineitem_tuple, L_ORDERKEY),
			sizeof(q18_projected_orders_tuple),
			offsetof(q18_projected_orders_tuple, O_ORDERKEY),
			sizeof(int),
			sizeof(q18_l_join_o_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q18_l_join_o_tuple *dest = aligned_cast<q18_l_join_o_tuple>(d.data);
		q18_projected_lineitem_tuple *line = aligned_cast<q18_projected_lineitem_tuple>(l.data);
		q18_projected_orders_tuple *order = aligned_cast<q18_projected_orders_tuple>(r.data);

		dest->L_QUANTITY = line->L_QUANTITY;
		memcpy(dest->O_ORDERDATE, order->O_ORDERDATE, sizeof(dest->O_ORDERDATE));
		dest->O_ORDERKEY = order->O_ORDERKEY;
		dest->O_TOTALPRICE = order->O_TOTALPRICE;
		dest->O_CUSTKEY = order->O_CUSTKEY;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %.2f %s %d %.2f %d\n", line->L_ORDERKEY, order->O_ORDERKEY, line->L_QUANTITY.to_double(), order->O_ORDERDATE, order->O_ORDERKEY,
		//															order->O_TOTALPRICE.to_double(), order->O_CUSTKEY);
	}

	virtual q18_l_join_o_t* clone() const {
		return new q18_l_join_o_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join LINEITEM, ORDERS; select L_QUANTITY, O_ORDERDATE, O_ORDERKEY, O_TOTALPRICE");
	}
};

struct q18_l_o_join_c_t : public tuple_join_t {

	q18_l_o_join_c_t()
	: tuple_join_t(sizeof(q18_l_join_o_tuple),
			offsetof(q18_l_join_o_tuple, O_CUSTKEY),
			sizeof(q18_projected_customer_tuple),
			offsetof(q18_projected_customer_tuple, C_CUSTKEY),
			sizeof(int),
			sizeof(q18_final_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q18_final_tuple *dest = aligned_cast<q18_final_tuple>(d.data);
		q18_l_join_o_tuple *left = aligned_cast<q18_l_join_o_tuple>(l.data);
		q18_projected_customer_tuple *cust = aligned_cast<q18_projected_customer_tuple>(r.data);

		dest->C_CUSTKEY = cust->C_CUSTKEY;
		memcpy(dest->C_NAME, cust->C_NAME, sizeof(dest->C_NAME));
		dest->L_QUANTITY = left->L_QUANTITY;
		memcpy(dest->O_ORDERDATE, left->O_ORDERDATE, sizeof(dest->O_ORDERDATE));
		dest->O_ORDERKEY = left->O_ORDERKEY;
		dest->O_TOTALPRICE = left->O_TOTALPRICE;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %s %.2f %s %d %.2f\n", left->O_CUSTKEY, cust->C_CUSTKEY, cust->C_CUSTKEY, cust->C_NAME, left->L_QUANTITY.to_double(),
		//																	left->O_ORDERDATE, left->O_ORDERKEY, left->O_TOTALPRICE.to_double());
	}

	virtual q18_l_o_join_c_t* clone() const {
		return new q18_l_o_join_c_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join LINEITEM_ORDERS, CUSTOMER; select C_CUSTKEY, C_NAME, L_QUANTITY, O_ORDERDATE, O_ORDERKEY, O_TOTALPRICE");
	}
};

struct q18_lineitem_aggregate_t : public tuple_aggregate_t {

	default_key_extractor_t _extractor;

	q18_lineitem_aggregate_t()
		: tuple_aggregate_t(sizeof(q18_projected_lineitem_tuple)), _extractor(default_key_extractor_t(sizeof(int), offsetof(q18_projected_lineitem_tuple, L_ORDERKEY)))
	{
	}

	virtual key_extractor_t* key_extractor() {
		return &_extractor;
	}

	virtual void aggregate(char* agg_data, const tuple_t &t) {
		q18_projected_lineitem_tuple *agg = aligned_cast<q18_projected_lineitem_tuple>(agg_data);
		q18_projected_lineitem_tuple *input = aligned_cast<q18_projected_lineitem_tuple>(t.data);

		agg->L_QUANTITY += input->L_QUANTITY;
	}
	virtual void finish(tuple_t &dest, const char* agg_data) {
		memcpy(dest.data, agg_data, dest.size);
	}
	virtual q18_lineitem_aggregate_t* clone() const {
		return new q18_lineitem_aggregate_t(*this);
	}
	virtual c_str to_string() const {
		return "q18_lineitem_aggregate_t";
	}
};


struct q18_qty_filter_t : public tuple_filter_t {

	decimal _quantity;

	q18_qty_filter_t(decimal quantity)
	: tuple_filter_t(sizeof(q18_projected_lineitem_tuple)), _quantity(quantity)
	{
		TRACE(TRACE_ALWAYS, "Random predicates:\nsum(l_quantity) > %.2f\n", _quantity.to_double());
	}

	bool select(const tuple_t &input) {
		q18_projected_lineitem_tuple *tuple = aligned_cast<q18_projected_lineitem_tuple>(input.data);
		return tuple->L_QUANTITY > _quantity;
	}

	virtual q18_qty_filter_t* clone() const {
		return new q18_qty_filter_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("q18_qty_filter_t");
	}
};

struct q18_aggregate_t : public tuple_aggregate_t {

	default_key_extractor_t _extractor;

	q18_aggregate_t()
		: tuple_aggregate_t(sizeof(q18_final_tuple)), _extractor(default_key_extractor_t(sizeof(q18_final_tuple)))
	{
	}

	virtual key_extractor_t* key_extractor() {
		return &_extractor;
	}

	virtual void aggregate(char* agg_data, const tuple_t &t) {
		memcpy(agg_data, t.data, sizeof(agg_data));
	}
	virtual void finish(tuple_t &dest, const char* agg_data) {
		memcpy(dest.data, agg_data, dest.size);
	}
	virtual q18_aggregate_t* clone() const {
		return new q18_aggregate_t(*this);
	}
	virtual c_str to_string() const {
		return "q18_aggregate_t";
	}
};

struct q18_key_extractor_t : public key_extractor_t {

	q18_key_extractor_t()
	: key_extractor_t(sizeof(q18_final_tuple))
	{
	}

	virtual int extract_hint(const char* key) const {
		q18_final_tuple *t = aligned_cast<q18_final_tuple>(key);

		return t->C_CUSTKEY;
	}

	virtual q18_key_extractor_t* clone() const {
		return new q18_key_extractor_t(*this);
	}
};

struct q18_key_compare_t : public key_compare_t {

	virtual int operator()(const void* key1, const void* key2) const {
		q18_final_tuple *k1 = aligned_cast<q18_final_tuple>(key1);
		q18_final_tuple *k2 = aligned_cast<q18_final_tuple>(key2);

		int diff_custkey = k1->C_CUSTKEY - k2->C_CUSTKEY;

		return (diff_custkey != 0 ? diff_custkey : (k1->O_ORDERKEY - k2->O_ORDERKEY));
	}

	virtual q18_key_compare_t* clone() const {
		return new q18_key_compare_t(*this);
	}
};

struct q18_sort_key_extractor_t : public key_extractor_t {

	q18_sort_key_extractor_t()
	: key_extractor_t(sizeof(q18_sort_key), offsetof(q18_final_tuple, O_ORDERDATE))
	{
	}

	virtual int extract_hint(const char* key) const {
		q18_sort_key *k = aligned_cast<q18_sort_key>(key);
		return - k->O_TOTALPRICE.to_int();
	}

	virtual q18_sort_key_extractor_t* clone() const {
		return new q18_sort_key_extractor_t(*this);
	}
};

struct q18_sort_key_compare_t : public key_compare_t {

	virtual int operator()(const void* key1, const void* key2) const {
		q18_sort_key *k1 = aligned_cast<q18_sort_key>(key1);
		q18_sort_key *k2 = aligned_cast<q18_sort_key>(key2);

		time_t d1 = str_to_timet(k1->O_ORDERDATE);
		time_t d2 = str_to_timet(k2->O_ORDERDATE);

		return (k1->O_TOTALPRICE > k2->O_TOTALPRICE ? -1 : (k1->O_TOTALPRICE < k2->O_TOTALPRICE ? 1 : d1 - d2));
	}

	virtual q18_sort_key_compare_t* clone() const {
		return new q18_sort_key_compare_t(*this);
	}
};




class tpch_q18_process_tuple_t : public process_tuple_t {

public:

	virtual void begin() {
		TRACE(TRACE_QUERY_RESULTS, "*** Q18 %s %s %s %s %s %s\n",
				"C_NAME", "C_CUSTKEY", "O_ORDERKEY", "O_ORDERDATE", "O_TOTALPRICE", "SUM_QUANTITY");
	}

	virtual void process(const tuple_t& output) {
		q18_final_tuple *agg = aligned_cast<q18_final_tuple>(output.data);

		TRACE(TRACE_QUERY_RESULTS, "*** Q18 %s %d %d %s %.4f %.4f\n", agg->C_NAME, agg->C_CUSTKEY, agg->O_ORDERKEY, agg->O_ORDERDATE, agg->O_TOTALPRICE.to_double(),
				agg->L_QUANTITY.to_double());
	}

};


/********************************************************************
 *
 * QPIPE q18 - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_qpipe_q18(const int xct_id,
		q18_input_t& in)
{
	TRACE( TRACE_ALWAYS, "********** q18 *********\n");

	policy_t* dp = this->get_sched_policy();
	xct_t* pxct = smthread_t::me()->xct();


	//TSCAN LINEITEM
	tuple_fifo* q18_lineitem_buffer = new tuple_fifo(sizeof(q18_projected_lineitem_tuple));
	packet_t* q18_lineitem_tscan_packet =
			new tscan_packet_t("lineitem TSCAN",
					q18_lineitem_buffer,
					new q18_lineitem_tscan_filter_t(this, in),
					this->db(),
					_plineitem_desc.get(),
					pxct);

	//TSCAN ORDERS
	tuple_fifo* q18_orders_buffer = new tuple_fifo(sizeof(q18_projected_orders_tuple));
	packet_t* q18_orders_tscan_packet =
			new tscan_packet_t("orders TSCAN",
					q18_orders_buffer,
					new q18_orders_tscan_filter_t(this, in),
					this->db(),
					_porders_desc.get(),
					pxct);

	//TSCAN CUSTOMER
	tuple_fifo* q18_customer_buffer = new tuple_fifo(sizeof(q18_projected_customer_tuple));
	packet_t* q18_customer_tscan_packet =
			new tscan_packet_t("customer TSCAN",
					q18_customer_buffer,
					new q18_customer_tscan_filter_t(this, in),
					this->db(),
					_pcustomer_desc.get(),
					pxct);

	//LINEITEM AGGREGATE
	tuple_fifo* q18_line_agg_buffer = new tuple_fifo(sizeof(q18_projected_lineitem_tuple));
	packet_t* q18_line_agg_packet =
			new partial_aggregate_packet_t("lineitem AGG",
					q18_line_agg_buffer,
					new q18_qty_filter_t((&in)->l_quantity),
					q18_lineitem_tscan_packet,
					new q18_lineitem_aggregate_t(),
					new default_key_extractor_t(sizeof(int), offsetof(q18_projected_lineitem_tuple, L_ORDERKEY)),
					new int_key_compare_t());

	//LINEITEM JOIN ORDERS
	tuple_fifo* q18_l_join_o_buffer = new tuple_fifo(sizeof(q18_l_join_o_tuple));
	packet_t* q18_l_join_o_packet =
			new hash_join_packet_t("lineitem - orders HJOIN",
					q18_l_join_o_buffer,
					new trivial_filter_t(sizeof(q18_l_join_o_tuple)),
					q18_line_agg_packet,
					q18_orders_tscan_packet,
					new q18_l_join_o_t());

	//LINEITEM_ORDERS JOIN CUSTOMER
	tuple_fifo* q18_l_o_join_c_buffer = new tuple_fifo(sizeof(q18_final_tuple));
	packet_t* q18_l_o_join_c_packet =
			new hash_join_packet_t("lineitem_orders - customer HJOIN",
					q18_l_o_join_c_buffer,
					new trivial_filter_t(sizeof(q18_final_tuple)),
					q18_l_join_o_packet,
					q18_customer_tscan_packet,
					new q18_l_o_join_c_t());

	//GROUP BY AGG
	tuple_fifo* q18_agg_buffer = new tuple_fifo(sizeof(q18_final_tuple));
	packet_t* q18_agg_packet =
			new partial_aggregate_packet_t("GROUP BY AGG",
					q18_agg_buffer,
					new trivial_filter_t(sizeof(q18_final_tuple)),
					q18_l_o_join_c_packet,
					new q18_aggregate_t(),
					new q18_key_extractor_t(),
					new q18_key_compare_t());

	//SORT
	tuple_fifo* q18_sort_buffer = new tuple_fifo(sizeof(q18_final_tuple));
	packet_t* q18_sort_packet =
			new sort_packet_t("SORT",
					q18_sort_buffer,
					new trivial_filter_t(sizeof(q18_final_tuple)),
					new q18_sort_key_extractor_t(),
					new q18_sort_key_compare_t(),
					q18_agg_packet);


	qpipe::query_state_t* qs = dp->query_state_create();
	q18_lineitem_tscan_packet->assign_query_state(qs);
	q18_orders_tscan_packet->assign_query_state(qs);
	q18_customer_tscan_packet->assign_query_state(qs);
	q18_line_agg_packet->assign_query_state(qs);
	q18_l_join_o_packet->assign_query_state(qs);
	q18_l_o_join_c_packet->assign_query_state(qs);
	q18_agg_packet->assign_query_state(qs);
	q18_sort_packet->assign_query_state(qs);


	// Dispatch packet
	tpch_q18_process_tuple_t pt;
	//LAST PACKET
	process_query(q18_sort_packet, pt);//TODO

	dp->query_state_destroy(qs);


	return (RCOK);
}
EXIT_NAMESPACE(tpch);
