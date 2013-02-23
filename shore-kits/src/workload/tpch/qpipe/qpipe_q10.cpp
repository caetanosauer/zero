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

/** @file:   qpipe_q10.cpp
 *
 *  @brief:  Implementation of QPIPE TPCH Q10 over Shore-MT
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
 * QPIPE Q10 - Structures needed by operators
 *
 ********************************************************************/

/*
select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	customer,
	orders,
	lineitem,
	nation
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= date '[DATE]'
	and o_orderdate < date '[DATE]' + interval '3' month
	and l_returnflag = 'R'
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc;
 */


struct q10_projected_lineitem_tuple {
	int L_ORDERKEY;
	decimal REVENUE;
};

struct q10_projected_orders_tuple {
	int O_ORDERKEY;
	int O_CUSTKEY;
};

struct q10_projected_customer_tuple {
	int C_CUSTKEY;
	int C_NATIONKEY;
	char C_NAME[STRSIZE(25)];
	decimal C_ACCTBAL;
	char C_ADDRESS[STRSIZE(40)];
	char C_PHONE[STRSIZE(15)];
	char C_COMMENT[STRSIZE(117)];
};

struct q10_projected_nation_tuple {
	int N_NATIONKEY;
	char N_NAME[STRSIZE(25)];
};

struct q10_l_join_o_tuple {
	int O_CUSTKEY;
	decimal REVENUE;
};

struct q10_c_join_l_o_tuple {
	int C_CUSTKEY;
	int C_NATIONKEY;
	char C_NAME[STRSIZE(25)];
	decimal C_ACCTBAL;
	char C_ADDRESS[STRSIZE(40)];
	char C_PHONE[STRSIZE(15)];
	char C_COMMENT[STRSIZE(117)];
	decimal REVENUE;
};

struct q10_final_tuple {
	int C_CUSTKEY;
	char C_NAME[STRSIZE(25)];
	decimal C_ACCTBAL;
	char C_PHONE[STRSIZE(15)];
	char N_NAME[STRSIZE(25)];
	char C_ADDRESS[STRSIZE(40)];
	char C_COMMENT[STRSIZE(117)];
	decimal REVENUE;
};


class q10_lineitem_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prline;
	rep_row_t _rr;

	tpch_lineitem_tuple _lineitem;

public:
	q10_lineitem_tscan_filter_t(ShoreTPCHEnv* tpchdb, q10_input_t &in)
	: tuple_filter_t(tpchdb->lineitem_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prline = _tpchdb->lineitem_man()->get_tuple();
		_rr.set_ts(_tpchdb->lineitem_man()->ts(),
				_tpchdb->lineitem_desc()->maxsize());
		_prline->_rep = &_rr;
	}

	virtual ~q10_lineitem_tscan_filter_t()
	{
		// Give back the lineitem tuple
		_tpchdb->lineitem_man()->give_tuple(_prline);
	}

	bool select(const tuple_t &input) {
		// Get next lineitem tuple and read its shipdate
		if (!_tpchdb->lineitem_man()->load(_prline, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		_prline->get_value(8, _lineitem.L_RETURNFLAG);

		return _lineitem.L_RETURNFLAG == 'R';
	}

	void project(tuple_t &d, const tuple_t &s) {

		q10_projected_lineitem_tuple *dest = aligned_cast<q10_projected_lineitem_tuple>(d.data);

		_prline->get_value(0, _lineitem.L_ORDERKEY);
		_prline->get_value(5, _lineitem.L_EXTENDEDPRICE);
		_prline->get_value(6, _lineitem.L_DISCOUNT);

		//TRACE(TRACE_RECORD_FLOW, "%d|%.2f\n", _lineitem.L_ORDERKEY, _lineitem.L_EXTENDEDPRICE / 100.0 * (1 - _lineitem.L_DISCOUNT / 100.0));

		dest->L_ORDERKEY = _lineitem.L_ORDERKEY;
		dest->REVENUE = _lineitem.L_EXTENDEDPRICE / 100.0 * (1 - _lineitem.L_DISCOUNT / 100.0);
#warning MA: Discount from TPCH dbgen is created between 0 and 100 instead between 0 and 1.
	}

	q10_lineitem_tscan_filter_t* clone() const {
		return new q10_lineitem_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q10_lineitem_tscan_filter_t()");
	}
};

class q10_orders_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prorders;
	rep_row_t _rr;

	tpch_orders_tuple _orders;
	time_t _orderdate;

	time_t _first_orderdate;
	time_t _last_orderdate;

public:
	q10_orders_tscan_filter_t(ShoreTPCHEnv* tpchdb, q10_input_t &in)
	: tuple_filter_t(tpchdb->orders_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prorders = _tpchdb->orders_man()->get_tuple();
		_rr.set_ts(_tpchdb->orders_man()->ts(),
				_tpchdb->orders_desc()->maxsize());
		_prorders->_rep = &_rr;

		_first_orderdate = (&in)->o_orderdate;
		struct tm *tm = gmtime(&_first_orderdate);
		tm->tm_mon += 3;
		_last_orderdate = mktime(tm);

		char f_orderdate[STRSIZE(10)];
		char l_orderdate[STRSIZE(10)];
		timet_to_str(f_orderdate, _first_orderdate);
		timet_to_str(l_orderdate, _last_orderdate);

		TRACE(TRACE_ALWAYS, "Random predicate:\nO_ORDERDATE between [%s, %s[\n", f_orderdate, l_orderdate);
	}

	virtual ~q10_orders_tscan_filter_t()
	{
		// Give back the orders tuple
		_tpchdb->orders_man()->give_tuple(_prorders);
	}

	bool select(const tuple_t &input) {
		// Get next orders tuple and read its orderdate
		if (!_tpchdb->orders_man()->load(_prorders, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		_prorders->get_value(4, _orders.O_ORDERDATE, sizeof(_orders.O_ORDERDATE));
		_orderdate = str_to_timet(_orders.O_ORDERDATE);

		return _orderdate >= _first_orderdate && _orderdate < _last_orderdate;
	}

	void project(tuple_t &d, const tuple_t &s) {

		q10_projected_orders_tuple *dest = aligned_cast<q10_projected_orders_tuple>(d.data);

		_prorders->get_value(0, _orders.O_ORDERKEY);
		_prorders->get_value(1, _orders.O_CUSTKEY);

		//TRACE(TRACE_RECORD_FLOW, "%d|%d\n", _orders.O_ORDERKEY, _orders.O_CUSTKEY);

		dest->O_ORDERKEY = _orders.O_ORDERKEY;
		dest->O_CUSTKEY = _orders.O_CUSTKEY;
	}

	q10_orders_tscan_filter_t* clone() const {
		return new q10_orders_tscan_filter_t(*this);
	}

	c_str to_string() const {
		char f_orderdate[STRSIZE(10)];
		char l_orderdate[STRSIZE(10)];
		timet_to_str(f_orderdate, _first_orderdate);
		timet_to_str(l_orderdate, _last_orderdate);
		return c_str("q10_orders_tscan_filter_t(O_ORDERDATE between [%s, %s[)", f_orderdate, l_orderdate);
	}
};

class q10_customer_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prcust;
	rep_row_t _rr;

	tpch_customer_tuple _customer;

public:
	q10_customer_tscan_filter_t(ShoreTPCHEnv* tpchdb, q10_input_t &in)
	: tuple_filter_t(tpchdb->customer_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prcust = _tpchdb->customer_man()->get_tuple();
		_rr.set_ts(_tpchdb->customer_man()->ts(),
				_tpchdb->customer_desc()->maxsize());
		_prcust->_rep = &_rr;
	}

	virtual ~q10_customer_tscan_filter_t()
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

		q10_projected_customer_tuple *dest = aligned_cast<q10_projected_customer_tuple>(d.data);

		_prcust->get_value(0, _customer.C_CUSTKEY);
		_prcust->get_value(1, _customer.C_NAME, sizeof(_customer.C_NAME));
		_prcust->get_value(2, _customer.C_ADDRESS, sizeof(_customer.C_ADDRESS));
		_prcust->get_value(3, _customer.C_NATIONKEY);
		_prcust->get_value(4, _customer.C_PHONE, sizeof(_customer.C_PHONE));
		_prcust->get_value(5, _customer.C_ACCTBAL);
		_prcust->get_value(7, _customer.C_COMMENT, sizeof(_customer.C_COMMENT));

		//TRACE(TRACE_RECORD_FLOW, "%d|%s|%s|%d|%s|%.4f|%s\n", _customer.C_CUSTKEY, _customer.C_NAME, _customer.C_ADDRESS, _customer.C_NATIONKEY, _customer.C_PHONE,
		//		_customer.C_ACCTBAL.to_double(), _customer.C_COMMENT);

		dest->C_CUSTKEY = _customer.C_CUSTKEY;
		memcpy(dest->C_NAME, _customer.C_NAME, sizeof(dest->C_NAME));
		memcpy(dest->C_ADDRESS, _customer.C_ADDRESS, sizeof(dest->C_ADDRESS));
		dest->C_NATIONKEY = _customer.C_NATIONKEY;
		memcpy(dest->C_PHONE, _customer.C_PHONE, sizeof(dest->C_PHONE));
		dest->C_ACCTBAL = _customer.C_ACCTBAL;
		memcpy(dest->C_COMMENT, _customer.C_COMMENT, sizeof(dest->C_COMMENT));
	}

	q10_customer_tscan_filter_t* clone() const {
		return new q10_customer_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q10_customer_tscan_filter_t");
	}
};

class q10_nation_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prnation;
	rep_row_t _rr;

	/*One nation tuple*/
	tpch_nation_tuple _nation;

public:
	q10_nation_tscan_filter_t(ShoreTPCHEnv* tpchdb, q10_input_t &in)
	: tuple_filter_t(tpchdb->nation_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prnation = _tpchdb->nation_man()->get_tuple();
		_rr.set_ts(_tpchdb->nation_man()->ts(),
				_tpchdb->nation_desc()->maxsize());
		_prnation->_rep = &_rr;
	}

	virtual ~q10_nation_tscan_filter_t()
	{
		_tpchdb->nation_man()->give_tuple(_prnation);
	}

	bool select(const tuple_t &input) {

		// Get next nation tuple and read its size and type
		if (!_tpchdb->nation_man()->load(_prnation, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		return true;
	}


	void project(tuple_t &d, const tuple_t &s) {

		q10_projected_nation_tuple *dest;
		dest = aligned_cast<q10_projected_nation_tuple>(d.data);

		_prnation->get_value(0, _nation.N_NATIONKEY);
		_prnation->get_value(1, _nation.N_NAME, sizeof(_nation.N_NAME));

		//TRACE( TRACE_RECORD_FLOW, "%d|%s\n",
		//		_nation.N_NATIONKEY, _nation.N_NAME);

		dest->N_NATIONKEY = _nation.N_NATIONKEY;
		memcpy(dest->N_NAME, _nation.N_NAME, sizeof(dest->N_NAME));
	}

	q10_nation_tscan_filter_t* clone() const {
		return new q10_nation_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q10_nation_tscan_filter_t()");
	}
};


struct q10_l_join_o_t : public tuple_join_t {

	q10_l_join_o_t()
	: tuple_join_t(sizeof(q10_projected_lineitem_tuple),
			offsetof(q10_projected_lineitem_tuple, L_ORDERKEY),
			sizeof(q10_projected_orders_tuple),
			offsetof(q10_projected_orders_tuple, O_ORDERKEY),
			sizeof(int),
			sizeof(q10_l_join_o_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q10_l_join_o_tuple *dest = aligned_cast<q10_l_join_o_tuple>(d.data);
		q10_projected_lineitem_tuple *line = aligned_cast<q10_projected_lineitem_tuple>(l.data);
		q10_projected_orders_tuple *order = aligned_cast<q10_projected_orders_tuple>(r.data);

		dest->O_CUSTKEY = order->O_CUSTKEY;
		dest->REVENUE = line->REVENUE;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %.2f\n", line->L_ORDERKEY, order->O_ORDERKEY, order->O_CUSTKEY, line->REVENUE.to_double());
	}

	virtual q10_l_join_o_t* clone() const {
		return new q10_l_join_o_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join LINEITEM, ORDERS; select O_CUSTKEY, REVENUE");
	}
};

struct q10_c_join_l_o_t : public tuple_join_t {

	q10_c_join_l_o_t()
	: tuple_join_t(sizeof(q10_projected_customer_tuple),
			offsetof(q10_projected_customer_tuple, C_CUSTKEY),
			sizeof(q10_l_join_o_tuple),
			offsetof(q10_l_join_o_tuple, O_CUSTKEY),
			sizeof(int),
			sizeof(q10_c_join_l_o_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q10_c_join_l_o_tuple *dest = aligned_cast<q10_c_join_l_o_tuple>(d.data);
		q10_projected_customer_tuple *cust = aligned_cast<q10_projected_customer_tuple>(l.data);
		q10_l_join_o_tuple *right = aligned_cast<q10_l_join_o_tuple>(r.data);

		dest->C_ACCTBAL = cust->C_ACCTBAL;
		memcpy(dest->C_ADDRESS, cust->C_ADDRESS, sizeof(dest->C_ADDRESS));
		memcpy(dest->C_COMMENT, cust->C_COMMENT, sizeof(dest->C_COMMENT));
		dest->C_CUSTKEY = cust->C_CUSTKEY;
		memcpy(dest->C_NAME, cust->C_NAME, sizeof(dest->C_NAME));
		dest->C_NATIONKEY = cust->C_NATIONKEY;
		memcpy(dest->C_PHONE, cust->C_PHONE, sizeof(dest->C_PHONE));
		dest->REVENUE = right->REVENUE;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %.2f %s %s %d %s %d %s %.2f\n", cust->C_CUSTKEY, right->O_CUSTKEY, cust->C_ACCTBAL.to_double(), cust->C_ADDRESS, cust->C_COMMENT,
		//																		cust->C_CUSTKEY, cust->C_NAME, cust->C_NATIONKEY, cust->C_PHONE, right->REVENUE.to_double());
	}

	virtual q10_c_join_l_o_t* clone() const {
		return new q10_c_join_l_o_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join CUSTOMER, LINEITEM_ORDERS; select C_ACCTBAL, C_ADDRESS, C_COMMENT, C_CUSTKEY, C_NAME, C_NATIONKEY, C_PHONE, REVENUE");
	}
};

struct q10_final_join_t : public tuple_join_t {

	q10_final_join_t()
	: tuple_join_t(sizeof(q10_projected_nation_tuple),
			offsetof(q10_projected_nation_tuple, N_NATIONKEY),
			sizeof(q10_c_join_l_o_tuple),
			offsetof(q10_c_join_l_o_tuple, C_NATIONKEY),
			sizeof(int),
			sizeof(q10_final_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q10_final_tuple *dest = aligned_cast<q10_final_tuple>(d.data);
		q10_projected_nation_tuple *nation = aligned_cast<q10_projected_nation_tuple>(l.data);
		q10_c_join_l_o_tuple *right = aligned_cast<q10_c_join_l_o_tuple>(r.data);

		dest->C_ACCTBAL = right->C_ACCTBAL;
		memcpy(dest->C_ADDRESS, right->C_ADDRESS, sizeof(dest->C_ADDRESS));
		memcpy(dest->C_COMMENT, right->C_COMMENT, sizeof(dest->C_COMMENT));
		dest->C_CUSTKEY = right->C_CUSTKEY;
		memcpy(dest->C_NAME, right->C_NAME, sizeof(dest->C_NAME));
		memcpy(dest->C_PHONE, right->C_PHONE, sizeof(dest->C_PHONE));
		memcpy(dest->N_NAME, nation->N_NAME, sizeof(dest->N_NAME));
		dest->REVENUE = right->REVENUE;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %.2f %s %s %d %s %s %s %.2f\n", nation->N_NATIONKEY, right->C_NATIONKEY, right->C_ACCTBAL.to_double(), right->C_ADDRESS, right->C_COMMENT,
		//																		right->C_NAME, right->C_PHONE, nation->N_NAME, right->REVENUE.to_double());
	}

	virtual q10_final_join_t* clone() const {
		return new q10_final_join_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join NATION, CUSTOMER_LINEITEM_ORDERS; select C_ACCTBAL, C_ADDRESS, C_COMMENT, C_CUSTKEY, C_NAME, C_PHONE, N_NAME, REVENUE");
	}
};

struct q10_top20_filter_t : public tuple_filter_t
{
	int _count;

	q10_top20_filter_t()
	: tuple_filter_t(sizeof(q10_final_tuple)), _count(0)
	{
	}

	bool select(const tuple_t &input) {
		return ++_count <= 20;
	}

	virtual q10_top20_filter_t* clone() const {
		return new q10_top20_filter_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("q10_top20_filter_t");
	}
};


struct q10_aggregate_t : public tuple_aggregate_t {

	default_key_extractor_t _extractor;

	q10_aggregate_t()
	: tuple_aggregate_t(sizeof(q10_final_tuple)), _extractor(sizeof(int))
	{
	}

	virtual key_extractor_t* key_extractor() {
		return &_extractor;
	}

	virtual void aggregate(char* agg_data, const tuple_t &t) {
		q10_final_tuple *agg = aligned_cast<q10_final_tuple>(agg_data);
		q10_final_tuple *in = aligned_cast<q10_final_tuple>(t.data);

		agg->C_ACCTBAL = in->C_ACCTBAL;
		memcpy(agg->C_ADDRESS, in->C_ADDRESS, sizeof(agg->C_ADDRESS));
		memcpy(agg->C_COMMENT, in->C_COMMENT, sizeof(agg->C_COMMENT));
		agg->C_CUSTKEY = in->C_CUSTKEY;
		memcpy(agg->C_NAME, in->C_NAME, sizeof(agg->C_NAME));
		memcpy(agg->C_PHONE, in->C_PHONE, sizeof(agg->C_PHONE));
		memcpy(agg->N_NAME, in->N_NAME, sizeof(agg->N_NAME));
		agg->REVENUE += in->REVENUE;
	}
	virtual void finish(tuple_t &dest, const char* agg_data) {
		memcpy(dest.data, agg_data, dest.size);
	}
	virtual q10_aggregate_t* clone() const {
		return new q10_aggregate_t(*this);
	}
	virtual c_str to_string() const {
		return "q10_aggregate_t";
	}
};

struct q10_sort_key_extractor_t : public key_extractor_t {

	q10_sort_key_extractor_t()
		: key_extractor_t(sizeof(decimal), offsetof(q10_final_tuple, REVENUE))
		{
		}

		virtual int extract_hint(const char* key) const {
			return -(*aligned_cast<decimal>(key)).to_int();
		}

		virtual q10_sort_key_extractor_t* clone() const {
			return new q10_sort_key_extractor_t(*this);
		}
};

struct q10_sort_key_compare_t : public key_compare_t {

	virtual int operator()(const void* key1, const void* key2) const {
			decimal rev1 = *aligned_cast<decimal>(key1);
			decimal rev2 = *aligned_cast<decimal>(key2);
			return rev1 > rev2 ? -1 : (rev1 < rev2 ? 1 : 0);
		}

		virtual q10_sort_key_compare_t* clone() const {
			return new q10_sort_key_compare_t(*this);
		}
};



class tpch_q10_process_tuple_t : public process_tuple_t {

public:

	virtual void begin() {
		TRACE(TRACE_QUERY_RESULTS, "*** Q10 %s %s %s %s %s %s %s %s\n",
				"C_CUSTKEY", "C_NAME", "REVENUE", "C_ACCTBAL", "N_NAME", "C_ADDRESS", "C_PHONE", "C_COMMENT");
	}

	virtual void process(const tuple_t& output) {
		q10_final_tuple *agg = aligned_cast<q10_final_tuple>(output.data);

		TRACE(TRACE_QUERY_RESULTS, "*** Q10 %d %s %.2f %.2f %s %s %s %s\n", agg->C_CUSTKEY, agg->C_NAME, agg->REVENUE.to_double(), agg->C_ACCTBAL.to_double(),
				agg->N_NAME, agg->C_ADDRESS, agg->C_PHONE, agg->C_COMMENT);
	}
};


/********************************************************************
 *
 * QPIPE Q10 - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_qpipe_q10(const int xct_id,
		q10_input_t& in)
{
	TRACE( TRACE_ALWAYS, "********** q10 *********\n");

	policy_t* dp = this->get_sched_policy();
	xct_t* pxct = smthread_t::me()->xct();


	//TSCAN ORDERS
	tuple_fifo* q10_orders_buffer = new tuple_fifo(sizeof(q10_projected_orders_tuple));
	packet_t* q10_orders_tscan_packet =
			new tscan_packet_t("orders TSCAN",
					q10_orders_buffer,
					new q10_orders_tscan_filter_t(this, in),
					this->db(),
					_porders_desc.get(),
					pxct);

	//TSCAN LINEITEM
	tuple_fifo* q10_lineitem_buffer = new tuple_fifo(sizeof(q10_projected_lineitem_tuple));
	packet_t* q10_lineitem_tscan_packet =
			new tscan_packet_t("lineitem TSCAN",
					q10_lineitem_buffer,
					new q10_lineitem_tscan_filter_t(this, in),
					this->db(),
					_plineitem_desc.get(),
					pxct);

	//TSCAN CUSTOMER
	tuple_fifo* q10_customer_buffer = new tuple_fifo(sizeof(q10_projected_customer_tuple));
	packet_t* q10_customer_tscan_packet =
			new tscan_packet_t("customer TSCAN",
					q10_customer_buffer,
					new q10_customer_tscan_filter_t(this, in),
					this->db(),
					_pcustomer_desc.get(),
					pxct);

	//TSCAN NATION
	tuple_fifo* q10_nation_buffer = new tuple_fifo(sizeof(q10_projected_nation_tuple));
	packet_t* q10_nation_tscan_packet =
			new tscan_packet_t("nation TSCAN",
					q10_nation_buffer,
					new q10_nation_tscan_filter_t(this, in),
					this->db(),
					_pnation_desc.get(),
					pxct);


	//LINEITEM JOIN ORDERS
	tuple_fifo* q10_l_join_o_buffer = new tuple_fifo(sizeof(q10_l_join_o_tuple));
	packet_t* q10_l_join_o_packet =
			new hash_join_packet_t("lineitem - orders HJOIN",
					q10_l_join_o_buffer,
					new trivial_filter_t(sizeof(q10_l_join_o_tuple)),
					q10_lineitem_tscan_packet,
					q10_orders_tscan_packet,
					new q10_l_join_o_t());

	//CUSTOMER JOIN LINEITEM_ORDERS
	tuple_fifo* q10_c_join_l_o_buffer = new tuple_fifo(sizeof(q10_c_join_l_o_tuple));
	packet_t* q10_c_join_l_o_packet =
			new hash_join_packet_t("customer - lineitem_orders HJOIN",
					q10_c_join_l_o_buffer,
					new trivial_filter_t(sizeof(q10_c_join_l_o_tuple)),
					q10_customer_tscan_packet,
					q10_l_join_o_packet,
					new q10_c_join_l_o_t());

	//NATION JOIN CUSTOMER_LINEITEM_ORDERS
	tuple_fifo* q10_all_joins_buffer = new tuple_fifo(sizeof(q10_final_tuple));
	packet_t* q10_all_joins_packet =
			new hash_join_packet_t("nation - customer_lineitem_orders HJOIN",
					q10_all_joins_buffer,
					new trivial_filter_t(sizeof(q10_final_tuple)),
					q10_nation_tscan_packet,
					q10_c_join_l_o_packet,
					new q10_final_join_t());

	//AGGREGATE
	tuple_fifo* q10_agg_buffer = new tuple_fifo(sizeof(q10_final_tuple));
	packet_t* q10_agg_packet =
			new partial_aggregate_packet_t("AGGREGATE",
					q10_agg_buffer,
					new trivial_filter_t(sizeof(q10_final_tuple)),
					q10_all_joins_packet,
					new q10_aggregate_t(),
					new default_key_extractor_t(sizeof(int), offsetof(q10_final_tuple, C_CUSTKEY)),
					new int_key_compare_t());

	//SORT
	tuple_fifo* q10_sort_buffer = new tuple_fifo(sizeof(q10_final_tuple));
	packet_t* q10_sort_packet =
			new sort_packet_t("SORT",
					q10_sort_buffer,
					new q10_top20_filter_t(),
					new q10_sort_key_extractor_t(),
					new q10_sort_key_compare_t(),
					q10_agg_packet);


	qpipe::query_state_t* qs = dp->query_state_create();
	q10_orders_tscan_packet->assign_query_state(qs);
	q10_lineitem_tscan_packet->assign_query_state(qs);
	q10_customer_tscan_packet->assign_query_state(qs);
	q10_nation_tscan_packet->assign_query_state(qs);
	q10_l_join_o_packet->assign_query_state(qs);
	q10_c_join_l_o_packet->assign_query_state(qs);
	q10_all_joins_packet->assign_query_state(qs);
	q10_agg_packet->assign_query_state(qs);
	q10_sort_packet->assign_query_state(qs);


	// Dispatch packet
	tpch_q10_process_tuple_t pt;
	//LAST PACKET
	process_query(q10_sort_packet, pt);//TODO

	dp->query_state_destroy(qs);


	return (RCOK);
}

EXIT_NAMESPACE(tpch);
