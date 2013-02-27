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

/** @file:   qpipe_q5.cpp
 *
 *  @brief:  Implementation of QPIPE TPCH Q5 over Shore-MT
 *
 *  @author:    Andreas Schädeli
 *  @date:      2011-10-29
 */

#include "workload/tpch/shore_tpch_env.h"
#include "workload/tpch/tpch_util.h"
#include "qpipe.h"

using namespace shore;
using namespace qpipe;



ENTER_NAMESPACE(tpch);


/********************************************************************
 *
 * QPIPE Q5 - Structures needed by operators
 *
 ********************************************************************/

/*
select
    n_name,
    sum(l_extendedprice*(1-l_discount) as revenue
from
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and l_suppkey = s_suppkey
    and c_nationkey = s_nationkey
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = '[REGION]'
    and o_orderdate >= date'[DATE]'
    and o_orderdate < date'[DATE]+ interval '1' year
group by
    n_name
order by
    revenue desc;
 */

struct q5_projected_region_tuple {
	int R_REGIONKEY;
};

struct q5_projected_nation_tuple {
	int N_NATIONKEY;
	char N_NAME[STRSIZE(25)];
	int N_REGIONKEY;
};

struct q5_projected_customer_tuple {
	int C_CUSTKEY;
	int C_NATIONKEY;
};

struct q5_projected_orders_tuple {
	int O_ORDERKEY;
	int O_CUSTKEY;
};

struct q5_projected_lineitem_tuple {
	int L_ORDERKEY;
	int L_SUPPKEY;
	decimal L_DISCOUNT;
	decimal L_EXTENDEDPRICE;
};

struct q5_projected_supplier_tuple {
	int S_SUPPKEY;
	int S_NATIONKEY;
};


struct q5_r_join_n_tuple {
	int N_NATIONKEY;
	char N_NAME[STRSIZE(25)];
};

struct q5_c_join_r_n_tuple {
	int C_CUSTKEY;
	int N_NATIONKEY;
	char N_NAME[STRSIZE(25)];
};

struct q5_o_join_c_r_n_tuple {
	int O_ORDERKEY;
	int N_NATIONKEY;
	char N_NAME[STRSIZE(25)];
};

struct q5_l_join_o_c_r_n_tuple {
	int L_SUPPKEY;
	int N_NATIONKEY;
	char N_NAME[STRSIZE(25)];
	decimal L_DISCOUNT;
	decimal L_EXTENDEDPRICE;
};

struct q5_all_join_tuple {
	char N_NAME[STRSIZE(25)];
	decimal L_DISCOUNT;
	decimal L_EXTENDEDPRICE;
};

struct q5_final_tuple {
	char N_NAME[STRSIZE(25)];
	decimal REVENUE;
};


class q5_region_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prregion;
	rep_row_t _rr;

	/*One region tuple*/
	tpch_region_tuple _region;

	char _name[STRSIZE(25)];
	q5_input_t* q5_input;

public:
	q5_region_tscan_filter_t(ShoreTPCHEnv* tpchdb, q5_input_t &in)
	: tuple_filter_t(tpchdb->region_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prregion = _tpchdb->region_man()->get_tuple();
		_rr.set_ts(_tpchdb->region_man()->ts(),
				_tpchdb->nation_desc()->maxsize());
		_prregion->_rep = &_rr;

		q5_input = &in;
		region_to_str(q5_input->r_name, _name);

		TRACE(TRACE_ALWAYS, "Random predicates:\nREGION.R_NAME = '%s'\n", _name);
	}

	virtual ~q5_region_tscan_filter_t()
	{
		_tpchdb->region_man()->give_tuple(_prregion);
	}

	bool select(const tuple_t &input) {

		// Get next region tuple and read its size and type
		if (!_tpchdb->region_man()->load(_prregion, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		_prregion->get_value(1, _region.R_NAME, 25);

		return (strcmp(_name, _region.R_NAME) == 0);
	}


	void project(tuple_t &d, const tuple_t &s) {

		q5_projected_region_tuple *dest;
		dest = aligned_cast<q5_projected_region_tuple>(d.data);

		_prregion->get_value(0, _region.R_REGIONKEY);

		//TRACE( TRACE_RECORD_FLOW, "%d\n",
		//       _region.R_REGIONKEY);

		dest->R_REGIONKEY = _region.R_REGIONKEY;
	}

	q5_region_tscan_filter_t* clone() const {
		return new q5_region_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q5_region_tscan_filter_t()");
	}
};

class q5_nation_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prnation;
	rep_row_t _rr;

	/*One nation tuple*/
	tpch_nation_tuple _nation;

public:
	q5_nation_tscan_filter_t(ShoreTPCHEnv* tpchdb, q5_input_t &in)
	: tuple_filter_t(tpchdb->nation_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prnation = _tpchdb->nation_man()->get_tuple();
		_rr.set_ts(_tpchdb->nation_man()->ts(),
				_tpchdb->nation_desc()->maxsize());
		_prnation->_rep = &_rr;
	}

	virtual ~q5_nation_tscan_filter_t()
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

		q5_projected_nation_tuple *dest;
		dest = aligned_cast<q5_projected_nation_tuple>(d.data);

		_prnation->get_value(0, _nation.N_NATIONKEY);
		_prnation->get_value(1, _nation.N_NAME, sizeof(_nation.N_NAME));
		_prnation->get_value(2, _nation.N_REGIONKEY);

		//TRACE( TRACE_RECORD_FLOW, "%d|%s|%d\n",
		//       _nation.N_NATIONKEY, _nation.N_NAME, _nation.N_REGIONKEY);

		dest->N_NATIONKEY = _nation.N_NATIONKEY;
		memcpy(dest->N_NAME, _nation.N_NAME, sizeof(dest->N_NAME));
		dest->N_REGIONKEY = _nation.N_REGIONKEY;
	}

	q5_nation_tscan_filter_t* clone() const {
		return new q5_nation_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q5_nation_tscan_filter_t()");
	}
};

class q5_customer_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prcust;
	rep_row_t _rr;

	tpch_customer_tuple _customer;

public:
	q5_customer_tscan_filter_t(ShoreTPCHEnv* tpchdb, q5_input_t &in)
	: tuple_filter_t(tpchdb->customer_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prcust = _tpchdb->customer_man()->get_tuple();
		_rr.set_ts(_tpchdb->customer_man()->ts(),
				_tpchdb->customer_desc()->maxsize());
		_prcust->_rep = &_rr;
	}

	virtual ~q5_customer_tscan_filter_t()
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

		q5_projected_customer_tuple *dest = aligned_cast<q5_projected_customer_tuple>(d.data);

		_prcust->get_value(0, _customer.C_CUSTKEY);
		_prcust->get_value(3, _customer.C_NATIONKEY);

		//TRACE(TRACE_RECORD_FLOW, "%d|%d\n", _customer.C_CUSTKEY, _customer.C_NATIONKEY);

		dest->C_CUSTKEY = _customer.C_CUSTKEY;
		dest->C_NATIONKEY = _customer.C_NATIONKEY;
	}

	q5_customer_tscan_filter_t* clone() const {
		return new q5_customer_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q5_customer_tscan_filter_t");
	}
};

class q5_orders_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prorders;
	rep_row_t _rr;

	tpch_orders_tuple _orders;
	time_t _orderdate;

	q5_input_t* q5_input;
	time_t _last_orderdate;

public:
	q5_orders_tscan_filter_t(ShoreTPCHEnv* tpchdb, q5_input_t &in)
	: tuple_filter_t(tpchdb->orders_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prorders = _tpchdb->orders_man()->get_tuple();
		_rr.set_ts(_tpchdb->orders_man()->ts(),
				_tpchdb->orders_desc()->maxsize());
		_prorders->_rep = &_rr;

		// Generate the random predicates
		q5_input = &in;
		struct tm date;
		gmtime_r(&(q5_input->o_orderdate), &date);
		date.tm_year++;
		_last_orderdate = mktime(&date);

		char t1[15];
		char t2[15];
		timet_to_str(t1, q5_input->o_orderdate);
		timet_to_str(t2, _last_orderdate);

		TRACE(TRACE_ALWAYS, "Random predicates:\n %s <= O_ORDERDATE < %s\n", t1, t2);
	}

	virtual ~q5_orders_tscan_filter_t()
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

		return _orderdate >= q5_input->o_orderdate && _orderdate < _last_orderdate;
	}

	void project(tuple_t &d, const tuple_t &s) {

		q5_projected_orders_tuple *dest = aligned_cast<q5_projected_orders_tuple>(d.data);

		_prorders->get_value(0, _orders.O_ORDERKEY);
		_prorders->get_value(1, _orders.O_CUSTKEY);

		//TRACE(TRACE_RECORD_FLOW, "%d|%d\n", _orders.O_ORDERKEY, _orders.O_CUSTKEY);

		dest->O_ORDERKEY = _orders.O_ORDERKEY;
		dest->O_CUSTKEY = _orders.O_CUSTKEY;
	}

	q5_orders_tscan_filter_t* clone() const {
		return new q5_orders_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q5_orders_tscan_filter_t()");
	}
};

class q5_lineitem_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prline;
	rep_row_t _rr;

	tpch_lineitem_tuple _lineitem;

public:
	q5_lineitem_tscan_filter_t(ShoreTPCHEnv* tpchdb, q5_input_t &in)
	: tuple_filter_t(tpchdb->lineitem_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prline = _tpchdb->lineitem_man()->get_tuple();
		_rr.set_ts(_tpchdb->lineitem_man()->ts(),
				_tpchdb->lineitem_desc()->maxsize());
		_prline->_rep = &_rr;
	}

	virtual ~q5_lineitem_tscan_filter_t()
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

		q5_projected_lineitem_tuple *dest = aligned_cast<q5_projected_lineitem_tuple>(d.data);

		_prline->get_value(0, _lineitem.L_ORDERKEY);
		_prline->get_value(2, _lineitem.L_SUPPKEY);
		_prline->get_value(5, _lineitem.L_EXTENDEDPRICE);
		_prline->get_value(6, _lineitem.L_DISCOUNT);

		//TRACE(TRACE_RECORD_FLOW, "%d|%d|%.2f|%.2f\n", _lineitem.L_ORDERKEY, _lineitem.L_SUPPKEY, _lineitem.L_EXTENDEDPRICE / 100.0, _lineitem.L_DISCOUNT / 100.0);

		dest->L_ORDERKEY = _lineitem.L_ORDERKEY;
		dest->L_SUPPKEY = _lineitem.L_SUPPKEY;
		dest->L_EXTENDEDPRICE = _lineitem.L_EXTENDEDPRICE / 100.0;
		dest->L_DISCOUNT = _lineitem.L_DISCOUNT / 100.0;
	}

	q5_lineitem_tscan_filter_t* clone() const {
		return new q5_lineitem_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q5_lineitem_tscan_filter_t");
	}
};

class q5_supplier_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prsupplier;
	rep_row_t _rr;

	/*One supplier tuple*/
	tpch_supplier_tuple _supplier;

public:
	q5_supplier_tscan_filter_t(ShoreTPCHEnv* tpchdb, q5_input_t &in)
	: tuple_filter_t(tpchdb->supplier_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prsupplier = _tpchdb->supplier_man()->get_tuple();
		_rr.set_ts(_tpchdb->supplier_man()->ts(),
				_tpchdb->supplier_desc()->maxsize());
		_prsupplier->_rep = &_rr;
	}

	virtual ~q5_supplier_tscan_filter_t()
	{
		_tpchdb->supplier_man()->give_tuple(_prsupplier);
	}

	bool select(const tuple_t &input) {

		// Get next supplier tuple
		if (!_tpchdb->supplier_man()->load(_prsupplier, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		return true;
	}


	void project(tuple_t &d, const tuple_t &s) {

		q5_projected_supplier_tuple *dest;
		dest = aligned_cast<q5_projected_supplier_tuple>(d.data);


		_prsupplier->get_value(0, _supplier.S_SUPPKEY);
		_prsupplier->get_value(3, _supplier.S_NATIONKEY);

		//TRACE( TRACE_RECORD_FLOW, "%d|%d\n",
		//       _supplier.S_SUPPKEY, _supplier.S_NATIONKEY);

		dest->S_SUPPKEY = _supplier.S_SUPPKEY;
		dest->S_NATIONKEY = _supplier.S_NATIONKEY;

	}

	q5_supplier_tscan_filter_t* clone() const {
		return new q5_supplier_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q5_supplier_tscan_filter_t");
	}
};


struct q5_r_join_n_t : public tuple_join_t {

	q5_r_join_n_t()
	: tuple_join_t(sizeof(q5_projected_region_tuple),
			offsetof(q5_projected_region_tuple, R_REGIONKEY),
			sizeof(q5_projected_nation_tuple),
			offsetof(q5_projected_nation_tuple, N_REGIONKEY),
			sizeof(int),
			sizeof(q5_r_join_n_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q5_r_join_n_tuple *dest = aligned_cast<q5_r_join_n_tuple>(d.data);
		q5_projected_region_tuple *region = aligned_cast<q5_projected_region_tuple>(l.data);
		q5_projected_nation_tuple *nation = aligned_cast<q5_projected_nation_tuple>(r.data);

		dest->N_NATIONKEY = nation->N_NATIONKEY;
		memcpy(dest->N_NAME, nation->N_NAME, sizeof(dest->N_NAME));

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d = %d: %d %s\n", region->R_REGIONKEY, nation->N_NATIONKEY, nation->N_NATIONKEY, nation->N_NAME);
	}

	virtual q5_r_join_n_t* clone() const {
		return new q5_r_join_n_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join REGION, NATION; select N_NATIONKEY, N_NAME");
	}
};

struct q5_c_join_r_n_t : public tuple_join_t {

	q5_c_join_r_n_t()
	: tuple_join_t(sizeof(q5_projected_customer_tuple),
			offsetof(q5_projected_customer_tuple, C_NATIONKEY),
			sizeof(q5_r_join_n_tuple),
			offsetof(q5_r_join_n_tuple, N_NATIONKEY),
			sizeof(int),
			sizeof(q5_c_join_r_n_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q5_c_join_r_n_tuple *dest = aligned_cast<q5_c_join_r_n_tuple>(d.data);
		q5_projected_customer_tuple *cust = aligned_cast<q5_projected_customer_tuple>(l.data);
		q5_r_join_n_tuple *right = aligned_cast<q5_r_join_n_tuple>(r.data);

		dest->C_CUSTKEY = cust->C_CUSTKEY;
		dest->N_NATIONKEY = right->N_NATIONKEY;
		memcpy(dest->N_NAME, right->N_NAME, sizeof(dest->N_NAME));

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d = %d: %d %d %s\n", cust->C_NATIONKEY, right->N_NATIONKEY, cust->C_CUSTKEY, right->N_NATIONKEY, right->N_NAME);
	}

	virtual q5_c_join_r_n_t* clone() const {
		return new q5_c_join_r_n_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join CUSTOMER, REGION_NATION; select C_CUSTKEY, N_NATIONKEY, N_NAME");
	}
};

struct q5_o_join_c_r_n_t : public tuple_join_t {

	q5_o_join_c_r_n_t()
	: tuple_join_t(sizeof(q5_projected_orders_tuple),
			offsetof(q5_projected_orders_tuple, O_CUSTKEY),
			sizeof(q5_c_join_r_n_tuple),
			offsetof(q5_c_join_r_n_tuple, C_CUSTKEY),
			sizeof(int),
			sizeof(q5_o_join_c_r_n_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q5_o_join_c_r_n_tuple *dest = aligned_cast<q5_o_join_c_r_n_tuple>(d.data);
		q5_projected_orders_tuple *orders = aligned_cast<q5_projected_orders_tuple>(l.data);
		q5_c_join_r_n_tuple *right = aligned_cast<q5_c_join_r_n_tuple>(r.data);

		dest->O_ORDERKEY = orders->O_ORDERKEY;
		dest->N_NATIONKEY = right->N_NATIONKEY;
		memcpy(dest->N_NAME, right->N_NAME, sizeof(dest->N_NAME));

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d = %d: %d %d %s\n", orders->O_CUSTKEY, right->C_CUSTKEY, orders->O_ORDERKEY, right->N_NATIONKEY, right->N_NAME);
	}

	virtual q5_o_join_c_r_n_t* clone() const {
		return new q5_o_join_c_r_n_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join ORDERS, CUSTOMER_REGION_NATION; select O_ORDERKEY, N_NATIONKEY, N_NAME");
	}
};


struct q5_l_join_o_c_r_n_t : public tuple_join_t {

	q5_l_join_o_c_r_n_t()
	: tuple_join_t(sizeof(q5_projected_lineitem_tuple),
			offsetof(q5_projected_lineitem_tuple, L_ORDERKEY),
			sizeof(q5_o_join_c_r_n_tuple),
			offsetof(q5_o_join_c_r_n_tuple, O_ORDERKEY),
			sizeof(int),
			sizeof(q5_l_join_o_c_r_n_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q5_l_join_o_c_r_n_tuple *dest = aligned_cast<q5_l_join_o_c_r_n_tuple>(d.data);
		q5_projected_lineitem_tuple *line = aligned_cast<q5_projected_lineitem_tuple>(l.data);
		q5_o_join_c_r_n_tuple *right = aligned_cast<q5_o_join_c_r_n_tuple>(r.data);

		dest->L_SUPPKEY = line->L_SUPPKEY;
		dest->N_NATIONKEY = right->N_NATIONKEY;
		memcpy(dest->N_NAME, right->N_NAME, sizeof(dest->N_NAME));
		dest->L_EXTENDEDPRICE = line->L_EXTENDEDPRICE;
		dest->L_DISCOUNT = line ->L_DISCOUNT;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d = %d: %d %d %s %.2f %.2f\n", line->L_ORDERKEY, right->O_ORDERKEY, line->L_SUPPKEY, right->N_NATIONKEY, right->N_NAME,
		//																line->L_EXTENDEDPRICE.to_double(), line->L_DISCOUNT.to_double());
	}

	virtual q5_l_join_o_c_r_n_t* clone() const {
		return new q5_l_join_o_c_r_n_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join LINEITEM, ORDERS_CUSTOMER_REGION_NATION; select L_SUPPKEY, N_NATIONKEY, N_NAME, L_EXTENDEDPRICE, L_DISCOUNT");
	}
};


struct q5_final_join : public tuple_join_t {

	q5_final_join()
	: tuple_join_t(sizeof(q5_l_join_o_c_r_n_tuple),
			offsetof(q5_l_join_o_c_r_n_tuple, L_SUPPKEY),
			sizeof(q5_projected_supplier_tuple),
			offsetof(q5_projected_supplier_tuple, S_SUPPKEY),
			2*sizeof(int),
			sizeof(q5_all_join_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q5_all_join_tuple *dest = aligned_cast<q5_all_join_tuple>(d.data);
		q5_l_join_o_c_r_n_tuple *left = aligned_cast<q5_l_join_o_c_r_n_tuple>(l.data);
		q5_projected_supplier_tuple *supp = aligned_cast<q5_projected_supplier_tuple>(r.data);

		memcpy(dest->N_NAME, left->N_NAME, sizeof(dest->N_NAME));
		dest->L_EXTENDEDPRICE = left->L_EXTENDEDPRICE;
		dest->L_DISCOUNT = left->L_DISCOUNT;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d = %d AND %d = %d: %s %.2f %.2f\n", left->L_SUPPKEY, supp->S_SUPPKEY, left->N_NATIONKEY, supp->S_NATIONKEY, left->N_NAME,
		//																		left->L_EXTENDEDPRICE.to_double(), left->L_DISCOUNT.to_double());
	}

	virtual q5_final_join* clone() const {
		return new q5_final_join(*this);
	}

	virtual c_str to_string() const {
		return c_str("join LINEITEM_ORDERS_CUSTOMER_REGION_NATION, SUPPLIER; select N_NAME, L_EXTENDEDPRICE, L_DISCOUNT");
	}
};


struct q5_aggregate_t : public tuple_aggregate_t {

	default_key_extractor_t _extractor;

	q5_aggregate_t()
	: tuple_aggregate_t(sizeof(q5_final_tuple)), _extractor(default_key_extractor_t(STRSIZE(25) * sizeof(char), offsetof(q5_all_join_tuple,N_NAME)))
	{
	}

	virtual key_extractor_t* key_extractor() {
		return &_extractor;
	}

	virtual void aggregate(char* agg_data, const tuple_t &t) {
		q5_final_tuple *agg = aligned_cast<q5_final_tuple>(agg_data);
		q5_all_join_tuple *input = aligned_cast<q5_all_join_tuple>(t.data);

		memcpy(agg->N_NAME, input->N_NAME, sizeof(agg->N_NAME));
		agg->REVENUE += input->L_EXTENDEDPRICE * (1 - input->L_DISCOUNT);
	}
	virtual void finish(tuple_t &dest, const char* agg_data) {
		memcpy(dest.data, agg_data, dest.size);
	}
	virtual q5_aggregate_t* clone() const {
		return new q5_aggregate_t(*this);
	}
	virtual c_str to_string() const {
		return "q5_aggregate_t";
	}
};

struct q5_agg_key_compare_t : public key_compare_t {

	q5_agg_key_compare_t()
	{
	}

	virtual int operator()(const void* key1, const void* key2) const {
		return strcmp((char*)key1, (char*)key2);
	}

	virtual q5_agg_key_compare_t* clone() const {
		return new q5_agg_key_compare_t(*this);
	}
};

struct q5_sort_key_extractor_t : public key_extractor_t {

	q5_sort_key_extractor_t()
	: key_extractor_t(sizeof(decimal), offsetof(q5_final_tuple, REVENUE))
	{
	}

	virtual int extract_hint(const char *key) const {
		decimal revenue = *(aligned_cast<decimal>(key));
		return -(revenue.to_int());
	}

	virtual key_extractor_t* clone() const {
		return new q5_sort_key_extractor_t(*this);
	}
};

struct q5_sort_key_compare_t : public key_compare_t {

	q5_sort_key_compare_t()
	{
	}

	virtual int operator()(const void* key1, const void* key2) const {
		TRACE(TRACE_RECORD_FLOW, "SORT_COMP\n");
		decimal rev1 = *(aligned_cast<decimal>(key1));
		decimal rev2 = *(aligned_cast<decimal>(key2));
		decimal diff = rev2 - rev1;
		return (diff < 0 ? -1 : (diff == 0 ? 0 : 1));
	}

	virtual q5_sort_key_compare_t* clone() const {
		return new q5_sort_key_compare_t(*this);
	}
};


class tpch_q5_process_tuple_t : public process_tuple_t {

public:

	virtual void begin() {
		TRACE(TRACE_QUERY_RESULTS, "*** Q5 %25s %25s\n",
				"N_NAME", "REVENUE");
	}

	virtual void process(const tuple_t& output) {
		q5_final_tuple *res = aligned_cast<q5_final_tuple>(output.data);

		TRACE(TRACE_QUERY_RESULTS, "*** Q5 %25s %25.2f\n",
				res->N_NAME, res->REVENUE.to_double());
	}

};


/********************************************************************
 *
 * QPIPE Q5 - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_qpipe_q5(const int xct_id,
		q5_input_t& in)
{
	TRACE( TRACE_ALWAYS, "********** Q5 *********\n");

	policy_t* dp = this->get_sched_policy();
	xct_t* pxct = smthread_t::me()->xct();


	//TSCAN NATION
	tuple_fifo* q5_nation_buffer = new tuple_fifo(sizeof(q5_projected_nation_tuple));
	packet_t* q5_nation_tscan_packet =
			new tscan_packet_t("nation TSCAN",
					q5_nation_buffer,
					new q5_nation_tscan_filter_t(this, in),
					this->db(),
					_pnation_desc.get(),
					pxct);

	//TSCAN REGION
	tuple_fifo* q5_region_buffer = new tuple_fifo(sizeof(q5_projected_region_tuple));
	packet_t* q5_region_tscan_packet =
			new tscan_packet_t("region TSCAN",
					q5_region_buffer,
					new q5_region_tscan_filter_t(this, in),
					this->db(),
					_pregion_desc.get(),
					pxct);

	//REGION JOIN NATION
	tuple_fifo* q5_r_join_n_buffer = new tuple_fifo(sizeof(q5_r_join_n_tuple));
	packet_t* q5_r_join_n_packet =
			new hash_join_packet_t("region - nation HJOIN",
					q5_r_join_n_buffer,
					new trivial_filter_t(sizeof(q5_r_join_n_tuple)),
					q5_region_tscan_packet,
					q5_nation_tscan_packet,
					new q5_r_join_n_t());

	//TSCAN CUSTOMER
	tuple_fifo* q5_customer_buffer = new tuple_fifo(sizeof(q5_projected_customer_tuple));
	packet_t* q5_customer_tscan_packet =
			new tscan_packet_t("customer TSCAN",
					q5_customer_buffer,
					new q5_customer_tscan_filter_t(this, in),
					this->db(),
					_pcustomer_desc.get(),
					pxct);

	//CUSTOMER JOIN R_N
	tuple_fifo* q5_c_join_r_n_buffer = new tuple_fifo(sizeof(q5_c_join_r_n_tuple));
	packet_t* q5_c_join_r_n_packet =
			new hash_join_packet_t("customer - region_nation HJOIN",
					q5_c_join_r_n_buffer,
					new trivial_filter_t(sizeof(q5_c_join_r_n_tuple)),
					q5_customer_tscan_packet,
					q5_r_join_n_packet,
					new q5_c_join_r_n_t());

	//TSCAN ORDERS
	tuple_fifo* q5_orders_buffer = new tuple_fifo(sizeof(q5_projected_orders_tuple));
	packet_t* q5_orders_tscan_packet =
			new tscan_packet_t("orders TSCAN",
					q5_orders_buffer,
					new q5_orders_tscan_filter_t(this, in),
					this->db(),
					_porders_desc.get(),
					pxct);

	//ORDERS JOIN C_R_N
	tuple_fifo* q5_o_join_c_r_n_buffer = new tuple_fifo(sizeof(q5_o_join_c_r_n_tuple));
	packet_t* q5_o_join_c_r_n_packet =
			new hash_join_packet_t("orders - customer_region_nation HJOIN",
					q5_o_join_c_r_n_buffer,
					new trivial_filter_t(sizeof(q5_o_join_c_r_n_tuple)),
					q5_orders_tscan_packet,
					q5_c_join_r_n_packet,
					new q5_o_join_c_r_n_t());

	//TSCAN LINEITEM
	tuple_fifo* q5_lineitem_buffer = new tuple_fifo(sizeof(q5_projected_lineitem_tuple));
	packet_t* q5_lineitem_tscan_packet =
			new tscan_packet_t("lineitem TSCAN",
					q5_lineitem_buffer,
					new q5_lineitem_tscan_filter_t(this, in),
					this->db(),
					_plineitem_desc.get(),
					pxct);

	//LINEITEM JOIN O_C_R_N
	tuple_fifo* q5_l_join_o_c_r_n_buffer = new tuple_fifo(sizeof(q5_l_join_o_c_r_n_tuple));
	packet_t* q5_l_join_o_c_r_n_packet =
			new hash_join_packet_t("lineitem - orders_customer_region_nation HJOIN",
					q5_l_join_o_c_r_n_buffer,
					new trivial_filter_t(sizeof(q5_l_join_o_c_r_n_tuple)),
					q5_lineitem_tscan_packet,
					q5_o_join_c_r_n_packet,
					new q5_l_join_o_c_r_n_t());

	//TSCAN SUPPLIER
	tuple_fifo* q5_supplier_buffer = new tuple_fifo(sizeof(q5_projected_supplier_tuple));
	packet_t* q5_supplier_tscan_packet =
			new tscan_packet_t("supplier TSCAN",
					q5_supplier_buffer,
					new q5_supplier_tscan_filter_t(this, in),
					this->db(),
					_psupplier_desc.get(),
					pxct);

	//L_O_C_R_N JOIN SUPPLIER
	tuple_fifo* q5_all_join_buffer = new tuple_fifo(sizeof(q5_all_join_tuple));
	packet_t* q5_all_join_packet =
			new hash_join_packet_t("lineitem_orders_customer_region_nation - supplier HJOIN",
					q5_all_join_buffer,
					new trivial_filter_t(sizeof(q5_all_join_tuple)),
					q5_l_join_o_c_r_n_packet,
					q5_supplier_tscan_packet,
					new q5_final_join());

	//AGGREGATION
	tuple_fifo* q5_aggregated_buffer = new tuple_fifo(sizeof(q5_final_tuple));
	packet_t* q5_aggregate_packet =
			new partial_aggregate_packet_t("SUM AGG",
					q5_aggregated_buffer,
					new trivial_filter_t(sizeof(q5_final_tuple)),
					q5_all_join_packet,
					new q5_aggregate_t(),
					new default_key_extractor_t(sizeof(char) * STRSIZE(25)),
					new q5_agg_key_compare_t());

	//ORDER BY
	tuple_fifo* q5_final_buffer = new tuple_fifo(sizeof(q5_final_tuple));
	packet_t* q5_sort_packet =
			new sort_packet_t("SORT",
					q5_final_buffer,
					new trivial_filter_t(sizeof(q5_final_tuple)),
					new q5_sort_key_extractor_t(),
					new q5_sort_key_compare_t(),
					q5_aggregate_packet);




	qpipe::query_state_t* qs = dp->query_state_create();
	q5_nation_tscan_packet->assign_query_state(qs);
	q5_region_tscan_packet->assign_query_state(qs);
	q5_r_join_n_packet->assign_query_state(qs);
	q5_customer_tscan_packet->assign_query_state(qs);
	q5_c_join_r_n_packet->assign_query_state(qs);
	q5_orders_tscan_packet->assign_query_state(qs);
	q5_o_join_c_r_n_packet->assign_query_state(qs);
	q5_lineitem_tscan_packet->assign_query_state(qs);
	q5_l_join_o_c_r_n_packet->assign_query_state(qs);
	q5_supplier_tscan_packet->assign_query_state(qs);
	q5_all_join_packet->assign_query_state(qs);
	q5_aggregate_packet->assign_query_state(qs);
	q5_sort_packet->assign_query_state(qs);

	// Dispatch packet
	tpch_q5_process_tuple_t pt;
	//LAST PACKET
	process_query(q5_sort_packet, pt);//TODO

	dp->query_state_destroy(qs);


	return (RCOK);
}


EXIT_NAMESPACE(tpch);
