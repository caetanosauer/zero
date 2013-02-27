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

/** @file:   qpipe_q8.cpp
 *
 *  @brief:  Implementation of QPIPE TPCH Q8 over Shore-MT
 *
 *  @author:    Andreas Schädeli
 *  @date:      2011-11-01
 */


#include "workload/tpch/shore_tpch_env.h"
#include "workload/tpch/tpch_util.h"
#include "qpipe.h"

using namespace shore;
using namespace qpipe;



ENTER_NAMESPACE(tpch);


/********************************************************************
 *
 * QPIPE Q8 - Structures needed by operators
 *
 ********************************************************************/

/*
select
	o_year,
	sum(case
		when nation='[NATION]'
		then volume
		else 0
		end) / sum(volume) as mkt_share
from (
	select
		extract(year from o_orderdate) as o_year,
		l_extendedprice * (1-l_discount) as volume,
		n2.n_name as nation
	from
		part,
		supplier,
		lineitem,
		orders,
		customer,
		nation n1,
		nation n2,
		region
	where
		p_partkey=l_partkey
		and s_suppkey=l_suppkey
		and l_orderkey=o_orderkey
		and o_custkey=c_custkey
		and c_nationkey=n1.n_nationkey
		and n1.n_regionkey=r_regionkey
		and r_name='[REGION]'
		and s_nationkey=n2.n_nationkey
		and o_orderdate between date'1995-01-01' and date'1996-12-31'
		and p_type='[TYPE]'
	) as all_nations
group by
	o_year
order by
	o_year;
 */


struct q8_projected_lineitem_tuple {
	int L_PARTKEY;
	int L_ORDERKEY;
	int L_SUPPKEY;
	decimal VOLUME;
};

struct q8_projected_part_tuple {
	int P_PARTKEY;
};

struct q8_projected_orders_tuple {
	int O_ORDERKEY;
	int O_CUSTKEY;
	int O_YEAR;
};

struct q8_projected_customer_tuple {
	int C_CUSTKEY;
	int C_NATIONKEY;
};

struct q8_projected_nation_n1_tuple {
	int N_NATIONKEY;
	int N_REGIONKEY;
};

struct q8_projected_region_tuple {
	int R_REGIONKEY;
};

struct q8_projected_supplier_tuple {
	int S_SUPPKEY;
	int S_NATIONKEY;
};

struct q8_projected_nation_n2_tuple {
	int N_NATIONKEY;
	char N_NAME[STRSIZE(25)];
};

struct q8_l_join_p_tuple {
	int L_ORDERKEY;
	int L_SUPPKEY;
	decimal VOLUME;
};

struct q8_o_join_l_p_tuple {
	int O_CUSTKEY;
	int O_YEAR;
	int L_SUPPKEY;
	decimal VOLUME;
};

struct q8_c_join_o_l_p_tuple {
	int C_NATIONKEY;
	int O_YEAR;
	int L_SUPPKEY;
	decimal VOLUME;
};

struct q8_c_o_l_p_join_n1_tuple {
	int N_REGIONKEY;
	int O_YEAR;
	int L_SUPPKEY;
	decimal VOLUME;
};

struct q8_c_o_l_p_n1_join_r_tuple {
	int O_YEAR;
	int L_SUPPKEY;
	decimal VOLUME;
};

struct q8_s_join_c_o_l_p_n1_r_tuple {
	int S_NATIONKEY;
	int O_YEAR;
	decimal VOLUME;
};

struct q8_all_joins_tuple {
	int O_YEAR;
	decimal VOLUME;
	char NATION[STRSIZE(25)];
};

struct q8_aggregate_tuple {
	int O_YEAR;
	decimal SUM_VOLUME;
	decimal SUM_VOLUME_NATION;
	double MKT_SHARE;
};


class q8_lineitem_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prline;
	rep_row_t _rr;

	tpch_lineitem_tuple _lineitem;

public:
	q8_lineitem_tscan_filter_t(ShoreTPCHEnv* tpchdb, q8_input_t &in)
	: tuple_filter_t(tpchdb->lineitem_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prline = _tpchdb->lineitem_man()->get_tuple();
		_rr.set_ts(_tpchdb->lineitem_man()->ts(),
				_tpchdb->lineitem_desc()->maxsize());
		_prline->_rep = &_rr;
	}

	virtual ~q8_lineitem_tscan_filter_t()
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

		q8_projected_lineitem_tuple *dest = aligned_cast<q8_projected_lineitem_tuple>(d.data);

		_prline->get_value(0, _lineitem.L_ORDERKEY);
		_prline->get_value(1, _lineitem.L_PARTKEY);
		_prline->get_value(2, _lineitem.L_SUPPKEY);
		_prline->get_value(5, _lineitem.L_EXTENDEDPRICE);
		_prline->get_value(6, _lineitem.L_DISCOUNT);

		//TRACE(TRACE_RECORD_FLOW, "%d|%d|%d|%.2f|%.2f\n", _lineitem.L_ORDERKEY, _lineitem.L_PARTKEY, _lineitem.L_SUPPKEY, _lineitem.L_EXTENDEDPRICE / 100.0, _lineitem.L_DISCOUNT / 100.0);

		dest->L_ORDERKEY = _lineitem.L_ORDERKEY;
		dest->L_PARTKEY = _lineitem.L_PARTKEY;
		dest->L_SUPPKEY = _lineitem.L_SUPPKEY;
		dest->VOLUME = _lineitem.L_EXTENDEDPRICE / 100.0 * (1 - _lineitem.L_DISCOUNT / 100.0);
#warning MA: Discount from TPCH dbgen is created between 0 and 100 instead between 0 and 1.
	}

	q8_lineitem_tscan_filter_t* clone() const {
		return new q8_lineitem_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q8_lineitem_tscan_filter_t()");
	}
};

class q8_part_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prpart;
	rep_row_t _rr;

	/*One part tuple*/
	tpch_part_tuple _part;
	/*The columns needed for the selection*/
	char _type[STRSIZE(25)];

	q8_input_t* q8_input;

public:

	q8_part_tscan_filter_t(ShoreTPCHEnv* tpchdb, q8_input_t &in)
	: tuple_filter_t(tpchdb->part_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prpart = _tpchdb->part_man()->get_tuple();
		_rr.set_ts(_tpchdb->part_man()->ts(),
				_tpchdb->part_desc()->maxsize());
		_prpart->_rep = &_rr;

		q8_input = &in;
		type_to_str(q8_input->p_type, _type);

		TRACE(TRACE_ALWAYS, "Random predicates:\nPART.P_TYPE = '%s'\n", _type);
	}

	virtual ~q8_part_tscan_filter_t()
	{
		// Give back the part tuple
		_tpchdb->part_man()->give_tuple(_prpart);
	}



	bool select(const tuple_t &input) {

		// Get next part and read its size and type
		if (!_tpchdb->part_man()->load(_prpart, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		_prpart->get_value(4, _part.P_TYPE, 25);

		return strcmp(_part.P_TYPE, _type) == 0;
	}


	void project(tuple_t &d, const tuple_t &s) {

		q8_projected_part_tuple *dest;
		dest = aligned_cast<q8_projected_part_tuple>(d.data);

		_prpart->get_value(0, _part.P_PARTKEY);

		//TRACE( TRACE_RECORD_FLOW, "%d\n",
		//		_part.P_PARTKEY);

		dest->P_PARTKEY = _part.P_PARTKEY;
	}

	q8_part_tscan_filter_t* clone() const {
		return new q8_part_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q8_part_tscan_filter_t(%s)", _type);
	}
};

class q8_orders_tscan_filter_t : public tuple_filter_t
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
	q8_orders_tscan_filter_t(ShoreTPCHEnv* tpchdb, q8_input_t &in)
	: tuple_filter_t(tpchdb->orders_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prorders = _tpchdb->orders_man()->get_tuple();
		_rr.set_ts(_tpchdb->orders_man()->ts(),
				_tpchdb->orders_desc()->maxsize());
		_prorders->_rep = &_rr;

		_first_orderdate = str_to_timet("1995-01-01");
		_last_orderdate = str_to_timet("1996-12-31");
	}

	virtual ~q8_orders_tscan_filter_t()
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

		return _orderdate >= _first_orderdate && _orderdate <= _last_orderdate;
	}

	void project(tuple_t &d, const tuple_t &s) {

		q8_projected_orders_tuple *dest = aligned_cast<q8_projected_orders_tuple>(d.data);

		_prorders->get_value(0, _orders.O_ORDERKEY);
		_prorders->get_value(1, _orders.O_CUSTKEY);
		_prorders->get_value(4, _orders.O_ORDERDATE, sizeof(_orders.O_ORDERDATE));
		_orderdate = str_to_timet(_orders.O_ORDERDATE);
		struct tm *tm_orderdate = gmtime(&_orderdate);

		//TRACE(TRACE_RECORD_FLOW, "%d|%d|%d\n", _orders.O_ORDERKEY, _orders.O_CUSTKEY, tm_orderdate->tm_year + 1900);

		dest->O_ORDERKEY = _orders.O_ORDERKEY;
		dest->O_CUSTKEY = _orders.O_CUSTKEY;
		dest->O_YEAR = tm_orderdate->tm_year + 1900;
	}

	q8_orders_tscan_filter_t* clone() const {
		return new q8_orders_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q8_orders_tscan_filter_t(between (%s, %s))", ctime(&(_first_orderdate)), ctime(&(_last_orderdate)));
	}
};

class q8_customer_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prcust;
	rep_row_t _rr;

	tpch_customer_tuple _customer;

public:
	q8_customer_tscan_filter_t(ShoreTPCHEnv* tpchdb, q8_input_t &in)
	: tuple_filter_t(tpchdb->customer_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prcust = _tpchdb->customer_man()->get_tuple();
		_rr.set_ts(_tpchdb->customer_man()->ts(),
				_tpchdb->customer_desc()->maxsize());
		_prcust->_rep = &_rr;
	}

	virtual ~q8_customer_tscan_filter_t()
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

		q8_projected_customer_tuple *dest = aligned_cast<q8_projected_customer_tuple>(d.data);

		_prcust->get_value(0, _customer.C_CUSTKEY);
		_prcust->get_value(3, _customer.C_NATIONKEY);

		//TRACE(TRACE_RECORD_FLOW, "%d|%d\n", _customer.C_CUSTKEY, _customer.C_NATIONKEY);

		dest->C_CUSTKEY = _customer.C_CUSTKEY;
		dest->C_NATIONKEY = _customer.C_NATIONKEY;
	}

	q8_customer_tscan_filter_t* clone() const {
		return new q8_customer_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q8_customer_tscan_filter_t");
	}
};

class q8_nation_n1_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prnation;
	rep_row_t _rr;

	/*One nation tuple*/
	tpch_nation_tuple _nation;

public:
	q8_nation_n1_tscan_filter_t(ShoreTPCHEnv* tpchdb, q8_input_t &in)
	: tuple_filter_t(tpchdb->nation_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prnation = _tpchdb->nation_man()->get_tuple();
		_rr.set_ts(_tpchdb->nation_man()->ts(),
				_tpchdb->nation_desc()->maxsize());
		_prnation->_rep = &_rr;
	}

	virtual ~q8_nation_n1_tscan_filter_t()
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

		q8_projected_nation_n1_tuple *dest;
		dest = aligned_cast<q8_projected_nation_n1_tuple>(d.data);

		_prnation->get_value(0, _nation.N_NATIONKEY);
		_prnation->get_value(2, _nation.N_REGIONKEY);

		//TRACE( TRACE_RECORD_FLOW, "%d|%d\n",
		//		_nation.N_NATIONKEY, _nation.N_REGIONKEY);

		dest->N_NATIONKEY = _nation.N_NATIONKEY;
		dest->N_REGIONKEY = _nation.N_REGIONKEY;
	}

	q8_nation_n1_tscan_filter_t* clone() const {
		return new q8_nation_n1_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q8_nation_n1_tscan_filter_t()");
	}
};

class q8_nation_n2_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prnation;
	rep_row_t _rr;

	/*One nation tuple*/
	tpch_nation_tuple _nation;

public:
	q8_nation_n2_tscan_filter_t(ShoreTPCHEnv* tpchdb, q8_input_t &in)
	: tuple_filter_t(tpchdb->nation_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prnation = _tpchdb->nation_man()->get_tuple();
		_rr.set_ts(_tpchdb->nation_man()->ts(),
				_tpchdb->nation_desc()->maxsize());
		_prnation->_rep = &_rr;
	}

	virtual ~q8_nation_n2_tscan_filter_t()
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

		q8_projected_nation_n2_tuple *dest;
		dest = aligned_cast<q8_projected_nation_n2_tuple>(d.data);

		_prnation->get_value(0, _nation.N_NATIONKEY);
		_prnation->get_value(1, _nation.N_NAME, 25);

		//TRACE( TRACE_RECORD_FLOW, "%d|%s\n",
		//		_nation.N_NATIONKEY, _nation.N_NAME);

		dest->N_NATIONKEY = _nation.N_NATIONKEY;
		memcpy(dest->N_NAME, _nation.N_NAME, sizeof(dest->N_NAME));
	}

	q8_nation_n2_tscan_filter_t* clone() const {
		return new q8_nation_n2_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q8_nation_n2_tscan_filter_t()");
	}
};

class q8_region_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prregion;
	rep_row_t _rr;

	/*One region tuple*/
	tpch_region_tuple _region;

	char _name[STRSIZE(25)];
	q8_input_t* q8_input;

public:
	q8_region_tscan_filter_t(ShoreTPCHEnv* tpchdb, q8_input_t &in)
	: tuple_filter_t(tpchdb->region_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prregion = _tpchdb->region_man()->get_tuple();
		_rr.set_ts(_tpchdb->region_man()->ts(),
				_tpchdb->nation_desc()->maxsize());
		_prregion->_rep = &_rr;

		q8_input = &in;
		region_to_str(q8_input->r_name, _name);

		TRACE(TRACE_ALWAYS, "Random predicates:\nREGION.R_NAME = '%s'\n", _name);
	}

	virtual ~q8_region_tscan_filter_t()
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

		q8_projected_region_tuple *dest;
		dest = aligned_cast<q8_projected_region_tuple>(d.data);

		_prregion->get_value(0, _region.R_REGIONKEY);

		//TRACE( TRACE_RECORD_FLOW, "%d\n",
		//		_region.R_REGIONKEY);

		dest->R_REGIONKEY = _region.R_REGIONKEY;
	}

	q8_region_tscan_filter_t* clone() const {
		return new q8_region_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q8_region_tscan_filter_t(%s)", _name);
	}
};

class q8_supplier_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prsupplier;
	rep_row_t _rr;

	/*One supplier tuple*/
	tpch_supplier_tuple _supplier;

public:
	q8_supplier_tscan_filter_t(ShoreTPCHEnv* tpchdb, q8_input_t &in)
	: tuple_filter_t(tpchdb->supplier_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prsupplier = _tpchdb->supplier_man()->get_tuple();
		_rr.set_ts(_tpchdb->supplier_man()->ts(),
				_tpchdb->supplier_desc()->maxsize());
		_prsupplier->_rep = &_rr;
	}

	virtual ~q8_supplier_tscan_filter_t()
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

		q8_projected_supplier_tuple *dest = aligned_cast<q8_projected_supplier_tuple>(d.data);


		_prsupplier->get_value(0, _supplier.S_SUPPKEY);
		_prsupplier->get_value(3, _supplier.S_NATIONKEY);

		//TRACE( TRACE_RECORD_FLOW, "%d|%d\n",
		//		_supplier.S_SUPPKEY, _supplier.S_NATIONKEY);

		dest->S_SUPPKEY = _supplier.S_SUPPKEY;
		dest->S_NATIONKEY = _supplier.S_NATIONKEY;

	}

	q8_supplier_tscan_filter_t* clone() const {
		return new q8_supplier_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q8_supplier_tscan_filter_t");
	}
};


struct q8_l_join_p_t : public tuple_join_t {

	q8_l_join_p_t()
	: tuple_join_t(sizeof(q8_projected_lineitem_tuple),
			offsetof(q8_projected_lineitem_tuple, L_PARTKEY),
			sizeof(q8_projected_part_tuple),
			offsetof(q8_projected_part_tuple, P_PARTKEY),
			sizeof(int),
			sizeof(q8_l_join_p_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q8_l_join_p_tuple *dest = aligned_cast<q8_l_join_p_tuple>(d.data);
		q8_projected_lineitem_tuple *line = aligned_cast<q8_projected_lineitem_tuple>(l.data);
		q8_projected_part_tuple *part = aligned_cast<q8_projected_part_tuple>(r.data);

		dest->L_ORDERKEY = line->L_ORDERKEY;
		dest->L_SUPPKEY = line->L_SUPPKEY;
		dest->VOLUME = line->VOLUME;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %d %.2f\n", line->L_PARTKEY, part->P_PARTKEY, line->L_ORDERKEY, line->L_SUPPKEY, line->VOLUME.to_double());
	}

	virtual q8_l_join_p_t* clone() const {
		return new q8_l_join_p_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join LINEITEM, PART; select L_ORDERKEY, L_SUPPKEY, VOLUME");
	}
};

struct q8_o_join_l_p_t : public tuple_join_t {

	q8_o_join_l_p_t()
	: tuple_join_t(sizeof(q8_projected_orders_tuple),
			offsetof(q8_projected_orders_tuple, O_ORDERKEY),
			sizeof(q8_l_join_p_tuple),
			offsetof(q8_l_join_p_tuple, L_ORDERKEY),
			sizeof(int),
			sizeof(q8_o_join_l_p_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q8_o_join_l_p_tuple *dest = aligned_cast<q8_o_join_l_p_tuple>(d.data);
		q8_projected_orders_tuple *order = aligned_cast<q8_projected_orders_tuple>(l.data);
		q8_l_join_p_tuple *right = aligned_cast<q8_l_join_p_tuple>(r.data);

		dest->L_SUPPKEY = right->L_SUPPKEY;
		dest->VOLUME = right->VOLUME;
		dest->O_CUSTKEY = order->O_CUSTKEY;
		dest->O_YEAR = order->O_YEAR;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %.2f %d %d\n", order->O_ORDERKEY, right->L_ORDERKEY, right->L_SUPPKEY, right->VOLUME.to_double(), order->O_CUSTKEY, order->O_YEAR);
	}

	virtual q8_o_join_l_p_t* clone() const {
		return new q8_o_join_l_p_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join ORDERS, LINEITEM_PART; select L_SUPPKEY, VOLUME, O_CUSTKEY, O_YEAR");
	}
};

struct q8_c_join_o_l_p_t : public tuple_join_t {

	q8_c_join_o_l_p_t()
	: tuple_join_t(sizeof(q8_projected_customer_tuple),
			offsetof(q8_projected_customer_tuple, C_CUSTKEY),
			sizeof(q8_o_join_l_p_tuple),
			offsetof(q8_o_join_l_p_tuple, O_CUSTKEY),
			sizeof(int),
			sizeof(q8_c_join_o_l_p_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q8_c_join_o_l_p_tuple *dest = aligned_cast<q8_c_join_o_l_p_tuple>(d.data);
		q8_projected_customer_tuple *cust = aligned_cast<q8_projected_customer_tuple>(l.data);
		q8_o_join_l_p_tuple *right = aligned_cast<q8_o_join_l_p_tuple>(r.data);

		dest->L_SUPPKEY = right->L_SUPPKEY;
		dest->VOLUME = right->VOLUME;
		dest->O_YEAR = right->O_YEAR;
		dest->C_NATIONKEY = cust->C_NATIONKEY;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %.2f %d %d\n", cust->C_CUSTKEY, right->O_CUSTKEY, right->L_SUPPKEY, right->VOLUME.to_double(), right->O_YEAR, cust->C_NATIONKEY);
	}

	virtual q8_c_join_o_l_p_t* clone() const {
		return new q8_c_join_o_l_p_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join CUSTOMER, ORDERS_LINEITEM_PART; select L_SUPPKEY, VOLUME, O_YEAR, C_NATIONKEY");
	}
};

struct q8_c_o_l_p_join_n1_t : public tuple_join_t {

	q8_c_o_l_p_join_n1_t()
	: tuple_join_t(sizeof(q8_c_join_o_l_p_tuple),
			offsetof(q8_c_join_o_l_p_tuple, C_NATIONKEY),
			sizeof(q8_projected_nation_n1_tuple),
			offsetof(q8_projected_nation_n1_tuple, N_NATIONKEY),
			sizeof(int),
			sizeof(q8_c_o_l_p_join_n1_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q8_c_o_l_p_join_n1_tuple *dest = aligned_cast<q8_c_o_l_p_join_n1_tuple>(d.data);
		q8_c_join_o_l_p_tuple *left = aligned_cast<q8_c_join_o_l_p_tuple>(l.data);
		q8_projected_nation_n1_tuple *nation = aligned_cast<q8_projected_nation_n1_tuple>(r.data);

		dest->L_SUPPKEY = left->L_SUPPKEY;
		dest->VOLUME = left->VOLUME;
		dest->O_YEAR = left->O_YEAR;
		dest->N_REGIONKEY = nation->N_REGIONKEY;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %.2f %d %d\n", left->C_NATIONKEY, nation->N_NATIONKEY, left->L_SUPPKEY, left->VOLUME.to_double(), left->O_YEAR, nation->N_REGIONKEY);
	}

	virtual q8_c_o_l_p_join_n1_t* clone() const {
		return new q8_c_o_l_p_join_n1_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join CUSTOMER_ORDERS_LINEITEM_PART, NATION n1; select L_SUPPKEY, VOLUME, O_YEAR, n1.N_REGIONKEY");
	}
};

struct q8_c_o_l_p_n1_join_r_t : public tuple_join_t {

	q8_c_o_l_p_n1_join_r_t()
		: tuple_join_t(sizeof(q8_c_o_l_p_join_n1_tuple),
				offsetof(q8_c_o_l_p_join_n1_tuple, N_REGIONKEY),
				sizeof(q8_projected_region_tuple),
				offsetof(q8_projected_region_tuple, R_REGIONKEY),
				sizeof(int),
				sizeof(q8_c_o_l_p_n1_join_r_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q8_c_o_l_p_n1_join_r_tuple *dest = aligned_cast<q8_c_o_l_p_n1_join_r_tuple>(d.data);
		q8_c_o_l_p_join_n1_tuple *left = aligned_cast<q8_c_o_l_p_join_n1_tuple>(l.data);
		q8_projected_region_tuple *region = aligned_cast<q8_projected_region_tuple>(r.data);

		dest->L_SUPPKEY = left->L_SUPPKEY;
		dest->VOLUME = left->VOLUME;
		dest->O_YEAR = left->O_YEAR;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %.2f %d\n", left->N_REGIONKEY, region->R_REGIONKEY, left->L_SUPPKEY, left->VOLUME.to_double(), left->O_YEAR);
	}

	virtual q8_c_o_l_p_n1_join_r_t* clone() const {
		return new q8_c_o_l_p_n1_join_r_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join CUSTOMER_ORDERS_LINEITEM_PART_NATION, REGION; select L_SUPPKEY, VOLUME, O_YEAR");
	}
};

struct q8_s_join_c_o_l_p_n1_r_t : public tuple_join_t {

	q8_s_join_c_o_l_p_n1_r_t()
		: tuple_join_t(sizeof(q8_projected_supplier_tuple),
				offsetof(q8_projected_supplier_tuple, S_SUPPKEY),
				sizeof(q8_c_o_l_p_n1_join_r_tuple),
				offsetof(q8_c_o_l_p_n1_join_r_tuple, L_SUPPKEY),
				sizeof(int),
				sizeof(q8_s_join_c_o_l_p_n1_r_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q8_s_join_c_o_l_p_n1_r_tuple *dest = aligned_cast<q8_s_join_c_o_l_p_n1_r_tuple>(d.data);
		q8_projected_supplier_tuple *supp = aligned_cast<q8_projected_supplier_tuple>(l.data);
		q8_c_o_l_p_n1_join_r_tuple *right = aligned_cast<q8_c_o_l_p_n1_join_r_tuple>(r.data);

		dest->S_NATIONKEY = supp->S_NATIONKEY;
		dest->VOLUME = right->VOLUME;
		dest->O_YEAR = right->O_YEAR;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %.2f %d\n", supp->S_SUPPKEY, right->L_SUPPKEY, supp->S_NATIONKEY, right->VOLUME.to_double(), right->O_YEAR);
	}

	virtual q8_s_join_c_o_l_p_n1_r_t* clone() const {
		return new q8_s_join_c_o_l_p_n1_r_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join SUPPLIER, CUSTOMER_ORDERS_LINEITEM_PART_NATION_REGION; select S_NATIONKEY, VOLUEM, O_YEAR");
	}
};

struct q8_final_join : public tuple_join_t {

	q8_final_join()
		: tuple_join_t(sizeof(q8_s_join_c_o_l_p_n1_r_tuple),
				offsetof(q8_s_join_c_o_l_p_n1_r_tuple, S_NATIONKEY),
				sizeof(q8_projected_nation_n2_tuple),
				offsetof(q8_projected_nation_n2_tuple, N_NATIONKEY),
				sizeof(int),
				sizeof(q8_all_joins_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q8_all_joins_tuple *dest = aligned_cast<q8_all_joins_tuple>(d.data);
		q8_s_join_c_o_l_p_n1_r_tuple *left = aligned_cast<q8_s_join_c_o_l_p_n1_r_tuple>(l.data);
		q8_projected_nation_n2_tuple *nation = aligned_cast<q8_projected_nation_n2_tuple>(r.data);

		dest->VOLUME = left->VOLUME;
		dest->O_YEAR = left->O_YEAR;
		memcpy(dest->NATION, nation->N_NAME, sizeof(dest->NATION));

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d %2.f %d %s\n", left->S_NATIONKEY, nation->N_NATIONKEY, left->VOLUME, left->O_YEAR, nation->N_NAME);
	}

	virtual q8_final_join* clone()const {
		return new q8_final_join(*this);
	}

	virtual c_str to_string() const {
		return c_str("join SUPPLIER_CUSTOMER_ORDERS_LINEITEM_PART_NATION_REGION, NATION n2; select VOLUME, O_YEAR, n2.N_NAME as NATION");
	}
};

struct q8_aggregate_t : public tuple_aggregate_t {

	default_key_extractor_t _extractor;
	char _nation[STRSIZE(25)];

	q8_aggregate_t(q8_input_t &in)
	: tuple_aggregate_t(sizeof(q8_aggregate_tuple))
	{
		nation_to_str((&in)->n_name, _nation);
		TRACE(TRACE_ALWAYS, "Random predicates:\n NATION.N_NAME = %s\n", _nation);
	}

	virtual key_extractor_t* key_extractor() {
		return &_extractor;
	}

	virtual void aggregate(char* agg_data, const tuple_t &t) {
		q8_aggregate_tuple *agg = aligned_cast<q8_aggregate_tuple>(agg_data);
		q8_all_joins_tuple *in = aligned_cast<q8_all_joins_tuple>(t.data);

		agg->O_YEAR = in->O_YEAR;
		if(strcmp(_nation, in->NATION) == 0) {
			agg->SUM_VOLUME_NATION += in->VOLUME;
		}
		agg->SUM_VOLUME += in->VOLUME;
		//TRACE(TRACE_RECORD_FLOW, "%2.f\t%.2f\t%.2f\t%d\t%s\t%s\n", in->VOLUME.to_double(), agg->SUM_VOLUME.to_double(), agg->SUM_VOLUME_NATION.to_double(), agg->O_YEAR, _nation, in->NATION);
	}
	virtual void finish(tuple_t &dest, const char* agg_data) {
		memcpy(dest.data, agg_data, dest.size);
		q8_aggregate_tuple *agg = aligned_cast<q8_aggregate_tuple>(dest.data);
		agg->MKT_SHARE = agg->SUM_VOLUME_NATION.to_double() / agg->SUM_VOLUME.to_double();
	}
	virtual q8_aggregate_t* clone() const {
		return new q8_aggregate_t(*this);
	}
	virtual c_str to_string() const {
		return "q8_aggregate_t";
	}
};

struct q8_sort_key_compare_t : public key_compare_t {

	virtual int operator()(const void* key1, const void* key2) const {
		return (int*)key1 - (int*)key2;
	}

	virtual q8_sort_key_compare_t* clone() const {
		return new q8_sort_key_compare_t(*this);
	}
};




class tpch_q8_process_tuple_t : public process_tuple_t {

public:

    virtual void begin() {
        TRACE(TRACE_QUERY_RESULTS, "*** Q8 %6s %25s\n",
              "O_YEAR", "MKT_SHARE");
    }

    virtual void process(const tuple_t& output) {
        q8_aggregate_tuple *agg = aligned_cast<q8_aggregate_tuple>(output.data);

        TRACE(TRACE_QUERY_RESULTS, "*** Q8 %6d %25.4f\n", agg->O_YEAR, agg->MKT_SHARE);
    }

};


/********************************************************************
 *
 * QPIPE q8 - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_qpipe_q8(const int xct_id,
                                  q8_input_t& in)
{
    TRACE( TRACE_ALWAYS, "********** q8 *********\n");

    policy_t* dp = this->get_sched_policy();
    xct_t* pxct = smthread_t::me()->xct();


    //TSCAN PART
    tuple_fifo* q8_part_buffer = new tuple_fifo(sizeof(q8_projected_part_tuple));
    packet_t* q8_part_tscan_packet =
    		new tscan_packet_t("part TSCAN",
    				q8_part_buffer,
    				new q8_part_tscan_filter_t(this, in),
    				this->db(),
    				_ppart_desc.get(),
    				pxct);

    //TSCAN LINEITEM
    tuple_fifo* q8_lineitem_buffer = new tuple_fifo(sizeof(q8_projected_lineitem_tuple));
    packet_t* q8_lineitem_tscan_packet =
    		new tscan_packet_t("lineitem TSCAN",
    				q8_lineitem_buffer,
    				new q8_lineitem_tscan_filter_t(this, in),
    				this->db(),
    				_plineitem_desc.get(),
    				pxct);

    //TSCAN ORDERS
    tuple_fifo* q8_orders_buffer = new tuple_fifo(sizeof(q8_projected_orders_tuple));
    packet_t* q8_orders_tscan_packet =
    		new tscan_packet_t("orders TSCAN",
    				q8_orders_buffer,
    				new q8_orders_tscan_filter_t(this, in),
    				this->db(),
    				_porders_desc.get(),
    				pxct);

    //TSCAN CUSTOMER
    tuple_fifo* q8_customer_buffer = new tuple_fifo(sizeof(q8_projected_customer_tuple));
    packet_t* q8_customer_tscan_packet =
    		new tscan_packet_t("customer TSCAN",
    				q8_customer_buffer,
    				new q8_customer_tscan_filter_t(this, in),
    				this->db(),
    				_pcustomer_desc.get(),
    				pxct);

    //TSCAN NATION n1
    tuple_fifo* q8_nation_n1_buffer = new tuple_fifo(sizeof(q8_projected_nation_n1_tuple));
    packet_t* q8_nation_n1_tscan_packet =
    		new tscan_packet_t("nation n1 TSCAN",
    				q8_nation_n1_buffer,
    				new q8_nation_n1_tscan_filter_t(this, in),
    				this->db(),
    				_pnation_desc.get(),
    				pxct);

    //TSCAN REGION
    tuple_fifo* q8_region_buffer = new tuple_fifo(sizeof(q8_projected_region_tuple));
    packet_t* q8_region_tscan_packet =
    		new tscan_packet_t("region TSCAN",
    				q8_region_buffer,
    				new q8_region_tscan_filter_t(this, in),
    				this->db(),
    				_pregion_desc.get(),
    				pxct);

    //TSCAN SUPPLIER
    tuple_fifo* q8_supplier_buffer = new tuple_fifo(sizeof(q8_projected_supplier_tuple));
    packet_t* q8_supplier_tscan_packet =
    		new tscan_packet_t("supplier TSCAN",
    				q8_supplier_buffer,
    				new q8_supplier_tscan_filter_t(this, in),
    				this->db(),
    				_psupplier_desc.get(),
    				pxct);

    //TSCAN NATION n2
    tuple_fifo* q8_nation_n2_buffer = new tuple_fifo(sizeof(q8_projected_nation_n2_tuple));
    packet_t* q8_nation_n2_tscan_packet =
    		new tscan_packet_t("nation n2 TSCAN",
    				q8_nation_n2_buffer,
    				new q8_nation_n2_tscan_filter_t(this, in),
    				this->db(),
    				_pnation_desc.get(),
    				pxct);


    //LINEITEM JOIN PART
    tuple_fifo* q8_l_join_p_buffer = new tuple_fifo(sizeof(q8_l_join_p_tuple));
    packet_t* q8_l_join_p_packet =
    		new hash_join_packet_t("lineitem - part HJOIN",
    				q8_l_join_p_buffer,
    				new trivial_filter_t(sizeof(q8_l_join_p_tuple)),
    				q8_lineitem_tscan_packet,
    				q8_part_tscan_packet,
    				new q8_l_join_p_t());

    //ORDERS JOIN L_P
    tuple_fifo* q8_o_join_l_p_buffer = new tuple_fifo(sizeof(q8_o_join_l_p_tuple));
    packet_t* q8_o_join_l_p_packet =
    		new hash_join_packet_t("orders - lineitem_part HJOIN",
    				q8_o_join_l_p_buffer,
    				new trivial_filter_t(sizeof(q8_o_join_l_p_tuple)),
    				q8_orders_tscan_packet,
    				q8_l_join_p_packet,
    				new q8_o_join_l_p_t());

    //CUSTOMER JOIN O_L_P
    tuple_fifo* q8_c_join_o_l_p_buffer = new tuple_fifo(sizeof(q8_c_join_o_l_p_tuple));
    packet_t* q8_c_join_o_l_p_packet =
    		new hash_join_packet_t("customer - orders_lineitem_part HJOIN",
    				q8_c_join_o_l_p_buffer,
    				new trivial_filter_t(sizeof(q8_c_join_o_l_p_tuple)),
    				q8_customer_tscan_packet,
    				q8_o_join_l_p_packet,
    				new q8_c_join_o_l_p_t());

    //C_O_L_P JOIN NATION n1
    tuple_fifo* q8_c_o_l_p_join_n1_buffer = new tuple_fifo(sizeof(q8_c_o_l_p_join_n1_tuple));
    packet_t* q8_c_o_l_p_join_n1_packet =
    		new hash_join_packet_t("customer_orders_lineitem_part - nation n1 HJOIN",
    				q8_c_o_l_p_join_n1_buffer,
    				new trivial_filter_t(sizeof(q8_c_o_l_p_join_n1_tuple)),
    				q8_c_join_o_l_p_packet,
    				q8_nation_n1_tscan_packet,
    				new q8_c_o_l_p_join_n1_t());

    //C_O_L_P_N1 JOIN REGION
    tuple_fifo* q8_c_o_l_p_n1_join_r_buffer = new tuple_fifo(sizeof(q8_c_o_l_p_n1_join_r_tuple));
    packet_t* q8_c_o_l_p_n1_join_r_packet =
    		new hash_join_packet_t("customer_orders_lineitem_part_nation - region HJOIN",
    				q8_c_o_l_p_n1_join_r_buffer,
    				new trivial_filter_t(sizeof(q8_c_o_l_p_n1_join_r_tuple)),
    				q8_c_o_l_p_join_n1_packet,
    				q8_region_tscan_packet,
    				new q8_c_o_l_p_n1_join_r_t());

    //SUPPLIER JOIN C_O_L_P_N1_R
    tuple_fifo* q8_s_join_c_o_l_p_n1_r_buffer = new tuple_fifo(sizeof(q8_s_join_c_o_l_p_n1_r_tuple));
    packet_t* q8_s_join_c_o_l_p_n1_r_packet =
    		new hash_join_packet_t("supplier - customer_orders_lineitem_part_nation_region HJOIN",
    				q8_s_join_c_o_l_p_n1_r_buffer,
    				new trivial_filter_t(sizeof(q8_s_join_c_o_l_p_n1_r_tuple)),
    				q8_supplier_tscan_packet,
    				q8_c_o_l_p_n1_join_r_packet,
    				new q8_s_join_c_o_l_p_n1_r_t());

    //S_C_O_L_P_N1_R JOIN NATION n2
    tuple_fifo* q8_all_joins_buffer = new tuple_fifo(sizeof(q8_all_joins_tuple));
    packet_t* q8_all_joins_packet =
    		new hash_join_packet_t("supplier_customer_orders_lineitem_part_nation_region - nation n2 HJOIN",
    				q8_all_joins_buffer,
    				new trivial_filter_t(sizeof(q8_all_joins_tuple)),
    				q8_s_join_c_o_l_p_n1_r_packet,
    				q8_nation_n2_tscan_packet,
    				new q8_final_join());

    //AGGREGATE
    tuple_fifo* q8_aggregate_buffer = new tuple_fifo(sizeof(q8_aggregate_tuple));
    packet_t* q8_aggregate_packet =
    		new partial_aggregate_packet_t("AGGREGATE",
    				q8_aggregate_buffer,
    				new trivial_filter_t(sizeof(q8_aggregate_tuple)),
    				q8_all_joins_packet,
    				new q8_aggregate_t(in),
    				new default_key_extractor_t(sizeof(int), offsetof(q8_all_joins_tuple, O_YEAR)),
    				new q8_sort_key_compare_t());

    qpipe::query_state_t* qs = dp->query_state_create();
    q8_part_tscan_packet->assign_query_state(qs);
    q8_lineitem_tscan_packet->assign_query_state(qs);
    q8_orders_tscan_packet->assign_query_state(qs);
    q8_customer_tscan_packet->assign_query_state(qs);
    q8_nation_n1_tscan_packet->assign_query_state(qs);
    q8_region_tscan_packet->assign_query_state(qs);
    q8_supplier_tscan_packet->assign_query_state(qs);
    q8_nation_n2_tscan_packet->assign_query_state(qs);
    q8_l_join_p_packet->assign_query_state(qs);
    q8_o_join_l_p_packet->assign_query_state(qs);
    q8_c_join_o_l_p_packet->assign_query_state(qs);
    q8_c_o_l_p_join_n1_packet->assign_query_state(qs);
    q8_c_o_l_p_n1_join_r_packet->assign_query_state(qs);
    q8_s_join_c_o_l_p_n1_r_packet->assign_query_state(qs);
    q8_all_joins_packet->assign_query_state(qs);
    q8_aggregate_packet->assign_query_state(qs);


    // Dispatch packet
    tpch_q8_process_tuple_t pt;
    //LAST PACKET
    process_query(q8_aggregate_packet, pt);//TODO

    dp->query_state_destroy(qs);


    return (RCOK);
}


EXIT_NAMESPACE(qpipe);
