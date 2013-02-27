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

/** @file:   qpipe_q9.cpp
 *
 *  @brief:  Implementation of QPIPE TPCH Q9 over Shore-MT
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
 * QPIPE Q9 - Structures needed by operators
 *
 ********************************************************************/

/*
select
	nation,
	o_year,
	sum(amount) as sum_profit
from (
	select
		n_name as nation,
		extract(year from o_orderdate) as o_year,
		l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
	from
		part,
		supplier,
		lineitem,
		partsupp,
		orders,
		nation
	where
		s_suppkey = l_suppkey
		and ps_suppkey = l_suppkey
		and ps_partkey = l_partkey
		and p_partkey = l_partkey
		and o_orderkey = l_orderkey
		and s_nationkey = n_nationkey
		and p_name like '%[COLOR]%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;
 */


struct q9_projected_lineitem_tuple {
	int L_PARTKEY;
	int L_SUPPKEY;
	int L_ORDERKEY;
	decimal VOLUME;
	decimal L_QUANTITY;
};

struct q9_projected_part_tuple {
	int P_PARTKEY;
};

struct q9_projected_supplier_tuple {
	int S_SUPPKEY;
	int S_NATIONKEY;
};

struct q9_projected_nation_tuple {
	int N_NATIONKEY;
	char N_NAME[STRSIZE(25)];
};

struct q9_projected_orders_tuple {
	int O_ORDERKEY;
	int O_YEAR;
};

struct q9_projected_partsupp_tuple {
	int PS_PARTKEY;
	int PS_SUPPKEY;
	decimal PS_SUPPLYCOST;
};

struct q9_l_join_p_tuple {
	int L_PARTKEY;
	int L_SUPPKEY;
	int L_ORDERKEY;
	decimal VOLUME;
	decimal L_QUANTITY;
};

struct q9_l_p_join_s_tuple {
	int L_PARTKEY;
	int L_SUPPKEY;
	int L_ORDERKEY;
	decimal VOLUME;
	decimal L_QUANTITY;
	int S_NATIONKEY;
};

struct q9_l_p_s_join_n_tuple {
	int L_PARTKEY;
	int L_SUPPKEY;
	int L_ORDERKEY;
	decimal VOLUME;
	decimal L_QUANTITY;
	char N_NAME[STRSIZE(25)];
};

struct q9_l_p_s_n_join_o_tuple {
	int L_PARTKEY;
	int L_SUPPKEY;
	decimal VOLUME;
	decimal L_QUANTITY;
	char N_NAME[STRSIZE(25)];
	int O_YEAR;
};

struct q9_all_joins_tuple {
	char N_NAME[STRSIZE(25)];
	int O_YEAR;
	decimal VOLUME;
	decimal L_QUANTITY;
	decimal PS_SUPPLYCOST;
};

struct q9_aggregate_tuple {
	char NATION[STRSIZE(25)];
	int O_YEAR;
	decimal SUM_PROFIT;
};

struct q9_key {
	char NATION[STRSIZE(25)];
	int O_YEAR;
};


class q9_lineitem_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prline;
	rep_row_t _rr;

	tpch_lineitem_tuple _lineitem;

public:
	q9_lineitem_tscan_filter_t(ShoreTPCHEnv* tpchdb, q9_input_t &in)
	: tuple_filter_t(tpchdb->lineitem_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prline = _tpchdb->lineitem_man()->get_tuple();
		_rr.set_ts(_tpchdb->lineitem_man()->ts(),
				_tpchdb->lineitem_desc()->maxsize());
		_prline->_rep = &_rr;
	}

	virtual ~q9_lineitem_tscan_filter_t()
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

		q9_projected_lineitem_tuple *dest = aligned_cast<q9_projected_lineitem_tuple>(d.data);

		_prline->get_value(0, _lineitem.L_ORDERKEY);
		_prline->get_value(1, _lineitem.L_PARTKEY);
		_prline->get_value(2, _lineitem.L_SUPPKEY);
		_prline->get_value(4, _lineitem.L_QUANTITY);
		_prline->get_value(5, _lineitem.L_EXTENDEDPRICE);
		_prline->get_value(6, _lineitem.L_DISCOUNT);

		//TRACE(TRACE_RECORD_FLOW, "%d|%d|%d|%.2f|%.2f|%.2f\n", _lineitem.L_ORDERKEY, _lineitem.L_PARTKEY, _lineitem.L_SUPPKEY, _lineitem.L_QUANTITY, _lineitem.L_EXTENDEDPRICE / 100.0,
		//														_lineitem.L_DISCOUNT / 100.0);

		dest->L_ORDERKEY = _lineitem.L_ORDERKEY;
		dest->L_PARTKEY = _lineitem.L_PARTKEY;
		dest->L_SUPPKEY = _lineitem.L_SUPPKEY;
		dest->L_QUANTITY = _lineitem.L_QUANTITY;
		dest->VOLUME = _lineitem.L_EXTENDEDPRICE / 100.0 * (1 - _lineitem.L_DISCOUNT / 100.0);
#warning MA: Discount from TPCH dbgen is created between 0 and 100 instead between 0 and 1.
	}

	q9_lineitem_tscan_filter_t* clone() const {
		return new q9_lineitem_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q9_lineitem_tscan_filter_t()");
	}
};

class q9_part_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prpart;
	rep_row_t _rr;

	/*One part tuple*/
	tpch_part_tuple _part;
	/*The columns needed for the selection*/
	char _color[STRSIZE(10)];

	q9_input_t* q9_input;

public:

	q9_part_tscan_filter_t(ShoreTPCHEnv* tpchdb, q9_input_t &in)
	: tuple_filter_t(tpchdb->part_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prpart = _tpchdb->part_man()->get_tuple();
		_rr.set_ts(_tpchdb->part_man()->ts(),
				_tpchdb->part_desc()->maxsize());
		_prpart->_rep = &_rr;

		q9_input = &in;
		pname_to_str(q9_input->p_name, _color);

		TRACE(TRACE_ALWAYS, "Random predicates:\nPART.P_NAME like '%%%s%%'\n", _color);
	}

	virtual ~q9_part_tscan_filter_t()
	{
		// Give back the part tuple
		_tpchdb->part_man()->give_tuple(_prpart);
	}



	bool select(const tuple_t &input) {

		// Get next part and read its size and type
		if (!_tpchdb->part_man()->load(_prpart, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		_prpart->get_value(1, _part.P_NAME, sizeof(_part.P_NAME));

		return strstr(_part.P_NAME, _color);
	}


	void project(tuple_t &d, const tuple_t &s) {

		q9_projected_part_tuple *dest;
		dest = aligned_cast<q9_projected_part_tuple>(d.data);

		_prpart->get_value(0, _part.P_PARTKEY);

		//TRACE( TRACE_RECORD_FLOW, "%d\n",
		//		_part.P_PARTKEY);

		dest->P_PARTKEY = _part.P_PARTKEY;
	}

	q9_part_tscan_filter_t* clone() const {
		return new q9_part_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q9_part_tscan_filter_t(%s)", _color);
	}
};

class q9_supplier_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prsupplier;
	rep_row_t _rr;

	/*One supplier tuple*/
	tpch_supplier_tuple _supplier;

public:
	q9_supplier_tscan_filter_t(ShoreTPCHEnv* tpchdb, q9_input_t &in)
	: tuple_filter_t(tpchdb->supplier_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prsupplier = _tpchdb->supplier_man()->get_tuple();
		_rr.set_ts(_tpchdb->supplier_man()->ts(),
				_tpchdb->supplier_desc()->maxsize());
		_prsupplier->_rep = &_rr;
	}

	virtual ~q9_supplier_tscan_filter_t()
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

		q9_projected_supplier_tuple *dest = aligned_cast<q9_projected_supplier_tuple>(d.data);


		_prsupplier->get_value(0, _supplier.S_SUPPKEY);
		_prsupplier->get_value(3, _supplier.S_NATIONKEY);

		//TRACE( TRACE_RECORD_FLOW, "%d|%d\n",
		//       _supplier.S_SUPPKEY, _supplier.S_NATIONKEY);

		dest->S_SUPPKEY = _supplier.S_SUPPKEY;
		dest->S_NATIONKEY = _supplier.S_NATIONKEY;

	}

	q9_supplier_tscan_filter_t* clone() const {
		return new q9_supplier_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q9_supplier_tscan_filter_t");
	}
};

class q9_nation_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prnation;
	rep_row_t _rr;

	/*One nation tuple*/
	tpch_nation_tuple _nation;

public:
	q9_nation_tscan_filter_t(ShoreTPCHEnv* tpchdb, q9_input_t &in)
	: tuple_filter_t(tpchdb->nation_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prnation = _tpchdb->nation_man()->get_tuple();
		_rr.set_ts(_tpchdb->nation_man()->ts(),
				_tpchdb->nation_desc()->maxsize());
		_prnation->_rep = &_rr;
	}

	virtual ~q9_nation_tscan_filter_t()
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

		q9_projected_nation_tuple *dest;
		dest = aligned_cast<q9_projected_nation_tuple>(d.data);

		_prnation->get_value(0, _nation.N_NATIONKEY);
		_prnation->get_value(1, _nation.N_NAME, 25);

		//TRACE( TRACE_RECORD_FLOW, "%d|%s\n",
		//       _nation.N_NATIONKEY, _nation.N_NAME);

		dest->N_NATIONKEY = _nation.N_NATIONKEY;
		memcpy(dest->N_NAME, _nation.N_NAME, sizeof(dest->N_NAME));
	}

	q9_nation_tscan_filter_t* clone() const {
		return new q9_nation_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q9_nation_tscan_filter_t()");
	}
};

class q9_orders_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prorders;
	rep_row_t _rr;

	tpch_orders_tuple _orders;
	time_t _orderdate;

public:
	q9_orders_tscan_filter_t(ShoreTPCHEnv* tpchdb, q9_input_t &in)
	: tuple_filter_t(tpchdb->orders_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prorders = _tpchdb->orders_man()->get_tuple();
		_rr.set_ts(_tpchdb->orders_man()->ts(),
				_tpchdb->orders_desc()->maxsize());
		_prorders->_rep = &_rr;
	}

	virtual ~q9_orders_tscan_filter_t()
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

		q9_projected_orders_tuple *dest = aligned_cast<q9_projected_orders_tuple>(d.data);

		_prorders->get_value(0, _orders.O_ORDERKEY);
		_prorders->get_value(4, _orders.O_ORDERDATE, sizeof(_orders.O_ORDERDATE));
		_orderdate = str_to_timet(_orders.O_ORDERDATE);
		struct tm *tm_orderdate = gmtime(&_orderdate);

		//TRACE(TRACE_RECORD_FLOW, "%d|%d\n", _orders.O_ORDERKEY, tm_orderdate->tm_year + 1900);

		dest->O_ORDERKEY = _orders.O_ORDERKEY;
		dest->O_YEAR = tm_orderdate->tm_year + 1900;
	}

	q9_orders_tscan_filter_t* clone() const {
		return new q9_orders_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q9_orders_tscan_filter_t()");
	}
};

class q9_partsupp_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prpartsupp;
	rep_row_t _rr;

	/*One partsupp tuple*/
	tpch_partsupp_tuple _partsupp;

public:
	q9_partsupp_tscan_filter_t(ShoreTPCHEnv* tpchdb, q9_input_t &in)
	: tuple_filter_t(tpchdb->partsupp_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prpartsupp = _tpchdb->partsupp_man()->get_tuple();
		_rr.set_ts(_tpchdb->partsupp_man()->ts(),
				_tpchdb->partsupp_desc()->maxsize());
		_prpartsupp->_rep = &_rr;
	}

	virtual ~q9_partsupp_tscan_filter_t()
	{
		_tpchdb->partsupp_man()->give_tuple(_prpartsupp);
	}

	bool select(const tuple_t &input) {

		// Get next partsupp tuple and read its size and type
		if (!_tpchdb->partsupp_man()->load(_prpartsupp, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		return true;
	}


	void project(tuple_t &d, const tuple_t &s) {

		q9_projected_partsupp_tuple *dest;
		dest = aligned_cast<q9_projected_partsupp_tuple>(d.data);

		_prpartsupp->get_value(1, _partsupp.PS_SUPPKEY);
		_prpartsupp->get_value(0, _partsupp.PS_PARTKEY);
		_prpartsupp->get_value(3, _partsupp.PS_SUPPLYCOST);

		/*TRACE( TRACE_RECORD_FLOW, "%d|%d|%.4f\n",
				_partsupp.PS_SUPPKEY,
				_partsupp.PS_PARTKEY,
				_partsupp.PS_SUPPLYCOST.to_double() / 100.0);*/

		dest->PS_SUPPKEY = _partsupp.PS_SUPPKEY;
		dest->PS_PARTKEY = _partsupp.PS_PARTKEY;
		dest->PS_SUPPLYCOST = _partsupp.PS_SUPPLYCOST / 100.0;

	}

	q9_partsupp_tscan_filter_t* clone() const {
		return new q9_partsupp_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q9_partsupp_tscan_filter_t()");
	}
};


struct q9_l_join_p_t : public tuple_join_t {

	q9_l_join_p_t()
	: tuple_join_t(sizeof(q9_projected_lineitem_tuple),
			offsetof(q9_projected_lineitem_tuple, L_PARTKEY),
			sizeof(q9_projected_part_tuple),
			offsetof(q9_projected_part_tuple, P_PARTKEY),
			sizeof(int),
			sizeof(q9_l_join_p_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q9_l_join_p_tuple *dest = aligned_cast<q9_l_join_p_tuple>(d.data);
		q9_projected_lineitem_tuple *line = aligned_cast<q9_projected_lineitem_tuple>(l.data);
		q9_projected_part_tuple *part = aligned_cast<q9_projected_part_tuple>(r.data);

		dest->L_ORDERKEY = line->L_ORDERKEY;
		dest->L_PARTKEY = line->L_PARTKEY;
		dest->L_QUANTITY = line->L_QUANTITY;
		dest->L_SUPPKEY = line->L_SUPPKEY;
		dest->VOLUME = line->VOLUME;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %d %.2f %d %.2f\n", line->L_PARTKEY, part->P_PARTKEY, line->L_ORDERKEY, line->L_PARTKEY, line->L_QUANTITY.to_double(),
		//																line->L_SUPPKEY, line->VOLUME.to_double());
	}

	virtual q9_l_join_p_t* clone() const {
		return new q9_l_join_p_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join LINEITEM, PART; select L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY, VOLUME");
	}
};

struct q9_l_p_join_s_t : public tuple_join_t {

	q9_l_p_join_s_t()
	: tuple_join_t(sizeof(q9_l_join_p_tuple),
			offsetof(q9_l_join_p_tuple, L_SUPPKEY),
			sizeof(q9_projected_supplier_tuple),
			offsetof(q9_projected_supplier_tuple, S_SUPPKEY),
			sizeof(int),
			sizeof(q9_l_p_join_s_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q9_l_p_join_s_tuple *dest = aligned_cast<q9_l_p_join_s_tuple>(d.data);
		q9_l_join_p_tuple *left = aligned_cast<q9_l_join_p_tuple>(l.data);
		q9_projected_supplier_tuple *supp = aligned_cast<q9_projected_supplier_tuple>(r.data);

		dest->L_ORDERKEY = left->L_ORDERKEY;
		dest->L_PARTKEY = left->L_PARTKEY;
		dest->L_QUANTITY = left->L_QUANTITY;
		dest->L_SUPPKEY = left->L_SUPPKEY;
		dest->S_NATIONKEY = supp->S_NATIONKEY;
		dest->VOLUME = left->VOLUME;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %d %.2f %d %d %.2f\n", left->L_SUPPKEY, supp->S_SUPPKEY, left->L_ORDERKEY, left->L_PARTKEY, left->L_QUANTITY.to_double(),
		//																	left->L_SUPPKEY, supp->S_NATIONKEY, left->VOLUME.to_double());
	}

	virtual q9_l_p_join_s_t* clone() const {
		return new q9_l_p_join_s_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join LINEITEM_PART, SUPPLIER; select L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY, S_NATIONKEY, VOLUME");
	}
};

struct q9_l_p_s_join_n_t : public tuple_join_t {

	q9_l_p_s_join_n_t()
	: tuple_join_t(sizeof(q9_l_p_join_s_tuple),
			offsetof(q9_l_p_join_s_tuple, S_NATIONKEY),
			sizeof(q9_projected_nation_tuple),
			offsetof(q9_projected_nation_tuple, N_NATIONKEY),
			sizeof(int),
			sizeof(q9_l_p_s_join_n_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q9_l_p_s_join_n_tuple *dest = aligned_cast<q9_l_p_s_join_n_tuple>(d.data);
		q9_l_p_join_s_tuple *left = aligned_cast<q9_l_p_join_s_tuple>(l.data);
		q9_projected_nation_tuple *nation = aligned_cast<q9_projected_nation_tuple>(r.data);

		dest->L_ORDERKEY = left->L_ORDERKEY;
		dest->L_PARTKEY = left->L_PARTKEY;
		dest->L_QUANTITY = left->L_QUANTITY;
		dest->L_SUPPKEY = left->L_SUPPKEY;
		memcpy(dest->N_NAME, nation->N_NAME, sizeof(dest->N_NAME));
		dest->VOLUME = left->VOLUME;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %d %.2f %d %s %.2f\n", left->S_NATIONKEY, nation->N_NATIONKEY, left->L_ORDERKEY, left->L_PARTKEY, left->L_QUANTITY.to_double(),
		//																left->L_SUPPKEY, nation->N_NAME, left->VOLUME.to_double());
	}

	virtual q9_l_p_s_join_n_t* clone() const {
		return new q9_l_p_s_join_n_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join LINEITEM_PART_SUPPLIER, NATION; select L_ORDERKEY, L_PARTKEY, L_QUANTITY, L_SUPPKEY, N_NAME, VOLUME");
	}
};

struct q9_l_p_s_n_join_o_t : public tuple_join_t {

	q9_l_p_s_n_join_o_t()
	: tuple_join_t(sizeof(q9_l_p_s_join_n_tuple),
			offsetof(q9_l_p_s_join_n_tuple, L_ORDERKEY),
			sizeof(q9_projected_orders_tuple),
			offsetof(q9_projected_orders_tuple, O_ORDERKEY),
			sizeof(int),
			sizeof(q9_l_p_s_n_join_o_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q9_l_p_s_n_join_o_tuple *dest = aligned_cast<q9_l_p_s_n_join_o_tuple>(d.data);
		q9_l_p_s_join_n_tuple *left = aligned_cast<q9_l_p_s_join_n_tuple>(l.data);
		q9_projected_orders_tuple *order = aligned_cast<q9_projected_orders_tuple>(r.data);

		dest->L_PARTKEY = left->L_PARTKEY;
		dest->L_QUANTITY = left->L_QUANTITY;
		dest->L_SUPPKEY = left->L_SUPPKEY;
		memcpy(dest->N_NAME, left->N_NAME, sizeof(dest->N_NAME));
		dest->O_YEAR = order->O_YEAR;
		dest->VOLUME = left->VOLUME;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %.2f %d %s %d %.2f\n", left->L_ORDERKEY, order->O_ORDERKEY, left->L_PARTKEY, left->L_QUANTITY.to_double(), left->L_SUPPKEY,
		//																left->N_NAME, order->O_YEAR, left->VOLUME.to_double());
	}

	virtual q9_l_p_s_n_join_o_t* clone() const {
		return new q9_l_p_s_n_join_o_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join LINEITEM_PART_SUPPLIER_NATION, ORDERS; select L_PARTKEY, L_QUANTITY, L_SUPPKEY, N_NAME, O_YEAR, VOLUME");
	}
};

struct q9_final_join_t : public tuple_join_t {

	q9_final_join_t()
	: tuple_join_t(sizeof(q9_l_p_s_n_join_o_tuple),
			offsetof(q9_l_p_s_n_join_o_tuple, L_PARTKEY),
			sizeof(q9_projected_partsupp_tuple),
			offsetof(q9_projected_partsupp_tuple, PS_PARTKEY),
			2*sizeof(int),
			sizeof(q9_all_joins_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q9_all_joins_tuple *dest = aligned_cast<q9_all_joins_tuple>(d.data);
		q9_l_p_s_n_join_o_tuple *left = aligned_cast<q9_l_p_s_n_join_o_tuple>(l.data);
		q9_projected_partsupp_tuple *partsupp = aligned_cast<q9_projected_partsupp_tuple>(r.data);

		dest->L_QUANTITY = left->L_QUANTITY;
		memcpy(dest->N_NAME, left->N_NAME, sizeof(dest->N_NAME));
		dest->O_YEAR = left->O_YEAR;
		dest->PS_SUPPLYCOST = partsupp->PS_SUPPLYCOST;
		dest->VOLUME = left->VOLUME;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d AND %d=%d: %.2f %s %d %.2f %.2f\n", left->L_PARTKEY, partsupp->PS_PARTKEY, left->L_SUPPKEY, partsupp->PS_SUPPKEY, left->L_QUANTITY.to_double(),
		//																			left->N_NAME, left->O_YEAR, partsupp->PS_SUPPLYCOST.to_double(), left->VOLUME.to_double());
	}

	virtual q9_final_join_t* clone() const {
		return new q9_final_join_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join LINEIETEM_PART_SUPPLIER_NATION_ORDERS, PARTSUPP; select L_QUANTITY, N_NAME, O_YEAR, PS_SUPPLYCOST, VOLUME");
	}
};

struct q9_key_extractor_t : public key_extractor_t {

	q9_key_extractor_t()
	: key_extractor_t(sizeof(q9_key))
	{
	}

	virtual int extract_hint(const char* key) const {
		char *k;
		k = aligned_cast<char>(key);

		int result = (*k << 24) + (*(k + sizeof(char)) << 16) + (*(k + 2*sizeof(char)) << 8) + *(k + 3*sizeof(char));

		return result;
	}

	virtual q9_key_extractor_t* clone() const {
		return new q9_key_extractor_t(*this);
	}
};

struct q9_key_compare_t : public key_compare_t {

	virtual int operator()(const void* key1, const void* key2) const {
		q9_key *k1 = aligned_cast<q9_key>(key1);
		q9_key *k2 = aligned_cast<q9_key>(key2);

		int diff_nation = strcmp(k1->NATION, k2->NATION);
		int diff_year = k2->O_YEAR - k1->O_YEAR;

		return (diff_nation != 0 ? diff_nation : diff_year);
	}

	virtual q9_key_compare_t* clone() const {
		return new q9_key_compare_t(*this);
	}
};

struct q9_aggregate_t : public tuple_aggregate_t {

	default_key_extractor_t _extractor;

	q9_aggregate_t()
	: tuple_aggregate_t(sizeof(q9_aggregate_tuple)), _extractor(sizeof(q9_key))
	{
	}

	virtual key_extractor_t* key_extractor() {
		return &_extractor;
	}

	virtual void aggregate(char* agg_data, const tuple_t &t) {
		q9_aggregate_tuple *agg = aligned_cast<q9_aggregate_tuple>(agg_data);
		q9_all_joins_tuple *in = aligned_cast<q9_all_joins_tuple>(t.data);

		memcpy(agg->NATION, in->N_NAME, sizeof(agg->NATION));
		agg->O_YEAR = in->O_YEAR;
		agg->SUM_PROFIT += (in->VOLUME - in->PS_SUPPLYCOST * in->L_QUANTITY);
		//TRACE(TRACE_RECORD_FLOW, "AGG: %.2f %.2f %.2f\n", in->VOLUME.to_double(), in->PS_SUPPLYCOST.to_double(), in->L_QUANTITY.to_double());
	}
	virtual void finish(tuple_t &dest, const char* agg_data) {
		memcpy(dest.data, agg_data, dest.size);
	}
	virtual q9_aggregate_t* clone() const {
		return new q9_aggregate_t(*this);
	}
	virtual c_str to_string() const {
		return "q9_aggregate_t";
	}
};


class tpch_q9_process_tuple_t : public process_tuple_t {

public:

	virtual void begin() {
		TRACE(TRACE_QUERY_RESULTS, "*** Q9 %25s %6s %25s\n",
				"NATION", "O_YEAR", "SUM_PROFIT");
	}

	virtual void process(const tuple_t& output) {
		q9_aggregate_tuple *agg = aligned_cast<q9_aggregate_tuple>(output.data);

		TRACE(TRACE_QUERY_RESULTS, "*** Q9 %25s %6d %25.4f\n", agg->NATION, agg->O_YEAR, agg->SUM_PROFIT.to_double());
	}
};


/********************************************************************
 *
 * QPIPE Q9 - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_qpipe_q9(const int xct_id,
		q9_input_t& in)
{
	TRACE( TRACE_ALWAYS, "********** q9 *********\n");

	policy_t* dp = this->get_sched_policy();
	xct_t* pxct = smthread_t::me()->xct();


	//TSCAN LINEITEM
	tuple_fifo* q9_lineitem_buffer = new tuple_fifo(sizeof(q9_projected_lineitem_tuple));
	packet_t* q9_lineitem_tscan_packet =
			new tscan_packet_t("lineitem TSCAN",
					q9_lineitem_buffer,
					new q9_lineitem_tscan_filter_t(this, in),
					this->db(),
					_plineitem_desc.get(),
					pxct);

	//TSCAN PART
	tuple_fifo* q9_part_buffer = new tuple_fifo(sizeof(q9_projected_part_tuple));
	packet_t* q9_part_tscan_packet =
			new tscan_packet_t("part TSCAN",
					q9_part_buffer,
					new q9_part_tscan_filter_t(this, in),
					this->db(),
					_ppart_desc.get(),
					pxct);

	//TSCAN SUPPLIER
	tuple_fifo* q9_supplier_buffer = new tuple_fifo(sizeof(q9_projected_supplier_tuple));
	packet_t* q9_supplier_tscan_packet =
			new tscan_packet_t("supplier TSCAN",
					q9_supplier_buffer,
					new q9_supplier_tscan_filter_t(this, in),
					this->db(),
					_psupplier_desc.get(),
					pxct);

	//TSCAN NATION
	tuple_fifo* q9_nation_buffer = new tuple_fifo(sizeof(q9_projected_nation_tuple));
	packet_t* q9_nation_tscan_packet =
			new tscan_packet_t("nation TSCAN",
					q9_nation_buffer,
					new q9_nation_tscan_filter_t(this, in),
					this->db(),
					_pnation_desc.get(),
					pxct);

	//TSCAN ORDERS
	tuple_fifo* q9_orders_buffer = new tuple_fifo(sizeof(q9_projected_orders_tuple));
	packet_t* q9_orders_tscan_packet =
			new tscan_packet_t("orders TSCAN",
					q9_orders_buffer,
					new q9_orders_tscan_filter_t(this, in),
					this->db(),
					_porders_desc.get(),
					pxct);

	//TSCAN PARTSUPP
	tuple_fifo* q9_partsupp_buffer = new tuple_fifo(sizeof(q9_projected_partsupp_tuple));
	packet_t* q9_partsupp_tscan_packet =
			new tscan_packet_t("partsupp TSCAN",
					q9_partsupp_buffer,
					new q9_partsupp_tscan_filter_t(this, in),
					this->db(),
					_ppartsupp_desc.get(),
					pxct);


	//LINEITEM JOIN PART
	tuple_fifo* q9_l_join_p_buffer = new tuple_fifo(sizeof(q9_l_join_p_tuple));
	packet_t* q9_l_join_p_packet =
			new hash_join_packet_t("lineitem - part HJOIN",
					q9_l_join_p_buffer,
					new trivial_filter_t(sizeof(q9_l_join_p_tuple)),
					q9_lineitem_tscan_packet,
					q9_part_tscan_packet,
					new q9_l_join_p_t());

	//LINEITEM_PART JOIN SUPPLIER
	tuple_fifo* q9_l_p_join_s_buffer = new tuple_fifo(sizeof(q9_l_p_join_s_tuple));
	packet_t* q9_l_p_join_s_packet =
			new hash_join_packet_t("lineitem_part - supplier HJOIN",
					q9_l_p_join_s_buffer,
					new trivial_filter_t(sizeof(q9_l_p_join_s_tuple)),
					q9_l_join_p_packet,
					q9_supplier_tscan_packet,
					new q9_l_p_join_s_t());

	//LINEITEM_PART_SUPPLIER JOIN NATION
	tuple_fifo* q9_l_p_s_join_n_buffer = new tuple_fifo(sizeof(q9_l_p_s_join_n_tuple));
	packet_t* q9_l_p_s_join_n_packet =
			new hash_join_packet_t("lineitem_part_supplier - nation HJOIN",
					q9_l_p_s_join_n_buffer,
					new trivial_filter_t(sizeof(q9_l_p_s_join_n_tuple)),
					q9_l_p_join_s_packet,
					q9_nation_tscan_packet,
					new q9_l_p_s_join_n_t());

	//LINEITEM_PART_SUPPLIER_NATION JOIN ORDERS
	tuple_fifo* q9_l_p_s_n_join_o_buffer = new tuple_fifo(sizeof(q9_l_p_s_n_join_o_tuple));
	packet_t* q9_l_p_s_n_join_o_packet =
			new hash_join_packet_t("lineitem_part_supplier_nation - orders HJOIN",
					q9_l_p_s_n_join_o_buffer,
					new trivial_filter_t(sizeof(q9_l_p_s_n_join_o_tuple)),
					q9_l_p_s_join_n_packet,
					q9_orders_tscan_packet,
					new q9_l_p_s_n_join_o_t());

	//LINEITEM_PART_SUPPLIER_NATION_ORDERS JOIN PARTSUPP
	tuple_fifo* q9_all_joins_buffer = new tuple_fifo(sizeof(q9_all_joins_tuple));
	packet_t* q9_all_joins_packet =
			new hash_join_packet_t("lineitem_part_supplier_nation_orders - partsupp HJOIN",
					q9_all_joins_buffer,
					new trivial_filter_t(sizeof(q9_all_joins_tuple)),
					q9_l_p_s_n_join_o_packet,
					q9_partsupp_tscan_packet,
					new q9_final_join_t());

	//AGGREGATE
	tuple_fifo* q9_agg_buffer = new tuple_fifo(sizeof(q9_aggregate_tuple));
	packet_t* q9_agg_packet =
			new partial_aggregate_packet_t("AGGREGATE",
					q9_agg_buffer,
					new trivial_filter_t(sizeof(q9_aggregate_tuple)),
					q9_all_joins_packet,
					new q9_aggregate_t(),
					new default_key_extractor_t(sizeof(q9_key)),
					new q9_key_compare_t());

	//SORT
	tuple_fifo* q9_sort_buffer = new tuple_fifo(sizeof(q9_aggregate_tuple));
	packet_t* q9_sort_packet =
			new sort_packet_t("SORT",
					q9_sort_buffer,
					new trivial_filter_t(sizeof(q9_aggregate_tuple)),
					new q9_key_extractor_t(),
					new q9_key_compare_t(),
					q9_agg_packet);


	qpipe::query_state_t* qs = dp->query_state_create();
	q9_lineitem_tscan_packet->assign_query_state(qs);
	q9_part_tscan_packet->assign_query_state(qs);
	q9_supplier_tscan_packet->assign_query_state(qs);
	q9_nation_tscan_packet->assign_query_state(qs);
	q9_orders_tscan_packet->assign_query_state(qs);
	q9_partsupp_tscan_packet->assign_query_state(qs);
	q9_l_join_p_packet->assign_query_state(qs);
	q9_l_p_join_s_packet->assign_query_state(qs);
	q9_l_p_s_join_n_packet->assign_query_state(qs);
	q9_l_p_s_n_join_o_packet->assign_query_state(qs);
	q9_all_joins_packet->assign_query_state(qs);
	q9_agg_packet->assign_query_state(qs);
	q9_sort_packet->assign_query_state(qs);


	// Dispatch packet
	tpch_q9_process_tuple_t pt;
	//LAST PACKET
	process_query(q9_sort_packet, pt);//TODO

	dp->query_state_destroy(qs);


	return (RCOK);
}


EXIT_NAMESPACE(tpch);
