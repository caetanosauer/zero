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

/** @file:   qpipe_q20.cpp
 *
 *  @brief:  Implementation of QPIPE TPCH Q20 over Shore-MT
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
 * QPIPE Q20 - Structures needed by operators
 *
 ********************************************************************/

/*
select
	s_name,
	s_address
from
	supplier, nation
where
	s_suppkey in (
		select
			ps_suppkey
		from
			partsupp
		where
			ps_partkey in (
				select
					p_partkey
				from
					part
				where
					p_name like '[COLOR]%' )
			and ps_availqty > (
				select
					0.5 * sum(l_quantity)
				from
					lineitem
				where
					l_partkey = ps_partkey
					and l_suppkey = ps_suppkey
					and l_shipdate >= date('[DATE]')
					and l_shipdate < date('[DATE]') + interval '1' year ))
	and s_nationkey = n_nationkey
	and n_name = '[NATION]'
order by
	s_name;
 */

struct q20_projected_partsupp_tuple {
	int PS_PARTKEY;
	int PS_SUPPKEY;
	int PS_AVAILQTY;
};

struct q20_projected_part_tuple {
	int P_PARTKEY;
};

struct q20_projected_supplier_tuple {
	int S_SUPPKEY;
	int S_NATIONKEY;
	char S_NAME[STRSIZE(25)];
	char S_ADDRESS[STRSIZE(40)];
};

struct q20_projected_nation_tuple {
	int N_NATIONKEY;
};

struct q20_projected_lineitem_tuple {
	int L_PARTKEY;
	int L_SUPPKEY;
	decimal L_QUANTITY;
};

struct q20_p_join_ps_tuple {
	int PS_PARTKEY;
	int PS_SUPPKEY;
	int PS_AVAILQTY;
};

struct q20_s_join_n_tuple {
	int S_SUPPKEY;
	char S_NAME[STRSIZE(25)];
	char S_ADDRESS[STRSIZE(40)];
};

struct q20_p_ps_join_s_n_tuple {
	int PS_PARTKEY;
	int S_SUPPKEY;
	int PS_AVAILQTY;
	char S_NAME[STRSIZE(25)];
	char S_ADDRESS[STRSIZE(40)];
};

struct q20_all_joins_tuple {
	int S_SUPPKEY;
	int PS_AVAILQTY;
	decimal AVAILQTY_THRESHOLD;
	char S_NAME[STRSIZE(25)];
	char S_ADDRESS[STRSIZE(40)];
};

struct q20_final_tuple {
	int S_SUPPKEY;
	char S_NAME[STRSIZE(25)];
	char S_ADDRESS[STRSIZE(40)];
};


class q20_partsupp_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prpartsupp;
	rep_row_t _rr;

	/*One partsupp tuple*/
	tpch_partsupp_tuple _partsupp;

public:
	q20_partsupp_tscan_filter_t(ShoreTPCHEnv* tpchdb, q20_input_t &in)
	: tuple_filter_t(tpchdb->partsupp_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prpartsupp = _tpchdb->partsupp_man()->get_tuple();
		_rr.set_ts(_tpchdb->partsupp_man()->ts(),
				_tpchdb->partsupp_desc()->maxsize());
		_prpartsupp->_rep = &_rr;
	}

	virtual ~q20_partsupp_tscan_filter_t()
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

		q20_projected_partsupp_tuple *dest;
		dest = aligned_cast<q20_projected_partsupp_tuple>(d.data);

		_prpartsupp->get_value(0, _partsupp.PS_PARTKEY);
		_prpartsupp->get_value(1, _partsupp.PS_SUPPKEY);
		_prpartsupp->get_value(2, _partsupp.PS_AVAILQTY);

		/*TRACE( TRACE_RECORD_FLOW, "%d|%d|%d\n",
				_partsupp.PS_SUPPKEY,
				_partsupp.PS_PARTKEY,
				_partsupp.PS_AVAILQTY);*/

		dest->PS_SUPPKEY = _partsupp.PS_SUPPKEY;
		dest->PS_PARTKEY = _partsupp.PS_PARTKEY;
		dest->PS_AVAILQTY = _partsupp.PS_AVAILQTY;
	}

	q20_partsupp_tscan_filter_t* clone() const {
		return new q20_partsupp_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q20_partsupp_tscan_filter_t()");
	}
};

class q20_part_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prpart;
	rep_row_t _rr;

	/*One part tuple*/
	tpch_part_tuple _part;
	/*The columns needed for the selection*/
	char _color[STRSIZE(25)];

	q20_input_t* q20_input;

public:

	q20_part_tscan_filter_t(ShoreTPCHEnv* tpchdb, q20_input_t &in)
	: tuple_filter_t(tpchdb->part_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prpart = _tpchdb->part_man()->get_tuple();
		_rr.set_ts(_tpchdb->part_man()->ts(),
				_tpchdb->part_desc()->maxsize());
		_prpart->_rep = &_rr;

		q20_input = &in;
		pname_to_str(q20_input->p_color, _color);

		TRACE(TRACE_ALWAYS, "Random predicates:\nPART.P_NAME like '%s%%'\n", _color);
	}

	virtual ~q20_part_tscan_filter_t()
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

		q20_projected_part_tuple *dest;
		dest = aligned_cast<q20_projected_part_tuple>(d.data);

		_prpart->get_value(0, _part.P_PARTKEY);

		//TRACE( TRACE_RECORD_FLOW, "%d\n", _part.P_PARTKEY);

		dest->P_PARTKEY = _part.P_PARTKEY;
	}

	q20_part_tscan_filter_t* clone() const {
		return new q20_part_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q20_part_tscan_filter_t()");
	}
};

class q20_supplier_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prsupplier;
	rep_row_t _rr;

	/*One supplier tuple*/
	tpch_supplier_tuple _supplier;

public:
	q20_supplier_tscan_filter_t(ShoreTPCHEnv* tpchdb, q20_input_t &in)
	: tuple_filter_t(tpchdb->supplier_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prsupplier = _tpchdb->supplier_man()->get_tuple();
		_rr.set_ts(_tpchdb->supplier_man()->ts(),
				_tpchdb->supplier_desc()->maxsize());
		_prsupplier->_rep = &_rr;
	}

	virtual ~q20_supplier_tscan_filter_t()
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

		q20_projected_supplier_tuple *dest = aligned_cast<q20_projected_supplier_tuple>(d.data);


		_prsupplier->get_value(0, _supplier.S_SUPPKEY);
		_prsupplier->get_value(1, _supplier.S_NAME, sizeof(_supplier.S_NAME));
		_prsupplier->get_value(2, _supplier.S_ADDRESS, sizeof(_supplier.S_ADDRESS));
		_prsupplier->get_value(3, _supplier.S_NATIONKEY);

		//TRACE( TRACE_RECORD_FLOW, "%d|%s|%s|%d\n", _supplier.S_SUPPKEY, _supplier.S_NAME, _supplier.S_ADDRESS, _supplier.S_NATIONKEY);

		dest->S_SUPPKEY = _supplier.S_SUPPKEY;
		memcpy(dest->S_NAME, _supplier.S_NAME, sizeof(dest->S_NAME));
		memcpy(dest->S_ADDRESS, _supplier.S_ADDRESS, sizeof(dest->S_ADDRESS));
		dest->S_NATIONKEY = _supplier.S_NATIONKEY;
	}

	q20_supplier_tscan_filter_t* clone() const {
		return new q20_supplier_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q20_supplier_tscan_filter_t");
	}
};

class q20_nation_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prnation;
	rep_row_t _rr;

	/*One nation tuple*/
	tpch_nation_tuple _nation;

	char _name[STRSIZE(25)];

public:
	q20_nation_tscan_filter_t(ShoreTPCHEnv* tpchdb, q20_input_t &in)
	: tuple_filter_t(tpchdb->nation_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prnation = _tpchdb->nation_man()->get_tuple();
		_rr.set_ts(_tpchdb->nation_man()->ts(),
				_tpchdb->nation_desc()->maxsize());
		_prnation->_rep = &_rr;

		nation_to_str((&in)->n_name, _name);
		TRACE(TRACE_ALWAYS, "Random predicate:\nNATION:N_NAME = %s\n", _name);
	}

	virtual ~q20_nation_tscan_filter_t()
	{
		_tpchdb->nation_man()->give_tuple(_prnation);
	}

	bool select(const tuple_t &input) {

		// Get next nation tuple and read its size and type
		if (!_tpchdb->nation_man()->load(_prnation, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		_prnation->get_value(1, _nation.N_NAME, sizeof(_nation.N_NAME));

		return (strcmp(_nation.N_NAME, _name) == 0);
	}


	void project(tuple_t &d, const tuple_t &s) {

		q20_projected_nation_tuple *dest;
		dest = aligned_cast<q20_projected_nation_tuple>(d.data);

		_prnation->get_value(0, _nation.N_NATIONKEY);

		//TRACE( TRACE_RECORD_FLOW, "%d\n",
		//		_nation.N_NATIONKEY);

		dest->N_NATIONKEY = _nation.N_NATIONKEY;
	}

	q20_nation_tscan_filter_t* clone() const {
		return new q20_nation_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q20_nation_tscan_filter_t(%s)", _name);
	}
};

class q20_lineitem_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prline;
	rep_row_t _rr;

	tpch_lineitem_tuple _lineitem;
	time_t _shipdate;

	time_t _first_shipdate;
	time_t _last_shipdate;

	q20_input_t *q20_input;

public:
	q20_lineitem_tscan_filter_t(ShoreTPCHEnv* tpchdb, q20_input_t &in)
	: tuple_filter_t(tpchdb->lineitem_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prline = _tpchdb->lineitem_man()->get_tuple();
		_rr.set_ts(_tpchdb->lineitem_man()->ts(),
				_tpchdb->lineitem_desc()->maxsize());
		_prline->_rep = &_rr;

		q20_input = &in;
		_first_shipdate = q20_input->l_shipdate;

		struct tm *tm = gmtime(&_first_shipdate);
		tm->tm_year++;
		_last_shipdate = mktime(tm);

		char f_shipdate[10];
		char l_shipdate[10];
		timet_to_str(f_shipdate, _first_shipdate);
		timet_to_str(l_shipdate, _last_shipdate);

		TRACE(TRACE_ALWAYS, "Random predicates:\n%s <= L_SHIPDATE < %s\n", f_shipdate, l_shipdate);
	}

	virtual ~q20_lineitem_tscan_filter_t()
	{
		// Give back the lineitem tuple
		_tpchdb->lineitem_man()->give_tuple(_prline);
	}

	bool select(const tuple_t &input) {
		// Get next lineitem tuple and read its shipdate
		if (!_tpchdb->lineitem_man()->load(_prline, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		_prline->get_value(10, _lineitem.L_SHIPDATE, sizeof(_lineitem.L_SHIPDATE));
		_shipdate = str_to_timet(_lineitem.L_SHIPDATE);

		return _shipdate >= _first_shipdate && _shipdate < _last_shipdate;
	}

	void project(tuple_t &d, const tuple_t &s) {

		q20_projected_lineitem_tuple *dest = aligned_cast<q20_projected_lineitem_tuple>(d.data);

		_prline->get_value(1, _lineitem.L_PARTKEY);
		_prline->get_value(2, _lineitem.L_SUPPKEY);
		_prline->get_value(4, _lineitem.L_QUANTITY);

		//TRACE(TRACE_RECORD_FLOW, "%d|%d|%.2f\n", _lineitem.L_PARTKEY, _lineitem.L_SUPPKEY, _lineitem.L_QUANTITY);

		dest->L_PARTKEY = _lineitem.L_PARTKEY;
		dest->L_SUPPKEY = _lineitem.L_SUPPKEY;
		dest->L_QUANTITY = _lineitem.L_QUANTITY;
	}

	q20_lineitem_tscan_filter_t* clone() const {
		return new q20_lineitem_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q20_lineitem_tscan_filter_t()");
	}
};



struct q20_p_join_ps_t : public tuple_join_t {

	q20_p_join_ps_t()
	: tuple_join_t(sizeof(q20_projected_part_tuple),
			offsetof(q20_projected_part_tuple, P_PARTKEY),
			sizeof(q20_projected_partsupp_tuple),
			offsetof(q20_projected_partsupp_tuple, PS_PARTKEY),
			sizeof(int),
			sizeof(q20_p_join_ps_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q20_p_join_ps_tuple *dest = aligned_cast<q20_p_join_ps_tuple>(d.data);
		q20_projected_part_tuple *part = aligned_cast<q20_projected_part_tuple>(l.data);
		q20_projected_partsupp_tuple *partsupp = aligned_cast<q20_projected_partsupp_tuple>(r.data);

		dest->PS_AVAILQTY = partsupp->PS_AVAILQTY;
		dest->PS_PARTKEY = partsupp->PS_PARTKEY;
		dest->PS_SUPPKEY = partsupp->PS_SUPPKEY;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %d %d\n", part->P_PARTKEY, partsupp->PS_PARTKEY, partsupp->PS_AVAILQTY, partsupp->PS_PARTKEY, partsupp->PS_SUPPKEY);
	}

	virtual q20_p_join_ps_t* clone() const {
		return new q20_p_join_ps_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join PART, PARTSUPP; select PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY");
	}
};

struct q20_s_join_n_t : public tuple_join_t {

	q20_s_join_n_t()
	: tuple_join_t(sizeof(q20_projected_supplier_tuple),
			offsetof(q20_projected_supplier_tuple, S_NATIONKEY),
			sizeof(q20_projected_nation_tuple),
			offsetof(q20_projected_nation_tuple, N_NATIONKEY),
			sizeof(int),
			sizeof(q20_s_join_n_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q20_s_join_n_tuple *dest = aligned_cast<q20_s_join_n_tuple>(d.data);
		q20_projected_supplier_tuple *supp = aligned_cast<q20_projected_supplier_tuple>(l.data);
		q20_projected_nation_tuple *nation = aligned_cast<q20_projected_nation_tuple>(r.data);

		dest->S_SUPPKEY = supp->S_SUPPKEY;
		memcpy(dest->S_ADDRESS, supp->S_ADDRESS, sizeof(dest->S_ADDRESS));
		memcpy(dest->S_NAME, supp->S_NAME, sizeof(dest->S_NAME));

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %s %s\n", supp->S_NATIONKEY, nation->N_NATIONKEY, supp->S_SUPPKEY, supp->S_ADDRESS, supp->S_NAME);
	}

	virtual q20_s_join_n_t* clone() const {
		return new q20_s_join_n_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join SUPPLIER, NATION; select S_SUPPKEY, S_ADDRESS, S_NAME");
	}
};

struct q20_p_ps_join_s_n_t : public tuple_join_t {

	q20_p_ps_join_s_n_t()
	: tuple_join_t(sizeof(q20_p_join_ps_tuple),
			offsetof(q20_p_join_ps_tuple, PS_SUPPKEY),
			sizeof(q20_s_join_n_tuple),
			offsetof(q20_s_join_n_tuple, S_SUPPKEY),
			sizeof(int),
			sizeof(q20_p_ps_join_s_n_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q20_p_ps_join_s_n_tuple *dest = aligned_cast<q20_p_ps_join_s_n_tuple>(d.data);
		q20_p_join_ps_tuple *left = aligned_cast<q20_p_join_ps_tuple>(l.data);
		q20_s_join_n_tuple *right = aligned_cast<q20_s_join_n_tuple>(r.data);

		dest->PS_AVAILQTY = left->PS_AVAILQTY;
		dest->PS_PARTKEY = left->PS_PARTKEY;
		memcpy(dest->S_ADDRESS, right->S_ADDRESS, sizeof(dest->S_ADDRESS));
		memcpy(dest->S_NAME, right->S_NAME, sizeof(dest->S_NAME));
		dest->S_SUPPKEY = right->S_SUPPKEY;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %d %s %s %d\n", left->PS_SUPPKEY, right->S_SUPPKEY, left->PS_AVAILQTY, left->PS_PARTKEY, right->S_ADDRESS, right->S_NAME, right->S_SUPPKEY);
	}

	virtual q20_p_ps_join_s_n_t* clone() const {
		return new q20_p_ps_join_s_n_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join PART_PARTSUPP, SUPPLIER_NATION; select PS_AVAILQTY, PS_PARTKEY, S_ADDRESS, S_NAME, S_SUPPKEY");
	}
};

struct q20_lineitem_aggregate_t : public tuple_aggregate_t {

	default_key_extractor_t _extractor;

	q20_lineitem_aggregate_t()
	: tuple_aggregate_t(sizeof(q20_projected_lineitem_tuple))
	{
	}

	virtual key_extractor_t* key_extractor() {
		return &_extractor;
	}

	virtual void aggregate(char* agg_data, const tuple_t &t) {
		q20_projected_lineitem_tuple *agg = aligned_cast<q20_projected_lineitem_tuple>(agg_data);
		q20_projected_lineitem_tuple *input = aligned_cast<q20_projected_lineitem_tuple>(t.data);

		agg->L_PARTKEY = input->L_PARTKEY;
		agg->L_SUPPKEY = input->L_SUPPKEY;
		agg->L_QUANTITY += input->L_QUANTITY;
	}
	virtual void finish(tuple_t &dest, const char* agg_data) {
		memcpy(dest.data, agg_data, dest.size);
	}
	virtual q20_lineitem_aggregate_t* clone() const {
		return new q20_lineitem_aggregate_t(*this);
	}
	virtual c_str to_string() const {
		return "q20_lineitem_aggregate_t";
	}
};

struct q20_final_join_t : public tuple_join_t {

	q20_final_join_t()
	: tuple_join_t(sizeof(q20_projected_lineitem_tuple),
			offsetof(q20_projected_lineitem_tuple, L_PARTKEY),
			sizeof(q20_p_ps_join_s_n_tuple),
			offsetof(q20_p_ps_join_s_n_tuple, PS_PARTKEY),
			2*sizeof(int),
			sizeof(q20_all_joins_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q20_all_joins_tuple *dest = aligned_cast<q20_all_joins_tuple>(d.data);
		q20_projected_lineitem_tuple *line = aligned_cast<q20_projected_lineitem_tuple>(l.data);
		q20_p_ps_join_s_n_tuple *right = aligned_cast<q20_p_ps_join_s_n_tuple>(r.data);

		dest->AVAILQTY_THRESHOLD = line->L_QUANTITY;
		dest->PS_AVAILQTY = right->PS_AVAILQTY;
		memcpy(dest->S_ADDRESS, right->S_ADDRESS, sizeof(dest->S_ADDRESS));
		memcpy(dest->S_NAME, right->S_NAME, sizeof(dest->S_NAME));
		dest->S_SUPPKEY = right->S_SUPPKEY;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d AND %d=%d: %.2f %d %s %s %d\n", line->L_PARTKEY, right->PS_PARTKEY, line->L_SUPPKEY, right->S_SUPPKEY,
		//		line->L_QUANTITY.to_double(), right->PS_AVAILQTY, right->S_ADDRESS, right->S_NAME, right->S_SUPPKEY);
	}

	virtual q20_final_join_t* clone() const {
		return new q20_final_join_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join LINEITEM, PART_PARTSUPP_SUPPLIER_NATION; select AVAILQTY_THRESHOLD, PS_AVAILQTY, S_ADDRESS, S_NAME");
	}
};

struct q20_final_join_filter_t : public tuple_filter_t {

	q20_final_join_filter_t()
	: tuple_filter_t(sizeof(q20_all_joins_tuple))
	{
	}

	bool select(const tuple_t &input) {
		q20_all_joins_tuple *in = aligned_cast<q20_all_joins_tuple>(input.data);
		return in->PS_AVAILQTY > (0.5 * in->AVAILQTY_THRESHOLD.to_double());
	}

	virtual void project(tuple_t &out, const tuple_t &in) {
		q20_final_tuple *d = aligned_cast<q20_final_tuple>(out.data);
		q20_all_joins_tuple *s = aligned_cast<q20_all_joins_tuple>(in.data);

		d->S_SUPPKEY = s->S_SUPPKEY;
		memcpy(d->S_NAME, s->S_NAME, sizeof(d->S_NAME));
		memcpy(d->S_ADDRESS, s->S_ADDRESS, sizeof(d->S_ADDRESS));

		//TRACE(TRACE_RECORD_FLOW, "%d\t%s\t%s\n", d->S_SUPPKEY, d->S_NAME, d->S_ADDRESS);
	}

	virtual q20_final_join_filter_t* clone() const {
		return new q20_final_join_filter_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("q20_final_join_filter_t");
	}
};

struct q20_distinct_agg_t : public tuple_aggregate_t {

	default_key_extractor_t _extractor;

	q20_distinct_agg_t()
	: tuple_aggregate_t(sizeof(q20_final_tuple)), _extractor(sizeof(int), offsetof(q20_final_tuple, S_SUPPKEY))
	{
	}

	virtual key_extractor_t* key_extractor() {
		return &_extractor;
	}

	virtual void aggregate(char* agg_data, const tuple_t &t) {
		memcpy(agg_data, t.data, sizeof(q20_final_tuple));
	}
	virtual void finish(tuple_t &dest, const char* agg_data) {
		memcpy(dest.data, agg_data, dest.size);
	}
	virtual q20_distinct_agg_t* clone() const {
		return new q20_distinct_agg_t(*this);
	}
	virtual c_str to_string() const {
		return "q20_distinct_agg_t";
	}
};

struct q20_sort_key_compare_t : public key_compare_t {

	q20_sort_key_compare_t()
	{
	}

	virtual int operator()(const void* key1, const void* key2) const {
		return strcmp((char*)key1, (char*)key2);
	}

	virtual q20_sort_key_compare_t* clone() const {
		return new q20_sort_key_compare_t(*this);
	}
};


class tpch_q20_process_tuple_t : public process_tuple_t {

public:

	virtual void begin() {
		TRACE(TRACE_QUERY_RESULTS, "*** Q20 %s %s\n",
				"S_NAME", "S_ADDRESS");
	}

	virtual void process(const tuple_t& output) {
		q20_final_tuple *agg = aligned_cast<q20_final_tuple>(output.data);

		TRACE(TRACE_QUERY_RESULTS, "*** Q20 %s %s\n", agg->S_NAME, agg->S_ADDRESS);
	}

};


/********************************************************************
 *
 * QPIPE q20 - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_qpipe_q20(const int xct_id,
		q20_input_t& in)
{
	TRACE( TRACE_ALWAYS, "********** q20 *********\n");

	policy_t* dp = this->get_sched_policy();
	xct_t* pxct = smthread_t::me()->xct();


	//TSCAN NATION
	tuple_fifo* q20_nation_buffer = new tuple_fifo(sizeof(q20_projected_nation_tuple));
	packet_t* q20_nation_tscan_packet =
			new tscan_packet_t("nation TSCAN",
					q20_nation_buffer,
					new q20_nation_tscan_filter_t(this, in),
					this->db(),
					_pnation_desc.get(),
					pxct);

	//TSCAN SUPPLIER
	tuple_fifo* q20_supplier_buffer = new tuple_fifo(sizeof(q20_projected_supplier_tuple));
	packet_t* q20_supplier_tscan_packet =
			new tscan_packet_t("supplier TSCAN",
					q20_supplier_buffer,
					new q20_supplier_tscan_filter_t(this, in),
					this->db(),
					_psupplier_desc.get(),
					pxct);

	//TSCAN PARTSUPP
	tuple_fifo* q20_partsupp_buffer = new tuple_fifo(sizeof(q20_projected_partsupp_tuple));
	packet_t* q20_partsupp_tscan_packet =
			new tscan_packet_t("partsupp TSCAN",
					q20_partsupp_buffer,
					new q20_partsupp_tscan_filter_t(this, in),
					this->db(),
					_ppartsupp_desc.get(),
					pxct);

	//TSCAN PART
	tuple_fifo* q20_part_buffer = new tuple_fifo(sizeof(q20_projected_part_tuple));
	packet_t* q20_part_tscan_packet =
			new tscan_packet_t("part TSCAN",
					q20_part_buffer,
					new q20_part_tscan_filter_t(this, in),
					this->db(),
					_ppart_desc.get(),
					pxct);

	//TSCAN LINEITEM
	tuple_fifo* q20_lineitem_buffer = new tuple_fifo(sizeof(q20_projected_lineitem_tuple));
	packet_t* q20_lineitem_tscan_packet =
			new tscan_packet_t("lineitem TSCAN",
					q20_lineitem_buffer,
					new q20_lineitem_tscan_filter_t(this, in),
					this->db(),
					_plineitem_desc.get(),
					pxct);

	//AGGREGATE LINEITEM
	tuple_fifo* q20_lineitem_aggregate_buffer = new tuple_fifo(sizeof(q20_projected_lineitem_tuple));
	packet_t* q20_lineitem_aggregate_packet =
			new partial_aggregate_packet_t("lineitem SUM AGGREGATE L_QUANTITY",
					q20_lineitem_aggregate_buffer,
					new trivial_filter_t(sizeof(q20_projected_lineitem_tuple)),
					q20_lineitem_tscan_packet,
					new q20_lineitem_aggregate_t(),
					new default_key_extractor_t(2 * sizeof(int), offsetof(q20_projected_lineitem_tuple, L_PARTKEY)),
					new int_key_compare_t());


	//SUPPLIER JOIN NATION
	tuple_fifo* q20_s_join_n_buffer = new tuple_fifo(sizeof(q20_s_join_n_tuple));
	packet_t* q20_s_join_n_packet =
			new hash_join_packet_t("supplier - nation HJOIN",
					q20_s_join_n_buffer,
					new trivial_filter_t(sizeof(q20_s_join_n_tuple)),
					q20_supplier_tscan_packet,
					q20_nation_tscan_packet,
					new q20_s_join_n_t());

	//PART JOIN PARTSUPP
	tuple_fifo* q20_p_join_ps_buffer = new tuple_fifo(sizeof(q20_p_join_ps_tuple));
	packet_t* q20_p_join_ps_packet =
			new hash_join_packet_t("part - partsupp HJOIN",
					q20_p_join_ps_buffer,
					new trivial_filter_t(sizeof(q20_p_join_ps_tuple)),
					q20_part_tscan_packet,
					q20_partsupp_tscan_packet,
					new q20_p_join_ps_t());

	//PART_PARTSUPP JOIN SUPPLIER_NATION
	tuple_fifo* q20_p_ps_join_s_n_buffer = new tuple_fifo(sizeof(q20_p_ps_join_s_n_tuple));
	packet_t* q20_p_ps_join_s_n_packet =
			new hash_join_packet_t("part_partsupp - supplier_nation HJOIN",
					q20_p_ps_join_s_n_buffer,
					new trivial_filter_t(sizeof(q20_p_ps_join_s_n_tuple)),
					q20_p_join_ps_packet,
					q20_s_join_n_packet,
					new q20_p_ps_join_s_n_t());

	//LINEITEM JOIN PART_PARTSUPP_SUPPLIER_NATION
	tuple_fifo* q20_all_joins_buffer = new tuple_fifo(sizeof(q20_final_tuple));
	packet_t* q20_all_joins_packet =
			new hash_join_packet_t("lineitem - part_partsupp_supplier_nation HJOIN",
					q20_all_joins_buffer,
					new q20_final_join_filter_t(),
					q20_lineitem_aggregate_packet,
					q20_p_ps_join_s_n_packet,
					new q20_final_join_t());

	tuple_fifo* q20_distinct_agg_buffer = new tuple_fifo(sizeof(q20_final_tuple));
	packet_t* q20_distinct_agg_packet =
			new partial_aggregate_packet_t("DISTINCT AGG",
					q20_distinct_agg_buffer,
					new trivial_filter_t(sizeof(q20_final_tuple)),
					q20_all_joins_packet,
					new q20_distinct_agg_t(),
					new default_key_extractor_t(sizeof(int), offsetof(q20_final_tuple, S_SUPPKEY)),
					new int_key_compare_t());

	//SORT BY S_NAME
	tuple_fifo* q20_final_buffer = new tuple_fifo(sizeof(q20_final_tuple));
	packet_t* q20_final_packet =
			new sort_packet_t("SORT BY S_NAME",
					q20_final_buffer,
					new trivial_filter_t(sizeof(q20_final_tuple)),
					new default_key_extractor_t(STRSIZE(25) * sizeof(char), offsetof(q20_final_tuple, S_NAME)),
					new q20_sort_key_compare_t(),
					q20_distinct_agg_packet);


	qpipe::query_state_t* qs = dp->query_state_create();
	q20_nation_tscan_packet->assign_query_state(qs);
	q20_supplier_tscan_packet->assign_query_state(qs);
	q20_partsupp_tscan_packet->assign_query_state(qs);
	q20_part_tscan_packet->assign_query_state(qs);
	q20_lineitem_tscan_packet->assign_query_state(qs);
	q20_lineitem_aggregate_packet->assign_query_state(qs);
	q20_s_join_n_packet->assign_query_state(qs);
	q20_p_join_ps_packet->assign_query_state(qs);
	q20_p_ps_join_s_n_packet->assign_query_state(qs);
	q20_all_joins_packet->assign_query_state(qs);
	q20_distinct_agg_packet->assign_query_state(qs);
	q20_final_packet->assign_query_state(qs);


	// Dispatch packet
	tpch_q20_process_tuple_t pt;
	//LAST PACKET
	process_query(q20_final_packet, pt);//TODO

	dp->query_state_destroy(qs);


	return (RCOK);
}

EXIT_NAMESPACE(tpch);
