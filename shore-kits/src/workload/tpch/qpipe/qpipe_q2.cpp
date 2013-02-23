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

/** @file:   qpipe_q2.cpp
 *
 *  @brief:  Implementation of QPIPE TPCH Q2 over Shore-MT
 *
 *  @author:    Andreas Schädeli
 *  @date:  2011-10-17
 */

#include "workload/tpch/shore_tpch_env.h"
#include "workload/tpch/tpch_util.h"
#include "qpipe.h"

using namespace shore;
using namespace qpipe;



ENTER_NAMESPACE(tpch);


/********************************************************************
 *
 * QPIPE Q2 - Structures needed by operators
 *
 ********************************************************************/

/*
select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
    part,
    supplier,
    partsupp,
    nation,
    region
where
    p_partkey = ps_partkey
    and s_suppkey = ps_suppkey
    and p_size = [SIZE]
    and p_type like '%[TYPE]'
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = '[REGION]'
    and ps_supplycost = (
        select
            min(ps_supplycost)
        from
            partsupp, supplier,
            nation, region
        where
            p_partkey = ps_partkey
            and s_suppkey = ps_suppkey
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and r_name = '[REGION]'
    )
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey;
 */


//The part tuples after tablescan projection
struct q2_projected_part_tuple {
	int P_PARTKEY;
	char P_MFGR[STRSIZE(25)];
};

//The partsupp tuples after tablescan projection
struct q2_projected_partsupp_tuple {
	int PS_SUPPKEY;
	int PS_PARTKEY;
	decimal PS_SUPPLYCOST;
};

//The supplier tuples after tablescan projection for main query
struct q2_projected_supplier_tuple {
	int S_SUPPKEY;
	char S_NAME[STRSIZE(25)];
	char S_ADDRESS[STRSIZE(40)];
	int S_NATIONKEY;
	char S_PHONE[STRSIZE(15)];
	decimal S_ACCTBAL;
	char S_COMMENT[STRSIZE(101)];
};

//The nation tuples after tablescan projection for main query
struct q2_projected_nation_tuple {
	int N_NATIONKEY;
	char N_NAME[STRSIZE(25)];
	int N_REGIONKEY;
};

//The region tuples after tablescan projection
struct q2_projected_region_tuple {
	int R_REGIONKEY;
};


//The supplier tuples after tablescan projection for subquery
struct q2_projected_supplier_subquery_tuple {
	int S_SUPPKEY;
	int S_NATIONKEY;
};

//The nation tuples after tablescan projection for subquery
struct q2_projected_nation_subquery_tuple {
	int N_NATIONKEY;
	int N_REGIONKEY;
};


struct q2_ps_join_p_tuple {
	int PS_SUPPKEY;
	int P_PARTKEY;
	decimal PS_SUPPLYCOST;
	char P_MFGR[STRSIZE(25)];
};

struct q2_s_join_ps_p_tuple {
	int P_PARTKEY;
	decimal PS_SUPPLYCOST;
	char P_MFGR[STRSIZE(25)];
	char S_NAME[STRSIZE(25)];
	char S_ADDRESS[STRSIZE(40)];
	int S_NATIONKEY;
	char S_PHONE[STRSIZE(15)];
	decimal S_ACCTBAL;
	char S_COMMENT[STRSIZE(101)];
};

struct q2_s_ps_p_join_n_tuple {
	int P_PARTKEY;
	decimal PS_SUPPLYCOST;
	char P_MFGR[STRSIZE(25)];
	char S_NAME[STRSIZE(25)];
	char S_ADDRESS[STRSIZE(40)];
	char S_PHONE[STRSIZE(15)];
	decimal S_ACCTBAL;
	char S_COMMENT[STRSIZE(101)];
	char N_NAME[STRSIZE(25)];
	int N_REGIONKEY;
};

struct q2_s_ps_p_n_join_r_tuple {
	int P_PARTKEY;
	decimal PS_SUPPLYCOST;
	char P_MFGR[STRSIZE(25)];
	char S_NAME[STRSIZE(25)];
	char S_ADDRESS[STRSIZE(40)];
	char S_PHONE[STRSIZE(15)];
	decimal S_ACCTBAL;
	char S_COMMENT[STRSIZE(101)];
	char N_NAME[STRSIZE(25)];
};

struct q2_n_join_r_subquery_tuple {
	int N_NATIONKEY;
};

struct q2_s_join_n_r_subquery_tuple {
	int S_SUPPKEY;
};

struct q2_ps_join_s_n_r_subquery_tuple {
	int PS_PARTKEY;
	decimal PS_SUPPLYCOST;
};


struct q2_aggregate_tuple {
	decimal S_ACCTBAL;
	char N_NAME[STRSIZE(25)];
	char S_NAME[STRSIZE(25)];
	int P_PARTKEY;
	char P_MFGR[STRSIZE(25)];
	char S_ADDRESS[STRSIZE(40)];
	char S_PHONE[STRSIZE(15)];
	char S_COMMENT[STRSIZE(101)];
};

struct q2_subquery_aggregate_tuple {
	int PS_PARTKEY;
	decimal PS_SUPPLYCOST;
};

struct q2_sort_key {
	decimal S_ACCTBAL;
	char N_NAME[STRSIZE(25)];
	char S_NAME[STRSIZE(25)];
	int P_PARTKEY;
};


class q2_part_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prpart;
	rep_row_t _rr;

	/*One part tuple*/
	tpch_part_tuple _part;
	/*The columns needed for the selection*/
	int _size;
	char _type[STRSIZE(25)];

	q2_input_t* q2_input;

public:

	q2_part_tscan_filter_t(ShoreTPCHEnv* tpchdb, q2_input_t &in)
	: tuple_filter_t(tpchdb->part_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prpart = _tpchdb->part_man()->get_tuple();
		_rr.set_ts(_tpchdb->part_man()->ts(),
				_tpchdb->part_desc()->maxsize());
		_prpart->_rep = &_rr;

		q2_input = &in;
		_size = q2_input->p_size;
		types3_to_str(_type, q2_input->p_types3);

		TRACE(TRACE_ALWAYS, "Random predicates:\nPART.P_SIZE=%d\tPART.P_TYPE like '%%%s'\n", _size, _type);
	}

	virtual ~q2_part_tscan_filter_t()
	{
		// Give back the part tuple
		_tpchdb->part_man()->give_tuple(_prpart);
	}



	bool select(const tuple_t &input) {

		// Get next part and read its size and type
		if (!_tpchdb->part_man()->load(_prpart, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		_prpart->get_value(5, _part.P_SIZE);
		_prpart->get_value(4, _part.P_TYPE, 25);

		return _part.P_SIZE == _size && strstr(_part.P_TYPE, _type);
	}


	void project(tuple_t &d, const tuple_t &s) {

		q2_projected_part_tuple *dest;
		dest = aligned_cast<q2_projected_part_tuple>(d.data);

		_prpart->get_value(0, _part.P_PARTKEY);
		_prpart->get_value(2, _part.P_MFGR, 25);

		/*TRACE( TRACE_RECORD_FLOW, "%d|%s\n",
				_part.P_PARTKEY,
				_part.P_MFGR);*/

		dest->P_PARTKEY = _part.P_PARTKEY;
		memcpy(dest->P_MFGR, _part.P_MFGR, sizeof(dest->P_MFGR));

	}

	q2_part_tscan_filter_t* clone() const {
		return new q2_part_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q2_part_tscan_filter_t(%d, %s)", _size, _type);
	}
};

class q2_partsupp_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prpartsupp;
	rep_row_t _rr;

	/*One partsupp tuple*/
	tpch_partsupp_tuple _partsupp;

public:
	q2_partsupp_tscan_filter_t(ShoreTPCHEnv* tpchdb, q2_input_t &in)
	: tuple_filter_t(tpchdb->partsupp_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prpartsupp = _tpchdb->partsupp_man()->get_tuple();
		_rr.set_ts(_tpchdb->partsupp_man()->ts(),
				_tpchdb->partsupp_desc()->maxsize());
		_prpartsupp->_rep = &_rr;
	}

	virtual ~q2_partsupp_tscan_filter_t()
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

		q2_projected_partsupp_tuple *dest;
		dest = aligned_cast<q2_projected_partsupp_tuple>(d.data);

		_prpartsupp->get_value(1, _partsupp.PS_SUPPKEY);
		_prpartsupp->get_value(0, _partsupp.PS_PARTKEY);
		_prpartsupp->get_value(3, _partsupp.PS_SUPPLYCOST);

		/*TRACE( TRACE_RECORD_FLOW, "%d|%d|%.4f\n",
				_partsupp.PS_SUPPKEY,
				_partsupp.PS_PARTKEY,
				_partsupp.PS_SUPPLYCOST.to_double());*/

		dest->PS_SUPPKEY = _partsupp.PS_SUPPKEY;
		dest->PS_PARTKEY = _partsupp.PS_PARTKEY;
		dest->PS_SUPPLYCOST = _partsupp.PS_SUPPLYCOST;

	}

	q2_partsupp_tscan_filter_t* clone() const {
		return new q2_partsupp_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q2_partsupp_tscan_filter_t()");
	}
};


class q2_supplier_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prsupplier;
	rep_row_t _rr;

	/*One supplier tuple*/
	tpch_supplier_tuple _supplier;

public:
	q2_supplier_tscan_filter_t(ShoreTPCHEnv* tpchdb, q2_input_t &in)
	: tuple_filter_t(tpchdb->supplier_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prsupplier = _tpchdb->supplier_man()->get_tuple();
		_rr.set_ts(_tpchdb->supplier_man()->ts(),
				_tpchdb->supplier_desc()->maxsize());
		_prsupplier->_rep = &_rr;
	}

	virtual ~q2_supplier_tscan_filter_t()
	{
		_tpchdb->supplier_man()->give_tuple(_prsupplier);
	}

	bool select(const tuple_t &input) {

		// Get next supplier tuple and read its size and type
		if (!_tpchdb->supplier_man()->load(_prsupplier, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		return true;
	}


	void project(tuple_t &d, const tuple_t &s) {

		q2_projected_supplier_tuple *dest;
		dest = aligned_cast<q2_projected_supplier_tuple>(d.data);


		_prsupplier->get_value(0, _supplier.S_SUPPKEY);
		_prsupplier->get_value(1, _supplier.S_NAME, sizeof(_supplier.S_NAME));
		_prsupplier->get_value(2, _supplier.S_ADDRESS, sizeof(_supplier.S_ADDRESS));
		_prsupplier->get_value(3, _supplier.S_NATIONKEY);
		_prsupplier->get_value(4, _supplier.S_PHONE, sizeof(_supplier.S_PHONE));
		_prsupplier->get_value(5, _supplier.S_ACCTBAL);
		_prsupplier->get_value(6, _supplier.S_COMMENT, sizeof(_supplier.S_COMMENT));

		/*TRACE( TRACE_RECORD_FLOW, "%d|%s|%s|%d|%s|%.4f|%s\n",
				_supplier.S_SUPPKEY, _supplier.S_NAME, _supplier.S_ADDRESS, _supplier.S_NATIONKEY, _supplier.S_PHONE, _supplier.S_ACCTBAL.to_double(), _supplier.S_COMMENT);*/

		dest->S_SUPPKEY = _supplier.S_SUPPKEY;
		memcpy(dest->S_NAME, _supplier.S_NAME, sizeof(dest->S_NAME));
		memcpy(dest->S_ADDRESS, _supplier.S_ADDRESS, sizeof(dest->S_ADDRESS));
		dest->S_NATIONKEY = _supplier.S_NATIONKEY;
		memcpy(dest->S_PHONE, _supplier.S_PHONE, sizeof(dest->S_PHONE));
		dest->S_ACCTBAL = _supplier.S_ACCTBAL;
		memcpy(dest->S_COMMENT, _supplier.S_COMMENT, sizeof(dest->S_COMMENT));

	}

	q2_supplier_tscan_filter_t* clone() const {
		return new q2_supplier_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q2_supplier_tscan_filter_t()");
	}
};


class q2_supplier_subquery_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prsupplier;
	rep_row_t _rr;

	/*One supplier tuple*/
	tpch_supplier_tuple _supplier;

public:
	q2_supplier_subquery_tscan_filter_t(ShoreTPCHEnv* tpchdb, q2_input_t &in)
	: tuple_filter_t(tpchdb->supplier_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prsupplier = _tpchdb->supplier_man()->get_tuple();
		_rr.set_ts(_tpchdb->supplier_man()->ts(),
				_tpchdb->supplier_desc()->maxsize());
		_prsupplier->_rep = &_rr;
	}

	virtual ~q2_supplier_subquery_tscan_filter_t()
	{
		_tpchdb->supplier_man()->give_tuple(_prsupplier);
	}

	bool select(const tuple_t &input) {

		// Get next supplier tuple and read its size and type
		if (!_tpchdb->supplier_man()->load(_prsupplier, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		return true;
	}


	void project(tuple_t &d, const tuple_t &s) {

		q2_projected_supplier_subquery_tuple *dest;
		dest = aligned_cast<q2_projected_supplier_subquery_tuple>(d.data);


		_prsupplier->get_value(0, _supplier.S_SUPPKEY);
		_prsupplier->get_value(3, _supplier.S_NATIONKEY);

		/*TRACE( TRACE_RECORD_FLOW, "%d|%d\n",
				_supplier.S_SUPPKEY, _supplier.S_NATIONKEY);*/

		dest->S_SUPPKEY = _supplier.S_SUPPKEY;
		dest->S_NATIONKEY = _supplier.S_NATIONKEY;
	}

	q2_supplier_subquery_tscan_filter_t* clone() const {
		return new q2_supplier_subquery_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q2_supplier_subquery_tscan_filter_t()");
	}
};


class q2_nation_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prnation;
	rep_row_t _rr;

	/*One nation tuple*/
	tpch_nation_tuple _nation;

public:
	q2_nation_tscan_filter_t(ShoreTPCHEnv* tpchdb, q2_input_t &in)
	: tuple_filter_t(tpchdb->nation_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prnation = _tpchdb->nation_man()->get_tuple();
		_rr.set_ts(_tpchdb->nation_man()->ts(),
				_tpchdb->nation_desc()->maxsize());
		_prnation->_rep = &_rr;
	}

	virtual ~q2_nation_tscan_filter_t()
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

		q2_projected_nation_tuple *dest;
		dest = aligned_cast<q2_projected_nation_tuple>(d.data);

		_prnation->get_value(0, _nation.N_NATIONKEY);
		_prnation->get_value(1, _nation.N_NAME, 25);
		_prnation->get_value(2, _nation.N_REGIONKEY);

		/*TRACE( TRACE_RECORD_FLOW, "%d|%s|%d\n",
				_nation.N_NATIONKEY, _nation.N_NAME, _nation.N_REGIONKEY);*/

		dest->N_NATIONKEY = _nation.N_NATIONKEY;
		memcpy(dest->N_NAME, _nation.N_NAME, sizeof(dest->N_NAME));
		dest->N_REGIONKEY = _nation.N_REGIONKEY;
	}

	q2_nation_tscan_filter_t* clone() const {
		return new q2_nation_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q2_nation_tscan_filter_t()");
	}
};


class q2_nation_subquery_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prnation;
	rep_row_t _rr;

	/*One nation tuple*/
	tpch_nation_tuple _nation;

public:
	q2_nation_subquery_tscan_filter_t(ShoreTPCHEnv* tpchdb, q2_input_t &in)
	: tuple_filter_t(tpchdb->nation_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prnation = _tpchdb->nation_man()->get_tuple();
		_rr.set_ts(_tpchdb->nation_man()->ts(),
				_tpchdb->nation_desc()->maxsize());
		_prnation->_rep = &_rr;
	}

	virtual ~q2_nation_subquery_tscan_filter_t()
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

		q2_projected_nation_subquery_tuple *dest;
		dest = aligned_cast<q2_projected_nation_subquery_tuple>(d.data);

		_prnation->get_value(0, _nation.N_NATIONKEY);
		_prnation->get_value(2, _nation.N_REGIONKEY);

		//TRACE( TRACE_RECORD_FLOW, "%d|%d\n",
		//	_nation.N_NATIONKEY, _nation.N_REGIONKEY);

		dest->N_NATIONKEY = _nation.N_NATIONKEY;
		dest->N_REGIONKEY = _nation.N_REGIONKEY;
	}

	q2_nation_subquery_tscan_filter_t* clone() const {
		return new q2_nation_subquery_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q2_nation_subquery_tscan_filter_t()");
	}
};


class q2_region_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prregion;
	rep_row_t _rr;

	/*One region tuple*/
	tpch_region_tuple _region;

	char _name[STRSIZE(25)];
	q2_input_t* q2_input;

public:
	q2_region_tscan_filter_t(ShoreTPCHEnv* tpchdb, q2_input_t &in)
	: tuple_filter_t(tpchdb->region_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prregion = _tpchdb->region_man()->get_tuple();
		_rr.set_ts(_tpchdb->region_man()->ts(),
				_tpchdb->nation_desc()->maxsize());
		_prregion->_rep = &_rr;

		q2_input = &in;
		region_to_str(q2_input->r_name, _name);

		TRACE(TRACE_ALWAYS, "Random predicates:\nREGION.R_NAME = '%s'\n", _name);
	}

	virtual ~q2_region_tscan_filter_t()
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

		q2_projected_region_tuple *dest;
		dest = aligned_cast<q2_projected_region_tuple>(d.data);

		_prregion->get_value(0, _region.R_REGIONKEY);

		/*TRACE( TRACE_RECORD_FLOW, "%d\n",
				_region.R_REGIONKEY);*/

		dest->R_REGIONKEY = _region.R_REGIONKEY;
	}

	q2_region_tscan_filter_t* clone() const {
		return new q2_region_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q2_region_tscan_filter_t(%s)", _name);
	}
};


struct q2_ps_join_p_t : public tuple_join_t
{

	q2_ps_join_p_t()
	: tuple_join_t(sizeof(q2_projected_partsupp_tuple),
			offsetof(q2_projected_partsupp_tuple, PS_PARTKEY),
			sizeof(q2_projected_part_tuple),
			offsetof(q2_projected_part_tuple, P_PARTKEY),
			sizeof(int),
			sizeof(q2_ps_join_p_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q2_ps_join_p_tuple *dest = aligned_cast<q2_ps_join_p_tuple>(d.data);
		q2_projected_partsupp_tuple *partsupp = aligned_cast<q2_projected_partsupp_tuple>(l.data);
		q2_projected_part_tuple *part = aligned_cast<q2_projected_part_tuple>(r.data);

		dest->PS_SUPPKEY = partsupp->PS_SUPPKEY;
		dest->PS_SUPPLYCOST = partsupp->PS_SUPPLYCOST;
		dest->P_PARTKEY = part->P_PARTKEY;
		memcpy(dest->P_MFGR, part->P_MFGR, sizeof(dest->P_MFGR));

		TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %d %.4f %s\n", partsupp->PS_PARTKEY, part->P_PARTKEY, part->P_PARTKEY, partsupp->PS_SUPPKEY, partsupp->PS_SUPPLYCOST.to_double(),
				part->P_MFGR);
	}

	virtual q2_ps_join_p_t* clone() const {
		return new q2_ps_join_p_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("JOIN PARTSUPP, PART; SELECT P_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST, P_MFGR");
	}
};

struct q2_s_join_ps_p_t : public tuple_join_t
{

	q2_s_join_ps_p_t()
	: tuple_join_t(sizeof(q2_projected_supplier_tuple),
			offsetof(q2_projected_supplier_tuple, S_SUPPKEY),
			sizeof(q2_ps_join_p_tuple),
			offsetof(q2_ps_join_p_tuple, PS_SUPPKEY),
			sizeof(int),
			sizeof(q2_s_join_ps_p_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q2_s_join_ps_p_tuple *dest = aligned_cast<q2_s_join_ps_p_tuple>(d.data);
		q2_projected_supplier_tuple *supplier = aligned_cast<q2_projected_supplier_tuple>(l.data);
		q2_ps_join_p_tuple *right = aligned_cast<q2_ps_join_p_tuple>(r.data);

		dest->S_NATIONKEY = supplier->S_NATIONKEY;
		dest->P_PARTKEY = right->P_PARTKEY;
		dest->PS_SUPPLYCOST = right->PS_SUPPLYCOST;
		dest->S_ACCTBAL = supplier->S_ACCTBAL;
		memcpy(dest->S_NAME, supplier->S_NAME, sizeof(dest->S_NAME));
		memcpy(dest->S_ADDRESS, supplier->S_ADDRESS, sizeof(dest->S_ADDRESS));
		memcpy(dest->S_PHONE, supplier->S_PHONE, sizeof(dest->S_PHONE));
		memcpy(dest->S_COMMENT, supplier->S_COMMENT, sizeof(dest->S_COMMENT));
		memcpy(dest->P_MFGR, right->P_MFGR, sizeof(dest->P_MFGR));

		/*TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %d %.4f %.4f %s %s %s %s %s\n", supplier->S_SUPPKEY, right->PS_SUPPKEY, supplier->S_NATIONKEY, right->P_PARTKEY,
				right->PS_SUPPLYCOST.to_double(), supplier->S_ACCTBAL.to_double(), supplier->S_NAME, supplier->S_ADDRESS,
				supplier->S_PHONE, supplier->S_COMMENT, right->P_MFGR);*/
	}

	virtual q2_s_join_ps_p_t* clone() const {
		return new q2_s_join_ps_p_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("JOIN SUPPLIER, PARTSUPP_PART; SELECT S_SUPPKEY, PS_SUPPKEY, S_NATIONKEY, P_PARTKEY, PS_SUPPLYCOST, S_ACCTBAL, S_NAME, S_ADDRESS, S_PHONE, S_COMMENT, P_MFGR");
	}
};

struct q2_s_ps_p_join_n_t : public tuple_join_t
{

	q2_s_ps_p_join_n_t()
	: tuple_join_t(sizeof(q2_s_join_ps_p_tuple),
			offsetof(q2_s_join_ps_p_tuple, S_NATIONKEY),
			sizeof(q2_projected_nation_tuple),
			offsetof(q2_projected_nation_tuple, N_NATIONKEY),
			sizeof(int),
			sizeof(q2_s_ps_p_join_n_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q2_s_ps_p_join_n_tuple *dest = aligned_cast<q2_s_ps_p_join_n_tuple>(d.data);
		q2_s_join_ps_p_tuple *left = aligned_cast<q2_s_join_ps_p_tuple>(l.data);
		q2_projected_nation_tuple *nation = aligned_cast<q2_projected_nation_tuple>(r.data);

		dest->N_REGIONKEY = nation->N_REGIONKEY;
		dest->P_PARTKEY = left->P_PARTKEY;
		dest->PS_SUPPLYCOST = left->PS_SUPPLYCOST;
		dest->S_ACCTBAL = left->S_ACCTBAL;
		memcpy(dest->N_NAME, nation->N_NAME, sizeof(dest->N_NAME));
		memcpy(dest->P_MFGR, left->P_MFGR, sizeof(dest->P_MFGR));
		memcpy(dest->S_ADDRESS, left->S_ADDRESS, sizeof(dest->S_ADDRESS));
		memcpy(dest->S_COMMENT, left->S_COMMENT, sizeof(dest->S_COMMENT));
		memcpy(dest->S_NAME, left->S_NAME, sizeof(dest->S_NAME));
		memcpy(dest->S_PHONE, left->S_PHONE, sizeof(dest->S_PHONE));

		TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %d %.4f %.4f %s %s %s %s %s %s\n", left->S_NATIONKEY, nation->N_NATIONKEY, nation->N_REGIONKEY, left->P_PARTKEY,
				left->PS_SUPPLYCOST.to_double(), left->S_ACCTBAL.to_double(), nation->N_NAME, left->P_MFGR,
				left->S_ADDRESS, left->S_COMMENT, left->S_NAME,	left->S_PHONE);
	}

	virtual q2_s_ps_p_join_n_t* clone() const {
		return new q2_s_ps_p_join_n_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("JOIN SUPPLIER_PARTSUPP_PART, NATION; SELECT N_REGIONKEY, P_PARTKEY, PS_SUPPLYCOST, S_ACCTBAL, N_NAME, P_MFGR, S_ADDRESS, S_COMMENT, S_NAME, S_PHONE");
	}
};

struct q2_s_ps_p_n_join_r_t : public tuple_join_t
{

	q2_s_ps_p_n_join_r_t()
	: tuple_join_t(sizeof(q2_s_ps_p_join_n_tuple),
			offsetof(q2_s_ps_p_join_n_tuple, N_REGIONKEY),
			sizeof(q2_projected_region_tuple),
			offsetof(q2_projected_region_tuple, R_REGIONKEY),
			sizeof(int),
			sizeof(q2_s_ps_p_n_join_r_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q2_s_ps_p_n_join_r_tuple *dest = aligned_cast<q2_s_ps_p_n_join_r_tuple>(d.data);
		q2_s_ps_p_join_n_tuple *left = aligned_cast<q2_s_ps_p_join_n_tuple>(l.data);
		q2_projected_region_tuple *region = aligned_cast<q2_projected_region_tuple>(r.data);

		dest->P_PARTKEY = left->P_PARTKEY;
		dest->PS_SUPPLYCOST = left->PS_SUPPLYCOST;
		dest->S_ACCTBAL = left->S_ACCTBAL;
		memcpy(dest->N_NAME, left->N_NAME, sizeof(dest->N_NAME));
		memcpy(dest->P_MFGR, left->P_MFGR, sizeof(dest->P_MFGR));
		memcpy(dest->S_ADDRESS, left->S_ADDRESS, sizeof(dest->S_ADDRESS));
		memcpy(dest->S_COMMENT, left->S_COMMENT, sizeof(dest->S_COMMENT));
		memcpy(dest->S_NAME, left->S_NAME, sizeof(dest->S_NAME));
		memcpy(dest->S_PHONE, left->S_PHONE, sizeof(dest->S_PHONE));

		TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %.4f %.4f %s %s %s %s %s %s\n", left->N_REGIONKEY, region->R_REGIONKEY, left->P_PARTKEY, left->PS_SUPPLYCOST.to_double(),
				left->S_ACCTBAL.to_double(), left->N_NAME, left->P_MFGR, left->S_ADDRESS, left->S_COMMENT,
				left->S_NAME, left->S_PHONE);
	}

	virtual q2_s_ps_p_n_join_r_t* clone() const {
		return new q2_s_ps_p_n_join_r_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("JOIN SUPPLIER_PARTSUPP_PART_NATION, REGION; SELECT P_PARTKEY, PS_SUPPLYCOST, S_ACCTBAL, N_NAME, P_MFGR, S_ADDRESS, S_COMMENT, S_NAME, S_PHONE");
	}
};

struct q2_sort_key_extractor_t : public key_extractor_t
{

	q2_sort_key_extractor_t()
	: key_extractor_t(sizeof(q2_sort_key), offsetof(q2_sort_key, S_ACCTBAL))
	{
	}

	virtual int extract_hint(const char *key) const {
		q2_sort_key *sort_key = aligned_cast<q2_sort_key>(key);
		return sort_key->S_ACCTBAL.to_int();
	}

	virtual q2_sort_key_extractor_t* clone() const {
		return new q2_sort_key_extractor_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("q2_sort_key_extractor_t");
	}
};

struct q2_sort_key_compare_t : public key_compare_t
{

	virtual int operator()(const void* k1, const void* k2) const {
		q2_sort_key *key1 = aligned_cast<q2_sort_key>(k1);
		q2_sort_key *key2 = aligned_cast<q2_sort_key>(k2);

		int diff_acctbal = key1->S_ACCTBAL < key2->S_ACCTBAL ? 1 : key1->S_ACCTBAL == key2->S_ACCTBAL ? 0 : -1;
		int diff_nname = strcmp(key1->N_NAME, key2->N_NAME);
		int diff_sname = strcmp(key1->S_NAME, key2->S_NAME);
		int diff_pkey = key1->P_PARTKEY - key2->P_PARTKEY;

		return diff_acctbal != 0 ? diff_acctbal : (diff_nname != 0 ? diff_nname : (diff_sname != 0 ? diff_sname : diff_pkey));
	}

	virtual q2_sort_key_compare_t* clone() const {
		return new q2_sort_key_compare_t(*this);
	}
};


//---Subquery---

struct q2_n_join_r_subquery_t : public tuple_join_t {

	q2_n_join_r_subquery_t()
	: tuple_join_t(sizeof(q2_projected_nation_subquery_tuple),
			offsetof(q2_projected_nation_subquery_tuple, N_REGIONKEY),
			sizeof(q2_projected_region_tuple),
			offsetof(q2_projected_region_tuple, R_REGIONKEY),
			sizeof(int),
			sizeof(q2_n_join_r_subquery_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q2_n_join_r_subquery_tuple *dest = aligned_cast<q2_n_join_r_subquery_tuple>(d.data);
		q2_projected_nation_subquery_tuple *nation = aligned_cast<q2_projected_nation_subquery_tuple>(l.data);
		q2_projected_region_tuple *region = aligned_cast<q2_projected_region_tuple>(r.data);

		dest->N_NATIONKEY = nation->N_NATIONKEY;

		TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d\n", nation->N_REGIONKEY, region->R_REGIONKEY, nation->N_NATIONKEY);
	}

	virtual q2_n_join_r_subquery_t* clone() const {
		return new q2_n_join_r_subquery_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("JOIN NATION, REGION; SELECT N_NATIONKEY; subquery");
	}
};

struct q2_s_join_n_r_subquery_t : public tuple_join_t {

	q2_s_join_n_r_subquery_t()
		: tuple_join_t(sizeof(q2_projected_supplier_subquery_tuple),
				offsetof(q2_projected_supplier_subquery_tuple, S_NATIONKEY),
				sizeof(q2_n_join_r_subquery_tuple),
				offsetof(q2_n_join_r_subquery_tuple, N_NATIONKEY),
				sizeof(int),
				sizeof(q2_s_join_n_r_subquery_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q2_s_join_n_r_subquery_tuple *dest = aligned_cast<q2_s_join_n_r_subquery_tuple>(d.data);
		q2_projected_supplier_subquery_tuple *supp = aligned_cast<q2_projected_supplier_subquery_tuple>(l.data);
		q2_n_join_r_subquery_tuple *right = aligned_cast<q2_n_join_r_subquery_tuple>(r.data);

		dest->S_SUPPKEY = supp->S_SUPPKEY;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d\n", supp->S_NATIONKEY, right->N_NATIONKEY, supp->S_SUPPKEY);
	}

	virtual q2_s_join_n_r_subquery_t* clone() const {
		return new q2_s_join_n_r_subquery_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("JOIN SUPPLIER, NATION_REGION; SELECT S_SUPPKEY; subquery");
	}
};

struct q2_ps_join_s_n_r_subquery_t : public tuple_join_t {

	q2_ps_join_s_n_r_subquery_t()
		: tuple_join_t(sizeof(q2_projected_partsupp_tuple),
				offsetof(q2_projected_partsupp_tuple, PS_SUPPKEY),
				sizeof(q2_s_join_n_r_subquery_tuple),
				offsetof(q2_s_join_n_r_subquery_tuple, S_SUPPKEY),
				sizeof(int),
				sizeof(q2_subquery_aggregate_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q2_subquery_aggregate_tuple *dest = aligned_cast<q2_subquery_aggregate_tuple>(d.data);
		q2_projected_partsupp_tuple *partsupp = aligned_cast<q2_projected_partsupp_tuple>(l.data);
		q2_s_join_n_r_subquery_tuple *right = aligned_cast<q2_s_join_n_r_subquery_tuple>(r.data);

		dest->PS_PARTKEY = partsupp->PS_PARTKEY;
		dest->PS_SUPPLYCOST = partsupp->PS_SUPPLYCOST;

		TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %.2f\n", partsupp->PS_SUPPKEY, right->S_SUPPKEY, partsupp->PS_PARTKEY, partsupp->PS_SUPPLYCOST.to_double());
	}

	virtual q2_ps_join_s_n_r_subquery_t* clone() const {
		return new q2_ps_join_s_n_r_subquery_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("JOIN PARTSUPP, SUPPLIER_NATION_REGION; SELECT PS_PARTKEY, PS_SUPPLYCOST; subquery");
	}
};

struct q2_subquery_aggregate_t : public tuple_aggregate_t
{
	default_key_extractor_t _extractor;

	q2_subquery_aggregate_t()
	: tuple_aggregate_t(sizeof(q2_subquery_aggregate_tuple))
	{
	}

	virtual key_extractor_t* key_extractor() {
		return &_extractor;
	}

	virtual void aggregate(char* agg_data, const tuple_t &s) {
		q2_subquery_aggregate_tuple *agg = aligned_cast<q2_subquery_aggregate_tuple>(agg_data);
		q2_subquery_aggregate_tuple *src = aligned_cast<q2_subquery_aggregate_tuple>(s.data);

		agg->PS_PARTKEY = src->PS_PARTKEY;
		agg->PS_SUPPLYCOST = (agg->PS_SUPPLYCOST < src->PS_SUPPLYCOST ? agg->PS_SUPPLYCOST : src->PS_SUPPLYCOST);
	}

	virtual void finish(tuple_t &d, const char* agg_data) {
		memcpy(d.data, agg_data, tuple_size());
		q2_subquery_aggregate_tuple* agg = aligned_cast<q2_subquery_aggregate_tuple>(agg_data);
		TRACE(TRACE_RECORD_FLOW, "SUB_AGG: %d|%.2f\n", agg->PS_PARTKEY, agg->PS_SUPPLYCOST.to_double());
	}

	virtual q2_subquery_aggregate_t* clone() const {
		return new q2_subquery_aggregate_t(*this);
	}
	virtual c_str to_string() const {
		return "q2_subquery_aggregate_t";
	}

	virtual void init(char* agg_data) {
		memset(agg_data, 0, tuple_size());
		q2_subquery_aggregate_tuple *tuple = aligned_cast<q2_subquery_aggregate_tuple>(agg_data);
		tuple->PS_SUPPLYCOST = decimal(10000000);
	}
};

//-----------------------------------

struct q2_final_join_t : public tuple_join_t
{

	q2_final_join_t()
	: tuple_join_t(sizeof(q2_s_ps_p_n_join_r_tuple),
			offsetof(q2_s_ps_p_n_join_r_tuple, P_PARTKEY),
			sizeof(q2_subquery_aggregate_tuple),
			offsetof(q2_subquery_aggregate_tuple, PS_PARTKEY),
			sizeof(int) + sizeof(decimal),
			sizeof(q2_aggregate_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q2_aggregate_tuple *dest = aligned_cast<q2_aggregate_tuple>(d.data);
		q2_subquery_aggregate_tuple *right = aligned_cast<q2_subquery_aggregate_tuple>(r.data);
		q2_s_ps_p_n_join_r_tuple *left = aligned_cast<q2_s_ps_p_n_join_r_tuple>(l.data);

		dest->P_PARTKEY = left->P_PARTKEY;
		dest->S_ACCTBAL = left->S_ACCTBAL;
		memcpy(dest->N_NAME, left->N_NAME, sizeof(dest->N_NAME));
		memcpy(dest->P_MFGR, left->P_MFGR, sizeof(dest->P_MFGR));
		memcpy(dest->S_ADDRESS, left->S_ADDRESS, sizeof(dest->S_ADDRESS));
		memcpy(dest->S_COMMENT, left->S_COMMENT, sizeof(dest->S_COMMENT));
		memcpy(dest->S_NAME, left->S_NAME, sizeof(dest->S_NAME));
		memcpy(dest->S_PHONE, left->S_PHONE, sizeof(dest->S_PHONE));

		TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d AND %.4f=%.4f: %d %.4f %s %s %s %s %s %s\n", right->PS_PARTKEY, left->P_PARTKEY, right->PS_SUPPLYCOST.to_double(),
				left->PS_SUPPLYCOST.to_double(), left->P_PARTKEY, left->S_ACCTBAL.to_double(),
				left->N_NAME, left->P_MFGR, left->S_ADDRESS, left->S_COMMENT, left->S_NAME,
				left->S_PHONE);
	}

	virtual q2_final_join_t* clone() const {
		return new q2_final_join_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("JOIN SUBQUERY, MAIN_QUERY; SELECT P_PARTKEY, S_ACCTBAL, N_NAME, P_MFGR, S_ADDRESS, S_COMMENT, S_NAME, S_PHONE");
	}
};

struct q2_top100_filter_t : public tuple_filter_t
{
	int _count;

	q2_top100_filter_t()
	: tuple_filter_t(sizeof(q2_aggregate_tuple)), _count(0)
	{
	}

	bool select(const tuple_t &input) {
		return ++_count <= 100;
	}

	virtual q2_top100_filter_t* clone() const {
		return new q2_top100_filter_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("q2_top100_filter_t");
	}
};

class tpch_q2_process_tuple_t : public process_tuple_t
{
public:

	void begin() {
		TRACE(TRACE_QUERY_RESULTS, "*** Q2 ANSWER ...\n");
		TRACE(TRACE_QUERY_RESULTS, "*** S_ACCTBAL\tS_NAME\tN_NAME\tP_PARTKEY\tP_MFGR\tS_ADDRESS\tS_PHONE\tS_COMMENT\n");
	}

	virtual void process(const tuple_t& output) {
		q2_aggregate_tuple *tuple = aligned_cast<q2_aggregate_tuple>(output.data);
		//q2_s_ps_p_n_join_r_tuple *tuple = aligned_cast<q2_s_ps_p_n_join_r_tuple>(output.data);
		//q2_subquery_aggregate_tuple *tuple = aligned_cast<q2_subquery_aggregate_tuple>(output.data);

		TRACE(TRACE_QUERY_RESULTS, "*** %.4f\t%s\t%s\t%d\t%s\t%s\t%s\t%s\n",
				tuple->S_ACCTBAL.to_double(),
				tuple->S_NAME,
				tuple->N_NAME,
				tuple->P_PARTKEY,
				tuple->P_MFGR,
				tuple->S_ADDRESS,
				tuple->S_PHONE,
				tuple->S_COMMENT);
		//TRACE(TRACE_QUERY_RESULTS, "*** %d\t%.4f\n", tuple->PS_PARTKEY, tuple->PS_SUPPLYCOST.to_double());
	}
};

w_rc_t ShoreTPCHEnv::xct_qpipe_q2(const int xct_id,
		q2_input_t& in)
{
	TRACE( TRACE_ALWAYS, "********** Q2 *********\n");
	//#define USE_ECHO
	//Define USE_ECHO if you want to split the tablescan from the predication for more work sharing opportunities
	policy_t* dp = this->get_sched_policy();
	xct_t* pxct = smthread_t::me()->xct();


	//TSCAN PART
	tuple_fifo* q2_part_buffer = new tuple_fifo(sizeof(q2_projected_part_tuple));
	packet_t* q2_part_tscan_packet =
			new tscan_packet_t("TSCAN PART",
					q2_part_buffer,
					new q2_part_tscan_filter_t(this,in),
					this->db(),
					_ppart_desc.get(),
					pxct
					/*, SH */
			);

	//TSCAN PARTSUPP
	tuple_fifo* q2_partsupp_buffer = new tuple_fifo(sizeof(q2_projected_partsupp_tuple));
	packet_t* q2_partsupp_tscan_packet =
			new tscan_packet_t("TSCAN PARTSUPP",
					q2_partsupp_buffer,
					new q2_partsupp_tscan_filter_t(this, in),
					this->db(),
					_ppartsupp_desc.get(),
					pxct);

	//PARTSUPP JOIN PART
	tuple_fifo* q2_ps_join_p_buffer = new tuple_fifo(sizeof(q2_ps_join_p_tuple));
	packet_t* q2_ps_join_p_packet =
			new hash_join_packet_t("partsupp-part HJOIN",
					q2_ps_join_p_buffer,
					new trivial_filter_t(sizeof(q2_ps_join_p_tuple)),
					q2_partsupp_tscan_packet,
					q2_part_tscan_packet,
					new q2_ps_join_p_t());

	//TSCAN SUPPLIER
	tuple_fifo* q2_supplier_buffer = new tuple_fifo(sizeof(q2_projected_supplier_tuple));
	packet_t* q2_supplier_tscan_packet =
			new tscan_packet_t("TSCAN SUPPLIER",
					q2_supplier_buffer,
					new q2_supplier_tscan_filter_t(this, in),
					this->db(),
					_psupplier_desc.get(),
					pxct);

	//SUPPLIER JOIN PARTSUPP_PART
	tuple_fifo* q2_s_join_ps_p_buffer = new tuple_fifo(sizeof(q2_s_join_ps_p_tuple));
	packet_t* q2_s_join_ps_p_paket =
			new hash_join_packet_t("supplier - partsupp_part HJOIN",
					q2_s_join_ps_p_buffer,
					new trivial_filter_t(sizeof(q2_s_join_ps_p_tuple)),
					q2_supplier_tscan_packet,
					q2_ps_join_p_packet,
					new q2_s_join_ps_p_t());

	//TSCAN NATION
	tuple_fifo* q2_nation_buffer = new tuple_fifo(sizeof(q2_projected_nation_tuple));
	packet_t* q2_nation_tscan_packet =
			new tscan_packet_t("TSCAN NATION",
					q2_nation_buffer,
					new q2_nation_tscan_filter_t(this, in),
					this->db(),
					_pnation_desc.get(),
					pxct);

	//SUPPLIER_PARTSUPP_PART JOIN NATION
	tuple_fifo* q2_s_ps_p_join_n_buffer = new tuple_fifo(sizeof(q2_s_ps_p_join_n_tuple));
	packet_t* q2_s_ps_p_join_n_packet =
			new hash_join_packet_t("supplier_partsupp_part - nation HJOIN",
					q2_s_ps_p_join_n_buffer,
					new trivial_filter_t(sizeof(q2_s_ps_p_join_n_tuple)),
					q2_s_join_ps_p_paket,
					q2_nation_tscan_packet,
					new q2_s_ps_p_join_n_t());

	//TSCAN REGION
	tuple_fifo* q2_region_buffer = new tuple_fifo(sizeof(q2_projected_region_tuple));
	packet_t* q2_region_tscan_packet =
			new tscan_packet_t("TSCAN REGION",
					q2_region_buffer,
					new q2_region_tscan_filter_t(this, in),
					this->db(),
					_pregion_desc.get(),
					pxct);

	//SUPPLIER_PARTSUPP_PART_NATION JOIN REGION
	tuple_fifo* q2_s_ps_p_n_join_r_buffer = new tuple_fifo(sizeof(q2_s_ps_p_n_join_r_tuple));
	packet_t* q2_s_ps_p_n_join_r_packet =
			new hash_join_packet_t("supplier_partsupp_part_nation - region HJOIN",
					q2_s_ps_p_n_join_r_buffer,
					new trivial_filter_t(sizeof(q2_s_ps_p_n_join_r_tuple)),
					q2_s_ps_p_join_n_packet,
					q2_region_tscan_packet,
					new q2_s_ps_p_n_join_r_t());

	//SORT
	tuple_fifo* q2_sort_buffer = new tuple_fifo(sizeof(q2_s_ps_p_n_join_r_tuple));
	packet_t* q2_sort_packet =
			new sort_packet_t("SORT",
					q2_sort_buffer,
					new trivial_filter_t(sizeof(q2_s_ps_p_n_join_r_tuple)),
					new q2_sort_key_extractor_t(),
					new q2_sort_key_compare_t(),
					q2_s_ps_p_n_join_r_packet);

	//---SUBQUERY---

	//TSCAN NATION
	tuple_fifo* q2_nation_subquery_buffer = new tuple_fifo(sizeof(q2_projected_nation_subquery_tuple));
	packet_t* q2_nation_tscan_subquery_packet =
			new tscan_packet_t("TSCAN NATION subquery",
					q2_nation_subquery_buffer,
					new q2_nation_subquery_tscan_filter_t(this, in),
					this->db(),
					_pnation_desc.get(),
					pxct);

	//TSCAN REGION
	tuple_fifo* q2_region_subquery_buffer = new tuple_fifo(sizeof(q2_projected_region_tuple));
	packet_t* q2_region_tscan_subquery_packet =
			new tscan_packet_t("TSCAN REGION subquery",
					q2_region_subquery_buffer,
					new q2_region_tscan_filter_t(this, in),
					this->db(),
					_pregion_desc.get(),
					pxct);

	//NATION JOIN REGION
	tuple_fifo* q2_n_join_r_subquery_buffer = new tuple_fifo(sizeof(q2_n_join_r_subquery_tuple));
	packet_t* q2_n_join_r_subquery_packet =
			new hash_join_packet_t("nation - region HJOIN subquery",
					q2_n_join_r_subquery_buffer,
					new trivial_filter_t(sizeof(q2_n_join_r_subquery_tuple)),
					q2_nation_tscan_subquery_packet,
					q2_region_tscan_subquery_packet,
					new q2_n_join_r_subquery_t());

	//TSCAN SUPPLIER
	tuple_fifo* q2_supplier_subquery_buffer = new tuple_fifo(sizeof(q2_projected_supplier_subquery_tuple));
	packet_t* q2_supplier_tscan_subquery_packet =
			new tscan_packet_t("TSCAN SUPPLIER subquery",
					q2_supplier_subquery_buffer,
					new q2_supplier_subquery_tscan_filter_t(this, in),
					this->db(),
					_psupplier_desc.get(),
					pxct);

	//SUPPLIER JOIN NATION_REGION
	tuple_fifo* q2_s_join_n_r_subquery_buffer = new tuple_fifo(sizeof(q2_s_join_n_r_subquery_tuple));
	packet_t* q2_s_join_n_r_subquery_packet =
			new hash_join_packet_t("supplier - nation_region HJOIN subquery",
					q2_s_join_n_r_subquery_buffer,
					new trivial_filter_t(sizeof(q2_s_join_n_r_subquery_tuple)),
					q2_supplier_tscan_subquery_packet,
					q2_n_join_r_subquery_packet,
					new q2_s_join_n_r_subquery_t());

	//TSCAN PARTSUPP
	tuple_fifo* q2_partsupp_subquery_buffer = new tuple_fifo(sizeof(q2_projected_partsupp_tuple));
	packet_t* q2_partsupp_tscan_subquery_packet =
			new tscan_packet_t("TSCAN PARTSUPP subquery",
					q2_partsupp_subquery_buffer,
					new q2_partsupp_tscan_filter_t(this, in),
					this->db(),
					_ppartsupp_desc.get(),
					pxct);

	//PARTSUPP JOIN SUPPLIER_NATION_REGION
	tuple_fifo* q2_ps_join_s_n_r_subquery_buffer = new tuple_fifo(sizeof(q2_subquery_aggregate_tuple));
	packet_t* q2_ps_join_s_n_r_subquery_packet =
			new hash_join_packet_t("partsupp - supplier_nation_region HJOIN subquery",
					q2_ps_join_s_n_r_subquery_buffer,
					new trivial_filter_t(sizeof(q2_subquery_aggregate_tuple)),
					q2_partsupp_tscan_subquery_packet,
					q2_s_join_n_r_subquery_packet,
					new q2_ps_join_s_n_r_subquery_t());

	//AGGREGATION: min(PS_PARTKEY)
	tuple_fifo* q2_subquery_aggregate_buffer = new tuple_fifo(sizeof(q2_subquery_aggregate_tuple));
	packet_t* q2_subquery_aggregate_packet =
			new partial_aggregate_packet_t("AGG: min(PS_PARTKEY)",
					q2_subquery_aggregate_buffer,
					new trivial_filter_t(sizeof(q2_subquery_aggregate_tuple)),
					q2_ps_join_s_n_r_subquery_packet,
					new q2_subquery_aggregate_t(),
					new default_key_extractor_t(sizeof(int), offsetof(q2_subquery_aggregate_tuple, PS_PARTKEY)),
					new int_key_compare_t());

	//---------------

	//FINAL JOIN + TOP100-Filter
	tuple_fifo* q2_final_buffer = new tuple_fifo(sizeof(q2_aggregate_tuple));
	packet_t* q2_final_packet =
			new hash_join_packet_t("subquery join main_query",
					q2_final_buffer,
					new q2_top100_filter_t(),
					q2_sort_packet,
					q2_subquery_aggregate_packet,
					new q2_final_join_t());

	qpipe::query_state_t* qs = dp->query_state_create();
	q2_part_tscan_packet->assign_query_state(qs);
	q2_partsupp_tscan_packet->assign_query_state(qs);
	q2_ps_join_p_packet->assign_query_state(qs);
	q2_supplier_tscan_packet->assign_query_state(qs);
	q2_s_join_ps_p_paket->assign_query_state(qs);
	q2_nation_tscan_packet->assign_query_state(qs);
	q2_s_ps_p_join_n_packet->assign_query_state(qs);
	q2_region_tscan_packet->assign_query_state(qs);
	q2_s_ps_p_n_join_r_packet->assign_query_state(qs);
	q2_sort_packet->assign_query_state(qs);
	//---subquery
	q2_nation_tscan_subquery_packet->assign_query_state(qs);
	q2_region_tscan_subquery_packet->assign_query_state(qs);
	q2_n_join_r_subquery_packet->assign_query_state(qs);
	q2_supplier_tscan_subquery_packet->assign_query_state(qs);
	q2_s_join_n_r_subquery_packet->assign_query_state(qs);
	q2_partsupp_tscan_subquery_packet->assign_query_state(qs);
	q2_ps_join_s_n_r_subquery_packet->assign_query_state(qs);
	q2_subquery_aggregate_packet->assign_query_state(qs);
	//---
	q2_final_packet->assign_query_state(qs);

	// Dispatch packet
	tpch_q2_process_tuple_t pt;
	process_query(q2_final_packet, pt);
	dp->query_state_destroy(qs);

	return (RCOK);

}
EXIT_NAMESPACE(qpipe);
