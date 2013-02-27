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

/** @file:   qpipe_q21.cpp
 *
 *  @brief:  Implementation of QPIPE TPCH Q21 over Shore-MT
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
 * QPIPE Q21 - Structures needed by operators
 *
 ********************************************************************/

/*
select
	s_name,
	count(*) as numwait
from
	supplier,
	lineitem l1,
	orders,
	nation
where
	s_suppkey = l1.l_suppkey
	and o_orderkey = l1.l_orderkey
	and o_orderstatus = 'F'
	and l1.l_receiptdate > l1.l_commitdate
	and exists (
		select *
		from
			lineitem l2
		where
			l2.l_orderkey = l1.l_orderkey
			and l2.l_suppkey <> l1.l_suppkey )
	and not exists (
		select *
		from
			lineitem l3
		where
			l3.l_orderkey = l1.l_orderkey
			and l3.l_suppkey <> l1.l_suppkey
			and l3.l_receiptdate > l3.l_commitdate )
	and s_nationkey = n_nationkey
	and n_name = '[NATION]'
group by
	s_name
order by
	numwait desc,
	s_name;
 */


struct q21_projected_nation_tuple {
	int N_NATIONKEY;
};

struct q21_projected_supplier_tuple {
	int S_SUPPKEY;
	char S_NAME[STRSIZE(25)];
	int S_NATIONKEY;
};

struct q21_projected_lineitem_l1_tuple {
	int L_ORDERKEY;
	int L_LINENUMBER;
	int L_SUPPKEY;
};

struct q21_projected_lineitem_l2_tuple {
	int L_ORDERKEY;
	int L_SUPPKEY;
};

struct q21_projected_orders_tuple {
	int O_ORDERKEY;
};

struct q21_s_join_n_tuple {
	int S_SUPPKEY;
	char S_NAME[STRSIZE(25)];
};

struct q21_l1_join_s_n_tuple {
	int L1_ORDERKEY;
	int L1_LINENUMBER;
	int L1_SUPPKEY;
	char S_NAME[STRSIZE(25)];
};

struct q21_l2_join_l1_s_n_tuple {
	int L1_ORDERKEY;
	int L1_LINENUMBER;
	int L1_SUPPKEY;
	int L2_SUPPKEY;
	char S_NAME[STRSIZE(25)];
};

struct q21_sub_agg_tuple {
	int L_ORDERKEY;
	char S_NAME[STRSIZE(25)];
};

struct q21_all_joins_tuple {
	char S_NAME[STRSIZE(25)];
};

struct q21_final_tuple {
	int NUMWAIT;
	char S_NAME[STRSIZE(25)];
};



class q21_nation_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prnation;
	rep_row_t _rr;

	/*One nation tuple*/
	tpch_nation_tuple _nation;

	char _nname[STRSIZE(25)];

public:
	q21_nation_tscan_filter_t(ShoreTPCHEnv* tpchdb, q21_input_t &in)
	: tuple_filter_t(tpchdb->nation_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prnation = _tpchdb->nation_man()->get_tuple();
		_rr.set_ts(_tpchdb->nation_man()->ts(),
				_tpchdb->nation_desc()->maxsize());
		_prnation->_rep = &_rr;

		nation_to_str((&in)->n_name, _nname);
		TRACE(TRACE_ALWAYS, "Random predicate:\nNATION.N_NAME = %s\n", _nname);
	}

	virtual ~q21_nation_tscan_filter_t()
	{
		_tpchdb->nation_man()->give_tuple(_prnation);
	}

	bool select(const tuple_t &input) {

		// Get next nation tuple and read its size and type
		if (!_tpchdb->nation_man()->load(_prnation, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		_prnation->get_value(1, _nation.N_NAME, sizeof(_nation.N_NAME));

		return (strcmp(_nation.N_NAME, _nname) == 0);

	}


	void project(tuple_t &d, const tuple_t &s) {

		q21_projected_nation_tuple *dest;
		dest = aligned_cast<q21_projected_nation_tuple>(d.data);

		_prnation->get_value(0, _nation.N_NATIONKEY);

		//TRACE( TRACE_RECORD_FLOW, "%d\n",
		//		_nation.N_NATIONKEY);

		dest->N_NATIONKEY = _nation.N_NATIONKEY;
	}

	q21_nation_tscan_filter_t* clone() const {
		return new q21_nation_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q21_nation_tscan_filter_t()");
	}
};


class q21_supplier_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prsupplier;
	rep_row_t _rr;

	/*One supplier tuple*/
	tpch_supplier_tuple _supplier;

public:
	q21_supplier_tscan_filter_t(ShoreTPCHEnv* tpchdb, q21_input_t &in)
	: tuple_filter_t(tpchdb->supplier_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prsupplier = _tpchdb->supplier_man()->get_tuple();
		_rr.set_ts(_tpchdb->supplier_man()->ts(),
				_tpchdb->supplier_desc()->maxsize());
		_prsupplier->_rep = &_rr;
	}

	virtual ~q21_supplier_tscan_filter_t()
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

		q21_projected_supplier_tuple *dest;
		dest = aligned_cast<q21_projected_supplier_tuple>(d.data);


		_prsupplier->get_value(0, _supplier.S_SUPPKEY);
		_prsupplier->get_value(1, _supplier.S_NAME, sizeof(_supplier.S_NAME));
		_prsupplier->get_value(3, _supplier.S_NATIONKEY);

		//TRACE( TRACE_RECORD_FLOW, "%d|%s|%d\n",
		//		_supplier.S_SUPPKEY, _supplier.S_NAME, _supplier.S_NATIONKEY);

		dest->S_SUPPKEY = _supplier.S_SUPPKEY;
		memcpy(dest->S_NAME, _supplier.S_NAME, sizeof(dest->S_NAME));
		dest->S_NATIONKEY = _supplier.S_NATIONKEY;

	}

	q21_supplier_tscan_filter_t* clone() const {
		return new q21_supplier_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q21_supplier_tscan_filter_t");
	}
};


class q21_lineitem_l1_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prline;
	rep_row_t _rr;

	tpch_lineitem_tuple _lineitem;

public:
	q21_lineitem_l1_tscan_filter_t(ShoreTPCHEnv* tpchdb, q21_input_t &in)
	: tuple_filter_t(tpchdb->lineitem_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prline = _tpchdb->lineitem_man()->get_tuple();
		_rr.set_ts(_tpchdb->lineitem_man()->ts(),
				_tpchdb->lineitem_desc()->maxsize());
		_prline->_rep = &_rr;
	}

	virtual ~q21_lineitem_l1_tscan_filter_t()
	{
		// Give back the lineitem tuple
		_tpchdb->lineitem_man()->give_tuple(_prline);
	}

	bool select(const tuple_t &input) {
		// Get next lineitem tuple and read its shipdate
		if (!_tpchdb->lineitem_man()->load(_prline, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		_prline->get_value(11, _lineitem.L_COMMITDATE, sizeof(_lineitem.L_COMMITDATE));
		_prline->get_value(12, _lineitem.L_RECEIPTDATE, sizeof(_lineitem.L_RECEIPTDATE));

		return str_to_timet(_lineitem.L_RECEIPTDATE) > str_to_timet(_lineitem.L_COMMITDATE);
	}

	void project(tuple_t &d, const tuple_t &s) {

		q21_projected_lineitem_l1_tuple *dest = aligned_cast<q21_projected_lineitem_l1_tuple>(d.data);

		_prline->get_value(0, _lineitem.L_ORDERKEY);
		_prline->get_value(2, _lineitem.L_SUPPKEY);
		_prline->get_value(3, _lineitem.L_LINENUMBER);

		//TRACE(TRACE_RECORD_FLOW, "%d|%d|%d\n", _lineitem.L_ORDERKEY, _lineitem.L_SUPPKEY, _lineitem.L_LINENUMBER);

		dest->L_ORDERKEY = _lineitem.L_ORDERKEY;
		dest->L_LINENUMBER = _lineitem.L_LINENUMBER;
		dest->L_SUPPKEY = _lineitem.L_SUPPKEY;
	}

	q21_lineitem_l1_tscan_filter_t* clone() const {
		return new q21_lineitem_l1_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q21_lineitem_l1_tscan_filter_t(l_receiptdate > l_commitdate)");
	}
};

class q21_lineitem_l2_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prline;
	rep_row_t _rr;

	tpch_lineitem_tuple _lineitem;

public:
	q21_lineitem_l2_tscan_filter_t(ShoreTPCHEnv* tpchdb, q21_input_t &in)
	: tuple_filter_t(tpchdb->lineitem_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prline = _tpchdb->lineitem_man()->get_tuple();
		_rr.set_ts(_tpchdb->lineitem_man()->ts(),
				_tpchdb->lineitem_desc()->maxsize());
		_prline->_rep = &_rr;
	}

	virtual ~q21_lineitem_l2_tscan_filter_t()
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

		q21_projected_lineitem_l2_tuple *dest = aligned_cast<q21_projected_lineitem_l2_tuple>(d.data);

		_prline->get_value(0, _lineitem.L_ORDERKEY);
		_prline->get_value(2, _lineitem.L_SUPPKEY);

		//TRACE(TRACE_RECORD_FLOW, "%d|%d\n", _lineitem.L_ORDERKEY, _lineitem.L_SUPPKEY);

		dest->L_ORDERKEY = _lineitem.L_ORDERKEY;
		dest->L_SUPPKEY = _lineitem.L_SUPPKEY;
	}

	q21_lineitem_l2_tscan_filter_t* clone() const {
		return new q21_lineitem_l2_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q21_lineitem_l2_tscan_filter_t");
	}
};


class q21_orders_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prorders;
	rep_row_t _rr;

	tpch_orders_tuple _orders;

public:
	q21_orders_tscan_filter_t(ShoreTPCHEnv* tpchdb, q21_input_t &in)
	: tuple_filter_t(tpchdb->orders_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prorders = _tpchdb->orders_man()->get_tuple();
		_rr.set_ts(_tpchdb->orders_man()->ts(),
				_tpchdb->orders_desc()->maxsize());
		_prorders->_rep = &_rr;
	}

	virtual ~q21_orders_tscan_filter_t()
	{
		// Give back the orders tuple
		_tpchdb->orders_man()->give_tuple(_prorders);
	}

	bool select(const tuple_t &input) {
		// Get next orders tuple and read its orderdate
		if (!_tpchdb->orders_man()->load(_prorders, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		_prorders->get_value(2, _orders.O_ORDERSTATUS);

		return _orders.O_ORDERSTATUS == 'F';
	}

	void project(tuple_t &d, const tuple_t &s) {

		q21_projected_orders_tuple *dest = aligned_cast<q21_projected_orders_tuple>(d.data);

		_prorders->get_value(0, _orders.O_ORDERKEY);

		//TRACE(TRACE_RECORD_FLOW, "%d\n", _orders.O_ORDERKEY);

		dest->O_ORDERKEY = _orders.O_ORDERKEY;
	}

	q21_orders_tscan_filter_t* clone() const {
		return new q21_orders_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q21_orders_tscan_filter_t(o_orderstatus = 'F')");
	}
};



struct q21_s_join_n_t : public tuple_join_t {

	q21_s_join_n_t()
		: tuple_join_t(sizeof(q21_projected_supplier_tuple),
				offsetof(q21_projected_supplier_tuple, S_NATIONKEY),
				sizeof(q21_projected_nation_tuple),
				offsetof(q21_projected_nation_tuple, N_NATIONKEY),
				sizeof(int),
				sizeof(q21_s_join_n_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q21_s_join_n_tuple *dest = aligned_cast<q21_s_join_n_tuple>(d.data);
		q21_projected_supplier_tuple *supp = aligned_cast<q21_projected_supplier_tuple>(l.data);
		q21_projected_nation_tuple *nation = aligned_cast<q21_projected_nation_tuple>(r.data);

		dest->S_SUPPKEY = supp->S_SUPPKEY;
		memcpy(dest->S_NAME, supp->S_NAME, sizeof(dest->S_NAME));

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %s\n", supp->S_NATIONKEY, nation->N_NATIONKEY, supp->S_SUPPKEY, supp->S_NAME);
	}

	virtual q21_s_join_n_t* clone() const {
		return new q21_s_join_n_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join SUPPLIER, NATION; select S_SUPPKEY, S_NAME");
	}
};

struct q21_l1_join_s_n_t : public tuple_join_t {

	q21_l1_join_s_n_t()
		: tuple_join_t(sizeof(q21_projected_lineitem_l1_tuple),
				offsetof(q21_projected_lineitem_l1_tuple, L_SUPPKEY),
				sizeof(q21_s_join_n_tuple),
				offsetof(q21_s_join_n_tuple, S_SUPPKEY),
				sizeof(int),
				sizeof(q21_l1_join_s_n_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q21_l1_join_s_n_tuple *dest = aligned_cast<q21_l1_join_s_n_tuple>(d.data);
		q21_projected_lineitem_l1_tuple *line = aligned_cast<q21_projected_lineitem_l1_tuple>(l.data);
		q21_s_join_n_tuple *right = aligned_cast<q21_s_join_n_tuple>(r.data);

		dest->L1_LINENUMBER = line->L_LINENUMBER;
		dest->L1_ORDERKEY = line->L_ORDERKEY;
		dest->L1_SUPPKEY = line->L_SUPPKEY;
		memcpy(dest->S_NAME, right->S_NAME, sizeof(dest->S_NAME));

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %d %d %s\n", line->L_SUPPKEY, right->S_SUPPKEY, line->L_LINENUMBER, line->L_ORDERKEY, line->L_SUPPKEY, right->S_NAME);
	}

	virtual q21_l1_join_s_n_t* clone() const {
		return new q21_l1_join_s_n_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join LINEITEM L1, SUPPLIER_NATION; select L1.L_LINENUMBER, L1.L_ORDERKEY, L1.L_SUPPKEY, S_NAME");
	}
};

struct q21_l2_join_l1_s_n_t : public tuple_join_t {

	q21_l2_join_l1_s_n_t()
		: tuple_join_t(sizeof(q21_projected_lineitem_l2_tuple),
				offsetof(q21_projected_lineitem_l2_tuple, L_ORDERKEY),
				sizeof(q21_l1_join_s_n_tuple),
				offsetof(q21_l1_join_s_n_tuple, L1_ORDERKEY),
				sizeof(int),
				sizeof(q21_l2_join_l1_s_n_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q21_l2_join_l1_s_n_tuple *dest = aligned_cast<q21_l2_join_l1_s_n_tuple>(d.data);
		q21_projected_lineitem_l2_tuple *line = aligned_cast<q21_projected_lineitem_l2_tuple>(l.data);
		q21_l1_join_s_n_tuple *right = aligned_cast<q21_l1_join_s_n_tuple>(r.data);

		dest->L1_LINENUMBER = right->L1_LINENUMBER;
		dest->L1_ORDERKEY = right->L1_ORDERKEY;
		dest->L1_SUPPKEY = right->L1_SUPPKEY;
		dest->L2_SUPPKEY = line->L_SUPPKEY;
		memcpy(dest->S_NAME, right->S_NAME, sizeof(dest->S_NAME));

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %d %d %d %s\n", line->L_ORDERKEY, right->L1_ORDERKEY, right->L1_LINENUMBER, right->L1_ORDERKEY, right->L1_SUPPKEY, line->L_SUPPKEY,
		//															right->S_NAME);
	}

	virtual q21_l2_join_l1_s_n_t* clone() const {
		return new q21_l2_join_l1_s_n_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join LINEITEM L2, LINEITEM_SUPPLIER_NATION; select L1_LINENUMBER, L1_ORDERKEY, L1_SUPPKEY, L2.L_SUPPKEY, S_NAME");
	}
};

struct q21_exists_join_filter_t : public tuple_filter_t {

	q21_exists_join_filter_t()
	: tuple_filter_t(sizeof(q21_l2_join_l1_s_n_tuple))
	{
	}

	bool select(const tuple_t &input) {
		q21_l2_join_l1_s_n_tuple *in = aligned_cast<q21_l2_join_l1_s_n_tuple>(input.data);
		return in->L1_SUPPKEY != in->L2_SUPPKEY;
	}

	virtual q21_exists_join_filter_t* clone() const {
		return new q21_exists_join_filter_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("q21_exists_join_filter_t");
	}
};

struct q21_sub_aggregate_t : public tuple_aggregate_t {

	default_key_extractor_t _extractor;

	q21_sub_aggregate_t()
	: tuple_aggregate_t(sizeof(q21_sub_agg_tuple))
	{
	}

	virtual key_extractor_t* key_extractor() {
		return &_extractor;
	}

	virtual void aggregate(char* agg_data, const tuple_t &t) {
		q21_sub_agg_tuple *agg = aligned_cast<q21_sub_agg_tuple>(agg_data);
		q21_l2_join_l1_s_n_tuple *input = aligned_cast<q21_l2_join_l1_s_n_tuple>(t.data);

		agg->L_ORDERKEY = input->L1_ORDERKEY;
		memcpy(agg->S_NAME, input->S_NAME, sizeof(agg->S_NAME));
	}
	virtual void finish(tuple_t &dest, const char* agg_data) {
		memcpy(dest.data, agg_data, dest.size);
	}
	virtual q21_sub_aggregate_t* clone() const {
		return new q21_sub_aggregate_t(*this);
	}
	virtual c_str to_string() const {
		return "q21_sub_aggregate_t";
	}
};


struct q21_final_join_t : public tuple_join_t {

	q21_final_join_t()
		: tuple_join_t(sizeof(q21_projected_orders_tuple),
				offsetof(q21_projected_orders_tuple, O_ORDERKEY),
				sizeof(q21_sub_agg_tuple),
				offsetof(q21_sub_agg_tuple, L_ORDERKEY),
				sizeof(int),
				sizeof(q21_all_joins_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q21_all_joins_tuple *dest = aligned_cast<q21_all_joins_tuple>(d.data);
		q21_projected_orders_tuple *order = aligned_cast<q21_projected_orders_tuple>(l.data);
		q21_sub_agg_tuple *right = aligned_cast<q21_sub_agg_tuple>(r.data);

		memcpy(dest->S_NAME, right->S_NAME, sizeof(dest->S_NAME));

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %s\n", order->O_ORDERKEY, right->L_ORDERKEY, right->S_NAME);
	}

	virtual q21_final_join_t* clone() const {
		return new q21_final_join_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join ORDERS, L2_L1_SUPPLIER_NATION; select S_NAME");
	}
};

struct q21_aggregate_t : public tuple_aggregate_t {

	default_key_extractor_t _extractor;

	q21_aggregate_t()
		: tuple_aggregate_t(sizeof(q21_final_tuple))
	{
	}

	virtual key_extractor_t* key_extractor() {
			return &_extractor;
		}

		virtual void aggregate(char* agg_data, const tuple_t &t) {
			q21_final_tuple *agg = aligned_cast<q21_final_tuple>(agg_data);
			q21_all_joins_tuple *input = aligned_cast<q21_all_joins_tuple>(t.data);

			memcpy(agg->S_NAME, input->S_NAME, sizeof(agg->S_NAME));
			agg->NUMWAIT++;
		}
		virtual void finish(tuple_t &dest, const char* agg_data) {
			memcpy(dest.data, agg_data, dest.size);
		}
		virtual q21_aggregate_t* clone() const {
			return new q21_aggregate_t(*this);
		}
		virtual c_str to_string() const {
			return "q21_aggregate_t";
		}
};


struct q21_sort_key_extractor_t : public key_extractor_t {

	q21_sort_key_extractor_t()
		: key_extractor_t(sizeof(q21_final_tuple))
	{
	}

	virtual int extract_hint(const char *key) const {
		q21_final_tuple *tuple = aligned_cast<q21_final_tuple>(key);
		return -(tuple->NUMWAIT);
	}

	virtual key_extractor_t* clone() const {
		return new q21_sort_key_extractor_t(*this);
	}
};

struct q21_sort_key_compare_t : public key_compare_t {

	q21_sort_key_compare_t()
	{
	}

	virtual int operator()(const void* key1, const void* key2) const {
		q21_final_tuple *t1 = aligned_cast<q21_final_tuple>(key1);
		q21_final_tuple *t2 = aligned_cast<q21_final_tuple>(key2);
		return (t1->NUMWAIT != t2->NUMWAIT ? t2->NUMWAIT - t1->NUMWAIT : strcmp(t1->S_NAME, t2->S_NAME));
	}

	virtual q21_sort_key_compare_t* clone() const {
		return new q21_sort_key_compare_t(*this);
	}
};



class tpch_q21_process_tuple_t : public process_tuple_t {

public:

    virtual void begin() {
        TRACE(TRACE_QUERY_RESULTS, "*** Q21 %s %s\n",
              "S_NAME", "NUMWAIT");
    }

    virtual void process(const tuple_t& output) {
    	q21_final_tuple *agg = aligned_cast<q21_final_tuple>(output.data);

        TRACE(TRACE_QUERY_RESULTS, "*** Q21 %s %d\n", agg->S_NAME, agg->NUMWAIT);
    }

};


/********************************************************************
 *
 * QPIPE q21 - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_qpipe_q21(const int xct_id,
                                  q21_input_t& in)
{
    TRACE( TRACE_ALWAYS, "********** q21 *********\n");

    policy_t* dp = this->get_sched_policy();
    xct_t* pxct = smthread_t::me()->xct();


    //TSCAN NATION
    tuple_fifo* q21_nation_buffer = new tuple_fifo(sizeof(q21_projected_nation_tuple));
    packet_t* q21_nation_tscan_packet =
    		new tscan_packet_t("nation TSCAN",
    				q21_nation_buffer,
    				new q21_nation_tscan_filter_t(this, in),
    				this->db(),
    				_pnation_desc.get(),
    				pxct);

    //TSCAN SUPPLIER
    tuple_fifo* q21_supplier_buffer = new tuple_fifo(sizeof(q21_projected_supplier_tuple));
    packet_t* q21_supplier_tscan_packet =
    		new tscan_packet_t("supplier TSCAN",
    				q21_supplier_buffer,
    				new q21_supplier_tscan_filter_t(this, in),
    				this->db(),
    				_psupplier_desc.get(),
    				pxct);

    //TSCAN LINEITEM L1
    tuple_fifo* q21_lineitem_l1_buffer = new tuple_fifo(sizeof(q21_projected_lineitem_l1_tuple));
    packet_t* q21_lineitem_l1_tscan_packet =
    		new tscan_packet_t("lineitem l1 TSCAN",
    				q21_lineitem_l1_buffer,
    				new q21_lineitem_l1_tscan_filter_t(this, in),
    				this->db(),
    				_plineitem_desc.get(),
    				pxct);

    //TSCAN LINEITEM L2
    tuple_fifo* q21_lineitem_l2_buffer = new tuple_fifo(sizeof(q21_projected_lineitem_l2_tuple));
    packet_t* q21_lineitem_l2_tscan_packet =
    		new tscan_packet_t("lineitem l2 TSCAN",
    				q21_lineitem_l2_buffer,
    				new q21_lineitem_l2_tscan_filter_t(this, in),
    				this->db(),
    				_plineitem_desc.get(),
    				pxct);

    //TSCAN ORDERS
    tuple_fifo* q21_orders_buffer = new tuple_fifo(sizeof(q21_projected_orders_tuple));
    packet_t* q21_orders_tscan_packet =
    		new tscan_packet_t("orders TSCAN",
    				q21_orders_buffer,
    				new q21_orders_tscan_filter_t(this, in),
    				this->db(),
    				_porders_desc.get(),
    				pxct);


    //SUPPLIER JOIN NATION
    tuple_fifo* q21_s_join_n_buffer = new tuple_fifo(sizeof(q21_s_join_n_tuple));
    packet_t* q21_s_join_n_packet =
    		new hash_join_packet_t("supplier - nation HJOIN",
    				q21_s_join_n_buffer,
    				new trivial_filter_t(sizeof(q21_s_join_n_tuple)),
    				q21_supplier_tscan_packet,
    				q21_nation_tscan_packet,
    				new q21_s_join_n_t());

    //LINEITEM L1 JOIN SUPPLIER_NATION
    tuple_fifo* q21_l1_join_s_n_buffer = new tuple_fifo(sizeof(q21_l1_join_s_n_tuple));
    packet_t* q21_l1_join_s_n_packet =
    		new hash_join_packet_t("lineitem l1 - supplier_nation HJOIN",
    				q21_l1_join_s_n_buffer,
    				new trivial_filter_t(sizeof(q21_l1_join_s_n_tuple)),
    				q21_lineitem_l1_tscan_packet,
    				q21_s_join_n_packet,
    				new q21_l1_join_s_n_t());

    //LINEITEM L2 JOIN L1_SUPPLIER_NATION
    tuple_fifo* q21_l2_join_l1_s_n_buffer = new tuple_fifo(sizeof(q21_l2_join_l1_s_n_tuple));
    packet_t* q21_l2_join_l1_s_n_packet =
    		new hash_join_packet_t("lineitem l2 - l1_supplier_nation HJOIN",
    				q21_l2_join_l1_s_n_buffer,
    				new q21_exists_join_filter_t(),
    				q21_lineitem_l2_tscan_packet,
    				q21_l1_join_s_n_packet,
    				new q21_l2_join_l1_s_n_t());

    //SUB AGGREGATION ON L_ORDERKEY, L_LINENUMBER (distinct agg)
    tuple_fifo* q21_sub_agg_buffer = new tuple_fifo(sizeof(q21_sub_agg_tuple));
    packet_t* q21_sub_agg_packet =
    		new partial_aggregate_packet_t("SUB AGGREGATE ON L_ORDERKEY, L_LINENUMBER",
    				q21_sub_agg_buffer,
    				new trivial_filter_t(sizeof(q21_sub_agg_tuple)),
    				q21_l2_join_l1_s_n_packet,
    				new q21_sub_aggregate_t(),
    				new default_key_extractor_t(2 * sizeof(int), offsetof(q21_l2_join_l1_s_n_tuple, L1_ORDERKEY)),
    				new int_key_compare_t());

    //ORDERS JOIN SUB_AGG_TUPLE
    tuple_fifo* q21_all_joins_buffer = new tuple_fifo(sizeof(q21_all_joins_tuple));
    packet_t* q21_all_joins_packet =
    		new hash_join_packet_t("orders - l2_l1_supplier_nation HJOIN",
    				q21_all_joins_buffer,
    				new trivial_filter_t(sizeof(q21_all_joins_tuple)),
    				q21_orders_tscan_packet,
    				q21_sub_agg_packet,
    				new q21_final_join_t());

    //AGGREGATE GROUP BY, CNT
    tuple_fifo* q21_aggregate_buffer = new tuple_fifo(sizeof(q21_final_tuple));
    packet_t* q21_aggregate_packet =
    		new partial_aggregate_packet_t("AGG GROUP BY S_NAME, COUNT",
    				q21_aggregate_buffer,
    				new trivial_filter_t(sizeof(q21_final_tuple)),
    				q21_all_joins_packet,
    				new q21_aggregate_t(),
    				new default_key_extractor_t(STRSIZE(25) * sizeof(char), offsetof(q21_all_joins_tuple, S_NAME)),
    				new int_key_compare_t());

    //SORT
    tuple_fifo* q21_final_buffer = new tuple_fifo(sizeof(q21_final_tuple));
    packet_t* q21_final_packet =
    		new sort_packet_t("ORDER BY NUMWAIT desc, S_NAME",
    				q21_final_buffer,
    				new trivial_filter_t(sizeof(q21_final_tuple)),
    				new q21_sort_key_extractor_t(),
    				new q21_sort_key_compare_t(),
    				q21_aggregate_packet);


    qpipe::query_state_t* qs = dp->query_state_create();
    q21_nation_tscan_packet->assign_query_state(qs);
    q21_supplier_tscan_packet->assign_query_state(qs);
    q21_lineitem_l1_tscan_packet->assign_query_state(qs);
    q21_lineitem_l2_tscan_packet->assign_query_state(qs);
    q21_orders_tscan_packet->assign_query_state(qs);
    q21_s_join_n_packet->assign_query_state(qs);
    q21_l1_join_s_n_packet->assign_query_state(qs);
    q21_l2_join_l1_s_n_packet->assign_query_state(qs);
    q21_sub_agg_packet->assign_query_state(qs);
    q21_all_joins_packet->assign_query_state(qs);
    q21_aggregate_packet->assign_query_state(qs);
    q21_final_packet->assign_query_state(qs);


    // Dispatch packet
    tpch_q21_process_tuple_t pt;
    //LAST PACKET
    process_query(q21_final_packet, pt);//TODO

    dp->query_state_destroy(qs);


    return (RCOK);
}

EXIT_NAMESPACE(tpch);
