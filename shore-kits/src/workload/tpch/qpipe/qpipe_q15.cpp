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

/** @file:   qpipe_q15.cpp
 *
 *  @brief:  Implementation of QPIPE TPCH Q15 over Shore-MT
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
 * QPIPE Q15 - Structures needed by operators
 *
 ********************************************************************/

/*
create view revenue[STREAM_ID] (supplier_no, total_revenue) as
	select
		l_suppkey,
		sum(l_extendedprice * (1 - l_discount))
	from
		lineitem
	where
		l_shipdate >= date '[DATE]'
		and l_shipdate < date '[DATE]' + interval '3' month
	group by
		l_suppkey;

select
	s_suppkey,
	s_name,
	s_address,
	s_phone,
	total_revenue
from
	supplier,
	revenue[STREAM_ID]
where
	s_suppkey = supplier_no
	and total_revenue = (
		select
			max(total_revenue)
		from
			revenue[STREAM_ID])
order by
	s_suppkey;

drop view revenue[STREAM_ID];
 */


struct q15_projected_lineitem_tuple {
	int L_SUPPKEY;
	decimal REVENUE;
};

struct q15_projected_supplier_tuple {
	int S_SUPPKEY;
	char S_NAME[STRSIZE(25)];
	char S_ADDRESS[STRSIZE(40)];
	char S_PHONE[STRSIZE(15)];
};

struct q15_final_tuple {
	int S_SUPPKEY;
	char S_NAME[STRSIZE(25)];
	char S_ADDRESS[STRSIZE(40)];
	char S_PHONE[STRSIZE(15)];
	decimal TOTAL_REVENUE;
};


class q15_lineitem_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prline;
	rep_row_t _rr;

	tpch_lineitem_tuple _lineitem;
	time_t _shipdate;

	time_t _firstdate;
	time_t _lastdate;

public:
	q15_lineitem_tscan_filter_t(ShoreTPCHEnv* tpchdb, q15_input_t &in)
	: tuple_filter_t(tpchdb->lineitem_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prline = _tpchdb->lineitem_man()->get_tuple();
		_rr.set_ts(_tpchdb->lineitem_man()->ts(),
				_tpchdb->lineitem_desc()->maxsize());
		_prline->_rep = &_rr;

		_firstdate = (&in)->l_shipdate;
		struct tm *tm = gmtime(&_firstdate);
		tm->tm_mon += 3;
		_lastdate = mktime(tm);

		char f_shipdate[STRSIZE(10)];
		char l_shipdate[STRSIZE(10)];
		timet_to_str(f_shipdate, _firstdate);
		timet_to_str(l_shipdate, _lastdate);

		TRACE(TRACE_ALWAYS, "Random predicate:\nLINEITEM.L_SHIPDATE between [%s, %s[\n", f_shipdate, l_shipdate);
	}

	virtual ~q15_lineitem_tscan_filter_t()
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

		return _shipdate >= _firstdate && _shipdate < _lastdate;
	}

	void project(tuple_t &d, const tuple_t &s) {

		q15_projected_lineitem_tuple *dest = aligned_cast<q15_projected_lineitem_tuple>(d.data);

		_prline->get_value(2, _lineitem.L_SUPPKEY);
		_prline->get_value(5, _lineitem.L_EXTENDEDPRICE);
		_prline->get_value(6, _lineitem.L_DISCOUNT);

		//TRACE(TRACE_RECORD_FLOW, "%d|%.2f\n", _lineitem.L_SUPPKEY, _lineitem.L_EXTENDEDPRICE / 100.0 * (1 - _lineitem.L_DISCOUNT / 100.0));

		dest->L_SUPPKEY = _lineitem.L_SUPPKEY;
		dest->REVENUE = _lineitem.L_EXTENDEDPRICE / 100.0 * (1 - _lineitem.L_DISCOUNT / 100.0);
#warning MA: Discount from TPCH dbgen is created between 0 and 100 instead between 0 and 1.
	}

	q15_lineitem_tscan_filter_t* clone() const {
		return new q15_lineitem_tscan_filter_t(*this);
	}

	c_str to_string() const {
		char f_shipdate[STRSIZE(10)];
		char l_shipdate[STRSIZE(10)];
		timet_to_str(f_shipdate, _firstdate);
		timet_to_str(l_shipdate, _lastdate);
		return c_str("q15_lineitem_tscan_filter_t(l_shipdate between [%s, %s[)", f_shipdate, l_shipdate);
	}
};


class q15_supplier_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prsupplier;
	rep_row_t _rr;

	/*One supplier tuple*/
	tpch_supplier_tuple _supplier;

public:
	q15_supplier_tscan_filter_t(ShoreTPCHEnv* tpchdb, q15_input_t &in)
	: tuple_filter_t(tpchdb->supplier_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prsupplier = _tpchdb->supplier_man()->get_tuple();
		_rr.set_ts(_tpchdb->supplier_man()->ts(),
				_tpchdb->supplier_desc()->maxsize());
		_prsupplier->_rep = &_rr;
	}

	virtual ~q15_supplier_tscan_filter_t()
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

		q15_projected_supplier_tuple *dest = aligned_cast<q15_projected_supplier_tuple>(d.data);


		_prsupplier->get_value(0, _supplier.S_SUPPKEY);
		_prsupplier->get_value(1, _supplier.S_NAME, sizeof(_supplier.S_NAME));
		_prsupplier->get_value(2, _supplier.S_ADDRESS, sizeof(_supplier.S_ADDRESS));
		_prsupplier->get_value(4, _supplier.S_PHONE, sizeof(_supplier.S_PHONE));

		//TRACE( TRACE_RECORD_FLOW, "%d|%s|%s|%s\n",
		//		_supplier.S_SUPPKEY, _supplier.S_NAME, _supplier.S_ADDRESS, _supplier.S_PHONE);

		dest->S_SUPPKEY = _supplier.S_SUPPKEY;
		memcpy(dest->S_NAME, _supplier.S_NAME, sizeof(dest->S_NAME));
		memcpy(dest->S_ADDRESS, _supplier.S_ADDRESS, sizeof(dest->S_ADDRESS));
		memcpy(dest->S_PHONE, _supplier.S_PHONE, sizeof(dest->S_PHONE));

	}

	q15_supplier_tscan_filter_t* clone() const {
		return new q15_supplier_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q15_supplier_tscan_filter_t");
	}
};


struct q15_lineitem_aggregate_t : public tuple_aggregate_t {

	default_key_extractor_t _extractor;

	q15_lineitem_aggregate_t()
	: tuple_aggregate_t(sizeof(q15_projected_lineitem_tuple)), _extractor(sizeof(int), offsetof(q15_projected_lineitem_tuple, L_SUPPKEY))
	{
	}

	virtual key_extractor_t* key_extractor() {
		return &_extractor;
	}

	virtual void aggregate(char* agg_data, const tuple_t &t) {
		q15_projected_lineitem_tuple *agg = aligned_cast<q15_projected_lineitem_tuple>(agg_data);
		q15_projected_lineitem_tuple *in = aligned_cast<q15_projected_lineitem_tuple>(t.data);

		agg->REVENUE += in->REVENUE;
	}
	virtual void finish(tuple_t &dest, const char* agg_data) {
		memcpy(dest.data, agg_data, dest.size);
	}
	virtual q15_lineitem_aggregate_t* clone() const {
		return new q15_lineitem_aggregate_t(*this);
	}
	virtual c_str to_string() const {
		return "q15_lineitem_aggregate_t";
	}
};

struct q15_l_sort_key_extractor_t : public key_extractor_t {

	q15_l_sort_key_extractor_t()
		: key_extractor_t(sizeof(decimal), offsetof(q15_projected_lineitem_tuple, REVENUE))
		{
		}

		virtual int extract_hint(const char* key) const {
			return -(*aligned_cast<decimal>(key)).to_int();
		}

		virtual q15_l_sort_key_extractor_t* clone() const {
			return new q15_l_sort_key_extractor_t(*this);
		}
};

struct q15_l_sort_key_compare_t : public key_compare_t {

	virtual int operator()(const void* key1, const void* key2) const {
			decimal rev1 = *aligned_cast<decimal>(key1);
			decimal rev2 = *aligned_cast<decimal>(key2);
			return rev1 > rev2 ? -1 : (rev1 < rev2 ? 1 : 0);
		}

		virtual q15_l_sort_key_compare_t* clone() const {
			return new q15_l_sort_key_compare_t(*this);
		}
};

struct q15_max_filter_t : public tuple_filter_t
{
	decimal _max;

	q15_max_filter_t()
	: tuple_filter_t(sizeof(q15_projected_lineitem_tuple)), _max(0)
	{
	}

	bool select(const tuple_t &input) {
		q15_projected_lineitem_tuple *tuple = aligned_cast<q15_projected_lineitem_tuple>(input.data);
		if(_max == 0) _max = tuple->REVENUE;
		return tuple->REVENUE == _max;
	}

	virtual q15_max_filter_t* clone() const {
		return new q15_max_filter_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("q15_max_filter_t");
	}
};



struct q15_l_join_s_t : public tuple_join_t {

	q15_l_join_s_t()
	: tuple_join_t(sizeof(q15_projected_lineitem_tuple),
			offsetof(q15_projected_lineitem_tuple, L_SUPPKEY),
			sizeof(q15_projected_supplier_tuple),
			offsetof(q15_projected_supplier_tuple, S_SUPPKEY),
			sizeof(int),
			sizeof(q15_final_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q15_final_tuple *dest = aligned_cast<q15_final_tuple>(d.data);
		q15_projected_lineitem_tuple *line = aligned_cast<q15_projected_lineitem_tuple>(l.data);
		q15_projected_supplier_tuple *supp = aligned_cast<q15_projected_supplier_tuple>(r.data);

		dest->S_SUPPKEY = supp->S_SUPPKEY;
		dest->TOTAL_REVENUE = line->REVENUE;
		memcpy(dest->S_ADDRESS, supp->S_ADDRESS, sizeof(dest->S_ADDRESS));
		memcpy(dest->S_NAME, supp->S_NAME, sizeof(dest->S_NAME));
		memcpy(dest->S_PHONE, supp->S_PHONE, sizeof(dest->S_PHONE));

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %.2f %s %s %s\n", line->L_SUPPKEY, supp->S_SUPPKEY, supp->S_SUPPKEY, line->REVENUE.to_double(), supp->S_ADDRESS, supp->S_NAME, supp->S_PHONE);
	}

	virtual q15_l_join_s_t* clone() const {
		return new q15_l_join_s_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join LINEITEM, SUPPLIER; select S_SUPPKEY, TOTAL_REVENUE, S_ADDRESS, S_NAME, S_PHONE");
	}
};



class tpch_q15_process_tuple_t : public process_tuple_t {

public:

	virtual void begin() {
		TRACE(TRACE_QUERY_RESULTS, "*** Q15 %s %s %s %s %s\n",
				"S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_PHONE", "TOTAL_REVENUE");
	}

	virtual void process(const tuple_t& output) {
		q15_final_tuple *agg = aligned_cast<q15_final_tuple>(output.data);

		TRACE(TRACE_QUERY_RESULTS, "*** Q15 %d %s %s %s %.4f\n", agg->S_SUPPKEY, agg->S_NAME, agg->S_ADDRESS, agg->S_PHONE, agg->TOTAL_REVENUE.to_double());
	}

};


/********************************************************************
 *
 * QPIPE q15 - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_qpipe_q15(const int xct_id,
		q15_input_t& in)
{
	TRACE( TRACE_ALWAYS, "********** q15 *********\n");

	policy_t* dp = this->get_sched_policy();
	xct_t* pxct = smthread_t::me()->xct();



	//TSCAN SUPPLIER
	tuple_fifo* q15_supplier_buffer = new tuple_fifo(sizeof(q15_projected_supplier_tuple));
	packet_t* q15_supplier_tscan_packet =
			new tscan_packet_t("supplier TSCAN",
					q15_supplier_buffer,
					new q15_supplier_tscan_filter_t(this, in),
					this->db(),
					_psupplier_desc.get(),
					pxct);

	//TSCAN LINEITEM
	tuple_fifo* q15_lineitem_buffer = new tuple_fifo(sizeof(q15_projected_lineitem_tuple));
	packet_t* q15_lineitem_tscan_packet =
			new tscan_packet_t("lineitem TSCAN",
					q15_lineitem_buffer,
					new q15_lineitem_tscan_filter_t(this, in),
					this->db(),
					_plineitem_desc.get(),
					pxct);

	//AGGREGATE: GROUP BY L_SUPPKEY, SUM
	tuple_fifo* q15_l_agg_buffer = new tuple_fifo(sizeof(q15_projected_lineitem_tuple));
	packet_t* q15_l_agg_packet =
			new partial_aggregate_packet_t("LINEITEM AGGREGATE",
					q15_l_agg_buffer,
					new trivial_filter_t(sizeof(q15_projected_lineitem_tuple)),
					q15_lineitem_tscan_packet,
					new q15_lineitem_aggregate_t(),
					new default_key_extractor_t(sizeof(int), offsetof(q15_projected_lineitem_tuple, L_SUPPKEY)),
					new int_key_compare_t());

	//SORT LINEITEM BY REVENUE + SELECT MAX
	tuple_fifo* q15_l_sort_buffer = new tuple_fifo(sizeof(q15_projected_lineitem_tuple));
	packet_t* q15_l_sort_packet =
			new sort_packet_t("SORT BY REVENUE",
					q15_l_sort_buffer,
					new q15_max_filter_t(),
					new q15_l_sort_key_extractor_t(),
					new q15_l_sort_key_compare_t(),
					q15_l_agg_packet);

	//LINEITEM JOIN SUPPLIER
	tuple_fifo* q15_l_join_s_buffer = new tuple_fifo(sizeof(q15_final_tuple));
	packet_t* q15_l_join_s_packet =
			new hash_join_packet_t("lineitem - supplier HJOIN",
					q15_l_join_s_buffer,
					new trivial_filter_t(sizeof(q15_final_tuple)),
					q15_l_sort_packet,
					q15_supplier_tscan_packet,
					new q15_l_join_s_t());

	//ORDER BY S_SUPPKEY
	tuple_fifo* q15_sort_buffer = new tuple_fifo(sizeof(q15_final_tuple));
	packet_t* q15_sort_packet =
			new sort_packet_t("SORT BY S_SUPPKEY",
					q15_sort_buffer,
					new trivial_filter_t(sizeof(q15_final_tuple)),
					new default_key_extractor_t(sizeof(int), offsetof(q15_final_tuple, S_SUPPKEY)),
					new int_key_compare_t(),
					q15_l_join_s_packet);


	qpipe::query_state_t* qs = dp->query_state_create();
	q15_supplier_tscan_packet->assign_query_state(qs);
	q15_lineitem_tscan_packet->assign_query_state(qs);
	q15_l_agg_packet->assign_query_state(qs);
	q15_l_sort_packet->assign_query_state(qs);
	q15_l_join_s_packet->assign_query_state(qs);
	q15_sort_packet->assign_query_state(qs);


	// Dispatch packet
	tpch_q15_process_tuple_t pt;
	//LAST PACKET
	process_query(q15_sort_packet, pt);//TODO

	dp->query_state_destroy(qs);


	return (RCOK);
}


EXIT_NAMESPACE(tpch);
