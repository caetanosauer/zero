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

/** @file:   qpipe_q16.cpp
 *
 *  @brief:  Implementation of QPIPE TPCH Q16 over Shore-MT
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
 * QPIPE Q16 - Structures needed by operators
 *
 ********************************************************************/

/*
select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from
	partsupp,
	part
where
	p_partkey = ps_partkey
	and p_brand <> '[BRAND]'
	and p_type not like '[TYPE]%'
	and p_size in ([SIZE1], [SIZE2], [SIZE3], [SIZE4], [SIZE5], [SIZE6], [SIZE7], [SIZE8])
	and ps_suppkey not in (
		select
			s_suppkey
		from
			supplier
		where
			s_comment like '%Customer%Complaints%' )
group by
	p_brand,
	p_type,
	p_size
order by
	supplier_cnt desc,
	p_brand,
	p_type,
	p_size;


	Query partly rewritten:
	...
	and ps_suppkey in (
		select
			s_suppkey
		from
			supplier
		where
			s_comment not like '%Customer%Complaints%' )
	...
 */


struct q16_projected_partsupp_tuple {
	int PS_PARTKEY;
	int PS_SUPPKEY;
};

struct q16_projected_part_tuple {
	int P_PARTKEY;
	char P_BRAND[STRSIZE(10)];
	char P_TYPE[STRSIZE(25)];
	int P_SIZE;
};

struct q16_projected_supplier_tuple {
	int S_SUPPKEY;
};

struct q16_ps_join_p_tuple {
	int PS_SUPPKEY;
	char P_BRAND[STRSIZE(10)];
	char P_TYPE[STRSIZE(25)];
	int P_SIZE;
};

struct q16_all_joins_tuple {
	char P_BRAND[STRSIZE(10)];
	char P_TYPE[STRSIZE(25)];
	int P_SIZE;
	int PS_SUPPKEY;
};

struct q16_aggregate_tuple {
	char P_BRAND[STRSIZE(10)];
	char P_TYPE[STRSIZE(25)];
	int P_SIZE;
	int SUPPLIER_CNT;
};

struct q16_aggregate_key {
	char P_BRAND[STRSIZE(10)];
	char P_TYPE[STRSIZE(25)];
	int P_SIZE;
};

struct q16_sort_key {
	char P_BRAND[STRSIZE(10)];
	char P_TYPE[STRSIZE(25)];
	int P_SIZE;
	int SUPP_CNT;
};


class q16_partsupp_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prpartsupp;
	rep_row_t _rr;

	/*One partsupp tuple*/
	tpch_partsupp_tuple _partsupp;

public:
	q16_partsupp_tscan_filter_t(ShoreTPCHEnv* tpchdb, q16_input_t &in)
	: tuple_filter_t(tpchdb->partsupp_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prpartsupp = _tpchdb->partsupp_man()->get_tuple();
		_rr.set_ts(_tpchdb->partsupp_man()->ts(),
				_tpchdb->partsupp_desc()->maxsize());
		_prpartsupp->_rep = &_rr;
	}

	virtual ~q16_partsupp_tscan_filter_t()
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

		q16_projected_partsupp_tuple *dest;
		dest = aligned_cast<q16_projected_partsupp_tuple>(d.data);

		_prpartsupp->get_value(0, _partsupp.PS_PARTKEY);
		_prpartsupp->get_value(1, _partsupp.PS_SUPPKEY);

		//TRACE( TRACE_RECORD_FLOW, "%d|%d\n",
		//		_partsupp.PS_SUPPKEY,
		//		_partsupp.PS_PARTKEY);

		dest->PS_SUPPKEY = _partsupp.PS_SUPPKEY;
		dest->PS_PARTKEY = _partsupp.PS_PARTKEY;
	}

	q16_partsupp_tscan_filter_t* clone() const {
		return new q16_partsupp_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q16_partsupp_tscan_filter_t()");
	}
};

class q16_part_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prpart;
	rep_row_t _rr;

	/*One part tuple*/
	tpch_part_tuple _part;
	/*The columns needed for the selection*/
	char _type[STRSIZE(25)];
	char _type2[STRSIZE(25)];
	char _brand[STRSIZE(10)];
	int _size[8];

	q16_input_t* q16_input;

public:

	q16_part_tscan_filter_t(ShoreTPCHEnv* tpchdb, q16_input_t &in)
	: tuple_filter_t(tpchdb->part_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prpart = _tpchdb->part_man()->get_tuple();
		_rr.set_ts(_tpchdb->part_man()->ts(),
				_tpchdb->part_desc()->maxsize());
		_prpart->_rep = &_rr;

		q16_input = &in;
		types1_to_str(_type, q16_input->p_type / 10);
		strcat(_type, " ");
		types2_to_str(_type2, q16_input->p_type % 10);
		strcat(_type, _type2);
		Brand_to_srt(_brand, q16_input->p_brand);
		memcpy(_size, q16_input->p_size, sizeof(_size));

		TRACE(TRACE_ALWAYS, "Random predicates:\nPART.P_BRAND <> '%s' AND PART.P_TYPE not like '%s%%'\nAND P_SIZE in (%d, %d, %d, %d, %d, %d, %d, %d)\n",
				_brand, _type, _size[0], _size[1], _size[2], _size[3], _size[4], _size[5], _size[6], _size[7]);
	}

	virtual ~q16_part_tscan_filter_t()
	{
		// Give back the part tuple
		_tpchdb->part_man()->give_tuple(_prpart);
	}



	bool select(const tuple_t &input) {

		// Get next part and read its size and type
		if (!_tpchdb->part_man()->load(_prpart, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		_prpart->get_value(3, _part.P_BRAND, sizeof(_part.P_BRAND));
		_prpart->get_value(4, _part.P_TYPE, sizeof(_part.P_TYPE));
		_prpart->get_value(5, _part.P_SIZE);
		int size = _part.P_SIZE;

		return strcmp(_part.P_BRAND, _brand) != 0 && !strstr(_part.P_TYPE, _type) &&
				(size == _size[0] || size == _size[1] || size == _size[2] || size == _size[3] || size == _size[4] || size == _size[5] || size == _size[6] || size == _size[7]);
	}


	void project(tuple_t &d, const tuple_t &s) {

		q16_projected_part_tuple *dest;
		dest = aligned_cast<q16_projected_part_tuple>(d.data);

		_prpart->get_value(0, _part.P_PARTKEY);
		_prpart->get_value(3, _part.P_BRAND, sizeof(_part.P_BRAND));
		_prpart->get_value(4, _part.P_TYPE, sizeof(_part.P_TYPE));
		_prpart->get_value(5, _part.P_SIZE);

		//TRACE( TRACE_RECORD_FLOW, "%d|%s|%s|%d\n",
		//	_part.P_PARTKEY, _part.P_BRAND, _part.P_TYPE, _part.P_SIZE);

		dest->P_PARTKEY = _part.P_PARTKEY;
		memcpy(dest->P_BRAND, _part.P_BRAND, sizeof(dest->P_BRAND));
		memcpy(dest->P_TYPE, _part.P_TYPE, sizeof(dest->P_TYPE));
		dest->P_SIZE = _part.P_SIZE;
	}

	q16_part_tscan_filter_t* clone() const {
		return new q16_part_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q16_part_tscan_filter_t()");
	}
};

class q16_supplier_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prsupplier;
	rep_row_t _rr;

	/*One supplier tuple*/
	tpch_supplier_tuple _supplier;

public:
	q16_supplier_tscan_filter_t(ShoreTPCHEnv* tpchdb, q16_input_t &in)
	: tuple_filter_t(tpchdb->supplier_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prsupplier = _tpchdb->supplier_man()->get_tuple();
		_rr.set_ts(_tpchdb->supplier_man()->ts(),
				_tpchdb->supplier_desc()->maxsize());
		_prsupplier->_rep = &_rr;
	}

	virtual ~q16_supplier_tscan_filter_t()
	{
		_tpchdb->supplier_man()->give_tuple(_prsupplier);
	}

	bool select(const tuple_t &input) {

		// Get next supplier tuple
		if (!_tpchdb->supplier_man()->load(_prsupplier, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		_prsupplier->get_value(6, _supplier.S_COMMENT, sizeof(_supplier.S_COMMENT));

		//Verified that 'Customer' and 'Complaints' always occur in that order
		return !(strstr(_supplier.S_COMMENT, "Customer") && strstr(_supplier.S_COMMENT, "Complaints"));
	}


	void project(tuple_t &d, const tuple_t &s) {

		q16_projected_supplier_tuple *dest = aligned_cast<q16_projected_supplier_tuple>(d.data);


		_prsupplier->get_value(0, _supplier.S_SUPPKEY);

		//TRACE( TRACE_RECORD_FLOW, "%d\n",
		//		_supplier.S_SUPPKEY);

		dest->S_SUPPKEY = _supplier.S_SUPPKEY;

	}

	q16_supplier_tscan_filter_t* clone() const {
		return new q16_supplier_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q16_supplier_tscan_filter_t");
	}
};



struct q16_ps_join_p_t : public tuple_join_t {

	q16_ps_join_p_t()
	: tuple_join_t(sizeof(q16_projected_partsupp_tuple),
			offsetof(q16_projected_partsupp_tuple, PS_PARTKEY),
			sizeof(q16_projected_part_tuple),
			offsetof(q16_projected_part_tuple, P_PARTKEY),
			sizeof(int),
			sizeof(q16_ps_join_p_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q16_ps_join_p_tuple *dest = aligned_cast<q16_ps_join_p_tuple>(d.data);
		q16_projected_partsupp_tuple *partsupp = aligned_cast<q16_projected_partsupp_tuple>(l.data);
		q16_projected_part_tuple *part = aligned_cast<q16_projected_part_tuple>(r.data);

		dest->PS_SUPPKEY = partsupp->PS_SUPPKEY;
		memcpy(dest->P_BRAND, part->P_BRAND, sizeof(dest->P_BRAND));
		dest->P_SIZE = part->P_SIZE;
		memcpy(dest->P_TYPE, part->P_TYPE, sizeof(dest->P_TYPE));

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %s %d %s\n", partsupp->PS_PARTKEY, part->P_PARTKEY, partsupp->PS_SUPPKEY, part->P_BRAND, part->P_SIZE, part->P_TYPE);
	}

	virtual q16_ps_join_p_t* clone() const {
		return new q16_ps_join_p_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join PARTSUPP, PART; select PS_SUPPKEY, P_BRAND, P_SIZE, P_TYPE");
	}
};

struct q16_ps_p_join_s_t : public tuple_join_t {

	q16_ps_p_join_s_t()
	: tuple_join_t(sizeof(q16_ps_join_p_tuple),
			offsetof(q16_ps_join_p_tuple, PS_SUPPKEY),
			sizeof(q16_projected_supplier_tuple),
			offsetof(q16_projected_supplier_tuple, S_SUPPKEY),
			sizeof(int),
			sizeof(q16_all_joins_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q16_all_joins_tuple *dest = aligned_cast<q16_all_joins_tuple>(d.data);
		q16_ps_join_p_tuple *left = aligned_cast<q16_ps_join_p_tuple>(l.data);
		q16_projected_supplier_tuple *supp = aligned_cast<q16_projected_supplier_tuple>(r.data);

		dest->PS_SUPPKEY = left->PS_SUPPKEY;
		memcpy(dest->P_BRAND, left->P_BRAND, sizeof(dest->P_BRAND));
		dest->P_SIZE = left->P_SIZE;
		memcpy(dest->P_TYPE, left->P_TYPE, sizeof(dest->P_TYPE));

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %s %d %s\n", left->PS_SUPPKEY, supp->S_SUPPKEY, left->PS_SUPPKEY, left->P_BRAND, left->P_SIZE, left->P_TYPE);
	}

	virtual q16_ps_p_join_s_t* clone() const {
		return new q16_ps_p_join_s_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join PARTSUPP_PART, SUPPLIER; select PS_SUPPKEY, P_BRAND, P_SIZE, P_TYPE");
	}
};


class q16_distinct_t : public tuple_aggregate_t {

public:

	class q16_distinct_key_extractor_t : public key_extractor_t {
	public:
		q16_distinct_key_extractor_t()
		: key_extractor_t(sizeof(q16_all_joins_tuple))
		{
		}

		virtual int extract_hint(const char* tuple_data) const {
			// store the return flag and line status in the
			q16_all_joins_tuple *item;
			item = aligned_cast<q16_all_joins_tuple>(tuple_data);

			return (item->P_BRAND[0] << 24) + (item->P_BRAND[1] << 16) + (item->P_BRAND[2] << 8) + item->P_BRAND[3];
		}

		virtual key_extractor_t* clone() const {
			return new q16_distinct_key_extractor_t(*this);
		}
	};

	private:

	q16_distinct_key_extractor_t _extractor;

	public:

	q16_distinct_t()
	: tuple_aggregate_t(sizeof(q16_all_joins_tuple)),
	  _extractor()
	{
	}

	virtual key_extractor_t* key_extractor() { return &_extractor; }

	virtual void aggregate(char* agg_data, const tuple_t& src) {
		//memcpy(agg_data, src.data, sizeof(agg_data));
	}

	virtual void finish(tuple_t &dest, const char* agg_data) {
		memcpy(dest.data, agg_data, sizeof(q16_all_joins_tuple));
	}

	virtual q16_distinct_t* clone() const {
		return new q16_distinct_t(*this);
	}

	virtual c_str to_string() const {
		return "q16_distinct_t";
	}
};

struct q16_key_compare_t : public key_compare_t {

	bool _distinct;

	q16_key_compare_t(bool distinct)
	: _distinct(distinct)
	{
	}

	virtual int operator()(const void* key1, const void* key2) const {
		int diff_brand;
		int diff_type;
		int diff_size;
		if(_distinct) {
			q16_all_joins_tuple *k1 = aligned_cast<q16_all_joins_tuple>(key1);
			q16_all_joins_tuple *k2 = aligned_cast<q16_all_joins_tuple>(key2);
			diff_brand = strcmp(k1->P_BRAND, k2->P_BRAND);
			diff_type = strcmp(k1->P_TYPE, k2->P_TYPE);
			diff_size = k1->P_SIZE - k2->P_SIZE;
			return(diff_brand != 0 ? diff_brand : (diff_type != 0 ? diff_type : (diff_size != 0 ? diff_size : k1->PS_SUPPKEY - k2->PS_SUPPKEY)));
		}
		else {
			q16_aggregate_key *k1 = aligned_cast<q16_aggregate_key>(key1);
			q16_aggregate_key *k2 = aligned_cast<q16_aggregate_key>(key2);
			diff_brand = strcmp(k1->P_BRAND, k2->P_BRAND);
			diff_type = strcmp(k1->P_TYPE, k2->P_TYPE);
			diff_size = k1->P_SIZE - k2->P_SIZE;
			return(diff_brand != 0 ? diff_brand : (diff_type != 0 ? diff_type : diff_size));
		}
	}

	virtual q16_key_compare_t* clone() const {
		return new q16_key_compare_t(*this);
	}
};

class q16_aggregate_t : public tuple_aggregate_t {
public:

	class q16_agg_key_extractor_t : public key_extractor_t {
	public:
		q16_agg_key_extractor_t()
		: key_extractor_t(sizeof(q16_aggregate_key))
		{
		}

		virtual int extract_hint(const char* tuple_data) const {
			q16_aggregate_key *item;
			item = aligned_cast<q16_aggregate_key>(tuple_data);

			return (item->P_BRAND[0] << 24) + (item->P_BRAND[1] << 16) + (item->P_BRAND[2] << 8) + item->P_BRAND[3];
		}

		virtual q16_agg_key_extractor_t* clone() const {
			return new q16_agg_key_extractor_t(*this);
		}
	};

	private:

	q16_agg_key_extractor_t _extractor;

	public:

	q16_aggregate_t()
	: tuple_aggregate_t(sizeof(q16_aggregate_tuple)),
	  _extractor()
	{
	}

	virtual key_extractor_t* key_extractor() { return &_extractor; }

	virtual void aggregate(char* agg_data, const tuple_t& src) {
		q16_aggregate_tuple *out = aligned_cast<q16_aggregate_tuple>(agg_data);
		q16_all_joins_tuple *in = aligned_cast<q16_all_joins_tuple>(src.data);
		out->SUPPLIER_CNT++;
		//TRACE(TRACE_RECORD_FLOW, "%s\t%d\t%s\t%d\n", out->P_BRAND, out->P_SIZE, out->P_TYPE, out->SUPPLIER_CNT);
	}

	virtual void finish(tuple_t &dest, const char* agg_data) {
		memcpy(dest.data, agg_data, dest.size);
	}

	virtual q16_aggregate_t* clone() const {
		return new q16_aggregate_t(*this);
	}

	virtual c_str to_string() const {
		return "q16_aggregate_t";
	}
};

struct q16_sort_key_extractor_t : public key_extractor_t {

	q16_sort_key_extractor_t()
	: key_extractor_t(sizeof(q16_sort_key))
	{
	}

	virtual int extract_hint(const char* key) const {
		q16_sort_key *k = aligned_cast<q16_sort_key>(key);
		return -(k->SUPP_CNT);
	}

	virtual q16_sort_key_extractor_t* clone() const {
		return new q16_sort_key_extractor_t(*this);
	}
};

struct q16_sort_key_compare_t : public key_compare_t {

	virtual int operator()(const void* key1, const void* key2) const {
		q16_sort_key *k1 = aligned_cast<q16_sort_key>(key1);
		q16_sort_key *k2 = aligned_cast<q16_sort_key>(key2);

		int diff_cnt = k2->SUPP_CNT - k1->SUPP_CNT;
		int diff_brand = strcmp(k1->P_BRAND, k2->P_BRAND);
		int diff_type = strcmp(k1->P_TYPE, k2->P_TYPE);
		return (diff_cnt != 0 ? diff_cnt : (diff_brand != 0 ? diff_brand : (diff_type != 0 ? diff_type : k1->P_SIZE - k2->P_SIZE)));
	}

	virtual q16_sort_key_compare_t* clone() const {
		return new q16_sort_key_compare_t(*this);
	}
};



class tpch_q16_process_tuple_t : public process_tuple_t {

public:

	virtual void begin() {
		TRACE(TRACE_QUERY_RESULTS, "*** Q16 %s %s %s %s\n",
				"P_BRAND", "P_TYPE", "P_SIZE", "SUPPLIER_CNT");
	}

	virtual void process(const tuple_t& output) {
		q16_aggregate_tuple *agg = aligned_cast<q16_aggregate_tuple>(output.data);

		TRACE(TRACE_QUERY_RESULTS, "*** Q16 %s %s %d %d\n", agg->P_BRAND, agg->P_TYPE, agg->P_SIZE, agg->SUPPLIER_CNT);
	}

};


/********************************************************************
 *
 * QPIPE q16 - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_qpipe_q16(const int xct_id,
		q16_input_t& in)
{
	TRACE( TRACE_ALWAYS, "********** q16 *********\n");

	policy_t* dp = this->get_sched_policy();
	xct_t* pxct = smthread_t::me()->xct();


	//TSCAN PARTSUPP
	tuple_fifo* q16_partsupp_buffer = new tuple_fifo(sizeof(q16_projected_partsupp_tuple));
	packet_t* q16_partsupp_tscan_packet =
			new tscan_packet_t("partsupp TSCAN",
					q16_partsupp_buffer,
					new q16_partsupp_tscan_filter_t(this, in),
					this->db(),
					_ppartsupp_desc.get(),
					pxct);

	//TSCAN PART
	tuple_fifo* q16_part_buffer = new tuple_fifo(sizeof(q16_projected_part_tuple));
	packet_t* q16_part_tscan_packet =
			new tscan_packet_t("part TSCAN",
					q16_part_buffer,
					new q16_part_tscan_filter_t(this, in),
					this->db(),
					_ppart_desc.get(),
					pxct);

	//TSCAN SUPPLIER
	tuple_fifo* q16_supplier_buffer = new tuple_fifo(sizeof(q16_projected_supplier_tuple));
	packet_t* q16_supplier_tscan_packet =
			new tscan_packet_t("supplier TSCAN",
					q16_supplier_buffer,
					new q16_supplier_tscan_filter_t(this, in),
					this->db(),
					_psupplier_desc.get(),
					pxct);


	//PARTSUPP JOIN PART
	tuple_fifo* q16_ps_join_p_buffer = new tuple_fifo(sizeof(q16_ps_join_p_tuple));
	packet_t* q16_ps_join_p_packet =
			new hash_join_packet_t("partsupp - part HJOIN",
					q16_ps_join_p_buffer,
					new trivial_filter_t(sizeof(q16_ps_join_p_tuple)),
					q16_partsupp_tscan_packet,
					q16_part_tscan_packet,
					new q16_ps_join_p_t());

	//PARTSUPP_PART JOIN SUPPLIER
	tuple_fifo* q16_ps_p_join_s_buffer = new tuple_fifo(sizeof(q16_all_joins_tuple));
	packet_t* q16_ps_p_join_s_packet =
			new hash_join_packet_t("partsupp_part - supplier HJOIN",
					q16_ps_p_join_s_buffer,
					new trivial_filter_t(sizeof(q16_all_joins_tuple)),
					q16_ps_join_p_packet,
					q16_supplier_tscan_packet,
					new q16_ps_p_join_s_t());

	//DISTINCT AGG
	tuple_fifo* q16_distinct_buffer = new tuple_fifo(sizeof(q16_all_joins_tuple));
	packet_t* q16_distinct_packet =
			new partial_aggregate_packet_t("DISTINCT AGG",
					q16_distinct_buffer,
					new trivial_filter_t(sizeof(q16_all_joins_tuple)),
					q16_ps_p_join_s_packet,
					new q16_distinct_t(),
					new q16_distinct_t::q16_distinct_key_extractor_t(),
					new q16_key_compare_t(true));

	//GROUP BY AGG
	tuple_fifo* q16_agg_buffer = new tuple_fifo(sizeof(q16_aggregate_tuple));
	packet_t* q16_agg_packet =
			new partial_aggregate_packet_t("GROUP BY AGG",
					q16_agg_buffer,
					new trivial_filter_t(sizeof(q16_aggregate_tuple)),
					q16_distinct_packet,
					new q16_aggregate_t(),
					new q16_aggregate_t::q16_agg_key_extractor_t(),
					new q16_key_compare_t(false));

	//SORT
	tuple_fifo* q16_sort_buffer = new tuple_fifo(sizeof(q16_aggregate_tuple));
	packet_t* q16_sort_packet =
			new sort_packet_t("ORDER BY SUPP_CNT",
					q16_sort_buffer,
					new trivial_filter_t(sizeof(q16_aggregate_tuple)),
					new q16_sort_key_extractor_t(),
					new q16_sort_key_compare_t(),
					q16_agg_packet);


	qpipe::query_state_t* qs = dp->query_state_create();
	q16_partsupp_tscan_packet->assign_query_state(qs);
	q16_part_tscan_packet->assign_query_state(qs);
	q16_supplier_tscan_packet->assign_query_state(qs);
	q16_ps_join_p_packet->assign_query_state(qs);
	q16_ps_p_join_s_packet->assign_query_state(qs);
	q16_distinct_packet->assign_query_state(qs);
	q16_agg_packet->assign_query_state(qs);
	q16_sort_packet->assign_query_state(qs);


	// Dispatch packet
	tpch_q16_process_tuple_t pt;
	//LAST PACKET
	process_query(q16_sort_packet, pt);//TODO

	dp->query_state_destroy(qs);


	return (RCOK);
}

EXIT_NAMESPACE(tpch);
