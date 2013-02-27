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

/** @file:   qpipe_q19.cpp
 *
 *  @brief:  Implementation of QPIPE TPCH Q19 over Shore-MT
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
 * QPIPE Q19 - Structures needed by operators
 *
 ********************************************************************/

/*
select
	sum(l_extendedprice * (1 - l_discount) ) as revenue
from
	lineitem,
	part
where
		(p_partkey = l_partkey
		and p_brand = '[BRAND1]'
		and p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		and l_quantity >= [QUANTITY1] and l_quantity <= [QUANTITY1] + 10
		and p_size between 1 and 5
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'  )
	or
		(p_partkey = l_partkey
		and p_brand = '[BRAND2]'
		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		and l_quantity >= [QUANTITY2] and l_quantity <= [QUANTITY2] + 10
		and p_size between 1 and 10
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON' )
	or
		(p_partkey = l_partkey
		and p_brand = '[BRAND3]'
		and p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		and l_quantity >= [QUANTITY3] and l_quantity <= [QUANTITY3] + 10
		and p_size between 1 and 15
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
);
 */


struct q19_projected_part_tuple {
	int P_PARTKEY;
	int CASE;
};

struct q19_projected_lineitem_tuple {
	int L_PARTKEY;
	decimal L_QUANTITY;
	decimal REVENUE;
};

struct q19_l_join_p_tuple {
	decimal REVENUE;
	decimal L_QUANTITY;
	int CASE;
};

struct q19_final_tuple {
	decimal REVENUE;
};


class q19_part_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prpart;
	rep_row_t _rr;

	/*One part tuple*/
	tpch_part_tuple _part;
	/*The columns needed for the selection*/
	char _brand1[STRSIZE(10)];
	char _brand2[STRSIZE(10)];
	char _brand3[STRSIZE(10)];

	q19_input_t* q19_input;

public:

	q19_part_tscan_filter_t(ShoreTPCHEnv* tpchdb, q19_input_t &in)
	: tuple_filter_t(tpchdb->part_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prpart = _tpchdb->part_man()->get_tuple();
		_rr.set_ts(_tpchdb->part_man()->ts(),
				_tpchdb->part_desc()->maxsize());
		_prpart->_rep = &_rr;

		q19_input = &in;
		Brand_to_srt(_brand1, q19_input->p_brand[0]);
		Brand_to_srt(_brand2, q19_input->p_brand[1]);
		Brand_to_srt(_brand3, q19_input->p_brand[2]);

		TRACE(TRACE_ALWAYS, "Random predicates:\nPART.P_BRAND = '%s' or PART.P_BRAND = '%s' or PART.P_BRAND = '%s'\n", _brand1, _brand2, _brand3);
	}

	virtual ~q19_part_tscan_filter_t()
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
		_prpart->get_value(5, _part.P_SIZE);
		_prpart->get_value(6, _part.P_CONTAINER, sizeof(_part.P_CONTAINER));

		if(strcmp(_part.P_BRAND, _brand1) == 0) {
			return (_part.P_SIZE >= 1 && _part.P_SIZE <= 5 &&
					(strcmp(_part.P_CONTAINER, "SM CASE") == 0 || strcmp(_part.P_CONTAINER, "SM BOX") == 0 ||
							strcmp(_part.P_CONTAINER, "SM PACK") == 0 || strcmp(_part.P_CONTAINER, "SM PKG") == 0));
		}
		else if(strcmp(_part.P_BRAND, _brand2) == 0) {
			return (_part.P_SIZE >= 1 && _part.P_SIZE <= 10 &&
					(strcmp(_part.P_CONTAINER, "MED BAG") == 0 || strcmp(_part.P_CONTAINER, "MED BOX") == 0 ||
							strcmp(_part.P_CONTAINER, "MED PACK") == 0 || strcmp(_part.P_CONTAINER, "MED PKG") == 0));
		}
		else if(strcmp(_part.P_BRAND, _brand3) == 0) {
			return (_part.P_SIZE >= 1 && _part.P_SIZE <= 15 &&
					(strcmp(_part.P_CONTAINER, "LG CASE") == 0 || strcmp(_part.P_CONTAINER, "LG BOX") == 0 ||
							strcmp(_part.P_CONTAINER, "LG PACK") == 0 || strcmp(_part.P_CONTAINER, "LG PKG") == 0));
		}
		else {
			return false;
		}
	}


	void project(tuple_t &d, const tuple_t &s) {

		q19_projected_part_tuple *dest;
		dest = aligned_cast<q19_projected_part_tuple>(d.data);

		_prpart->get_value(0, _part.P_PARTKEY);
		_prpart->get_value(3, _part.P_BRAND, sizeof(_part.P_BRAND));

		//TRACE( TRACE_RECORD_FLOW, "%d\t%s\n", _part.P_PARTKEY, _part.P_BRAND);

		dest->P_PARTKEY = _part.P_PARTKEY;
		if(strcmp(_part.P_BRAND, _brand1) == 0) {
			dest->CASE = 0;
		}
		else if(strcmp(_part.P_BRAND, _brand2) == 0) {
			dest->CASE = 1;
		}
		else {
			dest->CASE = 2;
		}
	}

	q19_part_tscan_filter_t* clone() const {
		return new q19_part_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q19_part_tscan_filter_t()");
	}
};

class q19_lineitem_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prline;
	rep_row_t _rr;

	tpch_lineitem_tuple _lineitem;

public:
	q19_lineitem_tscan_filter_t(ShoreTPCHEnv* tpchdb, q19_input_t &in)
	: tuple_filter_t(tpchdb->lineitem_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prline = _tpchdb->lineitem_man()->get_tuple();
		_rr.set_ts(_tpchdb->lineitem_man()->ts(),
				_tpchdb->lineitem_desc()->maxsize());
		_prline->_rep = &_rr;
	}

	virtual ~q19_lineitem_tscan_filter_t()
	{
		// Give back the lineitem tuple
		_tpchdb->lineitem_man()->give_tuple(_prline);
	}

	bool select(const tuple_t &input) {
		// Get next lineitem tuple and read its shipdate
		if (!_tpchdb->lineitem_man()->load(_prline, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		_prline->get_value(13, _lineitem.L_SHIPINSTRUCT, sizeof(_lineitem.L_SHIPINSTRUCT));
		_prline->get_value(14, _lineitem.L_SHIPMODE, sizeof(_lineitem.L_SHIPMODE));

		return (strcmp(_lineitem.L_SHIPINSTRUCT, "DELIVER IN PERSON") == 0 && (strcmp(_lineitem.L_SHIPMODE, "AIR") == 0 || strcmp(_lineitem.L_SHIPMODE, "AIR REG") == 0));
	}

	void project(tuple_t &d, const tuple_t &s) {

		q19_projected_lineitem_tuple *dest = aligned_cast<q19_projected_lineitem_tuple>(d.data);

		_prline->get_value(1, _lineitem.L_PARTKEY);
		_prline->get_value(4, _lineitem.L_QUANTITY);
		_prline->get_value(5, _lineitem.L_EXTENDEDPRICE);
		_prline->get_value(6, _lineitem.L_DISCOUNT);

		//TRACE(TRACE_RECORD_FLOW, "%d|%.2f\n", _lineitem.L_PARTKEY, _lineitem.L_EXTENDEDPRICE / 100.0 * (1 - _lineitem.L_DISCOUNT / 100.0));

		dest->L_PARTKEY = _lineitem.L_PARTKEY;
		dest->L_QUANTITY = _lineitem.L_QUANTITY;
		dest->REVENUE = _lineitem.L_EXTENDEDPRICE / 100.0 * (1 - _lineitem.L_DISCOUNT / 100.0);
	}

	q19_lineitem_tscan_filter_t* clone() const {
		return new q19_lineitem_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q19_lineitem_tscan_filter_t()");
	}
};



struct q19_l_join_p_t : public tuple_join_t {

	q19_l_join_p_t()
	: tuple_join_t(sizeof(q19_projected_lineitem_tuple),
			offsetof(q19_projected_lineitem_tuple, L_PARTKEY),
			sizeof(q19_projected_part_tuple),
			offsetof(q19_projected_part_tuple, P_PARTKEY),
			sizeof(int),
			sizeof(q19_l_join_p_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q19_l_join_p_tuple *dest = aligned_cast<q19_l_join_p_tuple>(d.data);
		q19_projected_lineitem_tuple *line = aligned_cast<q19_projected_lineitem_tuple>(l.data);
		q19_projected_part_tuple *part = aligned_cast<q19_projected_part_tuple>(r.data);

		dest->CASE = part->CASE;
		dest->L_QUANTITY = line->L_QUANTITY;
		dest->REVENUE = line->REVENUE;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %.2f %.2f\n", line->L_PARTKEY, part->P_PARTKEY, part->CASE, line->L_QUANTITY.to_double(), line->REVENUE.to_double());
	}

	virtual q19_l_join_p_t* clone() const {
		return new q19_l_join_p_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join LINEITEM, PART; select CASE, L_QUANTITY, REVENUE");
	}
};


struct q19_join_filter_t : public tuple_filter_t {

	int _quantity[3];

	q19_join_filter_t(int quantity[3])
	: tuple_filter_t(sizeof(q19_l_join_p_tuple))
	{
		memcpy(_quantity, quantity, sizeof(_quantity));
		TRACE(TRACE_ALWAYS, "Random predicates:\nCase 1: %d <= l_quantity <= %d\nCase 2: %d <= l_quantity <= %d\nCase 3: %d <= l_quantity <= %d\n", _quantity[0], _quantity[0] + 10,
								_quantity[1], _quantity[1] + 10, _quantity[2], _quantity[2] + 10);
	}

	bool select(const tuple_t &input) {
		q19_l_join_p_tuple *tuple = aligned_cast<q19_l_join_p_tuple>(input.data);
		return tuple->L_QUANTITY.to_int() >= _quantity[tuple->CASE] && tuple->L_QUANTITY.to_int() <= _quantity[tuple->CASE] + 10;
	}

	virtual void project(tuple_t &d, const tuple_t &s) {
		q19_final_tuple *dest = aligned_cast<q19_final_tuple>(d.data);
		q19_l_join_p_tuple *src = aligned_cast<q19_l_join_p_tuple>(s.data);

		dest->REVENUE = src->REVENUE;
	}

	virtual q19_join_filter_t* clone() const {
		return new q19_join_filter_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("q19_join_filter_t");
	}
};


struct q19_aggregate_t : public tuple_aggregate_t {

	default_key_extractor_t _extractor;

	q19_aggregate_t()
		: tuple_aggregate_t(sizeof(q19_final_tuple)), _extractor(0,0)
	{
	}

	virtual key_extractor_t* key_extractor() {
		return &_extractor;
	}

	virtual void aggregate(char* agg_data, const tuple_t &t) {
		q19_final_tuple *d = aligned_cast<q19_final_tuple>(agg_data);
		q19_final_tuple *s = aligned_cast<q19_final_tuple>(t.data);

		d->REVENUE += s->REVENUE;
	}
	virtual void finish(tuple_t &dest, const char* agg_data) {
		memcpy(dest.data, agg_data, dest.size);
	}
	virtual q19_aggregate_t* clone() const {
		return new q19_aggregate_t(*this);
	}
	virtual c_str to_string() const {
		return "q19_aggregate_t";
	}
};


class tpch_q19_process_tuple_t : public process_tuple_t {

public:

	virtual void begin() {
		TRACE(TRACE_QUERY_RESULTS, "*** Q19 %s\n",
				"REVENUE");
	}

	virtual void process(const tuple_t& output) {
		q19_final_tuple *agg = aligned_cast<q19_final_tuple>(output.data);

		TRACE(TRACE_QUERY_RESULTS, "*** Q19 %.4f\n", agg->REVENUE.to_double());
	}

};


/********************************************************************
 *
 * QPIPE q19 - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_qpipe_q19(const int xct_id,
		q19_input_t& in)
{
	TRACE( TRACE_ALWAYS, "********** q19 *********\n");

	policy_t* dp = this->get_sched_policy();
	xct_t* pxct = smthread_t::me()->xct();



	//TSCAN LINEITEM
	tuple_fifo* q19_lineitem_buffer = new tuple_fifo(sizeof(q19_projected_lineitem_tuple));
	packet_t* q19_lineitem_tscan_packet =
			new tscan_packet_t("lineitem TSCAN",
					q19_lineitem_buffer,
					new q19_lineitem_tscan_filter_t(this, in),
					this->db(),
					_plineitem_desc.get(),
					pxct);

	//TSCAN PART
	tuple_fifo* q19_part_buffer = new tuple_fifo(sizeof(q19_projected_part_tuple));
	packet_t* q19_part_tscan_packet =
			new tscan_packet_t("part TSCAN",
					q19_part_buffer,
					new q19_part_tscan_filter_t(this, in),
					this->db(),
					_ppart_desc.get(),
					pxct);


	//LINEITEM JOIN PART
	tuple_fifo* q19_l_join_p_buffer = new tuple_fifo(sizeof(q19_final_tuple));
	packet_t* q19_l_join_p_packet =
			new hash_join_packet_t("lineitem - part HJOIN",
					q19_l_join_p_buffer,
					new q19_join_filter_t((&in)->l_quantity),
					q19_lineitem_tscan_packet,
					q19_part_tscan_packet,
					new q19_l_join_p_t());

	//AGGREGATE
	tuple_fifo* q19_agg_buffer = new tuple_fifo(sizeof(q19_final_tuple));
	packet_t* q19_agg_packet =
			new aggregate_packet_t("SUM AGG",
					q19_agg_buffer,
					new trivial_filter_t(sizeof(q19_final_tuple)),
					new q19_aggregate_t(),
					new default_key_extractor_t(0,0),
					q19_l_join_p_packet);


	qpipe::query_state_t* qs = dp->query_state_create();
	q19_lineitem_tscan_packet->assign_query_state(qs);
	q19_part_tscan_packet->assign_query_state(qs);
	q19_l_join_p_packet->assign_query_state(qs);
	q19_agg_packet->assign_query_state(qs);


	// Dispatch packet
	tpch_q19_process_tuple_t pt;
	//LAST PACKET
	process_query(q19_agg_packet, pt);//TODO

	dp->query_state_destroy(qs);


	return (RCOK);
}

EXIT_NAMESPACE(tpch);
