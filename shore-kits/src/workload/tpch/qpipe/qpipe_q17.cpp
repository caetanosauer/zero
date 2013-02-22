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

/** @file:   qpipe_q17.cpp
 *
 *  @brief:  Implementation of QPIPE TPCH Q17 over Shore-MT
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
 * QPIPE Q17 - Structures needed by operators
 *
 ********************************************************************/

/*
select
	sum(l_extendedprice) / 7.0 as avg_yearly
from
	lineitem,
	part
where
	p_partkey = l_partkey
	and p_brand = '[BRAND]'
	and p_container = '[CONTAINER]'
	and l_quantity < (
		select
			0.2 * avg(l_quantity)
		from
			lineitem
		where
			l_partkey = p_partkey );
 */


struct q17_projected_lineitem_sub_tuple {
	int L_PARTKEY;
	decimal L_QUANTITY;
};

struct q17_projected_part_tuple {
	int P_PARTKEY;
};

struct q17_projected_lineitem_tuple {
	int L_PARTKEY;
	decimal L_QUANTITY;
	decimal L_EXTENDEDPRICE;
};

struct q17_sub_aggregate_tuple {
	int L_PARTKEY;
	decimal L_QUANTITY_SUM;
	int L_COUNT;
	decimal AVG_QTY;
};

struct q17_l_join_p_tuple {
	int P_PARTKEY;
	decimal L_EXTENDEDPRICE;
	decimal L_QUANTITY;
};

struct q17_all_join_tuple {
	decimal L_EXTENDEDPRICE;
	decimal AVG_QTY;
	decimal L_QUANTITY;
};

struct q17_final_tuple {
	decimal AVG_YEARLY;
};


class q17_lineitem_sub_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prline;
	rep_row_t _rr;

	tpch_lineitem_tuple _lineitem;

public:
	q17_lineitem_sub_tscan_filter_t(ShoreTPCHEnv* tpchdb, q17_input_t &in)
	: tuple_filter_t(tpchdb->lineitem_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prline = _tpchdb->lineitem_man()->get_tuple();
		_rr.set_ts(_tpchdb->lineitem_man()->ts(),
				_tpchdb->lineitem_desc()->maxsize());
		_prline->_rep = &_rr;
	}

	virtual ~q17_lineitem_sub_tscan_filter_t()
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

		q17_projected_lineitem_sub_tuple *dest = aligned_cast<q17_projected_lineitem_sub_tuple>(d.data);

		_prline->get_value(1, _lineitem.L_PARTKEY);
		_prline->get_value(4, _lineitem.L_QUANTITY);

		//TRACE(TRACE_RECORD_FLOW, "%d|%.2f\n", _lineitem.L_PARTKEY, _lineitem.L_QUANTITY);

		dest->L_PARTKEY = _lineitem.L_PARTKEY;
		dest->L_QUANTITY = _lineitem.L_QUANTITY;
	}

	q17_lineitem_sub_tscan_filter_t* clone() const {
		return new q17_lineitem_sub_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q17_lineitem_sub_tscan_filter_t()");
	}
};

class q17_part_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prpart;
	rep_row_t _rr;

	/*One part tuple*/
	tpch_part_tuple _part;
	/*The columns needed for the selection*/
	char _container[STRSIZE(10)];
	char _brand[STRSIZE(10)];

	q17_input_t* q17_input;

public:

	q17_part_tscan_filter_t(ShoreTPCHEnv* tpchdb, q17_input_t &in)
	: tuple_filter_t(tpchdb->part_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prpart = _tpchdb->part_man()->get_tuple();
		_rr.set_ts(_tpchdb->part_man()->ts(),
				_tpchdb->part_desc()->maxsize());
		_prpart->_rep = &_rr;

		q17_input = &in;
		Brand_to_srt(_brand, q17_input->p_brand);
		container_to_str(q17_input->p_container, _container);

		TRACE(TRACE_ALWAYS, "Random predicates:\nPART.P_BRAND = '%s' AND PART.P_CONTAINER = '%s'\n",
				_brand, _container);
	}

	virtual ~q17_part_tscan_filter_t()
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
		_prpart->get_value(6, _part.P_CONTAINER, sizeof(_part.P_CONTAINER));

		return strcmp(_part.P_BRAND, _brand) == 0 && strcmp(_part.P_CONTAINER, _container) == 0;
	}


	void project(tuple_t &d, const tuple_t &s) {

		q17_projected_part_tuple *dest;
		dest = aligned_cast<q17_projected_part_tuple>(d.data);

		_prpart->get_value(0, _part.P_PARTKEY);

		//TRACE( TRACE_RECORD_FLOW, "%d\n", _part.P_PARTKEY);

		dest->P_PARTKEY = _part.P_PARTKEY;
	}

	q17_part_tscan_filter_t* clone() const {
		return new q17_part_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q17_part_tscan_filter_t()");
	}
};

class q17_lineitem_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prline;
	rep_row_t _rr;

	tpch_lineitem_tuple _lineitem;

public:
	q17_lineitem_tscan_filter_t(ShoreTPCHEnv* tpchdb, q17_input_t &in)
	: tuple_filter_t(tpchdb->lineitem_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prline = _tpchdb->lineitem_man()->get_tuple();
		_rr.set_ts(_tpchdb->lineitem_man()->ts(),
				_tpchdb->lineitem_desc()->maxsize());
		_prline->_rep = &_rr;
	}

	virtual ~q17_lineitem_tscan_filter_t()
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

		q17_projected_lineitem_tuple *dest = aligned_cast<q17_projected_lineitem_tuple>(d.data);

		_prline->get_value(1, _lineitem.L_PARTKEY);
		_prline->get_value(4, _lineitem.L_QUANTITY);
		_prline->get_value(5, _lineitem.L_EXTENDEDPRICE);

		//TRACE(TRACE_RECORD_FLOW, "%d|%.2f|%.2f\n", _lineitem.L_PARTKEY, _lineitem.L_QUANTITY, _lineitem.L_EXTENDEDPRICE / 100.0);

		dest->L_PARTKEY = _lineitem.L_PARTKEY;
		dest->L_QUANTITY = _lineitem.L_QUANTITY;
		dest->L_EXTENDEDPRICE = _lineitem.L_EXTENDEDPRICE / 100.0;
	}

	q17_lineitem_tscan_filter_t* clone() const {
		return new q17_lineitem_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q17_lineitem_tscan_filter_t()");
	}
};


struct q17_sub_aggregate_t : public tuple_aggregate_t {

	default_key_extractor_t _extractor;

	q17_sub_aggregate_t()
	: tuple_aggregate_t(sizeof(q17_sub_aggregate_tuple)), _extractor(sizeof(int), offsetof(q17_projected_lineitem_sub_tuple, L_PARTKEY))
	{
	}

	virtual key_extractor_t* key_extractor() {
		return &_extractor;
	}

	virtual void aggregate(char* agg_data, const tuple_t &t) {
		q17_sub_aggregate_tuple *agg = aligned_cast<q17_sub_aggregate_tuple>(agg_data);
		q17_projected_lineitem_sub_tuple *input = aligned_cast<q17_projected_lineitem_sub_tuple>(t.data);

		agg->L_QUANTITY_SUM += input->L_QUANTITY;
		agg->L_COUNT++;
	}
	virtual void finish(tuple_t &dest, const char* agg_data) {
		memcpy(dest.data, agg_data, dest.size);
		q17_sub_aggregate_tuple *agg = aligned_cast<q17_sub_aggregate_tuple>(dest.data);
		agg->AVG_QTY = agg->L_QUANTITY_SUM.to_double() / agg->L_COUNT;
		//TRACE(TRACE_RECORD_FLOW, "%.2f\t%.2f\t%d\t%d\n", agg->AVG_QTY.to_double(), agg->L_QUANTITY_SUM.to_double(), agg->L_COUNT, agg->L_PARTKEY);
	}
	virtual q17_sub_aggregate_t* clone() const {
		return new q17_sub_aggregate_t(*this);
	}
	virtual c_str to_string() const {
		return "q17_sub_aggregate_t";
	}
};

struct q17_l_join_p_t : public tuple_join_t {

	q17_l_join_p_t()
	: tuple_join_t(sizeof(q17_projected_lineitem_tuple),
			offsetof(q17_projected_lineitem_tuple, L_PARTKEY),
			sizeof(q17_projected_part_tuple),
			offsetof(q17_projected_part_tuple, P_PARTKEY),
			sizeof(int),
			sizeof(q17_l_join_p_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q17_l_join_p_tuple *dest = aligned_cast<q17_l_join_p_tuple>(d.data);
		q17_projected_lineitem_tuple *line = aligned_cast<q17_projected_lineitem_tuple>(l.data);
		q17_projected_part_tuple *part = aligned_cast<q17_projected_part_tuple>(r.data);

		dest->P_PARTKEY = part->P_PARTKEY;
		dest->L_EXTENDEDPRICE = line->L_EXTENDEDPRICE;
		dest->L_QUANTITY = line->L_QUANTITY;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %.2f %.2f\n", line->L_PARTKEY, part->P_PARTKEY, part->P_PARTKEY, line->L_EXTENDEDPRICE.to_double(), line->L_QUANTITY.to_double());
	}

	virtual q17_l_join_p_t* clone() const {
		return new q17_l_join_p_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join LINEITEM, PART; select P_PARTKEY, L_EXTENDEDPRICE, L_QUANTITY");
	}
};

struct q17_final_join_t : public tuple_join_t {

	q17_final_join_t()
	: tuple_join_t(sizeof(q17_sub_aggregate_tuple),
			offsetof(q17_sub_aggregate_tuple, L_PARTKEY),
			sizeof(q17_l_join_p_tuple),
			offsetof(q17_l_join_p_tuple, P_PARTKEY),
			sizeof(int),
			sizeof(q17_all_join_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q17_all_join_tuple *dest = aligned_cast<q17_all_join_tuple>(d.data);
		q17_sub_aggregate_tuple *sub = aligned_cast<q17_sub_aggregate_tuple>(l.data);
		q17_l_join_p_tuple *right = aligned_cast<q17_l_join_p_tuple>(r.data);

		dest->AVG_QTY = sub->AVG_QTY;
		dest->L_EXTENDEDPRICE = right->L_EXTENDEDPRICE;
		dest->L_QUANTITY = right->L_QUANTITY;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %.2f %.2f %.2f\n", sub->L_PARTKEY, right->P_PARTKEY, sub->AVG_QTY.to_double(), right->L_EXTENDEDPRICE.to_double(), right->L_QUANTITY.to_double());
	}

	virtual q17_final_join_t* clone() const {
		return new q17_final_join_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join LINEITEM sub, LINEITEM_PART; select sub.AVG_QTY, L_EXTENDEDPRICE, L_QUANTITY");
	}
};

struct q17_join_filter_t : public tuple_filter_t {

	q17_join_filter_t()
	: tuple_filter_t(sizeof(q17_all_join_tuple))
	{
	}

	bool select(const tuple_t &input) {
		q17_all_join_tuple *in = aligned_cast<q17_all_join_tuple>(input.data);
		return in->L_QUANTITY < 0.2 * in->AVG_QTY;
	}

	virtual q17_join_filter_t* clone() const {
		return new q17_join_filter_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("q17_join_filter_t");
	}
};

struct q17_aggregate_t : public tuple_aggregate_t {

	default_key_extractor_t _extractor;

	q17_aggregate_t()
	: tuple_aggregate_t(sizeof(q17_final_tuple)), _extractor(0, 0)
	{
	}

	virtual key_extractor_t* key_extractor() {
		return &_extractor;
	}

	virtual void aggregate(char* agg_data, const tuple_t &t) {
		q17_final_tuple *agg = aligned_cast<q17_final_tuple>(agg_data);
		q17_all_join_tuple *in = aligned_cast<q17_all_join_tuple>(t.data);
		agg->AVG_YEARLY += in->L_EXTENDEDPRICE;
	}
	virtual void finish(tuple_t &dest, const char* agg_data) {
		memcpy(dest.data, agg_data, dest.size);
		q17_final_tuple *agg = aligned_cast<q17_final_tuple>(dest.data);
		agg->AVG_YEARLY /= 7.0;
	}
	virtual q17_aggregate_t* clone() const {
		return new q17_aggregate_t(*this);
	}
	virtual c_str to_string() const {
		return "q17_aggregate_t";
	}
};



class tpch_q17_process_tuple_t : public process_tuple_t {

public:

    virtual void begin() {
        TRACE(TRACE_QUERY_RESULTS, "*** Q17 %s\n",
              "AVG_YEARLY");
    }

    virtual void process(const tuple_t& output) {
    	q17_final_tuple *agg = aligned_cast<q17_final_tuple>(output.data);

        TRACE(TRACE_QUERY_RESULTS, "*** Q17 %.4f\n", agg->AVG_YEARLY.to_double());
    }

};


/********************************************************************
 *
 * QPIPE q17 - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_qpipe_q17(const int xct_id,
                                  q17_input_t& in)
{
    TRACE( TRACE_ALWAYS, "********** q19 *********\n");

    policy_t* dp = this->get_sched_policy();
    xct_t* pxct = smthread_t::me()->xct();



    //TSCAN PART
    tuple_fifo* q17_part_buffer = new tuple_fifo(sizeof(q17_projected_part_tuple));
    packet_t* q17_part_tscan_packet =
    		new tscan_packet_t("part TSCAN",
    				q17_part_buffer,
    				new q17_part_tscan_filter_t(this, in),
    				this->db(),
    				_ppart_desc.get(),
    				pxct);

    //TSCAN LINEITEM
    tuple_fifo* q17_lineitem_buffer = new tuple_fifo(sizeof(q17_projected_lineitem_tuple));
    packet_t* q17_lineitem_tscan_packet =
    		new tscan_packet_t("lineitem TSCAN",
    				q17_lineitem_buffer,
    				new q17_lineitem_tscan_filter_t(this, in),
    				this->db(),
    				_plineitem_desc.get(),
    				pxct);

    //TSCAN LINEITEM subquery
    tuple_fifo* q17_lineitem_sub_buffer = new tuple_fifo(sizeof(q17_projected_lineitem_sub_tuple));
    packet_t* q17_lineitem_sub_tscan_packet =
    		new tscan_packet_t("lineitem TSCAN subquery",
    				q17_lineitem_sub_buffer,
    				new q17_lineitem_sub_tscan_filter_t(this, in),
    				this->db(),
    				_plineitem_desc.get(),
    				pxct);


    //SUBQUERY AGGREGATE
    tuple_fifo* q17_sub_aggregate_buffer = new tuple_fifo(sizeof(q17_sub_aggregate_tuple));
    packet_t* q17_sub_aggregate_packet =
    		new partial_aggregate_packet_t("SUB AVG AGGREGATION",
    				q17_sub_aggregate_buffer,
    				new trivial_filter_t(sizeof(q17_sub_aggregate_tuple)),
    				q17_lineitem_sub_tscan_packet,
    				new q17_sub_aggregate_t(),
    				new default_key_extractor_t(sizeof(int), offsetof(q17_projected_lineitem_sub_tuple, L_PARTKEY)),
    				new int_key_compare_t());


    //LINEITEM JOIN PART
    tuple_fifo* q17_l_join_p_buffer = new tuple_fifo(sizeof(q17_l_join_p_tuple));
    packet_t* q17_l_join_p_packet =
    		new hash_join_packet_t("lineitem - part HJOIN",
    				q17_l_join_p_buffer,
    				new trivial_filter_t(sizeof(q17_l_join_p_tuple)),
    				q17_lineitem_tscan_packet,
    				q17_part_tscan_packet,
    				new q17_l_join_p_t());

    //LINEITEM sub JOIN LINEITEM_PART
    tuple_fifo* q17_all_join_buffer = new tuple_fifo(sizeof(q17_all_join_tuple));
    packet_t* q17_all_join_packet =
    		new hash_join_packet_t("lineitem sub - lineitem_part HJOIN",
    				q17_all_join_buffer,
    				new q17_join_filter_t(),
    				q17_sub_aggregate_packet,
    				q17_l_join_p_packet,
    				new q17_final_join_t());


    //SUM AGGREGATE
    tuple_fifo* q17_final_buffer = new tuple_fifo(sizeof(q17_final_tuple));
    packet_t* q17_final_packet =
    		new aggregate_packet_t("SUM AGGREGATION",
    				q17_final_buffer,
    				new trivial_filter_t(sizeof(q17_final_tuple)),
    				new q17_aggregate_t(),
    				new default_key_extractor_t(0, 0),
    				q17_all_join_packet);


    qpipe::query_state_t* qs = dp->query_state_create();
    q17_part_tscan_packet->assign_query_state(qs);
    q17_lineitem_tscan_packet->assign_query_state(qs);
    q17_lineitem_sub_tscan_packet->assign_query_state(qs);
    q17_sub_aggregate_packet->assign_query_state(qs);
    q17_l_join_p_packet->assign_query_state(qs);
    q17_all_join_packet->assign_query_state(qs);
    q17_final_packet->assign_query_state(qs);


    // Dispatch packet
    tpch_q17_process_tuple_t pt;
    //LAST PACKET
    process_query(q17_final_packet, pt);//TODO

    dp->query_state_destroy(qs);


    return (RCOK);
}

EXIT_NAMESPACE(tpch);
