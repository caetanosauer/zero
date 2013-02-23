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

/** @file:   qpipe_q11.cpp
 *
 *  @brief:  Implementation of QPIPE TPCH Q over Shore-MT
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
 * QPIPE Q11 - Structures needed by operators
 *
 ********************************************************************/

/*
select
	ps_partkey,
	sum(ps_supplycost * ps_availqty) as value
from
	partsupp,
	supplier,
	nation
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = '[NATION]'
group by
	ps_partkey having
		sum(ps_supplycost * ps_availqty) > (
			select
				sum(ps_supplycost * ps_availqty) * [FRACTION]
			from
				partsupp,
				supplier,
				nation
			where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = '[NATION]')
order by
	value desc;
*/


struct q11_projected_supplier_tuple {
	int S_SUPPKEY;
	int S_NATIONKEY;
};

struct q11_projected_nation_tuple {
	int N_NATIONKEY;
};

struct q11_projected_partsupp_tuple {
	int PS_PARTKEY;
	int PS_SUPPKEY;
	decimal VALUE;
};

struct q11_s_join_n_tuple {
	int S_SUPPKEY;
};

struct q11_ps_join_s_n_tuple {
	int PSEUDOKEY;
	int PS_PARTKEY;
	decimal VALUE;
};

struct q11_all_joins_tuple {
	int PS_PARTKEY;
	decimal VALUE;
	decimal THRESHOLD;
};

struct q11_final_tuple {
	int PS_PARTKEY;
	decimal VALUE;
};


class q11_supplier_tscan_filter_t : public tuple_filter_t
{
private:
    ShoreTPCHEnv* _tpchdb;
    table_row_t* _prsupplier;
    rep_row_t _rr;

    /*One supplier tuple*/
    tpch_supplier_tuple _supplier;

public:
    q11_supplier_tscan_filter_t(ShoreTPCHEnv* tpchdb, q11_input_t &in)
        : tuple_filter_t(tpchdb->supplier_desc()->maxsize()), _tpchdb(tpchdb)
        {
        _prsupplier = _tpchdb->supplier_man()->get_tuple();
        _rr.set_ts(_tpchdb->supplier_man()->ts(),
            _tpchdb->supplier_desc()->maxsize());
        _prsupplier->_rep = &_rr;
        }

    virtual ~q11_supplier_tscan_filter_t()
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

        q11_projected_supplier_tuple *dest = aligned_cast<q11_projected_supplier_tuple>(d.data);


        _prsupplier->get_value(0, _supplier.S_SUPPKEY);
        _prsupplier->get_value(3, _supplier.S_NATIONKEY);

        //TRACE( TRACE_RECORD_FLOW, "%d|%d\n",
        //       _supplier.S_SUPPKEY, _supplier.S_NATIONKEY);

        dest->S_SUPPKEY = _supplier.S_SUPPKEY;
        dest->S_NATIONKEY = _supplier.S_NATIONKEY;

    }

    q11_supplier_tscan_filter_t* clone() const {
        return new q11_supplier_tscan_filter_t(*this);
    }

    c_str to_string() const {
        return c_str("q11_supplier_tscan_filter_t");
    }
};

class q11_nation_tscan_filter_t : public tuple_filter_t
{
private:
    ShoreTPCHEnv* _tpchdb;
    table_row_t* _prnation;
    rep_row_t _rr;

    /*One nation tuple*/
    tpch_nation_tuple _nation;

    char _name[STRSIZE(25)];

public:
    q11_nation_tscan_filter_t(ShoreTPCHEnv* tpchdb, q11_input_t &in)
        : tuple_filter_t(tpchdb->nation_desc()->maxsize()), _tpchdb(tpchdb)
        {
        _prnation = _tpchdb->nation_man()->get_tuple();
        _rr.set_ts(_tpchdb->nation_man()->ts(),
            _tpchdb->nation_desc()->maxsize());
        _prnation->_rep = &_rr;

        nation_to_str((&in)->n_name, _name);
        TRACE(TRACE_ALWAYS, "Random predicate:\nNATION:N_NAME = %s\n", _name);
        }

    virtual ~q11_nation_tscan_filter_t()
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

        q11_projected_nation_tuple *dest;
        dest = aligned_cast<q11_projected_nation_tuple>(d.data);

        _prnation->get_value(0, _nation.N_NATIONKEY);

        //TRACE( TRACE_RECORD_FLOW, "%d\n",
        //       _nation.N_NATIONKEY);

        dest->N_NATIONKEY = _nation.N_NATIONKEY;
    }

    q11_nation_tscan_filter_t* clone() const {
        return new q11_nation_tscan_filter_t(*this);
    }

    c_str to_string() const {
        return c_str("q11_nation_tscan_filter_t(%s)", _name);
    }
};

class q11_partsupp_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prpartsupp;
	rep_row_t _rr;

	/*One partsupp tuple*/
	tpch_partsupp_tuple _partsupp;

public:
	q11_partsupp_tscan_filter_t(ShoreTPCHEnv* tpchdb, q11_input_t &in)
	: tuple_filter_t(tpchdb->partsupp_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prpartsupp = _tpchdb->partsupp_man()->get_tuple();
		_rr.set_ts(_tpchdb->partsupp_man()->ts(),
				_tpchdb->partsupp_desc()->maxsize());
		_prpartsupp->_rep = &_rr;
	}

	virtual ~q11_partsupp_tscan_filter_t()
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

		q11_projected_partsupp_tuple *dest;
		dest = aligned_cast<q11_projected_partsupp_tuple>(d.data);

		_prpartsupp->get_value(0, _partsupp.PS_PARTKEY);
		_prpartsupp->get_value(1, _partsupp.PS_SUPPKEY);
		_prpartsupp->get_value(2, _partsupp.PS_AVAILQTY);
		_prpartsupp->get_value(3, _partsupp.PS_SUPPLYCOST);

		/*TRACE( TRACE_RECORD_FLOW, "%d|%d|%.4f\n",
				_partsupp.PS_SUPPKEY,
				_partsupp.PS_PARTKEY,
				_partsupp.PS_SUPPLYCOST.to_double() / 100.0 * _partsupp.PS_AVAILQTY);*/

		dest->PS_SUPPKEY = _partsupp.PS_SUPPKEY;
		dest->PS_PARTKEY = _partsupp.PS_PARTKEY;
		dest->VALUE = _partsupp.PS_SUPPLYCOST / 100.0 * _partsupp.PS_AVAILQTY;

	}

	q11_partsupp_tscan_filter_t* clone() const {
		return new q11_partsupp_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q11_partsupp_tscan_filter_t()");
	}
};


struct q11_s_join_n_t : public tuple_join_t {

	q11_s_join_n_t()
		: tuple_join_t(sizeof(q11_projected_supplier_tuple),
				offsetof(q11_projected_supplier_tuple, S_NATIONKEY),
				sizeof(q11_projected_nation_tuple),
				offsetof(q11_projected_nation_tuple, N_NATIONKEY),
				sizeof(int),
				sizeof(q11_s_join_n_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q11_s_join_n_tuple *dest = aligned_cast<q11_s_join_n_tuple>(d.data);
		q11_projected_supplier_tuple *supp = aligned_cast<q11_projected_supplier_tuple>(l.data);
		q11_projected_nation_tuple *nation = aligned_cast<q11_projected_nation_tuple>(r.data);

		dest->S_SUPPKEY = supp->S_SUPPKEY;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d\n", supp->S_NATIONKEY, nation->N_NATIONKEY, supp->S_SUPPKEY);
	}

	virtual q11_s_join_n_t* clone() const {
		return new q11_s_join_n_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join SUPPLIER, NATION; select S_SUPPKEY");
	}
};

struct q11_ps_join_s_n_t : public tuple_join_t {

	q11_ps_join_s_n_t()
		: tuple_join_t(sizeof(q11_projected_partsupp_tuple),
				offsetof(q11_projected_partsupp_tuple, PS_SUPPKEY),
				sizeof(q11_s_join_n_tuple),
				offsetof(q11_s_join_n_tuple, S_SUPPKEY),
				sizeof(int),
				sizeof(q11_ps_join_s_n_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q11_ps_join_s_n_tuple *dest = aligned_cast<q11_ps_join_s_n_tuple>(d.data);
		q11_projected_partsupp_tuple *partsupp = aligned_cast<q11_projected_partsupp_tuple>(l.data);
		q11_s_join_n_tuple *right = aligned_cast<q11_s_join_n_tuple>(r.data);

		dest->PS_PARTKEY = partsupp->PS_PARTKEY;
		dest->VALUE = partsupp->VALUE;
		dest->PSEUDOKEY = 0;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d=%d: %d %.2f\n", partsupp->PS_SUPPKEY, right->S_SUPPKEY, partsupp->PS_PARTKEY, partsupp->VALUE);
	}

	virtual q11_ps_join_s_n_t* clone() const {
		return new q11_ps_join_s_n_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join PARTSUPP, SUPPLIER_NATION; select PS_PARTKEY, VALUE");
	}
};

struct q11_aggregate_t : public tuple_aggregate_t {

	default_key_extractor_t _extractor;
	bool _subQuery;

	q11_aggregate_t(bool subQuery)
	: tuple_aggregate_t(sizeof(q11_ps_join_s_n_tuple)), _subQuery(subQuery), _extractor((subQuery ? 0 : sizeof(int)), 0)
	{
	}

	virtual key_extractor_t* key_extractor() {
		return &_extractor;
	}

	virtual void aggregate(char* agg_data, const tuple_t &t) {
		q11_ps_join_s_n_tuple *agg = aligned_cast<q11_ps_join_s_n_tuple>(agg_data);
		q11_ps_join_s_n_tuple *in = aligned_cast<q11_ps_join_s_n_tuple>(t.data);

		agg->PS_PARTKEY = in->PS_PARTKEY;
		agg->VALUE += in->VALUE;
		agg->PSEUDOKEY = in->PSEUDOKEY;
	}
	virtual void finish(tuple_t &dest, const char* agg_data) {
		memcpy(dest.data, agg_data, dest.size);
	}
	virtual q11_aggregate_t* clone() const {
		return new q11_aggregate_t(*this);
	}
	virtual c_str to_string() const {
		return "q11_aggregate_t";
	}
};

struct q11_final_join_t : public tuple_join_t {

	q11_final_join_t()
		: tuple_join_t(sizeof(q11_ps_join_s_n_tuple),
				offsetof(q11_ps_join_s_n_tuple, PSEUDOKEY),
				sizeof(q11_ps_join_s_n_tuple),
				offsetof(q11_ps_join_s_n_tuple, PSEUDOKEY),
				sizeof(int),
				sizeof(q11_all_joins_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q11_all_joins_tuple *dest = aligned_cast<q11_all_joins_tuple>(d.data);
		q11_ps_join_s_n_tuple *sub = aligned_cast<q11_ps_join_s_n_tuple>(l.data);
		q11_ps_join_s_n_tuple *main = aligned_cast<q11_ps_join_s_n_tuple>(r.data);

		dest->PS_PARTKEY = main->PS_PARTKEY;
		dest->VALUE = main->VALUE;
		dest->THRESHOLD = sub->VALUE;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %d %.2f %.2f\n", main->PS_PARTKEY, main->VALUE.to_double(), sub->VALUE.to_double());
	}

	virtual q11_final_join_t* clone() const {
		return new q11_final_join_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join PARTSUPP_SUPPLIER_NATION sub, PARTSUPP_SUPPLIER_NATION main; select main.PS_PARTKEY, main.VALUE, sub.VALUE as THRESHOLD");
	}
};

struct q11_threshold_filter_t : public tuple_filter_t
{

	double _fraction;

	q11_threshold_filter_t(double fraction)
	: tuple_filter_t(sizeof(q11_all_joins_tuple)), _fraction(fraction)
	{
		TRACE(TRACE_ALWAYS, "Random predicate: FRACTION = %.6f\n", _fraction);
	}

	bool select(const tuple_t &input) {
		q11_all_joins_tuple *tuple = aligned_cast<q11_all_joins_tuple>(input.data);
		return (tuple->VALUE.to_double() > tuple->THRESHOLD.to_double() * _fraction);
	}

	void project(tuple_t &d, const tuple_t &s) {

	        q11_final_tuple *out = aligned_cast<q11_final_tuple>(d.data);
	        q11_all_joins_tuple *in = aligned_cast<q11_all_joins_tuple>(s.data);

	        out->PS_PARTKEY = in->PS_PARTKEY;
	        out->VALUE = in->VALUE;
	    }

	virtual q11_threshold_filter_t* clone() const {
		return new q11_threshold_filter_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("q11_threshold_filter_t");
	}
};

struct q11_sort_key_extractor_t : public key_extractor_t {

	q11_sort_key_extractor_t()
		: key_extractor_t(sizeof(decimal), offsetof(q11_final_tuple, VALUE))
		{
		}

		virtual int extract_hint(const char* key) const {
			return -(*aligned_cast<decimal>(key)).to_int();
		}

		virtual q11_sort_key_extractor_t* clone() const {
			return new q11_sort_key_extractor_t(*this);
		}
};

struct q11_sort_key_compare_t : public key_compare_t {

	virtual int operator()(const void* key1, const void* key2) const {
			decimal rev1 = *aligned_cast<decimal>(key1);
			decimal rev2 = *aligned_cast<decimal>(key2);
			return rev1 > rev2 ? -1 : (rev1 < rev2 ? 1 : 0);
		}

		virtual q11_sort_key_compare_t* clone() const {
			return new q11_sort_key_compare_t(*this);
		}
};


class tpch_q11_process_tuple_t : public process_tuple_t {

public:

    virtual void begin() {
        TRACE(TRACE_QUERY_RESULTS, "*** Q11 %s %s\n",
              "PS_PARTKEY", "VALUE");
    }

    virtual void process(const tuple_t& output) {
        q11_final_tuple *agg = aligned_cast<q11_final_tuple>(output.data);

        TRACE(TRACE_QUERY_RESULTS, "*** Q11 %d %.2f\n", agg->PS_PARTKEY, agg->VALUE.to_double());
    }
};



/********************************************************************
 *
 * QPIPE Q11 - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_qpipe_q11(const int xct_id,
                                  q11_input_t& in)
{
    TRACE( TRACE_ALWAYS, "********** q11 *********\n");

    policy_t* dp = this->get_sched_policy();
    xct_t* pxct = smthread_t::me()->xct();



    //TSCAN NATION
    tuple_fifo* q11_nation_buffer = new tuple_fifo(sizeof(q11_projected_nation_tuple));
    packet_t* q11_nation_tscan_packet =
    		new tscan_packet_t("nation TSCAN",
    				q11_nation_buffer,
    				new q11_nation_tscan_filter_t(this, in),
    				this->db(),
    				_pnation_desc.get(),
    				pxct);

    //TSCAN NATION subquery
    tuple_fifo* q11_nation_sub_buffer = new tuple_fifo(sizeof(q11_projected_nation_tuple));
    packet_t* q11_nation_sub_tscan_packet =
    		new tscan_packet_t("nation TSCAN subquery",
    				q11_nation_sub_buffer,
    				new q11_nation_tscan_filter_t(this, in),
    				this->db(),
    				_pnation_desc.get(),
    				pxct);

    //TSCAN SUPPLIER
    tuple_fifo* q11_supplier_buffer = new tuple_fifo(sizeof(q11_projected_supplier_tuple));
    packet_t* q11_supplier_tscan_packet =
    		new tscan_packet_t("supplier TSCAN",
    				q11_supplier_buffer,
    				new q11_supplier_tscan_filter_t(this, in),
    				this->db(),
    				_psupplier_desc.get(),
    				pxct);

    //TSCAN SUPPLIER subquery
    tuple_fifo* q11_supplier_sub_buffer = new tuple_fifo(sizeof(q11_projected_supplier_tuple));
    packet_t* q11_supplier_sub_tscan_packet =
    		new tscan_packet_t("supplier TSCAN subquery",
    				q11_supplier_sub_buffer,
    				new q11_supplier_tscan_filter_t(this, in),
    				this->db(),
    				_psupplier_desc.get(),
    				pxct);

    //TSCAN PARTSUPP
    tuple_fifo* q11_partsupp_buffer = new tuple_fifo(sizeof(q11_projected_partsupp_tuple));
    packet_t* q11_partsupp_tscan_packet =
    		new tscan_packet_t("partsupp TSCAN",
    				q11_partsupp_buffer,
    				new q11_partsupp_tscan_filter_t(this, in),
    				this->db(),
    				_ppartsupp_desc.get(),
    				pxct);

    //TSCAN PARTSUPP subquery
    tuple_fifo* q11_partsupp_sub_buffer = new tuple_fifo(sizeof(q11_projected_partsupp_tuple));
    packet_t* q11_partsupp_sub_tscan_packet =
    		new tscan_packet_t("partsupp TSCAN subquery",
    				q11_partsupp_sub_buffer,
    				new q11_partsupp_tscan_filter_t(this, in),
    				this->db(),
    				_ppartsupp_desc.get(),
    				pxct);

    //--- Main Query ---

    //SUPPLIER JOIN NATION
    tuple_fifo* q11_s_join_n_buffer = new tuple_fifo(sizeof(q11_s_join_n_tuple));
    packet_t* q11_s_join_n_packet =
    		new hash_join_packet_t("supplier - nation HJOIN",
    				q11_s_join_n_buffer,
    				new trivial_filter_t(sizeof(q11_s_join_n_tuple)),
    				q11_supplier_tscan_packet,
    				q11_nation_tscan_packet,
    				new q11_s_join_n_t());

    //PARTSUPP JOIN SUPPLIER_NATION
    tuple_fifo* q11_ps_join_s_n_buffer = new tuple_fifo(sizeof(q11_ps_join_s_n_tuple));
    packet_t* q11_ps_join_s_n_packet =
    		new hash_join_packet_t("partsupp - supplier_nation HJOIN",
    				q11_ps_join_s_n_buffer,
    				new trivial_filter_t(sizeof(q11_ps_join_s_n_tuple)),
    				q11_partsupp_tscan_packet,
    				q11_s_join_n_packet,
    				new q11_ps_join_s_n_t());

    //GROUP BY PS_PARTKEY and SUM
    tuple_fifo* q11_agg_buffer = new tuple_fifo(sizeof(q11_ps_join_s_n_tuple));
    packet_t* q11_agg_packet =
    		new partial_aggregate_packet_t("AGGREGATE: GROUP BY PS_PARTKEY + SUM",
    				q11_agg_buffer,
    				new trivial_filter_t(sizeof(q11_ps_join_s_n_tuple)),
    				q11_ps_join_s_n_packet,
    				new q11_aggregate_t(false),
    				new default_key_extractor_t(sizeof(int), offsetof(q11_ps_join_s_n_tuple, PS_PARTKEY)),
    				new int_key_compare_t());


    //--- SubQuery ---

    //SUPPLIER JOIN NATION
    tuple_fifo* q11_s_join_n_sub_buffer = new tuple_fifo(sizeof(q11_s_join_n_tuple));
    packet_t* q11_s_join_n_sub_packet =
    		new hash_join_packet_t("supplier - nation HJOIN subquery",
    				q11_s_join_n_sub_buffer,
    				new trivial_filter_t(sizeof(q11_s_join_n_tuple)),
    				q11_supplier_sub_tscan_packet,
    				q11_nation_sub_tscan_packet,
    				new q11_s_join_n_t());

    //PARTSUPP JOIN SUPPLIER_NATION
    tuple_fifo* q11_ps_join_s_n_sub_buffer = new tuple_fifo(sizeof(q11_ps_join_s_n_tuple));
    packet_t* q11_ps_join_s_n_sub_packet =
    		new hash_join_packet_t("partsupp - supplier_nation HJOIN subquery",
    				q11_ps_join_s_n_sub_buffer,
    				new trivial_filter_t(sizeof(q11_ps_join_s_n_tuple)),
    				q11_partsupp_sub_tscan_packet,
    				q11_s_join_n_sub_packet,
    				new q11_ps_join_s_n_t());

    //SUM AGGREGATE
    tuple_fifo* q11_agg_sub_buffer = new tuple_fifo(sizeof(q11_ps_join_s_n_tuple));
    packet_t* q11_agg_sub_packet =
    		new partial_aggregate_packet_t("AGGREGATE: SUM",
    				q11_agg_sub_buffer,
    				new trivial_filter_t(sizeof(q11_ps_join_s_n_tuple)),
    				q11_ps_join_s_n_sub_packet,
    				new q11_aggregate_t(true),
    				new default_key_extractor_t(0, 0),
    				new int_key_compare_t());

    //---

    //SUBQUERY JOIN MAINQUERY
    tuple_fifo* q11_all_joins_buffer = new tuple_fifo(sizeof(q11_final_tuple));
    packet_t* q11_all_joins_packet =
    		new hash_join_packet_t("partsupp_supplier_nation sub - partsupp_supplier_nation main HJOIN",
    				q11_all_joins_buffer,
    				new q11_threshold_filter_t((&in)->fraction),
    				q11_agg_sub_packet,
    				q11_agg_packet,
    				new q11_final_join_t());


    //SORT BY VALUE desc
    tuple_fifo* q11_sort_buffer = new tuple_fifo(sizeof(q11_final_tuple));
    packet_t* q11_sort_packet =
    		new sort_packet_t("SORT BY VALUE desc",
    				q11_sort_buffer,
    				new trivial_filter_t(sizeof(q11_final_tuple)),
    				new q11_sort_key_extractor_t(),
    				new q11_sort_key_compare_t(),
    				q11_all_joins_packet);


    qpipe::query_state_t* qs = dp->query_state_create();
    q11_nation_tscan_packet->assign_query_state(qs);
    q11_nation_sub_tscan_packet->assign_query_state(qs);
    q11_supplier_tscan_packet->assign_query_state(qs);
    q11_supplier_sub_tscan_packet->assign_query_state(qs);
    q11_partsupp_tscan_packet->assign_query_state(qs);
    q11_partsupp_sub_tscan_packet->assign_query_state(qs);
    q11_s_join_n_packet->assign_query_state(qs);
    q11_ps_join_s_n_packet->assign_query_state(qs);
    q11_agg_packet->assign_query_state(qs);
    q11_s_join_n_sub_packet->assign_query_state(qs);
    q11_ps_join_s_n_sub_packet->assign_query_state(qs);
    q11_agg_sub_packet->assign_query_state(qs);
    q11_all_joins_packet->assign_query_state(qs);
    q11_sort_packet->assign_query_state(qs);

    // Dispatch packet
    tpch_q11_process_tuple_t pt;
    //LAST PACKET
    process_query(q11_sort_packet, pt);//TODO

    dp->query_state_destroy(qs);


    return (RCOK);
}

EXIT_NAMESPACE(tpch);
