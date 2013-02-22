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

/** @file:   qpipe_q22.cpp
 *
 *  @brief:  Implementation of QPIPE TPCH Q22 over Shore-MT
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
 * QPIPE Q22 - Structures needed by operators
 *
 ********************************************************************/

/*
select
	cntrycode,
	count(*) as numcust,
	sum(c_acctbal) as totacctbal
from (
	select
		substring(c_phone from 1 for 2) as cntrycode,
		c_acctbal
	from
		customer
	where
		substring(c_phone from 1 for 2) in
			('[I1]','[I2]','[I3]','[I4]','[I5]','[I6]','[I7]')
		and c_acctbal > (
			select
				avg(c_acctbal)
			from
				customer
			where
				c_acctbal > 0.00
				and substring (c_phone from 1 for 2) in
					('[I1]','[I2]','[I3]','[I4]','[I5]','[I6]','[I7]'))
		and not exists (
			select *
			from
				orders
			where
				o_custkey = c_custkey )) as custsale
group by
	cntrycode
order by
	cntrycode;
 */


struct q22_projected_customer_tuple {
	int JOINCODE;
	int CNTRYCODE;
	decimal C_ACCTBAL;
};

struct q22_projected_customer_sub_tuple {
	decimal C_ACCTBAL;
};

struct q22_sub_agg_tuple {
	int JOINCODE;
	decimal SUM_ACCTBAL;
	int COUNT;
	decimal AVG_ACCTBAL;
};

struct q22_c_join_c_tuple {
	int CNTRYCODE;
	decimal C_ACCTBAL;
	decimal ACCTBAL_THRESHOLD;
};

struct q22_aggregate_tuple {
	int CNTRYCODE;
	int NUMCUST;
	decimal TOTACCTBAL;
};



class q22_customer_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prcust;
	rep_row_t _rr;

	tpch_customer_tuple _customer;
	char _cntrycode_char[3];
	int _cntrycode;

	int _cntrycodes[7];


public:
	q22_customer_tscan_filter_t(ShoreTPCHEnv* tpchdb, q22_input_t &in)
	: tuple_filter_t(tpchdb->customer_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prcust = _tpchdb->customer_man()->get_tuple();
		_rr.set_ts(_tpchdb->customer_man()->ts(),
				_tpchdb->customer_desc()->maxsize());
		_prcust->_rep = &_rr;

		memcpy(_cntrycodes, (&in)->cntrycode, sizeof(_cntrycodes));
		TRACE(TRACE_ALWAYS, "Random predicates:\nsubstring(c_phone from 1 for 2) in ('%d', '%d', '%d', '%d', '%d', '%d', '%d')\n", _cntrycodes[0], _cntrycodes[1], _cntrycodes[2], _cntrycodes[3],
				_cntrycodes[4], _cntrycodes[5], _cntrycodes[6]);
	}

	virtual ~q22_customer_tscan_filter_t()
	{
		// Give back the customer tuple
		_tpchdb->customer_man()->give_tuple(_prcust);
	}

	bool select(const tuple_t &input) {
		// Get next customer tuple
		if (!_tpchdb->customer_man()->load(_prcust, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		_prcust->get_value(4, _customer.C_PHONE, sizeof(_customer.C_PHONE));

		_cntrycode_char[0] = _customer.C_PHONE[0];
		_cntrycode_char[1] = _customer.C_PHONE[1];
		_cntrycode_char[2] = '\0';
		_cntrycode = atoi(_cntrycode_char);

		for(int i = 0; i < 7; i++) {
			if(_cntrycode == _cntrycodes[i]) {
				return true;
			}
		}
		return false;
	}

	void project(tuple_t &d, const tuple_t &s) {

		q22_projected_customer_tuple *dest = aligned_cast<q22_projected_customer_tuple>(d.data);

		_prcust->get_value(4, _customer.C_PHONE, sizeof(_customer.C_PHONE));
		_prcust->get_value(5, _customer.C_ACCTBAL);

		//TRACE(TRACE_RECORD_FLOW, "%c%c|%.2f\n", _customer.C_PHONE[0], _customer.C_PHONE[1], _customer.C_ACCTBAL.to_double());

		_cntrycode_char[0] = _customer.C_PHONE[0];
		_cntrycode_char[1] = _customer.C_PHONE[1];
		_cntrycode_char[2] = '\0';
		dest->CNTRYCODE = atoi(_cntrycode_char);
		dest->C_ACCTBAL = _customer.C_ACCTBAL / 100.0;
		dest->JOINCODE = 0;
	}

	q22_customer_tscan_filter_t* clone() const {
		return new q22_customer_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q22_customer_tscan_filter_t");
	}
};


class q22_customer_sub_tscan_filter_t : public tuple_filter_t
{
private:
	ShoreTPCHEnv* _tpchdb;
	table_row_t* _prcust;
	rep_row_t _rr;

	tpch_customer_tuple _customer;
	char _cntrycode_char[3];
	int _cntrycode;

	int _cntrycodes[7];

public:
	q22_customer_sub_tscan_filter_t(ShoreTPCHEnv* tpchdb, q22_input_t &in)
	: tuple_filter_t(tpchdb->customer_desc()->maxsize()), _tpchdb(tpchdb)
	{
		_prcust = _tpchdb->customer_man()->get_tuple();
		_rr.set_ts(_tpchdb->customer_man()->ts(),
				_tpchdb->customer_desc()->maxsize());
		_prcust->_rep = &_rr;

		memcpy(_cntrycodes, (&in)->cntrycode, sizeof(_cntrycodes));
	}

	virtual ~q22_customer_sub_tscan_filter_t()
	{
		// Give back the customer tuple
		_tpchdb->customer_man()->give_tuple(_prcust);
	}

	bool select(const tuple_t &input) {
		// Get next customer tuple
		if (!_tpchdb->customer_man()->load(_prcust, input.data)) {
			assert(false); // RC(se_WRONG_DISK_DATA)
		}

		_prcust->get_value(4, _customer.C_PHONE, sizeof(_customer.C_PHONE));
		_prcust->get_value(5, _customer.C_ACCTBAL);
		if(_customer.C_ACCTBAL <= 0) {
			return false;
		}
		_cntrycode_char[0] = _customer.C_PHONE[0];
		_cntrycode_char[1] = _customer.C_PHONE[1];
		_cntrycode_char[2] = '\0';
		_cntrycode = atoi(_cntrycode_char);

		for(int i = 0; i < 7; i++) {
			if(_cntrycode == _cntrycodes[i]) {
				return true;
			}
		}
		return false;
	}

	void project(tuple_t &d, const tuple_t &s) {

		q22_projected_customer_sub_tuple *dest = aligned_cast<q22_projected_customer_sub_tuple>(d.data);

		_prcust->get_value(5, _customer.C_ACCTBAL);

		//TRACE(TRACE_RECORD_FLOW, "%.2f\n", _customer.C_ACCTBAL.to_double());

		dest->C_ACCTBAL = _customer.C_ACCTBAL / 100.0;
	}

	q22_customer_sub_tscan_filter_t* clone() const {
		return new q22_customer_sub_tscan_filter_t(*this);
	}

	c_str to_string() const {
		return c_str("q22_customer_sub_tscan_filter_t");
	}
};

struct q22_sub_aggregate_t : public tuple_aggregate_t {

	default_key_extractor_t _extractor;

	q22_sub_aggregate_t()
	: tuple_aggregate_t(sizeof(q22_sub_agg_tuple))
	{
	}

	virtual key_extractor_t* key_extractor() {
		return &_extractor;
	}

	virtual void aggregate(char* agg_data, const tuple_t &t) {
		q22_sub_agg_tuple *agg = aligned_cast<q22_sub_agg_tuple>(agg_data);
		q22_projected_customer_sub_tuple *input = aligned_cast<q22_projected_customer_sub_tuple>(t.data);

		agg->COUNT++;
		agg->SUM_ACCTBAL += input->C_ACCTBAL;
	}
	virtual void finish(tuple_t &dest, const char* agg_data) {
		memcpy(dest.data, agg_data, dest.size);
		q22_sub_agg_tuple *agg = aligned_cast<q22_sub_agg_tuple>(dest.data);
		agg->AVG_ACCTBAL = agg->SUM_ACCTBAL / agg->COUNT;
		agg->JOINCODE = 0;
		//TRACE(TRACE_RECORD_FLOW, "%.2f\t%d\t%.2f\n", agg->AVG_ACCTBAL.to_double(), agg->COUNT, agg->SUM_ACCTBAL.to_double());
	}
	virtual q22_sub_aggregate_t* clone() const {
		return new q22_sub_aggregate_t(*this);
	}
	virtual c_str to_string() const {
		return "q22_sub_aggregate_t";
	}
};


struct q22_c_join_c_t : public tuple_join_t {

	q22_c_join_c_t()
		: tuple_join_t(sizeof(q22_projected_customer_tuple),
				offsetof(q22_projected_customer_tuple, JOINCODE),
				sizeof(q22_sub_agg_tuple),
				offsetof(q22_sub_agg_tuple, JOINCODE),
				sizeof(int),
				sizeof(q22_c_join_c_tuple))
	{
	}

	virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
		q22_c_join_c_tuple *dest = aligned_cast<q22_c_join_c_tuple>(d.data);
		q22_projected_customer_tuple *cust = aligned_cast<q22_projected_customer_tuple>(l.data);
		q22_sub_agg_tuple *sub = aligned_cast<q22_sub_agg_tuple>(r.data);

		dest->ACCTBAL_THRESHOLD = sub->AVG_ACCTBAL;
		dest->C_ACCTBAL = cust->C_ACCTBAL;
		dest->CNTRYCODE = cust->CNTRYCODE;

		//TRACE(TRACE_RECORD_FLOW, "JOIN: %.2f %.2f %d\n", sub->AVG_ACCTBAL.to_double(), cust->C_ACCTBAL.to_double(), cust->CNTRYCODE);
	}

	virtual q22_c_join_c_t* clone() const {
		return new q22_c_join_c_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("join CUSTOMER c, CUSTOMER sub; select sub.ACCTBAL_THRESHOLD, c.C_ACCTBAL, c.CNTRYCODE");
	}
};

struct q22_join_filter_t : public tuple_filter_t {

	q22_join_filter_t()
	: tuple_filter_t(sizeof(q22_c_join_c_tuple))
	{
	}

	bool select(const tuple_t &input) {
		q22_c_join_c_tuple *in = aligned_cast<q22_c_join_c_tuple>(input.data);
		return in->C_ACCTBAL > in->ACCTBAL_THRESHOLD;
	}

	virtual q22_join_filter_t* clone() const {
		return new q22_join_filter_t(*this);
	}

	virtual c_str to_string() const {
		return c_str("q22_join_filter_t");
	}
};

struct q22_aggregate_t : public tuple_aggregate_t {

	default_key_extractor_t _extractor;

	q22_aggregate_t()
	: tuple_aggregate_t(sizeof(q22_sub_agg_tuple))
	{
	}

	virtual key_extractor_t* key_extractor() {
		return &_extractor;
	}

	virtual void aggregate(char* agg_data, const tuple_t &t) {
		q22_aggregate_tuple *agg = aligned_cast<q22_aggregate_tuple>(agg_data);
		q22_c_join_c_tuple *input = aligned_cast<q22_c_join_c_tuple>(t.data);

		agg->NUMCUST++;
		agg->CNTRYCODE = input->CNTRYCODE;
		agg->TOTACCTBAL += input->C_ACCTBAL;
	}
	virtual void finish(tuple_t &dest, const char* agg_data) {
		memcpy(dest.data, agg_data, dest.size);
	}
	virtual q22_aggregate_t* clone() const {
		return new q22_aggregate_t(*this);
	}
	virtual c_str to_string() const {
		return "q22_aggregate_t";
	}
};

struct q22_sort_key_extractor_t : public key_extractor_t {

	q22_sort_key_extractor_t()
		: key_extractor_t(sizeof(int), offsetof(q22_c_join_c_tuple, CNTRYCODE))
	{
	}

	virtual int extract_hint(const char *key) const {
		int cntrycode = *(aligned_cast<int>(key));
		return cntrycode;
	}

	virtual key_extractor_t* clone() const {
		return new q22_sort_key_extractor_t(*this);
	}
};

struct q22_sort_key_compare_t : public key_compare_t {

	q22_sort_key_compare_t()
	{
	}

	virtual int operator()(const void* key1, const void* key2) const {
		int cntrycode1 = *(aligned_cast<int>(key1));
		int cntrycode2 = *(aligned_cast<int>(key2));
		return cntrycode1 - cntrycode2;
	}

	virtual q22_sort_key_compare_t* clone() const {
		return new q22_sort_key_compare_t(*this);
	}
};


class tpch_q22_process_tuple_t : public process_tuple_t {

public:

    virtual void begin() {
        TRACE(TRACE_QUERY_RESULTS, "*** Q22 %s %s %s\n",
              "CNTRYCOST", "NUMCUST", "TOTACCTBAL");
    }

    virtual void process(const tuple_t& output) {
    	q22_aggregate_tuple *agg = aligned_cast<q22_aggregate_tuple>(output.data);

        TRACE(TRACE_QUERY_RESULTS, "*** Q22 %d %d %.4f\n", agg->CNTRYCODE, agg->NUMCUST, agg->TOTACCTBAL.to_double());
    }

};


/********************************************************************
 *
 * QPIPE q22 - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_qpipe_q22(const int xct_id,
                                  q22_input_t& in)
{
    TRACE( TRACE_ALWAYS, "********** q22 *********\n");

    policy_t* dp = this->get_sched_policy();
    xct_t* pxct = smthread_t::me()->xct();


    //TSCAN CUSTOMER
    tuple_fifo* q22_customer_buffer = new tuple_fifo(sizeof(q22_projected_customer_tuple));
    packet_t* q22_customer_tscan_packet =
    		new tscan_packet_t("customer TSCAN",
    				q22_customer_buffer,
    				new q22_customer_tscan_filter_t(this, in),
    				this->db(),
    				_pcustomer_desc.get(),
    				pxct);

    //TSCAN CUSTOMER SUB
    tuple_fifo* q22_customer_sub_buffer = new tuple_fifo(sizeof(q22_projected_customer_sub_tuple));
    packet_t* q22_customer_sub_tscan_packet =
    		new tscan_packet_t("customer sub TSCAN",
    				q22_customer_sub_buffer,
    				new q22_customer_sub_tscan_filter_t(this, in),
    				this->db(),
    				_pcustomer_desc.get(),
    				pxct);

    //SUB AGGREGATE AVG C_ACCTBAL
    tuple_fifo* q22_sub_agg_buffer = new tuple_fifo(sizeof(q22_sub_agg_tuple));
    packet_t* q22_sub_agg_packet =
    		new aggregate_packet_t("SUB AGGREGATE AVG C_ACCTBAL",
    				q22_sub_agg_buffer,
    				new trivial_filter_t(sizeof(q22_sub_agg_tuple)),
    				new q22_sub_aggregate_t(),
    				new default_key_extractor_t(0, 0),
    				q22_customer_sub_tscan_packet);

    //CUSTOMER JOIN CUSTOMER SUB
    tuple_fifo* q22_c_join_c_buffer = new tuple_fifo(sizeof(q22_c_join_c_tuple));
    packet_t* q22_c_join_c_packet =
    		new hash_join_packet_t("customer - customer HJOIN",
    				q22_c_join_c_buffer,
    				new q22_join_filter_t(),
    				q22_customer_tscan_packet,
    				q22_sub_agg_packet,
    				new q22_c_join_c_t());

    //ORDER BY CNTRYCODE
    tuple_fifo* q22_sort_buffer = new tuple_fifo(sizeof(q22_c_join_c_tuple));
    packet_t* q22_sort_packet =
    		new sort_packet_t("ORDER BY CNTRYCODE",
    				q22_sort_buffer,
    				new trivial_filter_t(sizeof(q22_c_join_c_tuple)),
    				new q22_sort_key_extractor_t(),
    				new q22_sort_key_compare_t(),
    				q22_c_join_c_packet);

    //SUM, COUNT AGGREGATE, GROUP BY CNTRYCODE
    tuple_fifo* q22_final_buffer = new tuple_fifo(sizeof(q22_aggregate_tuple));
    packet_t* q22_final_packet =
    		new aggregate_packet_t("SUM, COUNT, GROUP BY AGGREGATE",
    				q22_final_buffer,
    				new trivial_filter_t(sizeof(q22_aggregate_tuple)),
    				new q22_aggregate_t(),
    				new default_key_extractor_t(sizeof(int), offsetof(q22_c_join_c_tuple, CNTRYCODE)),
    				q22_sort_packet);


    qpipe::query_state_t* qs = dp->query_state_create();
    q22_customer_tscan_packet->assign_query_state(qs);
    q22_customer_sub_tscan_packet->assign_query_state(qs);
    q22_sub_agg_packet->assign_query_state(qs);
    q22_c_join_c_packet->assign_query_state(qs);
    q22_sort_packet->assign_query_state(qs);
    q22_final_packet->assign_query_state(qs);


    // Dispatch packet
    tpch_q22_process_tuple_t pt;
    //LAST PACKET
    process_query(q22_final_packet, pt);//TODO

    dp->query_state_destroy(qs);


    return (RCOK);
}


EXIT_NAMESPACE(tpch);
