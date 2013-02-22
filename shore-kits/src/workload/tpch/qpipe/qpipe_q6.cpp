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

/** @file:   qpipe_q6.cpp
 *
 *  @brief:  Implementation of QPIPE TPCH Q6 over Shore-MT
 *
 *  @author: 
 *  @date:   
 */

#include "workload/tpch/shore_tpch_env.h"
#include "qpipe.h"

using namespace shore;
using namespace qpipe;



ENTER_NAMESPACE(tpch);


/******************************************************************** 
 *
 * QPIPE Q6 - Structures needed by operators 
 *
 ********************************************************************/

/*
select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '1995-01-01'
	and l_shipdate < date '1995-01-01' + interval '1' year
	and l_discount between 0.07 - 0.01 and 0.07 + 0.01
	and l_quantity < 24
;
*/

// the tuples after tablescan projection
struct q6_projected_lineitem_tuple {
    double L_EXTENDEDPRICE;
    double L_DISCOUNT;
};

// the tuples after sieve
struct q6_multiplied_lineitem_tuple {
    double L_EXTENDEDPRICE_MUL_DISCOUNT;
};

// the final aggregated tuples
struct q6_aggregate_tuple {
    double L_SUM_REVENUE;
	int L_COUNT;
};


//Q6 scan filter (selection and projection)
class q6_tscan_filter_t : public tuple_filter_t 
{
private:
    ShoreTPCHEnv* _tpchdb;
    table_row_t* _prline;
    rep_row_t _rr;

    /*One lineitem tuple*/
    tpch_lineitem_tuple _lineitem;
    /*The columns needed for the selection*/
    time_t _shipdate;
    double _discount;
    double _quantity;

    /* Random Predicates */
    /* TPC-H Specification 2.9.3 */
    q6_input_t* q6_input;
    time_t _last_l_shipdate;
public:

    q6_tscan_filter_t(ShoreTPCHEnv* tpchdb, q6_input_t &in)
        : tuple_filter_t(tpchdb->lineitem_desc()->maxsize()), _tpchdb(tpchdb)
          //: tuple_filter_t(sizeof(tpch_lineitem_tuple)), _tpchdb(tpchdb)
    {

    	// Get a lineitem tupple from the tuple cache and allocate space
        _prline = _tpchdb->lineitem_man()->get_tuple();
        _rr.set_ts(_tpchdb->lineitem_man()->ts(),
                   _tpchdb->lineitem_desc()->maxsize());
        _prline->_rep = &_rr;

        // Generate the random predicates
	/* Predicate:
   	   l_shipdate >= 'YEAR-01-01'
 	   and l_shipdate < 'YEAR-01-01' + interval '1' year
	   and l_discount between DISCOUNT - 0.01 and DISCOUNT + 0.01
	   and l_quantity < QUANTITY
	*/        
	q6_input=&in;
      	struct tm date;
	gmtime_r(&(q6_input->l_shipdate), &date);
	date.tm_year ++;
	_last_l_shipdate=mktime(&date);

	char date1[15];
	char date2[15];
	timet_to_str(date1,q6_input->l_shipdate);
	timet_to_str(date2,_last_l_shipdate);
	TRACE(TRACE_ALWAYS, "Random predicates: Date: %s-%s, Discount: %lf, Quantity: %lf\n", date1, date2, q6_input->l_discount, q6_input->l_quantity);
    }

    ~q6_tscan_filter_t()
    {
        // Give back the lineitem tuple 
        _tpchdb->lineitem_man()->give_tuple(_prline);
    }


    // Predication
    bool select(const tuple_t &input) {

        // Get next lineitem and read its shipdate
        if (!_tpchdb->lineitem_man()->load(_prline, input.data)) {
            assert(false); // RC(se_WRONG_DISK_DATA)
        }

        _prline->get_value(10, _lineitem.L_SHIPDATE, 15); //get column 10 (15 characters)
        _shipdate = str_to_timet(_lineitem.L_SHIPDATE);        
        _prline->get_value(6, _lineitem.L_DISCOUNT); //get column 6 (float)
        _discount=_lineitem.L_DISCOUNT/100.0;
#warning MA: Discount from TPCH dbgen is created between 0 and 100 instead between 0 and 1.
        _prline->get_value(4, _lineitem.L_QUANTITY); //get column 4 (float)
        _quantity=_lineitem.L_QUANTITY;



        // Return true if it passes the filter
		if  ( _shipdate >= q6_input->l_shipdate && _shipdate < _last_l_shipdate && _discount>=(q6_input->l_discount-0.01) &&
				_discount<=(q6_input->l_discount+0.01) && _quantity<q6_input->l_quantity) {

			//TRACE(TRACE_RECORD_FLOW, "+ %s, %lf, %lf\n", _lineitem.L_SHIPDATE, _lineitem.L_DISCOUNT, _lineitem.L_QUANTITY);
			return (true);
		}
		else {
				//TRACE(TRACE_RECORD_FLOW, ". %s, %lf, %lf\n", _lineitem.L_SHIPDATE, _lineitem.L_DISCOUNT, _lineitem.L_QUANTITY);
			return (false);
		}
    }

    
    // Projection
    void project(tuple_t &d, const tuple_t &s) {        

        q6_projected_lineitem_tuple *dest;
        dest = aligned_cast<q6_projected_lineitem_tuple>(d.data);

        _prline->get_value(5, _lineitem.L_EXTENDEDPRICE);
        _prline->get_value(6, _lineitem.L_DISCOUNT);

        /*TRACE( TRACE_RECORD_FLOW, "%.2f|%.2f\n",
               _lineitem.L_EXTENDEDPRICE / 100.0,
               _lineitem.L_DISCOUNT / 100.0);*/

        dest->L_EXTENDEDPRICE = _lineitem.L_EXTENDEDPRICE / 100.0;
        dest->L_DISCOUNT = _lineitem.L_DISCOUNT/100.0;
#warning MA: Discount from TPCH dbgen is created between 0 and 100 instead between 0 and 1.
    }

    q6_tscan_filter_t* clone() const {
        return new q6_tscan_filter_t(*this);
    }

    c_str to_string() const {
	char date[15];
	timet_to_str(date,q6_input->l_shipdate);
        return c_str("q6_tscan_filter_t(%s, %lf, %lf)", date, q6_input->l_discount, q6_input->l_quantity);
    }
};


//Multiplication
/**
 * @brief This sieve receives a double[2] and output a double.
 */
class q6_sieve_t : public tuple_sieve_t {

public:

    q6_sieve_t()
        : tuple_sieve_t(sizeof(q6_multiplied_lineitem_tuple))
    {
    }

    virtual bool pass(tuple_t& dest, const tuple_t &src) {
        double* in = aligned_cast<double>(src.data);
        double* out = aligned_cast<double>(dest.data);
        *out = in[0] * in[1];
        return true;
    }

    virtual tuple_sieve_t* clone() const {
        return new q6_sieve_t(*this);
    }

    virtual c_str to_string() const {
        return "q6_sieve_t";
    }
};


//Aggregation
class q6_aggregate_t : public tuple_aggregate_t {

public:

    class q6_key_extractor_t : public key_extractor_t {
    public:
        q6_key_extractor_t()
            : key_extractor_t(0, 0)
        {
        }

        virtual int extract_hint(const char*) const {
            /* should never be called! */
        	unreachable();
            return 0;
        }

        virtual key_extractor_t* clone() const {
            return new q6_key_extractor_t(*this);
        }
    };

private:

    q6_key_extractor_t _extractor;

public:

    q6_aggregate_t()
        : tuple_aggregate_t(sizeof(q6_aggregate_tuple)),
          _extractor()
    {
    }

    virtual key_extractor_t* key_extractor() { return &_extractor; }

    virtual void aggregate(char* agg_data, const tuple_t& src) {
    	q6_aggregate_tuple* agg = aligned_cast<q6_aggregate_tuple>(agg_data);
    	double * d = aligned_cast<double>(src.data);
        agg->L_COUNT++;
        agg->L_SUM_REVENUE += *d;
    }

    virtual void finish(tuple_t &dest, const char* agg_data) {
    	q6_aggregate_tuple* agg = aligned_cast<q6_aggregate_tuple>(agg_data);
    	q6_aggregate_tuple* output = aligned_cast<q6_aggregate_tuple>(dest.data);
        output->L_COUNT = agg->L_COUNT;
        output->L_SUM_REVENUE = agg->L_SUM_REVENUE;
        //TRACE (TRACE_QUERY_RESULTS, "Average Revenue: %lf\n",output->L_SUM_REVENUE/(double)output->L_COUNT);
    }

    virtual q6_aggregate_t* clone() const {
        return new q6_aggregate_t(*this);
    }

    virtual c_str to_string() const {
        return "q6pipe_aggregate_t";
    }
};




class tpch_q6_process_tuple_t : public process_tuple_t 
{    
public:
        
    void begin() {
        TRACE(TRACE_QUERY_RESULTS, "*** Q6 ANSWER ...\n");
        TRACE(TRACE_QUERY_RESULTS, "*** SUM_REVENUE(EXTPRICE*DISCOUNT)...\n");
    }
    
    virtual void process(const tuple_t& output) {
        q6_aggregate_tuple *tuple;
        tuple = aligned_cast<q6_aggregate_tuple>(output.data);
        TRACE(TRACE_QUERY_RESULTS, "*** %.2f\n",
              tuple->L_SUM_REVENUE);
    }
};



/******************************************************************** 
 *
 * QPIPE Q6 - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_qpipe_q6(const int xct_id, 
                                  q6_input_t& in)
{
    TRACE( TRACE_ALWAYS, "********** Q6 *********\n");
//#define USE_ECHO
//Define USE_ECHO if you want to split the tablescan from the predication for more work sharing opportunities

    /*
select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '1995-01-01'
	and l_shipdate < date '1995-01-01' + interval '1' year
	and l_discount between 0.07 - 0.01 and 0.07 + 0.01
	and l_quantity < 24
;
     */



////////////////////
    policy_t* dp = this->get_sched_policy();
    xct_t* pxct = smthread_t::me()->xct();


    // TSCAN
    //tuple_fifo* tscan_output = new tuple_fifo(tpch_lineitem->tuple_size);
#ifdef USE_ECHO
    tuple_fifo* tscan_output = new tuple_fifo(_plineitem_desc.get()->maxsize());
#else
    tuple_fifo* tscan_output = new tuple_fifo(sizeof(q6_projected_lineitem_tuple));
#endif

    tscan_packet_t *q6_tscan_packet =
        new tscan_packet_t("TSCAN LINEITEM",
                           tscan_output,
#ifdef USE_ECHO
                           new trivial_filter_t(tscan_output->tuple_size()),
#else
                           new q6_tscan_filter_t(this, in),
#endif
                           this->db(),
                           _plineitem_desc.get(),
                           pxct
                           /*, SH */
                           );


    // ECHO (filter)
#ifdef USE_ECHO
    tuple_fifo* echo_output = new tuple_fifo(sizeof(q6_projected_lineitem_tuple));
    echo_packet_t *q6_echo_packet =
        new echo_packet_t("Q6PIPE_ECHO_PACKET",
                          echo_output,
                          new q6_tscan_filter_t(this, in),
                          q6_tscan_packet);
#endif
    
    // SIEVE (multiplication)
    tuple_fifo* sieve_output = new tuple_fifo(sizeof(q6_multiplied_lineitem_tuple));
    sieve_packet_t *q6_sieve_packet =
        new sieve_packet_t("Q6PIPE_SIEVE_PACKET",
                           sieve_output,
                           new trivial_filter_t(sieve_output->tuple_size()),
                           new q6_sieve_t(),
#ifdef USE_ECHO
                           q6_echo_packet);
#else
                           q6_tscan_packet);
#endif
    
    
    // AGGREGATE
    tuple_fifo* agg_output = new tuple_fifo(sizeof(q6_aggregate_tuple));
    aggregate_packet_t* q6_agg_packet =
        new aggregate_packet_t(c_str("Q6PIPE_AGGREGATE_PACKET"),
                               agg_output,
                               new trivial_filter_t(agg_output->tuple_size()),
                               new q6_aggregate_t(),
                               new q6_aggregate_t::q6_key_extractor_t(),
                               q6_sieve_packet);
    
    qpipe::query_state_t* qs = dp->query_state_create();
    q6_agg_packet->assign_query_state(qs);
    q6_sieve_packet->assign_query_state(qs);
#ifdef USE_ECHO
    q6_echo_packet->assign_query_state(qs);
#endif
    q6_tscan_packet->assign_query_state(qs);

    tpch_q6_process_tuple_t pt;
    process_query(q6_agg_packet, pt);
    dp->query_state_destroy(qs);

////////////////////

    
  

    return (RCOK); 
}


EXIT_NAMESPACE(qpipe);
