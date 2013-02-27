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

/** @file:   qpipe_1_2.cpp
 *
 *  @brief:  Implementation of QPIPE SSB Q1_2 over Shore-MT
 *
 *  @author: Xuedong Jin                
 *  @date:   November 2011
 */

#include "workload/ssb/shore_ssb_env.h"
#include "qpipe.h"

using namespace shore;
using namespace qpipe;



ENTER_NAMESPACE(ssb);


/******************************************************************** 
 *
 * QPIPE Q1_2 - Structures needed by operators 
 *
 ********************************************************************/
/*
select 
        sum(lo_extendedprice*lo_discount) as
        revenue
from 
        lineorder, [date]
where 
        lo_orderdatekey =  d_datekey
        and d_yearmonthnum = 199401
        and lo_discount between 4 and 6
        and lo_quantity between 26 and 35;
*/

// the tuples after tablescan projection
struct q12_lo_tuple
{
  int LO_EXTENDEDPRICE;
  int LO_ORDERDATE;
  int LO_DISCOUNT;    
};

struct q12_d_tuple
{ 
  int D_DATEKEY;
};

struct q12_join_tuple
{
  int LO_EXTENDEDPRICE;
  int LO_DISCOUNT;
};

struct q12_agg_tuple
{
    double TOTAL_SUM;
    double REVENUE;
};


/*struct projected_tuple
{
  int KEY;
  };*/
typedef struct q12_agg_tuple projected_tuple;



class q12_lineorder_tscan_filter_t : public tuple_filter_t 
{
private:
    ShoreSSBEnv* _ssbdb;
    table_row_t* _prline;
    rep_row_t _rr;

    ssb_lineorder_tuple _lineorder;
    
    /*VARIABLES TAKING VALUES FROM INPUT FOR SELECTION*/
    int DISCOUNT_1;
    int DISCOUNT_2;
    int QUANTITY_1;
    int QUANTITY_2;

public:

    q12_lineorder_tscan_filter_t(ShoreSSBEnv* ssbdb)//,q1_2_input_t &in) 
        : tuple_filter_t(ssbdb->lineorder_desc()->maxsize()), _ssbdb(ssbdb)
    {

    	// Get a lineorder tupple from the tuple cache and allocate space
        _prline = _ssbdb->lineorder_man()->get_tuple();
        _rr.set_ts(_ssbdb->lineorder_man()->ts(),
                   _ssbdb->lineorder_desc()->maxsize());
        _prline->_rep = &_rr;
        
        DISCOUNT_1=4;
        DISCOUNT_2=6;
        QUANTITY_1=26;
        QUANTITY_2=35;

    }

    ~q12_lineorder_tscan_filter_t()
    {
        // Give back the lineorder tuple 
        _ssbdb->lineorder_man()->give_tuple(_prline);
    }


    // Predication
    bool select(const tuple_t &input) {

        // Get next lineorder and read its shipdate
        if (!_ssbdb->lineorder_man()->load(_prline, input.data)) {
            assert(false); // RC(se_WRONG_DISK_DATA)
        }

        _prline->get_value(11, _lineorder.LO_DISCOUNT);
        _prline->get_value(8, _lineorder.LO_QUANTITY);
        
        if (_lineorder.LO_DISCOUNT>=DISCOUNT_1 && _lineorder.LO_DISCOUNT<=DISCOUNT_2 && _lineorder.LO_QUANTITY>=QUANTITY_1 && _lineorder.LO_QUANTITY<=QUANTITY_2)
            {       
                TRACE( TRACE_RECORD_FLOW, "+ DISCOUNT |%d QUANTITY |%d --d\n",
		       _lineorder.LO_DISCOUNT, _lineorder.LO_QUANTITY);
		return (true);
            }
        else
            {
                TRACE( TRACE_RECORD_FLOW, ". DISCOUNT |%d QUANTITY |%d --d\n",
		       _lineorder.LO_DISCOUNT, _lineorder.LO_QUANTITY);
		return (false);
            }
    }

    
    // Projection
    void project(tuple_t &d, const tuple_t &s) {        

        q12_lo_tuple *dest;
        dest = aligned_cast<q12_lo_tuple>(d.data);

        _prline->get_value(5, _lineorder.LO_ORDERDATE);
        _prline->get_value(9, _lineorder.LO_EXTENDEDPRICE);
        _prline->get_value(11, _lineorder.LO_DISCOUNT);


        TRACE( TRACE_RECORD_FLOW, "%d|%d|%d --d\n",
               _lineorder.LO_ORDERDATE,
               _lineorder.LO_EXTENDEDPRICE,
               _lineorder.LO_DISCOUNT);

        dest->LO_ORDERDATE = _lineorder.LO_ORDERDATE;
        dest->LO_EXTENDEDPRICE = _lineorder.LO_EXTENDEDPRICE;
        dest->LO_DISCOUNT = _lineorder.LO_DISCOUNT;

    }

    q12_lineorder_tscan_filter_t* clone() const {
        return new q12_lineorder_tscan_filter_t(*this);
    }

    c_str to_string() const {
        return c_str("q1_2_lineorder_tscan_filter_t()");
    }
};





class q12_date_tscan_filter_t : public tuple_filter_t 
{
private:
    ShoreSSBEnv* _ssbdb;
    table_row_t* _prdate;
    rep_row_t _rr;

    ssb_date_tuple _date;

  /*VARIABLES TAKING VALUES FROM INPUT FOR SELECTION*/
    int YEARMONTHNUM;

public:

    q12_date_tscan_filter_t(ShoreSSBEnv* ssbdb, q1_2_input_t &in) 
        : tuple_filter_t(ssbdb->date_desc()->maxsize()), _ssbdb(ssbdb)
    {

    	// Get a date tupple from the tuple cache and allocate space
        _prdate = _ssbdb->date_man()->get_tuple();
        _rr.set_ts(_ssbdb->date_man()->ts(),
                   _ssbdb->date_desc()->maxsize());
        _prdate->_rep = &_rr;

	YEARMONTHNUM=199401;
    }

    ~q12_date_tscan_filter_t()
    {
        // Give back the date tuple 
        _ssbdb->date_man()->give_tuple(_prdate);
    }


    // Predication
    bool select(const tuple_t &input) {

        // Get next date and read its shipdate
        if (!_ssbdb->date_man()->load(_prdate, input.data)) {
            assert(false); // RC(se_WRONG_DISK_DATA)
        }

        _prdate->get_value(5, _date.D_YEARMONTHNUM);

	
	if (_date.D_YEARMONTHNUM==YEARMONTHNUM)
	    {
		TRACE( TRACE_RECORD_FLOW, "+ YEARMONTHNUM |%d --d\n",
		       _date.D_YEARMONTHNUM);
		return (true);
	    }
	else
	    {
		TRACE( TRACE_RECORD_FLOW, ". YEARMONTHNUM |%d --d\n",
		       _date.D_YEARMONTHNUM);
		return (false);
	    }

    }

    
    // Projection
    void project(tuple_t &d, const tuple_t &s) {        

        q12_d_tuple *dest;
        dest = aligned_cast<q12_d_tuple>(d.data);

        _prdate->get_value(0, _date.D_DATEKEY);

        TRACE( TRACE_RECORD_FLOW, "%d --d\n",
               _date.D_DATEKEY);


        dest->D_DATEKEY = _date.D_DATEKEY;
    }

    q12_date_tscan_filter_t* clone() const {
        return new q12_date_tscan_filter_t(*this);
    }

    c_str to_string() const {
        return c_str("q12_date_tscan_filter_t()");
    }
};

//Natural join
// left is lineorder, right is date
struct q12_join_t : public tuple_join_t {


    q12_join_t ()
        : tuple_join_t(sizeof(q12_lo_tuple),
                       offsetof(q12_lo_tuple, LO_ORDERDATE),
                       sizeof(q12_d_tuple),
                       offsetof(q12_d_tuple, D_DATEKEY),
                       sizeof(int),
                       sizeof(q12_join_tuple))
    {
    }


    virtual void join(tuple_t &dest,
                      const tuple_t &left,
                      const tuple_t &right)
    {
        // KLUDGE: this projection should go in a separate filter class
    	q12_lo_tuple* lo = aligned_cast<q12_lo_tuple>(left.data);
    	q12_d_tuple* d = aligned_cast<q12_d_tuple>(right.data);
	q12_join_tuple* ret = aligned_cast<q12_join_tuple>(dest.data);
	
	ret->LO_EXTENDEDPRICE = lo->LO_EXTENDEDPRICE;
	ret->LO_DISCOUNT = lo->LO_DISCOUNT;

        TRACE ( TRACE_RECORD_FLOW, "JOIN %d %d \n",ret->LO_EXTENDEDPRICE, ret->LO_DISCOUNT);

    }

    virtual q12_join_t*  clone() const {
        return new q12_join_t(*this);
    }

    virtual c_str to_string() const {
        return "join LINEORDER and DATE, select LO_EXTENDEDPRICE, LO_DISCOUNT";
    }
};




/*struct count_aggregate_t : public tuple_aggregate_t {
    default_key_extractor_t _extractor;
    
    count_aggregate_t()
        : tuple_aggregate_t(sizeof(projected_tuple))
    {
    }
    virtual key_extractor_t* key_extractor() { return &_extractor; }
    
    virtual void aggregate(char* agg_data, const tuple_t &) {
        count_tuple* agg = aligned_cast<count_tuple>(agg_data);
        agg->COUNT++;
    }

    virtual void finish(tuple_t &d, const char* agg_data) {
        memcpy(d.data, agg_data, tuple_size());
    }
    virtual count_aggregate_t* clone() const {
        return new count_aggregate_t(*this);
    }
    virtual c_str to_string() const {
        return "count_aggregate_t";
    }
};
*/



struct q12_aggregate : tuple_aggregate_t {
    default_key_extractor_t _extractor;
    //like_predicate_t _filter;

    q12_aggregate()
        : tuple_aggregate_t(sizeof(q12_agg_tuple)),
          _extractor(0, 0)//,
          //_filter("PROMO%", offsetof(join_tuple, P_TYPE))
    {
    }

    virtual key_extractor_t* key_extractor() {
        return &_extractor;
    }
    virtual void aggregate(char* agg_data, const tuple_t &t) {
        q12_agg_tuple* agg = aligned_cast<q12_agg_tuple>(agg_data);
        q12_join_tuple* tuple = aligned_cast<q12_join_tuple>(t.data);

        double value = (tuple->LO_EXTENDEDPRICE * tuple->LO_DISCOUNT)/100;
        agg->TOTAL_SUM += value;
       
    }
    virtual void finish(tuple_t &d, const char* agg_data) {
        q12_agg_tuple* dest = aligned_cast<q12_agg_tuple>(d.data);
        q12_agg_tuple* agg = aligned_cast<q12_agg_tuple>(agg_data);
        dest->REVENUE = agg->TOTAL_SUM;
    }
    virtual q12_aggregate* clone() const {
        return new q12_aggregate(*this);
    }
    virtual c_str to_string() const {
        return "q12_aggregate";
    }
};


class ssb_q12_process_tuple_t : public process_tuple_t 
{    
public:
        
    void begin() {
        TRACE(TRACE_QUERY_RESULTS, "*** q1_2 ANSWER ...\n");
        TRACE(TRACE_QUERY_RESULTS, "*** ...\n");
    }
    
    virtual void process(const tuple_t& output) {
        projected_tuple *tuple;

        tuple = aligned_cast<projected_tuple>(output.data);
        TRACE ( TRACE_QUERY_RESULTS, "PROCESS %lf \n", tuple->REVENUE);

        /*TRACE(TRACE_QUERY_RESULTS, "%d --\n",
	  tuple->KEY);*/
    }
};



/******************************************************************** 
 *
 * QPIPE q1_2 - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_qpipe_q1_2(const int xct_id, 
                                  q1_2_input_t& in)
{
    TRACE( TRACE_ALWAYS, "********** q1_2 *********\n");

   
    policy_t* dp = this->get_sched_policy();
    xct_t* pxct = smthread_t::me()->xct();
    

    // TSCAN PACKET
    tuple_fifo* lo_out_buffer = new tuple_fifo(sizeof(q12_lo_tuple));
        tscan_packet_t* lo_tscan_packet =
        new tscan_packet_t("TSCAN LINEORDER",
                           lo_out_buffer,
                           new q12_lineorder_tscan_filter_t(this),
                           this->db(),
                           _plineorder_desc.get(),
                           pxct
                           //, SH 
                           );
	
	//DATE
	tuple_fifo* d_out_buffer = new tuple_fifo(sizeof(q12_d_tuple));
        tscan_packet_t* d_tscan_packet =
        new tscan_packet_t("TSCAN DATE",
                           d_out_buffer,
                           new q12_date_tscan_filter_t(this,in),
                           this->db(),
                           _pdate_desc.get(),
                           pxct
                           //, SH 
                           );


	
	//JOIN Lineorder and Date
	tuple_fifo* join_out = new tuple_fifo(sizeof(q12_join_tuple));
	packet_t* join_packet =
	    new hash_join_packet_t("Lineorder - Date JOIN",
				   join_out,
				   new trivial_filter_t(sizeof(q12_join_tuple)),
				   lo_tscan_packet,
				   d_tscan_packet,
				   new q12_join_t() );
        
        
	//aggregation								
        tuple_fifo* q12_agg_buffer = new tuple_fifo(sizeof(q12_agg_tuple));
        packet_t* q12_agg_packet = new aggregate_packet_t("AGG Q1_2",
                                        q12_agg_buffer, 
					new trivial_filter_t(sizeof(q12_agg_tuple)),
                                        new q12_aggregate(),
                                        new default_key_extractor_t(0, 0),
                                        join_packet);
        
        
    qpipe::query_state_t* qs = dp->query_state_create();
    lo_tscan_packet->assign_query_state(qs);
    d_tscan_packet->assign_query_state(qs);
    join_packet->assign_query_state(qs);
    q12_agg_packet->assign_query_state(qs);
        
    // Dispatch packet
    ssb_q12_process_tuple_t pt;
//    process_query(join_packet, pt);
    process_query(q12_agg_packet, pt);
    dp->query_state_destroy(qs);

    return (RCOK); 
}


EXIT_NAMESPACE(ssb);
