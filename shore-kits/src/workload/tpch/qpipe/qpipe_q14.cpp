/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-kits -- Benchmark implementations for Shore-MT
   
                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne
   
                         All Rights Reserved.
   
   Permission1 to use, copy, modify and distribute this software and
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

/** @file:   qpipe_q14.cpp
 *
 *  @brief:  Implementation of QPIPE TPCH Q14 over Shore-MT
 *
 *  @author: 
 *  @date:   
 */

#include "workload/tpch/shore_tpch_env.h"
//#include "qpipe/common/predicates.h"
//#include "workload/tpch/tpch_struct.h"
#include "workload/tpch/tpch_util.h"
#include "qpipe.h"

using namespace shore;
using namespace qpipe;



ENTER_NAMESPACE(tpch);

/*
 * select
 *     100.00 * sum(case
 *         when p_type like 'PROMO%'
 *         then l_extendedprice*(1-l_discount)
 *         else 0
 *     end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
 * from
 *     lineitem,
 *     part
 * where
 *     l_partkey = p_partkey
 *     and l_shipdate >= date '[DATE]'
 *     and l_shipdate < date '[DATE]' + interval '1' month;
 */

struct q14_lineitem_scan_tuple {
    double L_EXTENDEDPRICE;
    double L_DISCOUNT;
    int L_PARTKEY;
};

struct q14_part_scan_tuple {
    int P_PARTKEY;
    char P_TYPE[STRSIZE(25)];
};

struct q14_join_tuple {
    double L_EXTENDEDPRICE;
    double L_DISCOUNT;
    char P_TYPE[STRSIZE(25)];
};

struct q14_agg_tuple {
    double PROMO_SUM;
    double TOTAL_SUM;
    char P_TYPE[STRSIZE(25)];
};

struct q14_tuple {
    double PROMO_REVENUE;
};

/**
 * @brief select L_PARTKEY, L_EXTENDEDPRICE, L_DISCOUNT from LINEITEM
 * where L_SHIPDATE >= [date] and L_SHIPDATE < [date] + 1 month
 */
class q14_lineitem_tscan_filter_t : public tuple_filter_t 
{
private:
    ShoreTPCHEnv* _tpchdb;
    table_row_t* _prline;
    rep_row_t _rr;

    /*One lineitem tuple*/
    tpch_lineitem_tuple _lineitem;
    /*The columns needed for the selection*/
    time_t _shipdate;

  //and_predicate_t _filter;

    /* Random Predicates */
    q14_input_t* q14_input;
    time_t date1, date2;
public:
    q14_lineitem_tscan_filter_t(ShoreTPCHEnv* tpchdb, q14_input_t &in)
        : tuple_filter_t(tpchdb->lineitem_desc()->maxsize()), _tpchdb(tpchdb)
    {
   	// Get a lineitem tupple from the tuple cache and allocate space
        _prline = _tpchdb->lineitem_man()->get_tuple();
        _rr.set_ts(_tpchdb->lineitem_man()->ts(),
                   _tpchdb->lineitem_desc()->maxsize());
        _prline->_rep = &_rr;

      //size_t offset = offsetof(tpch_lineitem_tuple, L_SHIPDATE);
      //predicate_t* p;

        // L_SHIPDATE >= [date]
	
	q14_input=&in;
        date1 = q14_input->l_shipdate;
	// L_SHIPDATE < [date] + 1 month
        date2 = time_add_month(date1, 1);

        char shdate1[15];
        char shdate2[15];

        timet_to_str(shdate1, date1);
        timet_to_str(shdate2, date2);

        TRACE(TRACE_ALWAYS, "Random predicates:\n%s <= L_SHIPDATE < %s\n", shdate1, shdate2);

        //p = new scalar_predicate_t<time_t, greater_equal>(date1, offset);
        //_filter.add(p);

        // L_SHIPDATE < [date] + 1 month
        //date2 = time_add_month(date1, 1);
        //p = new scalar_predicate_t<time_t, less>(date2, offset);
        //_filter.add(p);
    }

    ~q14_lineitem_tscan_filter_t()
    {
        // Give back the lineitem tuple
        _tpchdb->lineitem_man()->give_tuple(_prline);
    }

    virtual void project(tuple_t &d, const tuple_t &s) {
        q14_lineitem_scan_tuple *dest;
        dest = aligned_cast<q14_lineitem_scan_tuple>(d.data);

        _prline->get_value(1, _lineitem.L_PARTKEY);
        _prline->get_value(5, _lineitem.L_EXTENDEDPRICE);
        _prline->get_value(6, _lineitem.L_DISCOUNT);

        /*TRACE( TRACE_RECORD_FLOW, "%d %.2f|%.2f\n",
               _lineitem.L_PARTKEY,
               _lineitem.L_EXTENDEDPRICE / 100.0,
               _lineitem.L_DISCOUNT / 100.0);*/

        dest->L_PARTKEY=_lineitem.L_PARTKEY;
        dest->L_EXTENDEDPRICE = _lineitem.L_EXTENDEDPRICE/100.0;
        dest->L_DISCOUNT = _lineitem.L_DISCOUNT/100.0;
#warning MA: Discount from TPCH dbgen is created between 0 and 100 instead between 0 and 1.
    }

    virtual bool select(const tuple_t &t) 
    {
        if (!_tpchdb->lineitem_man()->load(_prline, t.data)) {
            assert(false); // RC(se_WRONG_DISK_DATA)
        }

	_prline->get_value(10, _lineitem.L_SHIPDATE, 15);
	_shipdate = str_to_timet(_lineitem.L_SHIPDATE);

	if (_shipdate>=date1 && _shipdate<date2)
	  {
	    return (true);
	  }
	else
	  {
	    return (false);
	  }

    }
    virtual q14_lineitem_tscan_filter_t* clone() const {
        return new q14_lineitem_tscan_filter_t(*this);
    }
    virtual c_str to_string() const {
      char* d1=new char[15];
      char* d2=new char[15];
      timet_to_str(d1,date1);
      timet_to_str(d2,date2);
        c_str result("select L_EXTENDEDPRICE, L_DISCOUNT, L_PARTKEY "
                     "where L_SHIPDATE >= %s and L_SHIPDATE < %s",
                     d1, d2);
        free(d1);
        free(d2);
        return result;
    }
};


/**
 * @brief select P_PARTKEY, P_TYPE from PART
 */
class q14_part_tscan_filter_t : public tuple_filter_t 
{
private:
    ShoreTPCHEnv* _tpchdb;
    table_row_t* _prpart;
    rep_row_t _rr;

    /*One lineitem tuple*/
    tpch_part_tuple _part;
public:
    q14_part_tscan_filter_t(ShoreTPCHEnv* tpchdb)
        : tuple_filter_t(tpchdb->part_desc()->maxsize()), _tpchdb(tpchdb)
    {
   	// Get a lineitem tupple from the tuple cache and allocate space
        _prpart = _tpchdb->part_man()->get_tuple();
        _rr.set_ts(_tpchdb->part_man()->ts(),
                   _tpchdb->part_desc()->maxsize());
        _prpart->_rep = &_rr;
    }

    ~q14_part_tscan_filter_t()
    {
        // Give back the lineitem tuple
        _tpchdb->part_man()->give_tuple(_prpart);
    }

    virtual void project(tuple_t &d, const tuple_t &s) {
        q14_part_scan_tuple *dest;
        dest = aligned_cast<q14_part_scan_tuple>(d.data);

        _prpart->get_value(0, _part.P_PARTKEY);
        _prpart->get_value(4, _part.P_TYPE, 25);

        /*TRACE( TRACE_RECORD_FLOW, "%d %s\n",
               _part.P_PARTKEY,
               _part.P_TYPE);*/

        dest->P_PARTKEY=_part.P_PARTKEY;
	memcpy(dest->P_TYPE, _part.P_TYPE, sizeof(dest->P_TYPE));
    }

    virtual bool select(const tuple_t &t) 
    {
        if (!_tpchdb->part_man()->load(_prpart, t.data)) {
            assert(false); // RC(se_WRONG_DISK_DATA)
        }

	return (true);
    }

    virtual q14_part_tscan_filter_t* clone() const {
        return new q14_part_tscan_filter_t(*this);
    }
    virtual c_str to_string() const {
      c_str result("select L_EXTENDEDPRICE, L_DISCOUNT, L_PARTKEY ");
        return result;
    }
};

//Join
/**
 * @brief join part, lineitem on P_PARTKEY = L_PARTKEY
 */
struct q14_join : tuple_join_t {

    q14_join()
        : tuple_join_t(sizeof(q14_part_scan_tuple),
                       offsetof(q14_part_scan_tuple, P_PARTKEY),
                       sizeof(q14_lineitem_scan_tuple),
                       offsetof(q14_lineitem_scan_tuple, L_PARTKEY),
                       sizeof(int),
                       sizeof(q14_join_tuple))
    {
    }
    
    virtual void join(tuple_t &d, const tuple_t &l, const tuple_t &r) {
        q14_join_tuple* dest = aligned_cast<q14_join_tuple>(d.data);
        q14_part_scan_tuple* left = aligned_cast<q14_part_scan_tuple>(l.data);
        q14_lineitem_scan_tuple* right = aligned_cast<q14_lineitem_scan_tuple>(r.data);

        // cheat and filter out the join key...
        dest->L_EXTENDEDPRICE = right->L_EXTENDEDPRICE;
        dest->L_DISCOUNT = right->L_DISCOUNT;
        memcpy(dest->P_TYPE, left->P_TYPE, sizeof(dest->P_TYPE));

        /*TRACE( TRACE_RECORD_FLOW, "%lf %.2lf %s\n",
               dest->L_EXTENDEDPRICE,
               dest->L_DISCOUNT,
               dest->P_TYPE);*/

    }

    virtual q14_join* clone() const {
        return new q14_join(*this);
    }

    virtual c_str to_string() const {
        return "PART join LINEITEM";
    }
};

/**
 * @brief 100.00 * sum(case
 *         when p_type like 'PROMO%'
 *         then l_extendedprice*(1-l_discount)
 *         else 0
 *     end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
 */
struct q14_aggregate : tuple_aggregate_t {
    default_key_extractor_t _extractor;
    //like_predicate_t _filter;

    q14_aggregate()
        : tuple_aggregate_t(sizeof(q14_tuple)),
          _extractor(0, 0)//,
          //_filter("PROMO%", offsetof(join_tuple, P_TYPE))
    {
    }

    virtual key_extractor_t* key_extractor() {
        return &_extractor;
    }
    virtual void aggregate(char* agg_data, const tuple_t &t) {
        q14_agg_tuple* agg = aligned_cast<q14_agg_tuple>(agg_data);
        q14_join_tuple* tuple = aligned_cast<q14_join_tuple>(t.data);

        double value = tuple->L_EXTENDEDPRICE*(1 - tuple->L_DISCOUNT);
        agg->TOTAL_SUM += value;
        //if(_filter.select(t))
	if (strstr(tuple->P_TYPE,"PROMO"))
	{
	    //TRACE ( TRACE_ALWAYS, "%s\n",tuple->P_TYPE);
	    agg->PROMO_SUM += value;
	}
    }
    virtual void finish(tuple_t &d, const char* agg_data) {
        q14_tuple* dest = aligned_cast<q14_tuple>(d.data);
        q14_agg_tuple* agg = aligned_cast<q14_agg_tuple>(agg_data);
        dest->PROMO_REVENUE = 100.*agg->PROMO_SUM/agg->TOTAL_SUM;
    }
    virtual q14_aggregate* clone() const {
        return new q14_aggregate(*this);
    }
    virtual c_str to_string() const {
        return "q14_aggregate";
    }
};


class tpch_q14_process_tuple_t : public process_tuple_t {
    
public:
     
    virtual void process(const tuple_t& output) {
        q14_tuple* r = aligned_cast<q14_tuple>(output.data);
        TRACE(TRACE_ALWAYS, "*** Q14 Promo Revenue: %5.2lf\n", r->PROMO_REVENUE);
    }

};




/******************************************************************** 
 *
 * QPIPE Q14 - Packet creation and submission
 *               
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_qpipe_q14(const int xct_id, 
                                  q14_input_t& in)
{
    TRACE( TRACE_ALWAYS, "********** Q14 *********\n");

    policy_t* dp = this->get_sched_policy();
    xct_t* pxct = smthread_t::me()->xct();

    //lineitem scan
    tuple_fifo* q14_tscan_lineitem_out = new tuple_fifo(sizeof(q14_lineitem_scan_tuple));
    tscan_packet_t* q14_tscan_lineitem_packet = new tscan_packet_t("lineitem TSCAN",
                                         q14_tscan_lineitem_out,
					 new q14_lineitem_tscan_filter_t(this,in),
					 this->db(),
                                         _plineitem_desc.get(),
                                         pxct
                                         //, SH
                                         );

    //part scan
    tuple_fifo* q14_tscan_part_out = new tuple_fifo(sizeof(q14_part_scan_tuple));
    tscan_packet_t* q14_tscan_part_packet = new tscan_packet_t("part TSCAN",
                                         q14_tscan_part_out,
					 new q14_part_tscan_filter_t(this),
					 this->db(),
                                         _ppart_desc.get(),
                                         pxct
                                         //, SH
                                         );

    //join
    tuple_fifo* q14_join_buffer = new tuple_fifo(sizeof(q14_join_tuple));
    packet_t* q14_join_packet = new hash_join_packet_t("part-lineitem HJOIN",
                                         q14_join_buffer, 
					 new trivial_filter_t(sizeof(q14_join_tuple)),
                                         q14_tscan_part_packet,
                                         q14_tscan_lineitem_packet,
                                         new q14_join());

    //aggregation								
    tuple_fifo* q14_agg_buffer = new tuple_fifo(sizeof(q14_tuple));
    packet_t* q14_agg_packet = new aggregate_packet_t("sum AGG",
                                        q14_agg_buffer, 
					new trivial_filter_t(sizeof(q14_tuple)),
                                        new q14_aggregate(),
                                        new default_key_extractor_t(0, 0),
                                        q14_join_packet);


    qpipe::query_state_t* qs = dp->query_state_create();
    //q14_*****->assign_query_state(qs);
    q14_tscan_lineitem_packet->assign_query_state(qs);
    q14_tscan_part_packet->assign_query_state(qs);
    q14_join_packet->assign_query_state(qs);
    q14_agg_packet->assign_query_state(qs);

    // Dispatch packet
    tpch_q14_process_tuple_t pt;
    //LAST PACKET
    //process_query(q14_tscan_lineitem_packet, pt);
    process_query(q14_agg_packet, pt);
    dp->query_state_destroy(qs);



    return (RCOK); 
}


EXIT_NAMESPACE(tpch);
