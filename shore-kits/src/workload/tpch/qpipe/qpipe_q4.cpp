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

/** @file:   qpipe_q4.cpp
 *
 *  @brief:  Implementation of QPIPE TPCH Q4 over Shore-MT
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
 * QPIPE Q4 - Structures needed by operators 
 *
 ********************************************************************/
    /*
     * Query 4 original:
     *
     * select o_orderpriority, count(*) as order_count
     * from orders
     * where o_orderdate <in range> and exists
     *      (select * from lineitem
     *       where l_orderkey = o_orderkey and l_commitdate < l_receiptdate)
     * group by o_orderpriority
     * order by o_orderpriority
     *
     *
     * Query 4 modified to make the nested query cleaner:
     *
     * select o_orderpriority, count(*) as order_count
     * from orders natural join (
     *      select distinct l_orderkey
     *      from lineitem
     *      where l_commitdate < l_receiptdate)
     */
/*
create table orders (o_orderkey integer not null, 
                     o_custkey integer not null,
                     o_orderstatus char(1) not null,-- comment 'lookup',
                     o_totalprice float not null, 
                     o_orderdate date not null, 
                     o_orderpriority char(15) not null,-- comment 'lookup',
                     o_clerk char(15) not null, 
                     o_shippriority integer not null,
                     o_comment varchar(79) not null
);
*/

// the tuples after tablescan projection
struct q4_projected_lineitem_tuple {
    int L_ORDERKEY;
};

// the tuples after tablescan projection
struct q4_projected_orders_tuple {
    int O_ORDERKEY;
    int O_ORDERPRIORITY;
};

// the tuples after join
struct q4_join_tuple {
	int O_ORDERPRIORITY;
};

// the tuples after aggregate
struct q4_aggregate_tuple {
	int O_ORDERPRIORITY;
	int O_COUNT;
};


//Q4 lineitem scan filter (selection and projection)
class q4_tscan_lineitem_filter_t : public tuple_filter_t
{
private:
    ShoreTPCHEnv* _tpchdb;
    table_row_t* _prline;
    rep_row_t _rr;

    /*One lineitem tuple*/
    tpch_lineitem_tuple _lineitem;
    /*The columns needed for the selection*/
    time_t _commitdate;
    time_t _receiptdate;
    /* No Random Predicates */
public:

    q4_tscan_lineitem_filter_t(ShoreTPCHEnv* tpchdb)
        : tuple_filter_t(tpchdb->lineitem_desc()->maxsize()), _tpchdb(tpchdb)
          //: tuple_filter_t(sizeof(tpch_lineitem_tuple)), _tpchdb(tpchdb)
    {
    	// Get a lineitem tupple from the tuple cache and allocate space
        _prline = _tpchdb->lineitem_man()->get_tuple();
        _rr.set_ts(_tpchdb->lineitem_man()->ts(),
                   _tpchdb->lineitem_desc()->maxsize());
        _prline->_rep = &_rr;
    }

    ~q4_tscan_lineitem_filter_t()
    {
        // Give back the lineitem tuple
        _tpchdb->lineitem_man()->give_tuple(_prline);
    }


    // Predication
    bool select(const tuple_t &input) {

        // Get next lineitem and read its receiptdate and commitdate
        if (!_tpchdb->lineitem_man()->load(_prline, input.data)) {
            assert(false); // RC(se_WRONG_DISK_DATA)
        }

        _prline->get_value(12, _lineitem.L_RECEIPTDATE, 15);
        _receiptdate = str_to_timet(_lineitem.L_RECEIPTDATE);

        _prline->get_value(11, _lineitem.L_COMMITDATE, 15);
        _commitdate = str_to_timet(_lineitem.L_COMMITDATE);


        // Return true if it passes the filter
		if  ( _receiptdate > _commitdate) {
			//TRACE(TRACE_RECORD_FLOW, "+ %s > %s\n", _lineitem.L_RECEIPTDATE, _lineitem.L_COMMITDATE);
			return (true);
		}
		else {
			//TRACE(TRACE_RECORD_FLOW, ". %s <= %s\n", _lineitem.L_RECEIPTDATE, _lineitem.L_COMMITDATE);
			return (false);
		}
    }


    // Projection
    void project(tuple_t &d, const tuple_t &/*s*/) {

        q4_projected_lineitem_tuple *dest;
        dest = aligned_cast<q4_projected_lineitem_tuple>(d.data);

        _prline->get_value(0, _lineitem.L_ORDERKEY);

        //TRACE( TRACE_RECORD_FLOW, "%d\n",
        //		_lineitem.L_ORDERKEY);

        dest->L_ORDERKEY = _lineitem.L_ORDERKEY;
    }

    q4_tscan_lineitem_filter_t* clone() const {
        return new q4_tscan_lineitem_filter_t(*this);
    }

    c_str to_string() const {
        return c_str("select L_ORDERKEY where L_COMMITDATE < L_RECEIPTDATE");
    }
};


//DISTINCT
class q4_distinct_t : public tuple_aggregate_t {

public:

    class q4_distinct_key_extractor_t : public key_extractor_t {
    public:
    	q4_distinct_key_extractor_t()
            : key_extractor_t(sizeof(int), offsetof(q4_projected_lineitem_tuple,L_ORDERKEY))
        {
        }

        virtual int extract_hint(const char* tuple_data) const {
            // store the return flag and line status in the
            q4_projected_lineitem_tuple *item;
            item = aligned_cast<q4_projected_lineitem_tuple>(tuple_data);
        	return item->L_ORDERKEY;
        }

        virtual key_extractor_t* clone() const {
            return new q4_distinct_key_extractor_t(*this);
        }
    };

private:

    q4_distinct_key_extractor_t _extractor;

public:

    q4_distinct_t()
        : tuple_aggregate_t(sizeof(q4_projected_lineitem_tuple)),
          _extractor()
    {
    }

    virtual key_extractor_t* key_extractor() { return &_extractor; }

    virtual void aggregate(char* agg_data, const tuple_t& src) {
    	q4_projected_lineitem_tuple* agg = aligned_cast<q4_projected_lineitem_tuple>(agg_data);
    	q4_projected_lineitem_tuple* in = aligned_cast<q4_projected_lineitem_tuple>(src.data);
        agg->L_ORDERKEY=in->L_ORDERKEY;

    }

    virtual void finish(tuple_t &dest, const char* agg_data) {
    	q4_projected_lineitem_tuple* agg = aligned_cast<q4_projected_lineitem_tuple>(agg_data);
    	q4_projected_lineitem_tuple* output = aligned_cast<q4_projected_lineitem_tuple>(dest.data);
        output->L_ORDERKEY = agg->L_ORDERKEY;
        //TRACE (TRACE_ALWAYS, "Orderkey: %d\n",output->L_ORDERKEY);
    }

    virtual q4_distinct_t* clone() const {
        return new q4_distinct_t(*this);
    }

    virtual c_str to_string() const {
        return "q4_distinct_t";
    }
};



//Q4 orders scan filter (selection and projection)
class q4_tscan_orders_filter_t : public tuple_filter_t
{
private:
    ShoreTPCHEnv* _tpchdb;
    table_row_t* _prorder;
    rep_row_t _rr;

    /*One lineitem tuple*/
    tpch_orders_tuple _orders;
    /*The columns needed for the selection*/
    time_t _orderdate;

    /* Random Predicates */
    /* TPC-H Specification 2.7.3 */
    /* MONTH randomly selected within [1-1993, 10-1997]*/
    q4_input_t* q4_input;
    time_t _last_o_orderdate;
public:

    q4_tscan_orders_filter_t(ShoreTPCHEnv* tpchdb, q4_input_t &in)
        : tuple_filter_t(tpchdb->orders_desc()->maxsize()), _tpchdb(tpchdb)
    {
    	// Get an orders tupple from the tuple cache and allocate space
        _prorder = _tpchdb->orders_man()->get_tuple();
        _rr.set_ts(_tpchdb->orders_man()->ts(),
                   _tpchdb->orders_desc()->maxsize());
        _prorder->_rep = &_rr;

        // Generate the random predicates
	/* Predicate:
		o_orderdate >= date '[DATE]'
		and o_orderdate < date '[DATE]' + interval '3' month
		DATE is the first day of a randomly selected month between the first month of 1993 and the 10th month of 1997.
	*/
	q4_input=&in;
	struct tm date;
	gmtime_r(&(q4_input->o_orderdate), &date);
	date.tm_mon += 3;
	_last_o_orderdate=mktime(&date);

	char date1[15];
	char date2[15];
	timet_to_str(date1,q4_input->o_orderdate);
	timet_to_str(date2,_last_o_orderdate);
	TRACE ( TRACE_ALWAYS, "Dates: %s - %s\n",date1,date2);
    }

    ~q4_tscan_orders_filter_t()
    {
        // Give back the orders tuple
        _tpchdb->orders_man()->give_tuple(_prorder);
    }


    // Predication
    bool select(const tuple_t &input) {

        // Get next order and read its orderdate
        if (!_tpchdb->orders_man()->load(_prorder, input.data)) {
            assert(false); // RC(se_WRONG_DISK_DATA)
        }

        _prorder->get_value(4, _orders.O_ORDERDATE, 15);
        _orderdate = str_to_timet(_orders.O_ORDERDATE);


        // Return true if it passes the filter
		if  ( _orderdate >= q4_input->o_orderdate && _orderdate < _last_o_orderdate ) {
			//TRACE(TRACE_RECORD_FLOW, "+ %s (between %s and %s)\n", _orders.O_ORDERDATE, q4_input->o_orderdate, _last_o_orderdate);
			return (true);
		}
		else {
			//TRACE(TRACE_RECORD_FLOW, ". %s (not between %s and %s)\n", _orders.O_ORDERDATE, q4_input->o_orderdate, _last_o_orderdate );
			return (false);
		}
    }


    // Projection
    void project(tuple_t &d, const tuple_t &/*s*/) {

        q4_projected_orders_tuple *dest;
        dest = aligned_cast<q4_projected_orders_tuple>(d.data);

        _prorder->get_value(0, _orders.O_ORDERKEY);
        _prorder->get_value(5, _orders.O_ORDERPRIORITY, 15);

        char number[2];
        strncpy(number,_orders.O_ORDERPRIORITY,1);
        number[1] = '\0';

        /*TRACE( TRACE_RECORD_FLOW, "%d, %s\n",
        		_orders.O_ORDERKEY,
        		_orders.O_ORDERPRIORITY);*/
        dest->O_ORDERKEY = _orders.O_ORDERKEY;
        dest->O_ORDERPRIORITY = atoi(number);
    }

    q4_tscan_orders_filter_t* clone() const {
        return new q4_tscan_orders_filter_t(*this);
    }

    c_str to_string() const {
        char date1[15];
        char date2[15];
        timet_to_str(date1, q4_input->o_orderdate);
        timet_to_str(date2, _last_o_orderdate);
        c_str result("select O_ORDERKEY, O_ORDERPRIORITY "
                     "where O_ORDERDATE >= %s and O_ORDERDATE < %s",
                     date1, date2);
    	return result;
    }
};

//Sort Lineitem
struct q4_extractor_lineitem_t : public key_extractor_t {

    q4_extractor_lineitem_t()
        : key_extractor_t(sizeof(q4_projected_lineitem_tuple))
    {
    }

    virtual int extract_hint(const char* key) const {
        return *aligned_cast<int>(key);
    }

    virtual q4_extractor_lineitem_t* clone() const {
        return new q4_extractor_lineitem_t(*this);
    }
};

struct q4_compare_lineitem_t : public key_compare_t {

    virtual int operator()(const void* key1, const void* key2) const {

        int* a = aligned_cast<int>(key1);
        int* b = aligned_cast<int>(key2);

        // sort by orderkey asc 
        
        return (*a - *b);
    }

    virtual q4_compare_lineitem_t* clone() const {
        return new q4_compare_lineitem_t(*this);
    }
};


//Natural join
// left is lineitem, right is orders
struct q4_join_t : public tuple_join_t {


    q4_join_t ()
        : tuple_join_t(sizeof(q4_projected_orders_tuple),
                       offsetof(q4_projected_orders_tuple, O_ORDERKEY),
                       sizeof(q4_projected_lineitem_tuple),
                       offsetof(q4_projected_lineitem_tuple, L_ORDERKEY),
                       sizeof(int),
                       sizeof(q4_join_tuple))
    {
    }


    virtual void join(tuple_t &dest,
                      const tuple_t &left,
                      const tuple_t &)
    {
        // KLUDGE: this projection should go in a separate filter class
    	q4_projected_orders_tuple* tuple = aligned_cast<q4_projected_orders_tuple>(left.data);
        q4_join_tuple *d = aligned_cast<q4_join_tuple>(dest.data);
        d->O_ORDERPRIORITY = tuple->O_ORDERPRIORITY;

        //TRACE ( TRACE_ALWAYS, "JOIN %d %d\n",tuple->O_ORDERPRIORITY,tuple->O_ORDERKEY);

    }

    virtual q4_join_t* clone() const {
        return new q4_join_t(*this);
    }

    virtual c_str to_string() const {
        return "join LINEITEM and ORDERS, select O_ORDERPRIORITY";
    }
};


struct q4_count_aggregate_t : public tuple_aggregate_t {
    default_key_extractor_t _extractor;
    
    q4_count_aggregate_t()
        : tuple_aggregate_t(sizeof(q4_aggregate_tuple))
    {
    }
    virtual key_extractor_t* key_extractor() { return &_extractor; }
    
    virtual void aggregate(char* agg_data, const tuple_t &) {
        q4_aggregate_tuple* agg = aligned_cast<q4_aggregate_tuple>(agg_data);
        agg->O_COUNT++;
    }

    virtual void finish(tuple_t &d, const char* agg_data) {
        memcpy(d.data, agg_data, tuple_size());
    }
    virtual q4_count_aggregate_t* clone() const {
        return new q4_count_aggregate_t(*this);
    }
    virtual c_str to_string() const {
        return "q4_count_aggregate_t";
    }
};


class tpch_q4_process_tuple_t : public process_tuple_t
{
public:

    void begin() {
        TRACE(TRACE_QUERY_RESULTS, "*** Q4 ANSWER ...\n");
        TRACE(TRACE_QUERY_RESULTS, "*** O_PRTY | COUNT...\n");
    }

    virtual void process(const tuple_t& output) {
        //TODO
    	q4_aggregate_tuple *tuple;
        tuple = aligned_cast<q4_aggregate_tuple>(output.data);
        TRACE(TRACE_QUERY_RESULTS, "*** %d-%d\n",
              tuple->O_ORDERPRIORITY,
              tuple->O_COUNT);
    }
};


/******************************************************************** 
 *
 * QPIPE Q4 - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_qpipe_q4(const int xct_id, 
                                  q4_input_t& in)
{
    TRACE( TRACE_ALWAYS, "********** Q4 *********\n");

    /*
     * Query 4 original:
     *
     * select o_orderpriority, count(*) as order_count
     * from orders
     * where o_order_data <in range> and exists
     *      (select * from lineitem
     *       where l_orderkey = o_orderkey and l_commitdate < l_receiptdate)
     * group by o_orderpriority
     * order by o_orderpriority
     *
     *
     * Query 4 modified to make the nested query cleaner:
     *
     * select o_orderpriority, count(*) as order_count
     * from orders natural join (
     *      select distinct l_orderkey
     *      from lineitem
     *      where l_commitdate < l_receiptdate)
     */
    
    policy_t* dp = this->get_sched_policy();
    xct_t* pxct = smthread_t::me()->xct();


    // TSCAN PACKET ON LINEITEM
    tuple_fifo* tscan_lineitem_out =
        new tuple_fifo(sizeof(q4_projected_lineitem_tuple));
    tscan_packet_t* q4_tscan_lineitem_packet =
        new tscan_packet_t("TSCAN LINEITEM",
                           tscan_lineitem_out,
                           new q4_tscan_lineitem_filter_t(this),
                           this->db(),
                           _plineitem_desc.get(),
                           pxct
                           //, SH
                           );

    //SORT LINEITEM
    tuple_filter_t* q4_linitem_sort_filter = new trivial_filter_t(sizeof(q4_projected_lineitem_tuple));
    tuple_fifo* buffer = new tuple_fifo(sizeof(q4_projected_lineitem_tuple));
    key_extractor_t* extractor = new q4_extractor_lineitem_t();
    key_compare_t* compare = new q4_compare_lineitem_t();
    packet_t* q4_sort_packet = new sort_packet_t("Q4_SORT LINEITEM",
                                              buffer,
                                              q4_linitem_sort_filter,
                                              extractor,
                                              compare,
                                              q4_tscan_lineitem_packet);


    // DISTINCT (IMPLEMENT WITH HASH AGGREGATE)
    tuple_fifo* q4_distinct_output = new tuple_fifo(sizeof(q4_projected_lineitem_tuple));
    partial_aggregate_packet_t* q4_distinct_packet =
        new partial_aggregate_packet_t(c_str("Q4DISTINCT_PACKET"),
							q4_distinct_output,
                               new trivial_filter_t(q4_distinct_output->tuple_size()),
                               q4_sort_packet,
                               new q4_distinct_t(),
                               new q4_distinct_t::q4_distinct_key_extractor_t(),
                               new int_key_compare_t());

    // TSCAN PACKET ON ORDERS
    tuple_fifo* tscan_orders_out =
        new tuple_fifo(sizeof(q4_projected_orders_tuple));
    tscan_packet_t* q4_tscan_orders_packet =
        new tscan_packet_t("TSCAN ORDERS",
						   tscan_orders_out,
                           new q4_tscan_orders_filter_t(this,in),
                           this->db(),
                           _porders_desc.get(),
                           pxct
                           //, SH
                           );

    // NANTURAL JOIN
    tuple_filter_t* filter = new trivial_filter_t(sizeof(q4_join_tuple));
    tuple_fifo* q4_join_out = new tuple_fifo(sizeof(q4_join_tuple));
    tuple_join_t* q4_join = new q4_join_t();
    packet_t* q4_join_packet = new hash_join_packet_t("Orders - Lineitem JOIN",
												   q4_join_out,
                                                   filter,
                                                   q4_tscan_orders_packet,
                                                   q4_distinct_packet,
                                                   q4_join);


    // sort/aggregate in one step
    tuple_filter_t* q4_agg_filter = new trivial_filter_t(sizeof(q4_aggregate_tuple));
    tuple_fifo* q4_agg_buffer = new tuple_fifo(sizeof(q4_aggregate_tuple));
    tuple_aggregate_t *q4_aggregate = new q4_count_aggregate_t();
    packet_t* q4_agg_packet;
    q4_agg_packet = new partial_aggregate_packet_t("O_ORDERPRIORITY COUNT",
                                                q4_agg_buffer,
                                                q4_agg_filter,
                                                q4_join_packet,
                                                q4_aggregate,
                                                new default_key_extractor_t(),
                                                new int_key_compare_t());



    qpipe::query_state_t* qs = dp->query_state_create();
    //q4_*****->assign_query_state(qs);
    q4_tscan_orders_packet->assign_query_state(qs);
    q4_tscan_lineitem_packet->assign_query_state(qs);
    q4_sort_packet->assign_query_state(qs);
    q4_distinct_packet->assign_query_state(qs);
    q4_join_packet->assign_query_state(qs);
    q4_agg_packet->assign_query_state(qs);

    // Dispatch packet
    tpch_q4_process_tuple_t pt;
    //LAST PACKET
    process_query(q4_agg_packet, pt);//TODO
    //process_query(q4_distinct_packet, pt);//TODO
    dp->query_state_destroy(qs);


    return (RCOK); 
}


EXIT_NAMESPACE(qpipe);
