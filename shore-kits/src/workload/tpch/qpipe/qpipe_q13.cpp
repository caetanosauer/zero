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

/** @file:   qpipe_q13.cpp
 *
 *  @brief:  Implementation of QPIPE TPCH Q13 over Shore-MT
 *
 *  @author: 
 *  @date:   
 */

#include "workload/tpch/shore_tpch_env.h"
#include "workload/tpch/tpch_struct.h"
#include "workload/tpch/tpch_util.h"
#include "qpipe.h"

using namespace shore;
using namespace qpipe;



ENTER_NAMESPACE(tpch);

/**
 * Original TPC-H Query 13:
 *
 * select c_count, count(*) as custdist
 * from (select c_custkey, count(o_orderkey)
 *       from customer
 *       left outer join orders on
 *       c_custkey = o_custkey
 *       and o_comment not like %[WORD1]%[WORD2]%
 *       group by c_custkey)
 *       as c_orders (c_custkey, c_count)
 * group by c_count
 * order by custdist desc, c_count desc;
 *
 */
/*
 * Implementation of Q13
 *
 * 1. select c_custkey from customer
 * 2. select o_custkey, count(*) as c_count
 *             from orders 
 *	       where o_comment not like %[WORD1]%[WORD2]% 
 *	       group by c_custkey
 *             order by c_custkey desc
 * 3. select c_count, count(*) as custdist
 *             from customer natural left outer join cust_order_count
 *             group by c_count
 *             order by custdist desc, c_count desc
*/

struct q13_customer_scan_tuple
{
  int C_CUSTKEY;
};

struct q13_orders_scan_tuple
{
  int O_CUSTKEY;
};

struct q13_key_count_tuple
{
    int KEY;   //O_CUSTKEY;  //C_COUNT;
    int COUNT; //C_COUNT;    //CUSTDIST;
};
typedef q13_key_count_tuple q13_cust_order_count_tuple;

struct q13_join_tuple
{
  int C_COUNT;
};

typedef q13_key_count_tuple q13_tuple;
/*struct q13_key_count_tuple
{
    int C_COUNT;
    int CUSTDIST;
    };
*/



//Q13 customer scan filter
class q13_customer_tscan_filter_t : public tuple_filter_t
{
private:
    ShoreTPCHEnv* _tpchdb;
    table_row_t* _prcustomer;
    rep_row_t _rr;

    /*One lineitem tuple*/
    tpch_customer_tuple _customer;
public:

    q13_customer_tscan_filter_t(ShoreTPCHEnv* tpchdb)
        : tuple_filter_t(tpchdb->customer_desc()->maxsize()), _tpchdb(tpchdb)
    {
    	// Get an orders tupple from the tuple cache and allocate space
        _prcustomer = _tpchdb->customer_man()->get_tuple();
        _rr.set_ts(_tpchdb->customer_man()->ts(),
                   _tpchdb->customer_desc()->maxsize());
        _prcustomer->_rep = &_rr;

    }

    ~q13_customer_tscan_filter_t()
    {
        // Give back the customer tuple
        _tpchdb->customer_man()->give_tuple(_prcustomer);
    }

    bool select(const tuple_t &input) {
        // Get next order
	/*Needed. Eventhough we do not apply any selection criteria we need this 
	 *statement to bring the data from the storage manager */
        if (!_tpchdb->customer_man()->load(_prcustomer, input.data)) {
            assert(false); // RC(se_WRONG_DISK_DATA)
        }

	return(true);
    }    

    // Projection
    void project(tuple_t &d, const tuple_t &s) {

        q13_customer_scan_tuple *dest;
        dest = aligned_cast<q13_customer_scan_tuple>(d.data);

        _prcustomer->get_value(0, _customer.C_CUSTKEY);


        //TRACE( TRACE_RECORD_FLOW, "%d\n",
	    //   _customer.C_CUSTKEY);

        dest->C_CUSTKEY = _customer.C_CUSTKEY;

    }

    q13_customer_tscan_filter_t* clone() const {
        return new q13_customer_tscan_filter_t(*this);
    }

    c_str to_string() const {
        c_str result("select C_CUSTKEY from CUSTOMER");
    	return result;
    }
};


//Q13 orders scan
class q13_orders_tscan_filter_t : public tuple_filter_t {
private:
    ShoreTPCHEnv* _tpchdb;
    table_row_t* _prorder;
    rep_row_t _rr;

    /*One lineitem tuple*/
    tpch_orders_tuple _orders;

    /* Random Predicates */
    /* TPC-H Specification 2.16.3 */
    q13_input_t* q13_input;
public:
      

    q13_orders_tscan_filter_t(ShoreTPCHEnv* tpchdb, q13_input_t &in)
        : tuple_filter_t(tpchdb->orders_desc()->maxsize()), _tpchdb(tpchdb)
    {
    	// Get an orders tupple from the tuple cache and allocate space
        _prorder = _tpchdb->orders_man()->get_tuple();
        _rr.set_ts(_tpchdb->orders_man()->ts(),
                   _tpchdb->orders_desc()->maxsize());
        _prorder->_rep = &_rr;

        // Generate the random predicates
	q13_input=&in;

	TRACE(TRACE_ALWAYS, "Random predicates:\nORDERS.O_COMMENT not like '%%%s%%%s%%'\n", q13_input->WORD1, q13_input->WORD2);
    }

    ~q13_orders_tscan_filter_t()
    {
        // Give back the orders tuple
        _tpchdb->orders_man()->give_tuple(_prorder);
    }


    virtual bool select(const tuple_t &input) {

        // Get next order 
        if (!_tpchdb->orders_man()->load(_prorder, input.data)) {
            assert(false); // RC(se_WRONG_DISK_DATA)
        }

        _prorder->get_value(8, _orders.O_COMMENT, 79);

	
        // search for all instances of the first substring. Make sure
        // the second search is *after* the first...
        char* first = strstr(_orders.O_COMMENT, q13_input->WORD1);
        if(!first)
            return true;

        char* second = strstr(first + strlen(q13_input->WORD1), q13_input->WORD2);
        if(!second)
            return true;

        // if we got here, match (and therefore reject)
        return false;
    }

    /* Projection */
    virtual void project(tuple_t &d, const tuple_t &s) {
        // project C_CUSTKEY
	q13_orders_scan_tuple *dest;
        dest = aligned_cast<q13_orders_scan_tuple>(d.data);

        _prorder->get_value(1, _orders.O_CUSTKEY);

        //TRACE( TRACE_RECORD_FLOW, "%d\n",
	    //   _orders.O_CUSTKEY);

        dest->O_CUSTKEY = _orders.O_CUSTKEY;
    }

    virtual q13_orders_tscan_filter_t* clone() const {
        return new q13_orders_tscan_filter_t(*this);
    }
    virtual c_str to_string() const {
        return c_str("select O_CUSTKEY "
                     "where O_COMMENT like %%%s%%%s%%",
                     q13_input->WORD1, q13_input->WORD2);
    }
};


// this comparator sorts its keys in descending order
struct int_desc_key_extractor_t : public key_extractor_t {
    virtual int extract_hint(const char* tuple_data) const {
        return -*aligned_cast<int>(tuple_data);
    }
    virtual int_desc_key_extractor_t* clone() const {
        return new int_desc_key_extractor_t(*this);
    }
};

struct q13_count_aggregate_t : public tuple_aggregate_t {
    default_key_extractor_t _extractor;
    
    q13_count_aggregate_t()
        : tuple_aggregate_t(sizeof(q13_cust_order_count_tuple))
    {
    }
    virtual key_extractor_t* key_extractor() { return &_extractor; }
    
    virtual void aggregate(char* agg_data, const tuple_t &) {
        q13_cust_order_count_tuple* agg = aligned_cast<q13_cust_order_count_tuple>(agg_data);
        agg->COUNT++;
    }

    virtual void finish(tuple_t &d, const char* agg_data) {
        memcpy(d.data, agg_data, tuple_size());
    }
    virtual q13_count_aggregate_t* clone() const {
        return new q13_count_aggregate_t(*this);
    }
    virtual c_str to_string() const {
        return "q13_count_aggregate_t";
    }
};

//Q13 Join Packet
struct q13_join_t : public tuple_join_t {

        q13_join_t()
            : tuple_join_t(sizeof(q13_customer_scan_tuple),
                           0,
                           sizeof(q13_cust_order_count_tuple),
                           offsetof(q13_cust_order_count_tuple, KEY),
                           sizeof(int),
                           sizeof(int))
        {
        }

        virtual void join(tuple_t &dest,
                          const tuple_t &,
                          const tuple_t &right)
        {
            // KLUDGE: this projection should go in a separate filter class
            q13_cust_order_count_tuple* tuple = aligned_cast<q13_cust_order_count_tuple>(right.data);
	    //q13_join_tuple
	    *aligned_cast<int>(dest.data) = tuple->COUNT;
        }

        virtual void left_outer_join(tuple_t &dest, const tuple_t &) {
	    //q13_join_tuple
	    *aligned_cast<int>(dest.data) = 0;
        }

        virtual c_str to_string() const {
            return "CUSTOMER left outer join CUST_ORDER_COUNT, select COUNT";
	}
};

//Sorting
struct q13_key_extract_t : public key_extractor_t 
{
        q13_key_extract_t() : key_extractor_t(sizeof(q13_tuple)) { }
            
        virtual int extract_hint(const char* tuple_data) const {
            q13_tuple* tuple = aligned_cast<q13_tuple>(tuple_data);
            // confusing -- custdist is a count of counts... and
            // descending sort
            return -tuple->COUNT;
        }
        virtual q13_key_extract_t* clone() const {
            return new q13_key_extract_t(*this);
        }
};

struct q13_key_compare_t : public key_compare_t 
{
     virtual int operator()(const void* key1, const void* key2) const {
            // at this point we know the custdist (count) fields are
            // different, so just check the c_count (key) fields
            q13_tuple* a = aligned_cast<q13_tuple>(key1);
            q13_tuple* b = aligned_cast<q13_tuple>(key2);
            return b->KEY - a->KEY;
        }
        virtual q13_key_compare_t* clone() const {
            return new q13_key_compare_t(*this);
        }
};



class tpch_q13_process_tuple_t : public process_tuple_t {
    
public:
     
    virtual void begin() {
        TRACE(TRACE_QUERY_RESULTS, "*** Q13 %10s %10s\n",
              "C_COUNT", "CUSTDIST");
    }
    
    virtual void process(const tuple_t& output) {
        q13_tuple* r = aligned_cast<q13_tuple>(output.data);

	        TRACE(TRACE_QUERY_RESULTS, "*** Q13 %10d %10d\n",
	    r->KEY,
	    r->COUNT);
   }

};



/******************************************************************** 
 *
 * QPIPE Q13 - Packet creation and submission
 *               
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_qpipe_q13(const int xct_id, 
                                  q13_input_t& in)
{
    TRACE( TRACE_ALWAYS, "********** Q13 *********\n");

    policy_t* dp = this->get_sched_policy();
    xct_t* pxct = smthread_t::me()->xct();

    //TSCAN customer
    tuple_fifo* q13_customer_buffer = new tuple_fifo(sizeof(q13_customer_scan_tuple));
    packet_t* q13_customer_tscan_packet =
        new tscan_packet_t("customer TSCAN",
                           q13_customer_buffer,
                           new q13_customer_tscan_filter_t(this),
       			   this->db(),
			   _pcustomer_desc.get(),
			   pxct
			   //, SH 
                           );

    //TSCAN orders
    tuple_fifo* q13_orders_buffer = new tuple_fifo(sizeof(q13_orders_scan_tuple));
    packet_t* q13_orders_tscan_packet =
        new tscan_packet_t("orders TSCAN",
                           q13_orders_buffer,
                           new q13_orders_tscan_filter_t(this,in),
			   this->db(),
			   _porders_desc.get(),
			   pxct
			   //, SH 
                           );

    //Group by orders
    tuple_fifo* q13_orders_groupby_buffer = new tuple_fifo(sizeof(q13_cust_order_count_tuple));
    packet_t* q13_orders_groupby_packet = new partial_aggregate_packet_t("Orders Group By",
                                                q13_orders_groupby_buffer,
						new trivial_filter_t(sizeof(q13_cust_order_count_tuple)),
                                                q13_orders_tscan_packet,
                                                new q13_count_aggregate_t(),
                                                new int_desc_key_extractor_t(),
                                                new int_key_compare_t()
					        );

    //Join
    tuple_fifo* q13_join_buffer = new tuple_fifo(sizeof(q13_join_tuple));
    packet_t* q13_join_packet = new hash_join_packet_t("Orders - Customer JOIN",
                                                   q13_join_buffer,
                                                   new trivial_filter_t(sizeof(q13_join_tuple)),
                                                   q13_customer_tscan_packet,
                                                   q13_orders_groupby_packet,
                                                   new q13_join_t(),
                                                   true);


    // group by c_count
    tuple_fifo* q13_groupby_c_count_buffer = new tuple_fifo(sizeof(q13_tuple));
    packet_t *q13_groupby_c_count_packet = new partial_aggregate_packet_t("c_count SORT",
                                                 q13_groupby_c_count_buffer,
                                                 new trivial_filter_t(sizeof(q13_tuple)),
                                                 q13_join_packet,
                                                 new q13_count_aggregate_t(),
                                                 new int_desc_key_extractor_t(),
                                                 new int_key_compare_t());

    // final sort of results
    tuple_fifo* q13_sort_buffer = new tuple_fifo(sizeof(q13_tuple));
    packet_t *q13_sort_packet = new sort_packet_t("custdist, c_count SORT",
                                    q13_sort_buffer,
                                    new trivial_filter_t(sizeof(q13_tuple)),
                                    new q13_key_extract_t(),
                                    new q13_key_compare_t(),
                                    q13_groupby_c_count_packet);

									
    qpipe::query_state_t* qs = dp->query_state_create();
    //q13_*****->assign_query_state(qs);
    q13_orders_tscan_packet->assign_query_state(qs);
    q13_customer_tscan_packet->assign_query_state(qs);
    q13_orders_groupby_packet->assign_query_state(qs);
    q13_join_packet->assign_query_state(qs);
    q13_groupby_c_count_packet->assign_query_state(qs);
    q13_sort_packet->assign_query_state(qs);

    // Dispatch packet
    tpch_q13_process_tuple_t pt;
    //LAST PACKET
    process_query(q13_sort_packet, pt);
    dp->query_state_destroy(qs);



    return (RCOK); 
}


EXIT_NAMESPACE(tpch);
