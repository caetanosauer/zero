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

/** @file:   qpipe_4_1.cpp
 *
 *  @brief:  Implementation of QPIPE SSB Q4_1 over Shore-MT
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
 * QPIPE Q4_1 - Structures needed by operators 
 *
 ********************************************************************/
/*
select d_year, c_nation,
sum(lo_revenue - lo_supplycost) as profit
from date, customer, supplier, part, lineorder
where lo_custkey = c_customerkey
and lo_suppkey = s_suppkey
and lo_partkey = p_partkey
and lo_orderdatekey = d_datekey
and c_region = 'AMERICA'
and s_region = 'AMERICA'
and (p_mfgr = 'MFGR#1'
or p_mfgr = 'MFGR#2')
group by d_year, c_nation
order by d_year, c_nation;
*/

// the tuples after tablescan projection
struct q41_lo_tuple
{
  int LO_CUSTKEY;
  int LO_SUPPKEY;
  int LO_ORDERDATE;
  int LO_PARTKEY;
  int LO_REVENUE;
  int LO_SUPPLYCOST;
};

struct q41_c_tuple
{
  int C_CUSTKEY;
  char C_NATION[16];
};

struct q41_s_tuple
{
  int S_SUPPKEY;
};

struct q41_p_tuple
{
  int P_PARTKEY;
};

struct q41_d_tuple
{ 
  int D_DATEKEY;
  int D_YEAR;
};

struct q41_join_s_tuple
{
  int LO_CUSTKEY;
  int LO_ORDERDATE;
  int LO_PARTKEY;
  int LO_REVENUE;
  int LO_SUPPLYCOST;
};

struct q41_join_s_c_tuple
{
  char C_NATION[16]; 
  int LO_ORDERDATE;
  int LO_PARTKEY;
  int LO_REVENUE;
  int LO_SUPPLYCOST;
};


struct q41_join_s_c_p_tuple
{
  char C_NATION[16]; 
  int LO_ORDERDATE;
  int LO_REVENUE;
  int LO_SUPPLYCOST;
};

struct q41_join_tuple
{
  char C_NATION[16]; 
  int D_YEAR;
  int LO_REVENUE;
  int LO_SUPPLYCOST;
};

struct q41_aggregate_tuple
{
  char C_NATION[16]; 
  int D_YEAR;
  int PROFIT;
};

/*struct projected_tuple
{
  int KEY;
  };*/

typedef struct q41_aggregate_tuple projected_tuple;


class q41_lineorder_tscan_filter_t : public tuple_filter_t 
{
private:
    ShoreSSBEnv* _ssbdb;
    table_row_t* _prline;
    rep_row_t _rr;

    ssb_lineorder_tuple _lineorder;

public:

    q41_lineorder_tscan_filter_t(ShoreSSBEnv* ssbdb)//,q4_1_input_t &in) 
        : tuple_filter_t(ssbdb->lineorder_desc()->maxsize()), _ssbdb(ssbdb)
    {

    	// Get a lineorder tupple from the tuple cache and allocate space
        _prline = _ssbdb->lineorder_man()->get_tuple();
        _rr.set_ts(_ssbdb->lineorder_man()->ts(),
                   _ssbdb->lineorder_desc()->maxsize());
        _prline->_rep = &_rr;

    }

    ~q41_lineorder_tscan_filter_t()
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

        return (true);
    }

    
    // Projection
    void project(tuple_t &d, const tuple_t &s) {        

        q41_lo_tuple *dest;
        dest = aligned_cast<q41_lo_tuple>(d.data);

        _prline->get_value(2, _lineorder.LO_CUSTKEY);
        _prline->get_value(3, _lineorder.LO_PARTKEY);
        _prline->get_value(4, _lineorder.LO_SUPPKEY);
        _prline->get_value(5, _lineorder.LO_ORDERDATE);
        _prline->get_value(12, _lineorder.LO_REVENUE);
        _prline->get_value(13, _lineorder.LO_SUPPLYCOST);

        TRACE( TRACE_RECORD_FLOW, "%d|%d|%d|%d|%d|%d --d\n",
               _lineorder.LO_CUSTKEY,
               _lineorder.LO_PARTKEY,
               _lineorder.LO_SUPPKEY,
               _lineorder.LO_ORDERDATE,
               _lineorder.LO_REVENUE,
               _lineorder.LO_SUPPLYCOST);

        dest->LO_CUSTKEY = _lineorder.LO_CUSTKEY;
        dest->LO_PARTKEY = _lineorder.LO_PARTKEY;
        dest->LO_SUPPKEY = _lineorder.LO_SUPPKEY;
        dest->LO_ORDERDATE = _lineorder.LO_ORDERDATE;
        dest->LO_REVENUE = _lineorder.LO_REVENUE;
        dest->LO_SUPPLYCOST = _lineorder.LO_SUPPLYCOST;

    }

    q41_lineorder_tscan_filter_t* clone() const {
        return new q41_lineorder_tscan_filter_t(*this);
    }

    c_str to_string() const {
        return c_str("q41_lineorder_tscan_filter_t()");
    }
};


class q41_supplier_tscan_filter_t : public tuple_filter_t 
{
private:
    ShoreSSBEnv* _ssbdb;
    table_row_t* _prsupp;
    rep_row_t _rr;

    ssb_supplier_tuple _supplier;

  /*VARIABLES TAKING VALUES FROM INPUT FOR SELECTION*/
    char REGION[13];
public:

    q41_supplier_tscan_filter_t(ShoreSSBEnv* ssbdb, q4_1_input_t &in) 
        : tuple_filter_t(ssbdb->supplier_desc()->maxsize()), _ssbdb(ssbdb)
    {

    	// Get a supplier tupple from the tuple cache and allocate space
        _prsupp = _ssbdb->supplier_man()->get_tuple();
        _rr.set_ts(_ssbdb->supplier_man()->ts(),
                   _ssbdb->supplier_desc()->maxsize());
        _prsupp->_rep = &_rr;

	
	strcpy(REGION,"AMERICA");
    }

    ~q41_supplier_tscan_filter_t()
    {
        // Give back the supplier tuple 
        _ssbdb->supplier_man()->give_tuple(_prsupp);
    }


    // Predication
    bool select(const tuple_t &input) {

        // Get next supplier and read its shipdate
        if (!_ssbdb->supplier_man()->load(_prsupp, input.data)) {
            assert(false); // RC(se_WRONG_DISK_DATA)
        }

        _prsupp->get_value(5, _supplier.S_REGION, STRSIZE(12));


	if (strcmp(_supplier.S_REGION,REGION)==0)
	    {
		TRACE( TRACE_RECORD_FLOW, "+ REGION |%s --d\n",
		       _supplier.S_REGION);
		return (true);
	    }
	else
	    {
		TRACE( TRACE_RECORD_FLOW, ". REGION |%s --d\n",
		       _supplier.S_REGION);
		return (false);
	    }
    }

    
    // Projection
    void project(tuple_t &d, const tuple_t &s) {        

        q41_s_tuple *dest;
        dest = aligned_cast<q41_s_tuple>(d.data);

        _prsupp->get_value(0, _supplier.S_SUPPKEY);

        TRACE( TRACE_RECORD_FLOW, "%d --d\n",
               _supplier.S_SUPPKEY);


        dest->S_SUPPKEY = _supplier.S_SUPPKEY;
    }

    q41_supplier_tscan_filter_t* clone() const {
        return new q41_supplier_tscan_filter_t(*this);
    }

    c_str to_string() const {
        return c_str("q41_supplier_tscan_filter_t()");
    }
};


class q41_customer_tscan_filter_t : public tuple_filter_t 
{
private:
    ShoreSSBEnv* _ssbdb;
    table_row_t* _prcust;
    rep_row_t _rr;

    ssb_customer_tuple _customer;

  /*VARIABLES TAKING VALUES FROM INPUT FOR SELECTION*/
    char REGION[13];
public:

    q41_customer_tscan_filter_t(ShoreSSBEnv* ssbdb, q4_1_input_t &in) 
        : tuple_filter_t(ssbdb->customer_desc()->maxsize()), _ssbdb(ssbdb)
    {

    	// Get a customer tupple from the tuple cache and allocate space
        _prcust = _ssbdb->customer_man()->get_tuple();
        _rr.set_ts(_ssbdb->customer_man()->ts(),
                   _ssbdb->customer_desc()->maxsize());
        _prcust->_rep = &_rr;

	
	strcpy(REGION,"AMERICA");
    }

    ~q41_customer_tscan_filter_t()
    {
        // Give back the customer tuple 
        _ssbdb->customer_man()->give_tuple(_prcust);
    }


    // Predication
    bool select(const tuple_t &input) {

        // Get next customer and read its shipdate
        if (!_ssbdb->customer_man()->load(_prcust, input.data)) {
            assert(false); // RC(se_WRONG_DISK_DATA)
        }

        _prcust->get_value(5, _customer.C_REGION, STRSIZE(12));

	if (strcmp(_customer.C_REGION,REGION)==0)
	    {
		TRACE( TRACE_RECORD_FLOW, "+ REGION |%s --d\n",
		       _customer.C_REGION);
		return (true);
	    }
	else
	    {
		TRACE( TRACE_RECORD_FLOW, ". REGION |%s --d\n",
		       _customer.C_REGION);
		return (false);
	    }
    }

    
    // Projection
    void project(tuple_t &d, const tuple_t &s) {        

        q41_c_tuple *dest;
        dest = aligned_cast<q41_c_tuple>(d.data);

        _prcust->get_value(0, _customer.C_CUSTKEY);
        _prcust->get_value(4, _customer.C_NATION, STRSIZE(15));

        TRACE( TRACE_RECORD_FLOW, "%d|%s --d\n",
               _customer.C_CUSTKEY,
               _customer.C_NATION);


        dest->C_CUSTKEY = _customer.C_CUSTKEY;
        strcpy(dest->C_NATION,_customer.C_NATION);
    }

    q41_customer_tscan_filter_t* clone() const {
        return new q41_customer_tscan_filter_t(*this);
    }

    c_str to_string() const {
        return c_str("q41_customer_tscan_filter_t()");
    }
};


class q41_part_tscan_filter_t : public tuple_filter_t 
{
private:
    ShoreSSBEnv* _ssbdb;
    table_row_t* _prpart;
    rep_row_t _rr;

    ssb_part_tuple _part;

  /*VARIABLES TAKING VALUES FROM INPUT FOR SELECTION*/
    char MFGR_1[7];
    char MFGR_2[7];

public:

    q41_part_tscan_filter_t(ShoreSSBEnv* ssbdb, q4_1_input_t &in) 
        : tuple_filter_t(ssbdb->part_desc()->maxsize()), _ssbdb(ssbdb)
    {

    	// Get a part tupple from the tuple cache and allocate space
        _prpart = _ssbdb->part_man()->get_tuple();
        _rr.set_ts(_ssbdb->part_man()->ts(),
                   _ssbdb->part_desc()->maxsize());
        _prpart->_rep = &_rr;

	strcpy(MFGR_1,"MFGR#1");
        strcpy(MFGR_2,"MFGR#2");
    }

    ~q41_part_tscan_filter_t()
    {
        // Give back the part tuple 
        _ssbdb->part_man()->give_tuple(_prpart);
    }


    // Predication
    bool select(const tuple_t &input) {

        // Get next part and read its shipdate
        if (!_ssbdb->part_man()->load(_prpart, input.data)) {
            assert(false); // RC(se_WRONG_DISK_DATA)
        }

        _prpart->get_value(2, _part.P_MFGR, STRSIZE(6));

	
	if (strcmp(_part.P_MFGR, MFGR_1)==0 || strcmp(_part.P_MFGR, MFGR_2)==0)
	    {
		TRACE( TRACE_RECORD_FLOW, "+ MFGR |%s --d\n",
		       _part.P_MFGR);
		return (true);
	    }
	else
	    {
		TRACE( TRACE_RECORD_FLOW, ". MFGR |%s --d\n",
		       _part.P_MFGR);
		return (false);
	    }

    }

    
    // Projection
    void project(tuple_t &d, const tuple_t &s) {        

        q41_p_tuple *dest;
        dest = aligned_cast<q41_p_tuple>(d.data);

        _prpart->get_value(0, _part.P_PARTKEY);

        TRACE( TRACE_RECORD_FLOW, "%d --d\n",
               _part.P_PARTKEY);


        dest->P_PARTKEY = _part.P_PARTKEY;
    }

    q41_part_tscan_filter_t* clone() const {
        return new q41_part_tscan_filter_t(*this);
    }

    c_str to_string() const {
        return c_str("q4_1_part_tscan_filter_t()");
    }
};


class q41_date_tscan_filter_t : public tuple_filter_t 
{
private:
    ShoreSSBEnv* _ssbdb;
    table_row_t* _prdate;
    rep_row_t _rr;

    ssb_date_tuple _date;


public:

    q41_date_tscan_filter_t(ShoreSSBEnv* ssbdb, q4_1_input_t &in) 
        : tuple_filter_t(ssbdb->date_desc()->maxsize()), _ssbdb(ssbdb)
    {

    	// Get a date tupple from the tuple cache and allocate space
        _prdate = _ssbdb->date_man()->get_tuple();
        _rr.set_ts(_ssbdb->date_man()->ts(),
                   _ssbdb->date_desc()->maxsize());
        _prdate->_rep = &_rr;

    }

    ~q41_date_tscan_filter_t()
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
        
	return (true);

    }

    
    // Projection
    void project(tuple_t &d, const tuple_t &s) {        

        q41_d_tuple *dest;
        dest = aligned_cast<q41_d_tuple>(d.data);

        _prdate->get_value(0, _date.D_DATEKEY);
        _prdate->get_value(4, _date.D_YEAR);

        TRACE( TRACE_RECORD_FLOW, "%d|%d --d\n",
               _date.D_DATEKEY,
               _date.D_YEAR);


        dest->D_DATEKEY = _date.D_DATEKEY;
        dest->D_YEAR=_date.D_YEAR;
    }

    q41_date_tscan_filter_t* clone() const {
        return new q41_date_tscan_filter_t(*this);
    }

    c_str to_string() const {
        return c_str("q4_1_date_tscan_filter_t()");
    }
};


//Natural join
// left is lineorder, right is supplier
struct q41_lo_s_join_t : public tuple_join_t {


    q41_lo_s_join_t ()
        : tuple_join_t(sizeof(q41_lo_tuple),
                       offsetof(q41_lo_tuple, LO_SUPPKEY),
                       sizeof(q41_s_tuple),
                       offsetof(q41_s_tuple, S_SUPPKEY),
                       sizeof(int),
                       sizeof(q41_join_s_tuple))
    {
    }


    virtual void join(tuple_t &dest,
                      const tuple_t &left,
                      const tuple_t &right)
    {
        // KLUDGE: this projection should go in a separate filter class
    	q41_lo_tuple* lo = aligned_cast<q41_lo_tuple>(left.data);
    	q41_s_tuple* s = aligned_cast<q41_s_tuple>(right.data);
	q41_join_s_tuple* ret = aligned_cast<q41_join_s_tuple>(dest.data);
	
	ret->LO_CUSTKEY = lo->LO_CUSTKEY;
        ret->LO_PARTKEY = lo->LO_PARTKEY;
        ret->LO_ORDERDATE = lo->LO_ORDERDATE;
        ret->LO_REVENUE = lo->LO_REVENUE;
        ret->LO_SUPPLYCOST = lo->LO_SUPPLYCOST;


        TRACE ( TRACE_RECORD_FLOW, "JOIN %d %d %d %d %d\n",ret->LO_CUSTKEY, ret->LO_PARTKEY, ret->LO_ORDERDATE, ret->LO_REVENUE, ret->LO_SUPPLYCOST);

    }

    virtual q41_lo_s_join_t*  clone() const {
        return new q41_lo_s_join_t(*this);
    }

    virtual c_str to_string() const {
        return "join LINEORDER and SUPPLIER, select LO_CUSTKEY, LO_PARTKEY, LO_ORDERDATE, LO_REVENUE, LO_SUPPLYCOST";
    }
};

//Natural join
// left is lineorder, supplier right is customer
struct q41_lo_s_c_join_t : public tuple_join_t {


    q41_lo_s_c_join_t ()
        : tuple_join_t(sizeof(q41_join_s_tuple),
                       offsetof(q41_join_s_tuple, LO_CUSTKEY),
                       sizeof(q41_c_tuple),
                       offsetof(q41_c_tuple, C_CUSTKEY),
                       sizeof(int),
                       sizeof(q41_join_s_c_tuple))
    {
    }


    virtual void join(tuple_t &dest,
                      const tuple_t &left,
                      const tuple_t &right)
    {
        // KLUDGE: this projection should go in a separate filter class
    	q41_join_s_tuple* jo = aligned_cast<q41_join_s_tuple>(left.data);
    	q41_c_tuple* c = aligned_cast<q41_c_tuple>(right.data);
	q41_join_s_c_tuple* ret = aligned_cast<q41_join_s_c_tuple>(dest.data);
	
	ret->LO_PARTKEY = jo->LO_PARTKEY ;
        ret->LO_ORDERDATE = jo->LO_ORDERDATE ;
        ret->LO_REVENUE = jo->LO_REVENUE ;
        ret->LO_SUPPLYCOST = jo->LO_SUPPLYCOST ;
	
	strcpy(ret->C_NATION,c->C_NATION);
	
        TRACE ( TRACE_RECORD_FLOW, "JOIN {%s} %d %d %d %d\n",ret->C_NATION, ret->LO_PARTKEY, ret->LO_ORDERDATE, ret->LO_REVENUE, ret->LO_SUPPLYCOST);

    }

    virtual q41_lo_s_c_join_t* clone() const {
        return new q41_lo_s_c_join_t(*this);
    }

    virtual c_str to_string() const {
        return "join LINEORDER and SUPPLIER and CUSTOMER, select C_NATION, LO_PARTKEY, LO_ORDERDATE, LO_REVENUE, LO_SUPPLYCOST";
    }
};

//Natural join
// left is lineorder, supplier, customer, right is part
struct q41_lo_s_c_p_join_t : public tuple_join_t {


    q41_lo_s_c_p_join_t ()
        : tuple_join_t(sizeof(q41_join_s_c_tuple),
                       offsetof(q41_join_s_c_tuple, LO_PARTKEY),
                       sizeof(q41_p_tuple),
                       offsetof(q41_p_tuple, P_PARTKEY),
                       sizeof(int),
                       sizeof(q41_join_s_c_p_tuple))
    {
    }


    virtual void join(tuple_t &dest,
                      const tuple_t &left,
                      const tuple_t &right)
    {
        // KLUDGE: this projection should go in a separate filter class
    	q41_join_s_c_tuple* jo = aligned_cast<q41_join_s_c_tuple>(left.data);
    	q41_p_tuple* p = aligned_cast<q41_p_tuple>(right.data);
	q41_join_s_c_p_tuple* ret = aligned_cast<q41_join_s_c_p_tuple>(dest.data);
	
	ret->LO_ORDERDATE = jo->LO_ORDERDATE ;
        ret->LO_REVENUE = jo->LO_REVENUE ;
        ret->LO_SUPPLYCOST = jo->LO_SUPPLYCOST ;
        
	
	strcpy(ret->C_NATION,jo->C_NATION);
	
        TRACE ( TRACE_RECORD_FLOW, "JOIN {%s} %d %d %d\n",ret->C_NATION, ret->LO_ORDERDATE, ret->LO_REVENUE, ret->LO_SUPPLYCOST);

    }

    virtual q41_lo_s_c_p_join_t* clone() const {
        return new q41_lo_s_c_p_join_t(*this);
    }

    virtual c_str to_string() const {
        return "join LINEORDER and SUPPLIER and CUSTOMER and PART, select C_NATION, LO_ORDERDATE, LO_REVENUE, LO_SUPPLYCOST";
    }
};

//Natural join
// left is lineorder, supplier, customer, part right is date
struct q41_join_t : public tuple_join_t {


    q41_join_t ()
        : tuple_join_t(sizeof(q41_join_s_c_p_tuple),
                       offsetof(q41_join_s_c_p_tuple, LO_ORDERDATE),
                       sizeof(q41_d_tuple),
                       offsetof(q41_d_tuple, D_DATEKEY),
                       sizeof(int),
                       sizeof(q41_join_tuple))
    {
    }


    virtual void join(tuple_t &dest,
                      const tuple_t &left,
                      const tuple_t &right)
    {
        // KLUDGE: this projection should go in a separate filter class
    	q41_join_s_c_p_tuple* jo = aligned_cast<q41_join_s_c_p_tuple>(left.data);
    	q41_d_tuple* d = aligned_cast<q41_d_tuple>(right.data);
	q41_join_tuple* ret = aligned_cast<q41_join_tuple>(dest.data);
	
	strcpy(ret->C_NATION,jo->C_NATION);
        ret->D_YEAR = d->D_YEAR;
	ret->LO_REVENUE = jo->LO_REVENUE;
	ret->LO_SUPPLYCOST = jo->LO_SUPPLYCOST;


        TRACE ( TRACE_RECORD_FLOW, "JOIN {%s} %d %d %d\n",ret->C_NATION, ret->D_YEAR, ret->LO_REVENUE, ret->LO_SUPPLYCOST);

    }

    virtual q41_join_t* clone() const {
        return new q41_join_t(*this);
    }

    virtual c_str to_string() const {
        return "join LINEORDER and SUPPLIER and CUSTOMER and PART and DATE, select C_NATION, D_YEAR, LO_REVENUE, LO_SUPPLYCOST";
    }
};


// Key extractor and Comparator for Aggregation filter

struct q41_agg_input_tuple_key {
    
    char C_NATION[STRSIZE(15)];
    int D_YEAR;
    
    
    int extract_hint() {
        return (this->D_YEAR - 1990<< 24) + (this->C_NATION[0]  << 16) + (this->C_NATION[1] << 8) + (this->C_NATION[2]);
    }
};

struct q41_agg_input_tuple_key_extractor_t : public key_extractor_t {

    q41_agg_input_tuple_key_extractor_t() : key_extractor_t(sizeof(q41_agg_input_tuple_key), offsetof(q41_join_tuple, C_NATION)) {
    }

    virtual int extract_hint(const char* key) const {
        q41_agg_input_tuple_key* aligned_key = aligned_cast<q41_agg_input_tuple_key>(key);

        // We assume a 4-byte integer and fill it with: The three first 
        // characters of C_NATION, and the
        // last digit of year (according to the specification, years
        // can be between 1992-1998). According to this hint,
        // the ordering can be made faster and only in ties, will
        // the key_compare_t be used to extract the full comparison.
        // Keep in mind that by using such a hint, performance is gained,
        // as we don't have to compare full strings if the first
        // characters are different, but we don't gain full ordering
        // if the cities start with the same letters: The resulting groups will be sorted primarily by year, because
        // the hint will only differ in the year. Groups are not affected, as on ties the full comparison is used.
        // If ordering is wanted, you'll need to use a hint
        // that is equal for everyone (like a zero), so that the full
        // comparison is used for everyone. In Q3.2, we don't need
        // full ordering in the aggregator, as in the end
        // the tuples are again ordered differently.
//        int result = (aligned_key->C_NATION[0] << 24) + (aligned_key->C_NATION[1] << 16) + (aligned_key->C_NATION[2] << 8) + (aligned_key->D_YEAR - 1990);
//        return result;
        return aligned_key->extract_hint();
        
        //return 0;
    }

    virtual q41_agg_input_tuple_key_extractor_t * clone() const {
        return new q41_agg_input_tuple_key_extractor_t(*this);
    }
};

struct q41_agg_input_tuple_key_compare_t : public key_compare_t {

    virtual int operator()(const void* key1, const void* key2) const {
        q41_agg_input_tuple_key* a = aligned_cast<q41_agg_input_tuple_key>(key1);
        q41_agg_input_tuple_key* b = aligned_cast<q41_agg_input_tuple_key>(key2);

        int cnationcomparison = strcmp(a->C_NATION, b->C_NATION);
        if (cnationcomparison != 0) return cnationcomparison;
        return a->D_YEAR - b->D_YEAR;
    }

    virtual q41_agg_input_tuple_key_compare_t * clone() const {
        return new q41_agg_input_tuple_key_compare_t(*this);
    }
};

// Aggregate's tuple's key is the same as the input tuple's key.
typedef struct q41_agg_input_tuple_key q41_agg_tuple_key;

class q41_agg_aggregate_t : public tuple_aggregate_t {
private:

    struct q41_agg_output_tuple_key_extractor_t : public key_extractor_t {

        q41_agg_output_tuple_key_extractor_t() : key_extractor_t(sizeof (q41_agg_tuple_key), offsetof(q41_aggregate_tuple, C_NATION)) {
        }

        virtual int extract_hint(const char* key) const {
            q41_agg_tuple_key* aligned_key = aligned_cast<q41_agg_tuple_key > (key);
            return aligned_key->extract_hint();
        }

        virtual q41_agg_output_tuple_key_extractor_t * clone() const {
            return new q41_agg_output_tuple_key_extractor_t(*this);
        }
    };
    
    q41_agg_output_tuple_key_extractor_t _extractor;

public:

    q41_agg_aggregate_t()
    : tuple_aggregate_t(sizeof(q41_aggregate_tuple)) {
    }

    key_extractor_t* key_extractor() {
        return &_extractor;
    }

    void aggregate(char* agg_data, const tuple_t &s) {
        q41_join_tuple *src;
        src = aligned_cast<q41_join_tuple> (s.data);
        q41_aggregate_tuple* tuple = aligned_cast<q41_aggregate_tuple>(agg_data);

        tuple->PROFIT += (src->LO_REVENUE - src->LO_SUPPLYCOST);
        TRACE(TRACE_RECORD_FLOW, "%.2f\n", tuple->PROFIT);
    }

    void finish(tuple_t &d, const char* agg_data) {
        q41_aggregate_tuple *dest;
        dest = aligned_cast<q41_aggregate_tuple > (d.data);
        q41_aggregate_tuple* tuple = aligned_cast<q41_aggregate_tuple > (agg_data);

        *dest = *tuple;
    }

    q41_agg_aggregate_t* clone() const {
        return new q41_agg_aggregate_t(*this);
    }

    c_str to_string() const {
        return "q41_agg_aggregate_t";
    }
};



// Final Ordering

struct q41_sort_tuple_key {
    char C_NATION[STRSIZE(15)];
    int D_YEAR;

};

struct q41_order_key_extractor_t : public key_extractor_t {

    q41_order_key_extractor_t() : key_extractor_t(sizeof (q41_sort_tuple_key), offsetof(q41_aggregate_tuple, C_NATION)) {
    }

    virtual int extract_hint(const char* key) const {
        q41_sort_tuple_key* aligned_key = aligned_cast<q41_sort_tuple_key> (key);

        // We assume a 4-byte integer and fill it with: The last digit of year
        // in the 3 most significant bits. The remaining bits are filled with the
        // integer representation of Revenue. But they are subtracted
        // in order to keep a decreasing order.

        int result = ((aligned_key->D_YEAR - 1992) << 28) + (aligned_key->C_NATION[0] << 16)+(aligned_key->C_NATION[1] << 8)+(aligned_key->C_NATION[2]);
        return result;

        //return 0;
    }

    virtual q41_order_key_extractor_t * clone() const {
        return new q41_order_key_extractor_t(*this);
    }
};

struct q41_order_key_compare_t : public key_compare_t {

    virtual int operator()(const void* key1, const void* key2) const {
        q41_sort_tuple_key* a = aligned_cast<q41_sort_tuple_key>(key1);
        q41_sort_tuple_key* b = aligned_cast<q41_sort_tuple_key>(key2);

        int yearcomparison = a->D_YEAR - b->D_YEAR;
        if (yearcomparison != 0) return yearcomparison;
        int cnationcomparison = strcmp(a->C_NATION, b->C_NATION);
            return  cnationcomparison;
    }

    virtual q41_order_key_compare_t * clone() const {
        return new q41_order_key_compare_t(*this);
    }
};





class ssb_q41_process_tuple_t : public process_tuple_t 
{    
public:
        
    void begin() {
        TRACE(TRACE_QUERY_RESULTS, "*** q4_1 ANSWER ...\n");
        TRACE(TRACE_QUERY_RESULTS, "*** ...\n");
    }
    
    virtual void process(const tuple_t& output) {
        projected_tuple *tuple;
        tuple = aligned_cast<projected_tuple>(output.data);

        TRACE ( TRACE_QUERY_RESULTS, "PROCESS %d {%s} %d\n",tuple->D_YEAR, tuple->C_NATION, tuple->PROFIT);
        
        /*TRACE(TRACE_QUERY_RESULTS, "%d --\n",
	  tuple->KEY);*/
    }
};



/******************************************************************** 
 *
 * QPIPE q4_1 - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_qpipe_q4_1(const int xct_id, 
                                  q4_1_input_t& in)
{
    TRACE( TRACE_ALWAYS, "********** q4_1 *********\n");

   
    policy_t* dp = this->get_sched_policy();
    xct_t* pxct = smthread_t::me()->xct();
    

    // TSCAN PACKET
        //LINEORDER
        tuple_fifo* lo_out_buffer = new tuple_fifo(sizeof(q41_lo_tuple));
        tscan_packet_t* lo_tscan_packet =
        new tscan_packet_t("TSCAN LINEORDER",
                           lo_out_buffer,
                           new q41_lineorder_tscan_filter_t(this),
                           this->db(),
                           _plineorder_desc.get(),
                           pxct
                           //, SH 
                           );
        //SUPPLIER
	tuple_fifo* s_out_buffer = new tuple_fifo(sizeof(q41_s_tuple));
        tscan_packet_t* s_tscan_packet =
        new tscan_packet_t("TSCAN SUPPLIER",
                           s_out_buffer,
                           new q41_supplier_tscan_filter_t(this,in),
                           this->db(),
                           _psupplier_desc.get(),
                           pxct
                           //, SH 
                           );
	//CUSTOMER
	tuple_fifo* c_out_buffer = new tuple_fifo(sizeof(q41_c_tuple));
        tscan_packet_t* c_tscan_packet =
        new tscan_packet_t("TSCAN CUSTOMER",
                           c_out_buffer,
                           new q41_customer_tscan_filter_t(this,in),
                           this->db(),
                           _pcustomer_desc.get(),
                           pxct
                           //, SH 
                           );
	//PART
	tuple_fifo* p_out_buffer = new tuple_fifo(sizeof(q41_p_tuple));
        tscan_packet_t* p_tscan_packet =
        new tscan_packet_t("TSCAN part",
                           p_out_buffer,
                           new q41_part_tscan_filter_t(this,in),
                           this->db(),
                           _ppart_desc.get(),
                           pxct
                           //, SH 
                           );	
	//DATE
	tuple_fifo* d_out_buffer = new tuple_fifo(sizeof(q41_d_tuple));
        tscan_packet_t* d_tscan_packet =
        new tscan_packet_t("TSCAN DATE",
                           d_out_buffer,
                           new q41_date_tscan_filter_t(this,in),
                           this->db(),
                           _pdate_desc.get(),
                           pxct
                           //, SH 
                           );


	//JOIN Lineorder and supplier
	tuple_fifo* join_lo_s_out = new tuple_fifo(sizeof(q41_join_s_tuple));
	packet_t* join_lo_s_packet =
	    new hash_join_packet_t("Lineorder - Supplier JOIN",
				   join_lo_s_out,
				   new trivial_filter_t(sizeof(q41_join_s_tuple)),
				   lo_tscan_packet,
				   s_tscan_packet,
				   new q41_lo_s_join_t() );

	//JOIN Lineorder and Supplier and Customer
	tuple_fifo* join_lo_s_c_out = new tuple_fifo(sizeof(q41_join_s_c_tuple));
	packet_t* join_lo_s_c_packet =
	    new hash_join_packet_t("Lineorder - Supplier - Customer JOIN",
				   join_lo_s_c_out,
				   new trivial_filter_t(sizeof(q41_join_s_c_tuple)),
				   join_lo_s_packet,
				   c_tscan_packet,
				   new q41_lo_s_c_join_t() );
        
        //JOIN Lineorder and Supplier and Customer and Part
	tuple_fifo* join_lo_s_c_p_out = new tuple_fifo(sizeof(q41_join_s_c_p_tuple));
	packet_t* join_lo_s_c_p_packet =
	    new hash_join_packet_t("Lineorder - Supplier - Customer - Part JOIN",
				   join_lo_s_c_p_out,
				   new trivial_filter_t(sizeof(q41_join_s_c_p_tuple)),
				   join_lo_s_c_packet,
				   p_tscan_packet,
				   new q41_lo_s_c_p_join_t() );
	
	//JOIN Lineorder and Supplier and Customer and Part and Date
	tuple_fifo* join_out = new tuple_fifo(sizeof(q41_join_tuple));
	packet_t* join_packet =
	    new hash_join_packet_t("Lineorder - Supplier - Customer - Part - Date JOIN",
				   join_out,
				   new trivial_filter_t(sizeof(q41_join_tuple)),
				   join_lo_s_c_p_packet,
				   d_tscan_packet,
                                   new q41_join_t() );
	
        // AGG PACKET CREATION

        tuple_fifo* agg_output_buffer =
            new tuple_fifo(sizeof (q41_aggregate_tuple));
        packet_t* q41_agg_packet =
            new partial_aggregate_packet_t("AGG Q4_1",
            agg_output_buffer,
            new trivial_filter_t(agg_output_buffer->tuple_size()),
            join_packet,
            new q41_agg_aggregate_t(),
            new q41_agg_input_tuple_key_extractor_t(),
            new q41_agg_input_tuple_key_compare_t());
  
    
        tuple_fifo* sort_final_out = new tuple_fifo(sizeof(q41_aggregate_tuple));
	packet_t* q41_sort_final_packet =
	    new sort_packet_t("ORDER BY D_YEAR ASC, REVENUE DESC",
				   sort_final_out,
				   new trivial_filter_t(sizeof(q41_aggregate_tuple)),
                                   new q41_order_key_extractor_t(),
                                   new q41_order_key_compare_t(),
				   q41_agg_packet);

       
    qpipe::query_state_t* qs = dp->query_state_create();
    lo_tscan_packet->assign_query_state(qs);
    s_tscan_packet->assign_query_state(qs);
    c_tscan_packet->assign_query_state(qs);
    p_tscan_packet->assign_query_state(qs);
    d_tscan_packet->assign_query_state(qs);
    join_lo_s_packet->assign_query_state(qs);
    join_lo_s_c_packet->assign_query_state(qs);
    join_lo_s_c_p_packet->assign_query_state(qs);
    join_packet->assign_query_state(qs);
    q41_agg_packet->assign_query_state(qs);
    q41_sort_final_packet->assign_query_state(qs);
        
    // Dispatch packet
    ssb_q41_process_tuple_t pt;
    process_query(q41_sort_final_packet, pt);
    dp->query_state_destroy(qs);

    return (RCOK); 
}


EXIT_NAMESPACE(ssb);

