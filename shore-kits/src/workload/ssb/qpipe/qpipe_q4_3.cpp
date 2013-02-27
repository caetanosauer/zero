

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

/** @file:   qpipe_4_3.cpp
 *
 *  @brief:  Implementation of QPIPE SSB Q4_3 over Shore-MT
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
 * QPIPE Q4_3 - Structures needed by operators 
 *
 ********************************************************************/

/*
select d_year, s_city, p_brand,
sum(lo_revenue - lo_supplycost) as profit
from [date], customer, supplier, part, lineorder
where lo_custkey = C_CustomerKey
and lo_suppkey = s_suppkey
and lo_partkey = p_partkey
and lo_orderdatekey = d_datekey
and s_nation = 'UNITED STATES'
and (d_year = 1997 or d_year = 1998)
and p_category = 'MFGR#14'
group by d_year, s_city, p_brand
order by d_year, s_city, p_brand;
*/

// the tuples after tablescan projection
struct q43_lo_tuple
{
  int LO_CUSTKEY;
  int LO_SUPPKEY;
  int LO_ORDERDATE;
  int LO_PARTKEY;
  int LO_REVENUE;
  int LO_SUPPLYCOST;
};

struct q43_c_tuple
{
  int C_CUSTKEY;
};

struct q43_s_tuple
{
  int S_SUPPKEY;
  char S_CITY[10];
};

struct q43_p_tuple
{
  int P_PARTKEY;
  char P_BRAND[10];
};

struct q43_d_tuple
{ 
  int D_DATEKEY;
  int D_YEAR;
};

struct q43_join_s_tuple
{
  char S_CITY[11];
  int LO_CUSTKEY;
  int LO_ORDERDATE;
  int LO_PARTKEY;
  int LO_REVENUE;
  int LO_SUPPLYCOST;
};

struct q43_join_s_p_tuple
{
  char P_BRAND[10];
  char S_CITY[11];
  int LO_CUSTKEY;
  int LO_ORDERDATE;
  int LO_REVENUE;
  int LO_SUPPLYCOST;
};


struct q43_join_s_p_d_tuple
{
  int D_YEAR;
  char P_BRAND[10];
  char S_CITY[11];
  int LO_CUSTKEY;
  int LO_REVENUE;
  int LO_SUPPLYCOST;
};

struct q43_join_tuple
{
  char S_CITY[11];
  char P_BRAND[10];
  int D_YEAR;
  int LO_REVENUE;
  int LO_SUPPLYCOST;
};

struct q43_aggregate_tuple
{
  
  char S_CITY[11];
  char P_BRAND[10];
  int D_YEAR;
  int PROFIT;
};

/*struct projected_tuple
{
  int KEY;
  };*/
typedef struct q43_aggregate_tuple projected_tuple;


class q43_lineorder_tscan_filter_t : public tuple_filter_t 
{
private:
    ShoreSSBEnv* _ssbdb;
    table_row_t* _prline;
    rep_row_t _rr;

    ssb_lineorder_tuple _lineorder;

public:

    q43_lineorder_tscan_filter_t(ShoreSSBEnv* ssbdb)//,q4_3_input_t &in) 
        : tuple_filter_t(ssbdb->lineorder_desc()->maxsize()), _ssbdb(ssbdb)
    {

    	// Get a lineorder tupple from the tuple cache and allocate space
        _prline = _ssbdb->lineorder_man()->get_tuple();
        _rr.set_ts(_ssbdb->lineorder_man()->ts(),
                   _ssbdb->lineorder_desc()->maxsize());
        _prline->_rep = &_rr;

    }

    ~q43_lineorder_tscan_filter_t()
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

        q43_lo_tuple *dest;
        dest = aligned_cast<q43_lo_tuple>(d.data);

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

    q43_lineorder_tscan_filter_t* clone() const {
        return new q43_lineorder_tscan_filter_t(*this);
    }

    c_str to_string() const {
        return c_str("q43_lineorder_tscan_filter_t()");
    }
};


class q43_supplier_tscan_filter_t : public tuple_filter_t 
{
private:
    ShoreSSBEnv* _ssbdb;
    table_row_t* _prsupp;
    rep_row_t _rr;

    ssb_supplier_tuple _supplier;

  /*VARIABLES TAKING VALUES FROM INPUT FOR SELECTION*/
    char NATION[16];
public:

    q43_supplier_tscan_filter_t(ShoreSSBEnv* ssbdb, q4_3_input_t &in) 
        : tuple_filter_t(ssbdb->supplier_desc()->maxsize()), _ssbdb(ssbdb)
    {

    	// Get a supplier tupple from the tuple cache and allocate space
        _prsupp = _ssbdb->supplier_man()->get_tuple();
        _rr.set_ts(_ssbdb->supplier_man()->ts(),
                   _ssbdb->supplier_desc()->maxsize());
        _prsupp->_rep = &_rr;

	
	strcpy(NATION,"UNITED STATES");
    }

    ~q43_supplier_tscan_filter_t()
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

        _prsupp->get_value(4, _supplier.S_NATION, STRSIZE(15));


	if (strcmp(_supplier.S_NATION,NATION)==0)
	    {
		TRACE( TRACE_RECORD_FLOW, "+ NATION |%s --d\n",
		       _supplier.S_NATION);
		return (true);
	    }
	else
	    {
		TRACE( TRACE_RECORD_FLOW, ". NATION |%s --d\n",
		       _supplier.S_NATION);
		return (false);
	    }
    }

    
    // Projection
    void project(tuple_t &d, const tuple_t &s) {        

        q43_s_tuple *dest;
        dest = aligned_cast<q43_s_tuple>(d.data);

        _prsupp->get_value(0, _supplier.S_SUPPKEY);
        _prsupp->get_value(3, _supplier.S_CITY, STRSIZE(10));

        TRACE( TRACE_RECORD_FLOW, "%d|%s --d\n",
               _supplier.S_SUPPKEY,
               _supplier.S_CITY);


        dest->S_SUPPKEY = _supplier.S_SUPPKEY;
        strcpy(dest->S_CITY,_supplier.S_CITY);
    }

    q43_supplier_tscan_filter_t* clone() const {
        return new q43_supplier_tscan_filter_t(*this);
    }

    c_str to_string() const {
        return c_str("q43_supplier_tscan_filter_t()");
    }
};


class q43_customer_tscan_filter_t : public tuple_filter_t 
{
private:
    ShoreSSBEnv* _ssbdb;
    table_row_t* _prcust;
    rep_row_t _rr;

    ssb_customer_tuple _customer;

  /*VARIABLES TAKING VALUES FROM INPUT FOR SELECTION*/

public:

    q43_customer_tscan_filter_t(ShoreSSBEnv* ssbdb, q4_3_input_t &in) 
        : tuple_filter_t(ssbdb->customer_desc()->maxsize()), _ssbdb(ssbdb)
    {

    	// Get a customer tupple from the tuple cache and allocate space
        _prcust = _ssbdb->customer_man()->get_tuple();
        _rr.set_ts(_ssbdb->customer_man()->ts(),
                   _ssbdb->customer_desc()->maxsize());
        _prcust->_rep = &_rr;

    }

    ~q43_customer_tscan_filter_t()
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

	return (true);
	    
    }

    
    // Projection
    void project(tuple_t &d, const tuple_t &s) {        

        q43_c_tuple *dest;
        dest = aligned_cast<q43_c_tuple>(d.data);

        _prcust->get_value(0, _customer.C_CUSTKEY);

        TRACE( TRACE_RECORD_FLOW, "%d --d\n",
               _customer.C_CUSTKEY);


        dest->C_CUSTKEY = _customer.C_CUSTKEY;
    }

    q43_customer_tscan_filter_t* clone() const {
        return new q43_customer_tscan_filter_t(*this);
    }

    c_str to_string() const {
        return c_str("q43_customer_tscan_filter_t()");
    }
};


class q43_part_tscan_filter_t : public tuple_filter_t 
{
private:
    ShoreSSBEnv* _ssbdb;
    table_row_t* _prpart;
    rep_row_t _rr;

    ssb_part_tuple _part;

  /*VARIABLES TAKING VALUES FROM INPUT FOR SELECTION*/
    char CATEGORY[8];

public:

    q43_part_tscan_filter_t(ShoreSSBEnv* ssbdb, q4_3_input_t &in) 
        : tuple_filter_t(ssbdb->part_desc()->maxsize()), _ssbdb(ssbdb)
    {

    	// Get a part tupple from the tuple cache and allocate space
        _prpart = _ssbdb->part_man()->get_tuple();
        _rr.set_ts(_ssbdb->part_man()->ts(),
                   _ssbdb->part_desc()->maxsize());
        _prpart->_rep = &_rr;

	strcpy(CATEGORY,"MFGR#14");

    }

    ~q43_part_tscan_filter_t()
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

        _prpart->get_value(3, _part.P_CATEGORY, STRSIZE(7));

	
	if (strcmp(_part.P_CATEGORY, CATEGORY)==0)
	    {
		TRACE( TRACE_RECORD_FLOW, "+ CATEGORY |%s --d\n",
		       _part.P_CATEGORY);
		return (true);
	    }
	else
	    {
		TRACE( TRACE_RECORD_FLOW, ". CATEGORY |%s --d\n",
		       _part.P_CATEGORY);
		return (false);
	    }

    }

    
    // Projection
    void project(tuple_t &d, const tuple_t &s) {        

        q43_p_tuple *dest;
        dest = aligned_cast<q43_p_tuple>(d.data);

        _prpart->get_value(0, _part.P_PARTKEY);
        _prpart->get_value(4, _part.P_BRAND, STRSIZE(9));
        

        TRACE( TRACE_RECORD_FLOW, "%d |%s --d\n",
               _part.P_PARTKEY,
               _part.P_BRAND);


        dest->P_PARTKEY = _part.P_PARTKEY;
        strcpy(dest->P_BRAND, _part.P_BRAND);
        
    }

    q43_part_tscan_filter_t* clone() const {
        return new q43_part_tscan_filter_t(*this);
    }

    c_str to_string() const {
        return c_str("q43_part_tscan_filter_t()");
    }
};


class q43_date_tscan_filter_t : public tuple_filter_t 
{
private:
    ShoreSSBEnv* _ssbdb;
    table_row_t* _prdate;
    rep_row_t _rr;

    ssb_date_tuple _date;
     /*VARIABLES TAKING VALUES FROM INPUT FOR SELECTION*/
    int YEAR_LOW;
    int YEAR_HIGH;

public:

    q43_date_tscan_filter_t(ShoreSSBEnv* ssbdb, q4_3_input_t &in) 
        : tuple_filter_t(ssbdb->date_desc()->maxsize()), _ssbdb(ssbdb)
    {

    	// Get a date tupple from the tuple cache and allocate space
        _prdate = _ssbdb->date_man()->get_tuple();
        _rr.set_ts(_ssbdb->date_man()->ts(),
                   _ssbdb->date_desc()->maxsize());
        _prdate->_rep = &_rr;

        YEAR_LOW=1997;
	YEAR_HIGH=1998;
    }

    ~q43_date_tscan_filter_t()
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
        
	_prdate->get_value(4, _date.D_YEAR);

	
	if (_date.D_YEAR==YEAR_LOW || _date.D_YEAR==YEAR_HIGH)
	    {
		TRACE( TRACE_RECORD_FLOW, "+ YEAR |%d --d\n",
		       _date.D_YEAR);
		return (true);
	    }
	else
	    {
		TRACE( TRACE_RECORD_FLOW, ". YEAR |%d --d\n",
		       _date.D_YEAR);
		return (false);
	    }

    }

    
    // Projection
    void project(tuple_t &d, const tuple_t &s) {        

        q43_d_tuple *dest;
        dest = aligned_cast<q43_d_tuple>(d.data);

        _prdate->get_value(0, _date.D_DATEKEY);
        _prdate->get_value(4, _date.D_YEAR);

        TRACE( TRACE_RECORD_FLOW, "%d|%d --d\n",
               _date.D_DATEKEY,
               _date.D_YEAR);


        dest->D_DATEKEY = _date.D_DATEKEY;
        dest->D_YEAR=_date.D_YEAR;
    }

    q43_date_tscan_filter_t* clone() const {
        return new q43_date_tscan_filter_t(*this);
    }

    c_str to_string() const {
        return c_str("q4_3_date_tscan_filter_t()");
    }
};


//Natural join
// left is lineorder, right is supplier
struct q43_lo_s_join_t : public tuple_join_t {


    q43_lo_s_join_t ()
        : tuple_join_t(sizeof(q43_lo_tuple),
                       offsetof(q43_lo_tuple, LO_SUPPKEY),
                       sizeof(q43_s_tuple),
                       offsetof(q43_s_tuple, S_SUPPKEY),
                       sizeof(int),
                       sizeof(q43_join_s_tuple))
    {
    }


    virtual void join(tuple_t &dest,
                      const tuple_t &left,
                      const tuple_t &right)
    {
        // KLUDGE: this projection should go in a separate filter class
    	q43_lo_tuple* lo = aligned_cast<q43_lo_tuple>(left.data);
    	q43_s_tuple* s = aligned_cast<q43_s_tuple>(right.data);
	q43_join_s_tuple* ret = aligned_cast<q43_join_s_tuple>(dest.data);
	
	ret->LO_CUSTKEY = lo->LO_CUSTKEY;
        ret->LO_PARTKEY = lo->LO_PARTKEY;
        ret->LO_ORDERDATE = lo->LO_ORDERDATE;
        ret->LO_REVENUE = lo->LO_REVENUE;
        ret->LO_SUPPLYCOST = lo->LO_SUPPLYCOST;
        
        strcpy(ret->S_CITY, s->S_CITY);

        TRACE ( TRACE_RECORD_FLOW, "JOIN {%s} %d %d %d %d %d\n",ret->S_CITY, ret->LO_CUSTKEY, ret->LO_PARTKEY, ret->LO_ORDERDATE, ret->LO_REVENUE, ret->LO_SUPPLYCOST);

    }

    virtual q43_lo_s_join_t*  clone() const {
        return new q43_lo_s_join_t(*this);
    }

    virtual c_str to_string() const {
        return "join LINEORDER and SUPPLIER, select S_NATION, LO_CUSTKEY, LO_PARTKEY, LO_ORDERDATE, LO_REVENUE, LO_REVENUE";
    }
};

//Natural join
// left is lineorder, supplier right is date
struct q43_lo_s_p_join_t : public tuple_join_t {


    q43_lo_s_p_join_t ()
        : tuple_join_t(sizeof(q43_join_s_tuple),
                       offsetof(q43_join_s_tuple, LO_PARTKEY),
                       sizeof(q43_p_tuple),
                       offsetof(q43_p_tuple, P_PARTKEY),
                       sizeof(int),
                       sizeof(q43_join_s_p_tuple))
    {
    }


    virtual void join(tuple_t &dest,
                      const tuple_t &left,
                      const tuple_t &right)
    {
        // KLUDGE: this projection should go in a separate filter class
    	q43_join_s_tuple* jo = aligned_cast<q43_join_s_tuple>(left.data);
    	q43_p_tuple* p = aligned_cast<q43_p_tuple>(right.data);
	q43_join_s_p_tuple* ret = aligned_cast<q43_join_s_p_tuple>(dest.data);
	
        
        ret->LO_CUSTKEY = jo->LO_CUSTKEY ;
        ret->LO_ORDERDATE = jo->LO_ORDERDATE ;
        ret->LO_REVENUE = jo->LO_REVENUE ;
        ret->LO_SUPPLYCOST = jo->LO_SUPPLYCOST;
	
	strcpy(ret->P_BRAND,p->P_BRAND);
        strcpy(ret->S_CITY, jo->S_CITY);
	
        TRACE ( TRACE_RECORD_FLOW, "JOIN {%s} {%s} %d %d %d %d\n",ret->P_BRAND, ret->S_CITY, ret->LO_CUSTKEY, ret->LO_ORDERDATE, ret->LO_REVENUE, ret->LO_SUPPLYCOST);

    }

    virtual q43_lo_s_p_join_t* clone() const {
        return new q43_lo_s_p_join_t(*this);
    }

    virtual c_str to_string() const {
        return "join LINEORDER and SUPPLIER and PART, select P_BRAND, S_CITY, LO_CUSTKEY, LO_ORDERDATE, LO_REVENUE, LO_SUPPLYCOST";
    }
};

//Natural join
// left is lineorder, supplier, date, right is part
struct q43_lo_s_p_d_join_t : public tuple_join_t {


    q43_lo_s_p_d_join_t ()
        : tuple_join_t(sizeof(q43_join_s_p_tuple),
                       offsetof(q43_join_s_p_tuple, LO_ORDERDATE),
                       sizeof(q43_d_tuple),
                       offsetof(q43_d_tuple, D_DATEKEY),
                       sizeof(int),
                       sizeof(q43_join_s_p_d_tuple))
    {
    }


    virtual void join(tuple_t &dest,
                      const tuple_t &left,
                      const tuple_t &right)
    {
        // KLUDGE: this projection should go in a separate filter class
    	q43_join_s_p_tuple* jo = aligned_cast<q43_join_s_p_tuple>(left.data);
    	q43_d_tuple* d = aligned_cast<q43_d_tuple>(right.data);
	q43_join_s_p_d_tuple* ret = aligned_cast<q43_join_s_p_d_tuple>(dest.data);
	
	ret->LO_CUSTKEY = jo->LO_CUSTKEY ;
        ret->D_YEAR = d->D_YEAR ;
        ret->LO_REVENUE = jo->LO_REVENUE ;
        ret->LO_SUPPLYCOST = jo->LO_SUPPLYCOST;
	
	strcpy(ret->P_BRAND,jo->P_BRAND);
        strcpy(ret->S_CITY, jo->S_CITY);
	
        TRACE ( TRACE_RECORD_FLOW, "JOIN {%s} {%s} %d %d %d %d\n",ret->P_BRAND, ret->S_CITY, ret->LO_CUSTKEY, ret->D_YEAR, ret->LO_REVENUE, ret->LO_SUPPLYCOST);

    }

    virtual q43_lo_s_p_d_join_t* clone() const {
        return new q43_lo_s_p_d_join_t(*this);
    }

    virtual c_str to_string() const {
        return "join LINEORDER and SUPPLIER and PART and DATE, select P_BRAND, S_CITY, LO_CUSTKEY, D_YEAR, LO_REVENUE, LO_SUPPLYCOST";
    }
};

//Natural join
// left is lineorder, supplier, customer, part right is date
struct q43_join_t : public tuple_join_t {


    q43_join_t ()
        : tuple_join_t(sizeof(q43_join_s_p_d_tuple),
                       offsetof(q43_join_s_p_d_tuple, LO_CUSTKEY),
                       sizeof(q43_c_tuple),
                       offsetof(q43_c_tuple, C_CUSTKEY),
                       sizeof(int),
                       sizeof(q43_join_tuple))
    {
    }


    virtual void join(tuple_t &dest,
                      const tuple_t &left,
                      const tuple_t &right)
    {
        // KLUDGE: this projection should go in a separate filter class
    	q43_join_s_p_d_tuple* jo = aligned_cast<q43_join_s_p_d_tuple>(left.data);
    	q43_c_tuple* c = aligned_cast<q43_c_tuple>(right.data);
	q43_join_tuple* ret = aligned_cast<q43_join_tuple>(dest.data);
        
        ret->D_YEAR = jo->D_YEAR;
        ret->LO_REVENUE = jo->LO_REVENUE;
        ret->LO_SUPPLYCOST = jo->LO_SUPPLYCOST;
	
	strcpy(ret->P_BRAND,jo->P_BRAND);
        strcpy(ret->S_CITY, jo->S_CITY);

        TRACE ( TRACE_RECORD_FLOW, "JOIN {%s} {%s} %d %d %d\n",ret->P_BRAND, ret->S_CITY, ret->D_YEAR, ret->LO_REVENUE, ret->LO_SUPPLYCOST);

    }

    virtual q43_join_t* clone() const {
        return new q43_join_t(*this);
    }

    virtual c_str to_string() const {
        return "join LINEORDER and SUPPLIER and PART and DATE and CUSTOMER, select P_BRAND, S_CITY, D_YEAR, LO_REVENUE, LO_SUPPLYCOST";
    }
};



// Key extractor and Comparator for Aggregation filter

struct q43_agg_input_tuple_key {
 
  char S_CITY[STRSIZE(10)];
  char P_BRAND[STRSIZE(9)];
  int D_YEAR;
  
  
    int extract_hint() {
        return (this->D_YEAR - 1990 << 24) + (this->S_CITY[9] << 16) + (this->P_BRAND[7] << 8) + (this->P_BRAND[8]);
 //       return (this->D_YEAR - 1990 << 24) + (this->S_CITY[0] << 16) + (this->S_CITY[1] << 8) + (this->P_BRAND[8]);
    }
};

struct q43_agg_input_tuple_key_extractor_t : public key_extractor_t {

    q43_agg_input_tuple_key_extractor_t() : key_extractor_t(sizeof(q43_agg_input_tuple_key), offsetof(q43_join_tuple, S_CITY)) {
    }

    virtual int extract_hint(const char* key) const {
        q43_agg_input_tuple_key* aligned_key = aligned_cast<q43_agg_input_tuple_key>(key);

        // We assume a 4-byte integer and fill it with: The two first 
        // characters of S_NATION, and the seventh character of P_CATEGORY and the
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
//        int result = (aligned_key->S_NATION[0] << 24) + (aligned_key->S_NATION[1] << 16) + (aligned_key->P_CATEGORY[6] << 8) + (aligned_key->D_YEAR - 1990);
//        return result;
        return aligned_key->extract_hint();
        
        //return 0;
    }

    virtual q43_agg_input_tuple_key_extractor_t * clone() const {
        return new q43_agg_input_tuple_key_extractor_t(*this);
    }
};

struct q43_agg_input_tuple_key_compare_t : public key_compare_t {

    virtual int operator()(const void* key1, const void* key2) const {
        q43_agg_input_tuple_key* a = aligned_cast<q43_agg_input_tuple_key>(key1);
        q43_agg_input_tuple_key* b = aligned_cast<q43_agg_input_tuple_key>(key2);

        
        int scitycomparison = strcmp(a->S_CITY, b->S_CITY);
        if (scitycomparison != 0) return scitycomparison;
        int pbrandcomparison = strcmp(a->P_BRAND, b->P_BRAND);
        if (pbrandcomparison != 0) return pbrandcomparison;
        return a->D_YEAR - b->D_YEAR;
    }

    virtual q43_agg_input_tuple_key_compare_t * clone() const {
        return new q43_agg_input_tuple_key_compare_t(*this);
    }
};

// Aggregate's tuple's key is the same as the input tuple's key.
typedef struct q43_agg_input_tuple_key q43_agg_tuple_key;

class q43_agg_aggregate_t : public tuple_aggregate_t {
private:

    struct q43_agg_output_tuple_key_extractor_t : public key_extractor_t {

        q43_agg_output_tuple_key_extractor_t() : key_extractor_t(sizeof (q43_agg_tuple_key), offsetof(q43_aggregate_tuple, S_CITY)) {
        }

        virtual int extract_hint(const char* key) const {
            q43_agg_tuple_key* aligned_key = aligned_cast<q43_agg_tuple_key > (key);
            return aligned_key->extract_hint();
        }

        virtual q43_agg_output_tuple_key_extractor_t * clone() const {
            return new q43_agg_output_tuple_key_extractor_t(*this);
        }
    };
    
    q43_agg_output_tuple_key_extractor_t _extractor;

public:

    q43_agg_aggregate_t()
    : tuple_aggregate_t(sizeof(q43_aggregate_tuple)) {
    }

    key_extractor_t* key_extractor() {
        return &_extractor;
    }

    void aggregate(char* agg_data, const tuple_t &s) {
        q43_join_tuple *src;
        src = aligned_cast<q43_join_tuple> (s.data);
        q43_aggregate_tuple* tuple = aligned_cast<q43_aggregate_tuple>(agg_data);

        tuple->PROFIT += (src->LO_REVENUE - src->LO_SUPPLYCOST);
        TRACE(TRACE_RECORD_FLOW, "%.2f\n", tuple->PROFIT);
    }

    void finish(tuple_t &d, const char* agg_data) {
        q43_aggregate_tuple *dest;
        dest = aligned_cast<q43_aggregate_tuple > (d.data);
        q43_aggregate_tuple* tuple = aligned_cast<q43_aggregate_tuple > (agg_data);

        *dest = *tuple;
    }

    q43_agg_aggregate_t* clone() const {
        return new q43_agg_aggregate_t(*this);
    }

    c_str to_string() const {
        return "q43_agg_aggregate_t";
    }
};



// Final Ordering

struct q43_sort_tuple_key {
  char S_CITY[STRSIZE(10)];
  char P_BRAND[STRSIZE(9)];
  int D_YEAR;
};

struct q43_order_key_extractor_t : public key_extractor_t {

    q43_order_key_extractor_t() : key_extractor_t(sizeof (q43_sort_tuple_key), offsetof(q43_aggregate_tuple, S_CITY)) {
    }

    virtual int extract_hint(const char* key) const {
        q43_sort_tuple_key* aligned_key = aligned_cast<q43_sort_tuple_key> (key);

        // We assume a 4-byte integer and fill it with: The last digit of year
        // in the 3 most significant bits. The remaining bits are filled with the
        // integer representation of Revenue. But they are subtracted
        // in order to keep a decreasing order.
  //          int result = ((aligned_key->D_YEAR - 1992) << 28) + (aligned_key->S_CITY[9] << 16)+(aligned_key->P_BRAND[5] << 8)+(aligned_key->P_BRAND[6] );
    int result = ((aligned_key->D_YEAR - 1992) << 28) + (aligned_key->S_CITY[9] << 16)+(aligned_key->P_BRAND[7] << 8)+(aligned_key->P_BRAND[8] );

            return result;
        
        //return 0;
    }

    virtual q43_order_key_extractor_t * clone() const {
        return new q43_order_key_extractor_t(*this);
    }
};

struct q43_order_key_compare_t : public key_compare_t {

    virtual int operator()(const void* key1, const void* key2) const {
        q43_sort_tuple_key* a = aligned_cast<q43_sort_tuple_key>(key1);
        q43_sort_tuple_key* b = aligned_cast<q43_sort_tuple_key>(key2);

        int yearcomparison = a->D_YEAR - b->D_YEAR;
        if (yearcomparison != 0) return yearcomparison;
        int ccitycomparison = strcmp(a->S_CITY, b->S_CITY);
            return  ccitycomparison;
        int pbrandcomparison = strcmp(a->P_BRAND, b->P_BRAND);
            return  pbrandcomparison;
    }

    virtual q43_order_key_compare_t * clone() const {
        return new q43_order_key_compare_t(*this);
    }
};





class ssb_q43_process_tuple_t : public process_tuple_t 
{    
public:
        
    void begin() {
        TRACE(TRACE_QUERY_RESULTS, "*** q4_3 ANSWER ...\n");
        TRACE(TRACE_QUERY_RESULTS, "*** ...\n");
    }
    
    virtual void process(const tuple_t& output) {
        projected_tuple *tuple;
        tuple = aligned_cast<projected_tuple>(output.data);
        
        TRACE ( TRACE_QUERY_RESULTS, "PROCESS %d {%s} {%s} %d\n",tuple->D_YEAR, tuple->S_CITY, tuple->P_BRAND, tuple->PROFIT);
        
        /*TRACE(TRACE_QUERY_RESULTS, "%d --\n",
	  tuple->KEY);*/
    }
};



/******************************************************************** 
 *
 * QPIPE q4_3 - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_qpipe_q4_3(const int xct_id, 
                                  q4_3_input_t& in)
{
    TRACE( TRACE_ALWAYS, "********** q4_3 *********\n");

   
    policy_t* dp = this->get_sched_policy();
    xct_t* pxct = smthread_t::me()->xct();
    

    // TSCAN PACKET
        //LINEORDER
        tuple_fifo* lo_out_buffer = new tuple_fifo(sizeof(q43_lo_tuple));
        tscan_packet_t* lo_tscan_packet =
        new tscan_packet_t("TSCAN LINEORDER",
                           lo_out_buffer,
                           new q43_lineorder_tscan_filter_t(this),
                           this->db(),
                           _plineorder_desc.get(),
                           pxct
                           //, SH 
                           );
        //SUPPLIER
	tuple_fifo* s_out_buffer = new tuple_fifo(sizeof(q43_s_tuple));
        tscan_packet_t* s_tscan_packet =
        new tscan_packet_t("TSCAN SUPPLIER",
                           s_out_buffer,
                           new q43_supplier_tscan_filter_t(this,in),
                           this->db(),
                           _psupplier_desc.get(),
                           pxct
                           //, SH 
                           );
        //PART
	tuple_fifo* p_out_buffer = new tuple_fifo(sizeof(q43_p_tuple));
        tscan_packet_t* p_tscan_packet =
        new tscan_packet_t("TSCAN part",
                           p_out_buffer,
                           new q43_part_tscan_filter_t(this,in),
                           this->db(),
                           _ppart_desc.get(),
                           pxct
                           //, SH 
                           );
        //DATE
	tuple_fifo* d_out_buffer = new tuple_fifo(sizeof(q43_d_tuple));
        tscan_packet_t* d_tscan_packet =
        new tscan_packet_t("TSCAN DATE",
                           d_out_buffer,
                           new q43_date_tscan_filter_t(this,in),
                           this->db(),
                           _pdate_desc.get(),
                           pxct
                           //, SH 
                           );
	//CUSTOMER
	tuple_fifo* c_out_buffer = new tuple_fifo(sizeof(q43_c_tuple));
        tscan_packet_t* c_tscan_packet =
        new tscan_packet_t("TSCAN CUSTOMER",
                           c_out_buffer,
                           new q43_customer_tscan_filter_t(this,in),
                           this->db(),
                           _pcustomer_desc.get(),
                           pxct
                           //, SH 
                           );
		
	//JOIN Lineorder and supplier
	tuple_fifo* join_lo_s_out = new tuple_fifo(sizeof(q43_join_s_tuple));
	packet_t* join_lo_s_packet =
	    new hash_join_packet_t("Lineorder - Supplier JOIN",
				   join_lo_s_out,
				   new trivial_filter_t(sizeof(q43_join_s_tuple)),
				   lo_tscan_packet,
				   s_tscan_packet,
				   new q43_lo_s_join_t() );

	//JOIN Lineorder and Supplier and Date
	tuple_fifo* join_lo_s_p_out = new tuple_fifo(sizeof(q43_join_s_p_tuple));
	packet_t* join_lo_s_p_packet =
	    new hash_join_packet_t("Lineorder - Supplier - Part JOIN",
				   join_lo_s_p_out,
				   new trivial_filter_t(sizeof(q43_join_s_p_tuple)),
				   join_lo_s_packet,
				   p_tscan_packet,
				   new q43_lo_s_p_join_t() );
        
        //JOIN Lineorder and Supplier and Date and Part
	tuple_fifo* join_lo_s_p_d_out = new tuple_fifo(sizeof(q43_join_s_p_d_tuple));
	packet_t* join_lo_s_p_d_packet =
	    new hash_join_packet_t("Lineorder - Supplier - Part - Date JOIN",
				   join_lo_s_p_d_out,
				   new trivial_filter_t(sizeof(q43_join_s_p_d_tuple)),
				   join_lo_s_p_packet,
				   d_tscan_packet,
				   new q43_lo_s_p_d_join_t() );
	
	//JOIN Lineorder and Supplier and Date and Part and Customer
	tuple_fifo* join_out = new tuple_fifo(sizeof(q43_join_tuple));
	packet_t* join_packet =
	    new hash_join_packet_t("Lineorder - Supplier - Part - Date - Customer JOIN",
				   join_out,
				   new trivial_filter_t(sizeof(q43_join_tuple)),
				   join_lo_s_p_d_packet,
				   c_tscan_packet,
				   new q43_join_t() );
       // AGG PACKET CREATION

    tuple_fifo* agg_output_buffer =
            new tuple_fifo(sizeof (q43_aggregate_tuple));
    packet_t* q43_agg_packet =
            new partial_aggregate_packet_t("AGG Q4_3",
            agg_output_buffer,
            new trivial_filter_t(agg_output_buffer->tuple_size()),
            join_packet,
            new q43_agg_aggregate_t(),
            new q43_agg_input_tuple_key_extractor_t(),
            new q43_agg_input_tuple_key_compare_t());
  
    
        tuple_fifo* sort_final_out = new tuple_fifo(sizeof(q43_aggregate_tuple));
	packet_t* q43_sort_final_packet =
	    new sort_packet_t("ORDER BY D_YEAR ASC, REVENUE DESC",
				   sort_final_out,
				   new trivial_filter_t(sizeof(q43_aggregate_tuple)),
                                   new q43_order_key_extractor_t(),
                                   new q43_order_key_compare_t(),
				   q43_agg_packet);
    
        
    qpipe::query_state_t* qs = dp->query_state_create();
    lo_tscan_packet->assign_query_state(qs);
    s_tscan_packet->assign_query_state(qs);
    p_tscan_packet->assign_query_state(qs);
    d_tscan_packet->assign_query_state(qs);
    c_tscan_packet->assign_query_state(qs);
    join_lo_s_packet->assign_query_state(qs);
    join_lo_s_p_packet->assign_query_state(qs);
    join_lo_s_p_d_packet->assign_query_state(qs);
    join_packet->assign_query_state(qs);
    q43_agg_packet->assign_query_state(qs);
    q43_sort_final_packet->assign_query_state(qs);
        
    // Dispatch packet
    ssb_q43_process_tuple_t pt;
    process_query(q43_sort_final_packet, pt);
    dp->query_state_destroy(qs);

    return (RCOK); 
}


EXIT_NAMESPACE(ssb);

