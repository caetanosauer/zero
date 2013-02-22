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

/** @file:   qpipe_qtest.cpp
 *
 *  @brief:  Implementation of QPIPE SSB TEST over Shore-MT
 *           Performs Sort-Merge-Join between Lineorders
 *           and Dates on the Date key.
 *
 *  @author: Iraklis Psaroudakis
 *  @date:   November 2011
 */

#include "workload/ssb/shore_ssb_env.h"
#include "qpipe.h"

using namespace shore;
using namespace qpipe;

ENTER_NAMESPACE(ssb);


/******************************************************************** 
 *
 * QPIPE QTEST - Structures needed by operators 
 *
 ********************************************************************/

/*********************
 * Tuple Definitions *
 *********************/

// Lineorders table scan tuple
struct qtest_lo_tuple
{
  int LO_ORDERKEY;
  int LO_LINENUMBER;
  int LO_ORDERDATE;
  int LO_REVENUE;    
};

// Dates table scan tuple
struct qtest_d_tuple
{ 
  int D_DATEKEY;
  int D_YEAR;
  int D_DAYNUMINWEEK;
  char D_DATE [STRSIZE(18)];
};

// Join tuple
struct qtest_join_d_tuple
{
    int LO_ORDERKEY;
    int LO_LINENUMBER;
    int LO_ORDERDATE;
    int D_DATEKEY;
    char D_DATE [STRSIZE(18)];
    int LO_REVENUE;  
};

// Output tuple
typedef struct qtest_join_d_tuple projected_tuple;

/************************************
 * Table Scan filter for Lineorders *
 ************************************/

class qtest_lineorder_tscan_filter_t : public tuple_filter_t 
{
private:
    ShoreSSBEnv* _ssbdb;
    table_row_t* _prline;
    rep_row_t _rr;

    ssb_lineorder_tuple _lineorder;

public:

    qtest_lineorder_tscan_filter_t(ShoreSSBEnv* ssbdb) //,qtest_input_t &in) 
        : tuple_filter_t(ssbdb->lineorder_desc()->maxsize()), _ssbdb(ssbdb)
    {
        _prline = _ssbdb->lineorder_man()->get_tuple();
        _rr.set_ts(_ssbdb->lineorder_man()->ts(),
                   _ssbdb->lineorder_desc()->maxsize());
        _prline->_rep = &_rr;
    }

    ~qtest_lineorder_tscan_filter_t()
    {
        _ssbdb->lineorder_man()->give_tuple(_prline);
    }

    bool select(const tuple_t &input) {
        if (!_ssbdb->lineorder_man()->load(_prline, input.data)) {
            assert(false); // RC(se_WRONG_DISK_DATA)
        }

        // Select it for sure
        return (true);
    }

    void project(tuple_t &d, const tuple_t &s) {        
        qtest_lo_tuple *dest;
        dest = aligned_cast<qtest_lo_tuple>(d.data); 

        _prline->get_value(0, _lineorder.LO_ORDERKEY);
        _prline->get_value(1, _lineorder.LO_LINENUMBER);
        _prline->get_value(5, _lineorder.LO_ORDERDATE);
        _prline->get_value(12, _lineorder.LO_REVENUE);

        TRACE( TRACE_RECORD_FLOW, "%d|%d|%d|%d --d\n",
               _lineorder.LO_ORDERKEY,
               _lineorder.LO_LINENUMBER,
               _lineorder.LO_ORDERDATE,
               _lineorder.LO_REVENUE);

        dest->LO_ORDERKEY = _lineorder.LO_ORDERKEY;
        dest->LO_LINENUMBER = _lineorder.LO_LINENUMBER;
        dest->LO_ORDERDATE = _lineorder.LO_ORDERDATE;
        dest->LO_REVENUE = _lineorder.LO_REVENUE;
    }

    qtest_lineorder_tscan_filter_t* clone() const {
        return new qtest_lineorder_tscan_filter_t(*this);
    }

    c_str to_string() const {
        return c_str("qtest_lineorder_tscan_filter_t()");
    }
};

/*******************************
 * Table Scan filter for Dates *
 *******************************/

class qtest_date_tscan_filter_t : public tuple_filter_t 
{
private:
    ShoreSSBEnv* _ssbdb;
    table_row_t* _prdate;
    rep_row_t _rr;

    ssb_date_tuple _date;

public:

    qtest_date_tscan_filter_t(ShoreSSBEnv* ssbdb, qtest_input_t &in) 
        : tuple_filter_t(ssbdb->date_desc()->maxsize()), _ssbdb(ssbdb)
    {
        _prdate = _ssbdb->date_man()->get_tuple();
        _rr.set_ts(_ssbdb->date_man()->ts(),
                   _ssbdb->date_desc()->maxsize());
        _prdate->_rep = &_rr;
    }

    ~qtest_date_tscan_filter_t()
    {
        _ssbdb->date_man()->give_tuple(_prdate);
    }

    bool select(const tuple_t &input) {
        if (!_ssbdb->date_man()->load(_prdate, input.data)) {
            assert(false); // RC(se_WRONG_DISK_DATA)
        }

        // Select it for sure
        return true;
    }

    void project(tuple_t &d, const tuple_t &s) {        
        qtest_d_tuple *dest;
        dest = aligned_cast<qtest_d_tuple>(d.data);

        _prdate->get_value(0, _date.D_DATEKEY);
        _prdate->get_value(1, _date.D_DATE, STRSIZE(18));
        _prdate->get_value(4, _date.D_YEAR);
        _prdate->get_value(7, _date.D_DAYNUMINWEEK);

        TRACE( TRACE_RECORD_FLOW, "%s|%d|%d|%d --d\n",
               _date.D_DATE,
               _date.D_DATEKEY,
               _date.D_YEAR,
               _date.D_DAYNUMINWEEK);

        dest->D_DATEKEY = _date.D_DATEKEY;
        strcpy(dest->D_DATE,_date.D_DATE);
        dest->D_YEAR=_date.D_YEAR;
        dest->D_DAYNUMINWEEK=_date.D_DAYNUMINWEEK;
    }

    qtest_date_tscan_filter_t* clone() const {
        return new qtest_date_tscan_filter_t(*this);
    }

    c_str to_string() const {
        return c_str("qtest_date_tscan_filter_t()");
    }
};

/************************************************
 * Key Extractor and Comparer for Dates Sorting *
 ************************************************/

struct qtest_date_key_extractor_t : public key_extractor_t 
{
        qtest_date_key_extractor_t() : key_extractor_t(sizeof(qtest_d_tuple), 0) { }
        
        virtual int extract_hint(const char* tuple_data) const {
            qtest_d_tuple* tuple = aligned_cast<qtest_d_tuple>(tuple_data);
            return tuple->D_DATEKEY;
        }
        
        virtual qtest_date_key_extractor_t* clone() const {
            return new qtest_date_key_extractor_t(*this);
        }
};

struct qtest_date_key_compare_t : public key_compare_t 
{
     virtual int operator()(const void* key1, const void* key2) const {
            qtest_d_tuple* a = aligned_cast<qtest_d_tuple>(key1);
            qtest_d_tuple* b = aligned_cast<qtest_d_tuple>(key2);
            
            return a->D_DATEKEY - b->D_DATEKEY;
        }
        virtual qtest_date_key_compare_t* clone() const {
            return new qtest_date_key_compare_t(*this);
        }
};

/*****************************************************
 * Key Extractor and Comparer for Lineorders Sorting *
 *****************************************************/

struct qtest_lo_key_extractor_t : public key_extractor_t 
{
        qtest_lo_key_extractor_t() : key_extractor_t(sizeof(qtest_lo_tuple), 0) { }
        
        virtual int extract_hint(const char* tuple_data) const {
            qtest_lo_tuple* tuple = aligned_cast<qtest_lo_tuple>(tuple_data);
            return tuple->LO_ORDERDATE;
        }
        
        virtual qtest_lo_key_extractor_t* clone() const {
            return new qtest_lo_key_extractor_t(*this);
        }
};

struct qtest_lo_key_compare_t : public key_compare_t 
{
     virtual int operator()(const void* key1, const void* key2) const {
            qtest_lo_tuple* a = aligned_cast<qtest_lo_tuple>(key1);
            qtest_lo_tuple* b = aligned_cast<qtest_lo_tuple>(key2);
            
            return a->LO_ORDERDATE - b->LO_ORDERDATE;
        }
        virtual qtest_lo_key_compare_t* clone() const {
            return new qtest_lo_key_compare_t(*this);
        }
};

/***********************************************
 * Joiner and Comparer for the Sort-Merge-Join *
 ***********************************************/

// Left tuples are Lineorders
// Right tuples are Dates

struct qtest_lo_d_join_t : public tuple_join_t {

    qtest_lo_d_join_t ()
        : tuple_join_t(sizeof(qtest_lo_tuple),
                       offsetof(qtest_lo_tuple, LO_ORDERDATE),
                       sizeof(qtest_d_tuple),
                       offsetof(qtest_d_tuple, D_DATEKEY),
                       sizeof(int),
                       sizeof(qtest_join_d_tuple))
    { }

    virtual void join(tuple_t &dest,
                      const tuple_t &left,
                      const tuple_t &right)
    {
    	qtest_lo_tuple* lo = aligned_cast<qtest_lo_tuple>(left.data);
    	qtest_d_tuple* d = aligned_cast<qtest_d_tuple>(right.data);
	qtest_join_d_tuple* ret = aligned_cast<qtest_join_d_tuple>(dest.data);
	
        strcpy(ret->D_DATE, d->D_DATE);
        ret->D_DATEKEY = d->D_DATEKEY;
        ret->LO_ORDERDATE = lo->LO_ORDERDATE;
        ret->LO_ORDERKEY = lo->LO_ORDERKEY;
        ret->LO_LINENUMBER = lo->LO_LINENUMBER;
        ret->LO_REVENUE = lo->LO_REVENUE;

        TRACE ( TRACE_RECORD_FLOW, "JOIN %d %d %d %d {%s} %d\n",ret->LO_LINENUMBER,ret->LO_ORDERKEY,ret->LO_ORDERDATE, ret->D_DATEKEY, ret->D_DATE, ret->LO_REVENUE);
    }

    virtual qtest_lo_d_join_t*  clone() const {
        return new qtest_lo_d_join_t(*this);
    }

    virtual c_str to_string() const {
        return "join LINEORDER and DATE, select LO_LINENUMBER, LO_ORDERKEY, LO_ORDERDATE, D_DATEKEY, D_DATE, LO_REVENUE";
    }
};

// Comparer for the Date keys of the tuples.

struct qtest_lo_d_key_compare_t : public key_compare_t 
{
     virtual int operator()(const void* key1, const void* key2) const {
            int* a = aligned_cast<int>(key1);
            int* b = aligned_cast<int>(key2);
            
            return (*a) - (*b);
        }
        virtual qtest_lo_d_key_compare_t* clone() const {
            return new qtest_lo_d_key_compare_t(*this);
        }
};

/********************
 * Processing Query *
 ********************/

class ssb_qtest_process_tuple_t : public process_tuple_t 
{    
public:
        
    void begin() {
        TRACE(TRACE_QUERY_RESULTS, "*** qtest ANSWER ...\n");
        TRACE(TRACE_QUERY_RESULTS, "*** ...\n");
    }
    
    virtual void process(const tuple_t& output) {
        projected_tuple *tuple;
        tuple = aligned_cast<projected_tuple>(output.data);
        // lo
        // TRACE ( TRACE_QUERY_RESULTS, "PROCESS %d %d %d\n",tuple->LO_ORDERKEY, tuple->LO_ORDERDATE, tuple->LO_REVENUE);
        // d
        // TRACE ( TRACE_QUERY_RESULTS, "PROCESS {%s} %d %d %d\n",tuple->D_DATE, tuple->D_DATEKEY, tuple->D_DAYNUMINWEEK, tuple->D_YEAR);
        // output
        TRACE ( TRACE_QUERY_RESULTS, "PROCESS %d %d %d %d {%s} %d\n", tuple->LO_LINENUMBER, tuple->LO_ORDERKEY, tuple->LO_ORDERDATE, tuple->D_DATEKEY, tuple->D_DATE, tuple->LO_REVENUE);
    }
};

/******************************************************************** 
 *
 * QPIPE q3_2 - Packet creation and submission
 *
 ********************************************************************/


w_rc_t ShoreSSBEnv::xct_qpipe_qtest(const int xct_id, qtest_input_t& in) {
    TRACE(TRACE_ALWAYS, "********** qtest *********\n");

    policy_t* dp = this->get_sched_policy();
    xct_t* pxct = smthread_t::me()->xct();

    // Table Scan Date
    tuple_fifo* d_out_buffer = new tuple_fifo(sizeof (qtest_d_tuple));
    tscan_packet_t* d_tscan_packet =
            new tscan_packet_t("TSCAN DATE",
            d_out_buffer,
            new qtest_date_tscan_filter_t(this, in),
            this->db(),
            _pdate_desc.get(),
            pxct
            //, SH 
            );

    // Sort Dates
    tuple_fifo* sort_date_out = new tuple_fifo(sizeof (qtest_d_tuple));
    packet_t* sort_date_packet =
            new sort_packet_t("ORDER BY D_DATEKEY",
            sort_date_out,
            new trivial_filter_t(sizeof (qtest_d_tuple)),
            new qtest_date_key_extractor_t(),
            new qtest_date_key_compare_t(),
            d_tscan_packet);

    // Table Scan Lineorders
    tuple_fifo* lo_out_buffer = new tuple_fifo(sizeof (qtest_lo_tuple));
    tscan_packet_t* lo_tscan_packet =
            new tscan_packet_t("TSCAN LINEORDER",
            lo_out_buffer,
            new qtest_lineorder_tscan_filter_t(this),
            this->db(),
            _plineorder_desc.get(),
            pxct
            //, SH 
            );

    // Sort Lineorders
    tuple_fifo* sort_lo_out = new tuple_fifo(sizeof (qtest_lo_tuple));
    packet_t* sort_lo_packet =
            new sort_packet_t("ORDER BY LO_ORDERDATE",
            sort_lo_out,
            new trivial_filter_t(sizeof (qtest_lo_tuple)),
            new qtest_lo_key_extractor_t(),
            new qtest_lo_key_compare_t(),
            lo_tscan_packet);

    // Merge-Sort-Join Lineorders and Dates
    tuple_fifo* join_lo_d_out = new tuple_fifo(sizeof (qtest_join_d_tuple));
    packet_t* join_lo_d_packet =
            new sort_merge_join_packet_t("Lineorder - Date JOIN",
            join_lo_d_out,
            new trivial_filter_t(sizeof (qtest_join_d_tuple)),
            sort_lo_packet,
            sort_date_packet,
            new qtest_lo_d_join_t(),
            new qtest_lo_d_key_compare_t());

    qpipe::query_state_t* qs = dp->query_state_create();
    lo_tscan_packet->assign_query_state(qs);
    d_tscan_packet->assign_query_state(qs);
    sort_date_packet->assign_query_state(qs);
    sort_lo_packet->assign_query_state(qs);
    join_lo_d_packet->assign_query_state(qs);

    // Dispatch packet
    ssb_qtest_process_tuple_t pt;

    TRACE(TRACE_ALWAYS, "********** Executing Q TEST *********\n");

    process_query(join_lo_d_packet, pt);
    dp->query_state_destroy(qs);

    return (RCOK);
}


EXIT_NAMESPACE(ssb);
