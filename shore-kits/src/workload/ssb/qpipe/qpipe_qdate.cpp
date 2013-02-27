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

/** @file:   qpipe_qdate.cpp
 *
 *  @brief:  Implementation of simple QPIPE SSB tablescans over Shore-MT
 *
 *  @author: Manos Athanassoulis
 *  @date:   July 2010
 */

#include "workload/ssb/shore_ssb_env.h"
#include "qpipe.h"

using namespace shore;
using namespace qpipe;



ENTER_NAMESPACE(ssb);


/******************************************************************** 
 *
 * QPIPE qdate - Structures needed by operators 
 *
 ********************************************************************/

// We encapsulate the query's helper classes and structures
// inside the class ssb_qcustomer. The purpose of this class
// is just to contain the helper classes and not be instantiated.

class ssb_qdate {
    
public:
    
    typedef ssb_date_tuple date_tuple;
    
    struct count_tuple {
        int COUNT;
    };

    typedef struct count_tuple projected_tuple;

    class date_tscan_filter_t : public tuple_filter_t {
    private:
        ShoreSSBEnv* _ssbdb;
        table_row_t* _prdate;
        rep_row_t _rr;

        ssb_date_tuple _date;

    public:

        date_tscan_filter_t(ShoreSSBEnv* ssbdb, qdate_input_t &in)
        : tuple_filter_t(ssbdb->date_desc()->maxsize()), _ssbdb(ssbdb) {

            // Get a date tupple from the tuple cache and allocate space
            _prdate = _ssbdb->date_man()->get_tuple();
            _rr.set_ts(_ssbdb->date_man()->ts(),
                    _ssbdb->date_desc()->maxsize());
            _prdate->_rep = &_rr;

        }

        ~date_tscan_filter_t() {
            // Give back the date tuple 
            _ssbdb->date_man()->give_tuple(_prdate);
        }


        // Predication

        bool select(const tuple_t &input) {

            // Get next date and read its shipdate
            if (!_ssbdb->date_man()->load(_prdate, input.data)) {
                assert(false); // RC(se_WRONG_DISK_DATA)
            }

            return true;
        }


        // Projection

        void project(tuple_t &d, const tuple_t &s) {

            ssb_qdate::date_tuple *dest;
            dest = aligned_cast<ssb_qdate::date_tuple> (d.data);

            _prdate->get_value(0, _date.D_DATEKEY);
            _prdate->get_value(1, _date.D_DATE, STRSIZE(18));
            _prdate->get_value(2, _date.D_DAYOFWEEK, STRSIZE(9));
            _prdate->get_value(3, _date.D_MONTH, STRSIZE(9));
            _prdate->get_value(4, _date.D_YEAR);
            _prdate->get_value(5, _date.D_YEARMONTHNUM);
            _prdate->get_value(6, _date.D_YEARMONTH, STRSIZE(7));
            _prdate->get_value(7, _date.D_DAYNUMINWEEK);
            _prdate->get_value(8, _date.D_DAYNUMINMONTH);
            _prdate->get_value(9, _date.D_DAYNUMINYEAR);
            _prdate->get_value(10, _date.D_MONTHNUMINYEAR);
            _prdate->get_value(11, _date.D_WEEKNUMINYEAR);
            _prdate->get_value(12, _date.D_SELLINGSEASON, STRSIZE(12));
            _prdate->get_value(13, _date.D_LASTDAYINWEEKFL, 2);
            _prdate->get_value(14, _date.D_LASTDAYINMONTHFL, 2);
            _prdate->get_value(15, _date.D_HOLIDAYFL, 2);
            _prdate->get_value(16, _date.D_WEEKDAYFL, 2);

            TRACE(TRACE_RECORD_FLOW, "%d|%s --\n",
                    _date.D_DATEKEY,
                    _date.D_DATE);
            
            memcpy(dest, &_date, sizeof(_date));
        }

        date_tscan_filter_t* clone() const {
            return new date_tscan_filter_t(*this);
        }

        c_str to_string() const {
            return c_str("date_tscan_filter_t()");
        }
    };

    struct qdate_count_aggregate_t : public tuple_aggregate_t {

        class count_key_extractor_t : public key_extractor_t {
        public:

            count_key_extractor_t()
            : key_extractor_t(0, 0) {
                // We don't need to form groups in the count,
                // so the key size is 0. As such, No hints or comparisons 
                // should be made by the aggregator.
            }

            virtual key_extractor_t* clone() const {
                return new count_key_extractor_t(*this);
            }
        };

        count_key_extractor_t _extractor;

        qdate_count_aggregate_t()
        : tuple_aggregate_t(sizeof (count_tuple)) {

        }

        virtual key_extractor_t * key_extractor() {
            return &_extractor;
        }

        virtual void aggregate(char* agg_data, const tuple_t &) {
            count_tuple* agg = aligned_cast<count_tuple > (agg_data);
            agg->COUNT++;
        }

        virtual void finish(tuple_t &d, const char* agg_data) {
            count_tuple* agg = aligned_cast<count_tuple > (agg_data);
            count_tuple* output = aligned_cast<count_tuple > (d.data);
            output->COUNT = agg->COUNT;
        }

        virtual qdate_count_aggregate_t * clone() const {
            return new qdate_count_aggregate_t(*this);
        }

        virtual c_str to_string() const {
            return "qdate_count_aggregate_t";
        }
    };

    static const c_str* dump_tuple(tuple_t* tup) {
        ssb_qdate::date_tuple *dest;
        dest = aligned_cast<ssb_qdate::date_tuple> (tup->data);
        return new c_str("%d|%s|%s|%s|%d|%d|%s|%d|%d|%d|%d|%d|%s|%s|%s|%s|%s|\n",
                dest->D_DATEKEY,
                dest->D_DATE,
                dest->D_DAYOFWEEK,
                dest->D_MONTH,
                dest->D_YEAR,
                dest->D_YEARMONTHNUM,
                dest->D_YEARMONTH,
                dest->D_DAYNUMINWEEK,
                dest->D_DAYNUMINMONTH,
                dest->D_DAYNUMINYEAR,
                dest->D_MONTHNUMINYEAR,
                dest->D_WEEKNUMINYEAR,
                dest->D_SELLINGSEASON,
                dest->D_LASTDAYINWEEKFL,
                dest->D_LASTDAYINMONTHFL,
                dest->D_HOLIDAYFL,
                dest->D_WEEKDAYFL);
    }
    
};

class ssb_qdate_process_tuple_t : public process_tuple_t 
{    
public:
        
    void begin() {
        TRACE(TRACE_QUERY_RESULTS, "*** QDATE ANSWER ...\n");
        TRACE(TRACE_QUERY_RESULTS, "*** QDATE COUNT...\n");
    }
    
    virtual void process(const tuple_t& output) {
        ssb_qdate::projected_tuple *tuple;
        tuple = aligned_cast<ssb_qdate::projected_tuple>(output.data);
        TRACE( TRACE_QUERY_RESULTS, "%d\n", tuple->COUNT);
    }
};



/******************************************************************** 
 *
 * QPIPE qdate - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_qpipe_qdate(const int xct_id, 
                                  qdate_input_t& in)
{
    TRACE( TRACE_ALWAYS, "********** qdate *********\n");

   
    policy_t* dp = this->get_sched_policy();
    xct_t* pxct = smthread_t::me()->xct();
    

    // TSCAN PACKET
    tuple_fifo* tscan_out_buffer =
        new tuple_fifo(sizeof(ssb_qdate::date_tuple));
    tscan_packet_t* date_tscan_packet =
        new tscan_packet_t("TSCAN DATE",
                           tscan_out_buffer,
                           new ssb_qdate::date_tscan_filter_t(this,in),
                           this->db(),
                           _pdate_desc.get(),
                           pxct
                           //, SH 
                           );

    tuple_fifo* fdump_output = new tuple_fifo(sizeof (ssb_qdate::date_tuple));
    fdump_packet_t* fdump_packet =
            new fdump_packet_t(c_str("FDUMP"),
            fdump_output,
            new trivial_filter_t(fdump_output->tuple_size()),
            NULL,
            c_str("%s/dates.tbl", getenv("HOME")),
            NULL,
            date_tscan_packet,
            ssb_qdate::dump_tuple);

    tuple_fifo* agg_output = new tuple_fifo(sizeof (ssb_qdate::projected_tuple));
    aggregate_packet_t* agg_packet =
            new aggregate_packet_t(c_str("COUNT_AGGREGATE"),
            agg_output,
            new trivial_filter_t(agg_output->tuple_size()),
            new ssb_qdate::qdate_count_aggregate_t,
            new ssb_qdate::qdate_count_aggregate_t::count_key_extractor_t(),
            fdump_packet);
    
    qpipe::query_state_t* qs = dp->query_state_create();
    date_tscan_packet->assign_query_state(qs);
    fdump_packet->assign_query_state(qs);
    agg_packet->assign_query_state(qs);
        
    // Dispatch packet
    ssb_qdate_process_tuple_t pt;
    process_query(agg_packet, pt);
    dp->query_state_destroy(qs);

    return (RCOK); 
}


EXIT_NAMESPACE(ssb);
