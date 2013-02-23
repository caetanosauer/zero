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

/** @file:   qpipe_qpart.cpp
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
 * QPIPE QPART - Structures needed by operators 
 *
 ********************************************************************/

// We encapsulate the query's helper classes and structures
// inside the class ssb_qcustomer. The purpose of this class
// is just to contain the helper classes and not be instantiated.

class ssb_qpart {

public:

    typedef ssb_part_tuple part_tuple;
    
//    struct part_tuple {
//        int P_PARTKEY;
//        char P_NAME [STRSIZE(22)];
//    };

    struct count_tuple {
        long COUNT;
    };

    typedef struct count_tuple projected_tuple;

    class part_tscan_filter_t : public tuple_filter_t {
    private:
        ShoreSSBEnv* _ssbdb;
        table_row_t* _prpart;
        rep_row_t _rr;

        ssb_part_tuple _part;

    public:

        part_tscan_filter_t(ShoreSSBEnv* ssbdb, qpart_input_t &in)
        : tuple_filter_t(ssbdb->part_desc()->maxsize()), _ssbdb(ssbdb) {

            // Get a part tupple from the tuple cache and allocate space
            _prpart = _ssbdb->part_man()->get_tuple();
            _rr.set_ts(_ssbdb->part_man()->ts(),
                    _ssbdb->part_desc()->maxsize());
            _prpart->_rep = &_rr;

        }

        ~part_tscan_filter_t() {
            // Give back the part tuple 
            _ssbdb->part_man()->give_tuple(_prpart);
        }


        // Predication

        bool select(const tuple_t &input) {

            // Get next part and read its shippart
            if (!_ssbdb->part_man()->load(_prpart, input.data)) {
                assert(false); // RC(se_WRONG_DISK_DATA)
            }

            return (true);
        }


        // Projection

        void project(tuple_t &d, const tuple_t &/*s*/) {

            ssb_qpart::part_tuple *dest;
            dest = aligned_cast<ssb_qpart::part_tuple> (d.data);

            _prpart->get_value(0, _part.P_PARTKEY);
            _prpart->get_value(1, _part.P_NAME, STRSIZE(22));
            _prpart->get_value(2, _part.P_MFGR, STRSIZE(6));
            _prpart->get_value(3, _part.P_CATEGORY, STRSIZE(7));
            _prpart->get_value(4, _part.P_BRAND, STRSIZE(9));
            _prpart->get_value(5, _part.P_COLOR, STRSIZE(11));
            _prpart->get_value(6, _part.P_TYPE, STRSIZE(25));
            _prpart->get_value(7, _part.P_SIZE);
            _prpart->get_value(8, _part.P_CONTAINER, STRSIZE(10));

            TRACE(TRACE_RECORD_FLOW, "%d|%s|%s| --d\n",
                    _part.P_PARTKEY,
                    _part.P_NAME,
                    _part.P_CATEGORY);

//            dest->P_PARTKEY = _part.P_PARTKEY;
//            strcpy(dest->P_NAME, _part.P_NAME);
            memcpy(dest, &_part, sizeof(_part));
        }

        part_tscan_filter_t* clone() const {
            return new part_tscan_filter_t(*this);
        }

        c_str to_string() const {
            return c_str("part_tscan_filter_t()");
        }
    };

    struct qpart_count_aggregate_t : public tuple_aggregate_t {

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

        qpart_count_aggregate_t()
        : tuple_aggregate_t(sizeof(count_tuple)) {

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

        virtual qpart_count_aggregate_t * clone() const {
            return new qpart_count_aggregate_t(*this);
        }

        virtual c_str to_string() const {
            return "qpart_count_aggregate_t";
        }
    };

    static const c_str* dump_tuple(tuple_t* tup) {
        ssb_qpart::part_tuple *dest;
        dest = aligned_cast<ssb_qpart::part_tuple> (tup->data);
        return new c_str("%d|%s|%s|%s|%s|%s|%s|%d|%s|\n",
                dest->P_PARTKEY,
                dest->P_NAME,
                dest->P_MFGR,
                dest->P_CATEGORY,
                dest->P_BRAND,
                dest->P_COLOR,
                dest->P_TYPE,
                dest->P_SIZE,
                dest->P_CONTAINER);
    }

};

class ssb_qpart_process_tuple_t : public process_tuple_t 
{    
public:
        
    void begin() {
        TRACE(TRACE_QUERY_RESULTS, "*** QPART ANSWER ...\n");
        TRACE(TRACE_QUERY_RESULTS, "*** SUM_QTY\tSUM_BASE\tSUM_DISC...\n");
    }
    
    virtual void process(const tuple_t& output) {
        ssb_qpart::projected_tuple *tuple;
        tuple = aligned_cast<ssb_qpart::projected_tuple>(output.data);
        TRACE( TRACE_QUERY_RESULTS, "%ld\n", tuple->COUNT);

    }
};



/******************************************************************** 
 *
 * QPIPE QPART - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_qpipe_qpart(const int xct_id, 
                                  qpart_input_t& in)
{
    TRACE( TRACE_ALWAYS, "********** QPART *********\n");

   
    policy_t* dp = this->get_sched_policy();
    xct_t* pxct = smthread_t::me()->xct();
    

    // TSCAN PACKET
    tuple_fifo* tscan_out_buffer =
        new tuple_fifo(sizeof (ssb_qpart::part_tuple));
    tscan_packet_t* part_tscan_packet =
        new tscan_packet_t("TSCAN PART",
                           tscan_out_buffer,
                           new ssb_qpart::part_tscan_filter_t(this,in),
                           this->db(),
                           _ppart_desc.get(),
                           pxct
                           //, SH 
                           );

    tuple_fifo* fdump_output = new tuple_fifo(sizeof (ssb_qpart::part_tuple));
    fdump_packet_t* fdump_packet =
            new fdump_packet_t(c_str("FDUMP"),
            fdump_output,
            new trivial_filter_t(fdump_output->tuple_size()),
            NULL,
            c_str("%s/parts.tbl", getenv("HOME")),
            NULL,
            part_tscan_packet,
            ssb_qpart::dump_tuple);

    tuple_fifo* agg_output = new tuple_fifo(sizeof (ssb_qpart::count_tuple));
    aggregate_packet_t* agg_packet =
            new aggregate_packet_t(c_str("COUNT_AGGREGATE"),
            agg_output,
            new trivial_filter_t(agg_output->tuple_size()),
            new ssb_qpart::qpart_count_aggregate_t(),
            new ssb_qpart::qpart_count_aggregate_t::count_key_extractor_t(),
            fdump_packet);
    
    qpipe::query_state_t* qs = dp->query_state_create();
    part_tscan_packet->assign_query_state(qs);
    fdump_packet->assign_query_state(qs);
    agg_packet->assign_query_state(qs);

    // Dispatch packet
    ssb_qpart_process_tuple_t pt;
    process_query(agg_packet, pt);
    dp->query_state_destroy(qs);

    return (RCOK); 
}


EXIT_NAMESPACE(ssb);
