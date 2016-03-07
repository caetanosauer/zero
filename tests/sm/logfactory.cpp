#include "logfactory.h"

const uint32_t LogFactory::factory_version_major = 6;
const uint32_t LogFactory::factory_version_minor = 1;

/**
 *    params:
 *        max_page_id: self-explained
 *        th: increase max_page_id after th records were generated
 *        ratio: increase max_page_id by ratio (max_page_id =* ratio)
 */
LogFactory::LogFactory(bool sorted, unsigned max_page_id, unsigned th,
        unsigned increment)
    :
    INCR_TH(th), INCR_RATIO(increment), sorted(sorted),
    current_page_id(0), max_page_id(max_page_id),
    prev_lsn(max_page_id, lsn_t::null), generatedCount(0), nextLSN(1,0),
    gen(1729), dDist(0.0,1.0)
{
    // if (factory_version_major != log_storage::_version_major
    //     ||
    //     factory_version_minor > log_storage::_version_minor) {
    //     W_FATAL_MSG(fcINTERNAL,
    //             << "Version of LogFactory does not match");
    // }
    // if (Stats::stats_version_major != factory_version_major
    //     ||
    //     Stats::stats_version_minor > factory_version_minor) {
    //     W_FATAL_MSG(fcINTERNAL,
    //             << "Version of LogFactory Stats does not match");
    // }
}

LogFactory::~LogFactory() {
}

bool LogFactory::next(void* addr) {
    fake_logrec_t* lr = new (addr) fake_logrec_t;

    lr->header._type = stats.nextType();
    lr->header._len = stats.nextLength(lr->header._type);
    lr->header._cat = fake_logrec_t::t_status;
    if(sorted) {
        lr->header._pid = nextSorted();
    }
    else {
        lr->header._pid = nextZipf();
    }
    lr->header._page_tag = 1;
    lr->header._stid = 1;    // storage number
    lr->header._page_prv = prev_lsn[lr->header._pid];

    lr->xidInfo._xid = 1;    // transaction id
    lr->xidInfo._xid_prv = lsn_t::null;

    // TODO: do we need to differentiate between SSX and non-SSX logrecs?
    /* No. The only difference is that SSX records do not have the xidInfo
     * structure. Since the LogFactory is not interested in these fields, we
     * can treat SSX as normal log records. */

    /* Fill the data portion of the log record with 6s (luck number) */
    memset(lr->_data, 6, lr->header._len - (logrec_t::hdr_non_ssx_sz + sizeof(lsn_t)));

    /* Check if currentLSN will overflow */
    if((nextLSN.rba()+lr->header._len) > lsn_t::max.rba()) {
        nextLSN = lsn_t(nextLSN.file()+1, 0);
    }

    lr->set_lsn_ck(nextLSN); // set lsn in the last 8 bytes
    prev_lsn[lr->header._pid] = lr->get_lsn_ck();
    nextLSN += lr->header._len;

    generatedCount++;

    //DBGTHRD(<< "New log record created: ["
                //<< ((logrec_t*)r)->type_str() << ", "
        //<< r->_len << "," << r->_pid << ","
                //<< r->_prev_page << "," << *(r->_lsn_ck()) << "]");
    return true;
}

/**
 * Used to generated a page_id following a Zipfian Distribution of (80,20).
 * Generate page_id in the range [0, max_page_id] (at least I hope so).
 */
unsigned LogFactory::nextZipf() {
    if(generatedCount != 0 && generatedCount % INCR_TH == 0) {
        max_page_id += INCR_RATIO;
        prev_lsn.resize(max_page_id, lsn_t::null);
    }

    //if (sorted) {
    //    return max_page_id;
    //}

    double h = 0.2;
    int r = (int) ((max_page_id+1) * pow(dDist(gen), (log(h)/log(1-h))));

    return max_page_id - r; /* Try to favor higher page ids */
}

unsigned LogFactory::nextSorted() {
    if(generatedCount != 0 && generatedCount % INCR_TH == 0) {
        max_page_id += INCR_RATIO;
        prev_lsn.resize(max_page_id, lsn_t::null);
        DBGTHRD(<< "Increased max_page_id to " << max_page_id);
    }

    if(dDist(gen) <= 0.75) {
        DBGTHRD(<< current_page_id);
        return current_page_id; // returns the same page_id with 75% chance
    }
    else{
        /* Increase the current_page_id to anything between the current_page_id
         * and the max_page_id. Use Zipf distribution to force the new
           current_page_id to not jump too much. In other words, the new
           current_page_id has a 80% chance (1-h) to have a value in lower range
           of 20% (h) of the range [current_page_id, max_page_id]. */
        double h = 0.2;
        current_page_id += (int) ((max_page_id-current_page_id+1) * pow(dDist(gen), (log(h)/log(1-h))));
        DBGTHRD(<< current_page_id);
        return current_page_id;
    }
}

void LogFactory::resetRun() {
    current_page_id = 0;
    DBGTHRD(<< "resetRun()");
}
