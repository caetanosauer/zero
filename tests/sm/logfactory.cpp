#include "logfactory.h"

/**
 *	params:
 *		max_page_id: self-explained
 *		th: increase max_page_id after th records were generated
 *		ratio: increase max_page_id by ratio (max_page_id =* ratio)
 */
LogFactory::LogFactory(bool sorted, unsigned max_page_id, unsigned th,
        unsigned increment)
    :
    INCR_TH(th), INCR_RATIO(increment), sorted(sorted),
    max_page_id(max_page_id), prev_lsn(max_page_id, lsn_t::null),
    generatedCount(0), gen(1729), dDist(0.0,1.0)
{
}

LogFactory::~LogFactory() {
}

LogFactory::fake_logrec_t::fake_logrec_t() { }

bool LogFactory::next(fake_logrec_t*& lr) {
	new (lr) fake_logrec_t;

	lr->_type = stats.nextType();
	lr->_len = stats.nextLength(lr->_type);
	lr->_cat = 1;
	lr->_shpid = nextZipf();
	lr->_tid = 1;	// transaction id
	lr->_vid = 1;	// volume id
	lr->_page_tag = 0;
	lr->_snum = 1;	// storage number
	lr->_prev = lsn_t::null;
	lr->_prev_page = prev_lsn[lr->_shpid];
        // TODO: do we need to differentiate between SSX and non-SSX logrecs?
	/* Fill the data portion of the log record with 6s (luck number) */
	memset(lr->_data, 6, lr->_len - (logrec_t::hdr_non_ssx_sz + sizeof(lsn_t)));

	/* Check if currentLSN will overflow */
	if((nextLSN.rba()+lr->_len) > lsn_t::max.rba()) {
		nextLSN = lsn_t(nextLSN.file()+1, 0);
	}

	*(lr->_lsn_ck()) = nextLSN;
	prev_lsn[lr->_shpid] = *(lr->_lsn_ck());
	nextLSN += lr->_len;

	generatedCount++;

	//DBGTHRD(<< "New log record created: ["
                //<< ((logrec_t*)r)->type_str() << ", "
		//<< r->_len << "," << r->_shpid << ","
                //<< r->_prev_page << "," << *(r->_lsn_ck()) << "]");
	return true;        
}

/**
 * Used to generated a page_id following a Zipfian Distribution of (80,20).
 * Generate page_id in the range [0, max_page_id] (at least I hope so).
 */
unsigned LogFactory::nextZipf() {
	if(generatedCount % INCR_TH == 0) {
		max_page_id += INCR_RATIO;
        prev_lsn.resize(max_page_id, lsn_t::null);
	}

    if (sorted) {
        return max_page_id;
    }

	double h = 0.2;
	int r = (int) ((max_page_id+1) * pow(dDist(gen), (log(h)/log(1-h))));

	return max_page_id - r; /* Try to favor higher page ids */
}
