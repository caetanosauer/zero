#include "logfactory.h"

/**
 *	params:
 *		max_page_id: self-explained
 *		th: increase max_page_id after th records were generated
 *		ratio: increase max_page_id by ratio (max_page_id =* ratio)
 */
LogFactory::LogFactory(uint_t max_page_id, uint4_t th, float ratio):
						INCR_TH(th), INCR_RATIO(ratio), max_page_id(max_page_id),
						gen(1729),  dDist(0.0,1.0),
						generatedCount(0),  currentFile(1), currentLSN(0), dd_type(stats.probType), prev_lsn(max_page_id, lsn_t::null) {
}

LogFactory::~LogFactory() {
}

LogFactory::fake_logrec_t::fake_logrec_t() { }

void LogFactory::nextRecord(char* addr) {
	fake_logrec_t* r = new(addr) fake_logrec_t();

	r->_type = nextType();
	r->_len = nextLength(r->_type);
	r->_cat = 1;
	r->_shpid = nextZipf();
	r->_tid = 1;	// transaction id
	r->_vid = 1;	// volume id
	r->_page_tag = 0;
	r->_snum = 1;	// storage number
	r->_prev = lsn_t::null;
	r->_prev_page = prev_lsn[r->_shpid];
	//strcpy(r->_data,"myLogRecord");
	memset(r->_data, 6, r->_len - (logrec_t::hdr_sz + sizeof(lsn_t)));

	/* Check if currentLSN will overflow */
	if((currentLSN+r->_len) <= currentLSN) {
		currentFile++;
		currentLSN = 0;
	}

	*(r->_lsn_ck()) = lsn_t(currentFile,currentLSN);
	prev_lsn[r->_shpid] = *(r->_lsn_ck());

	currentLSN += r->_len;

	generatedCount++;

	DBGTHRD(<< "New log record created: [" << ((logrec_t*)r)->type_str() << ", "
										   << r->_len << ","
										   << r->_shpid << ","
										   << r->_prev_page << ","
										   << *(r->_lsn_ck()) << "]");
}

/**
 * Generates a random type following a distribution based on the statistics of log records
 * created by the execution of a benchmark.
 */
uint4_t LogFactory::nextType() {
	uint4_t type = uint4_t(dd_type(gen));
	return type;
}

/* Generates a random length given a certain type, following a distribution based on the statistics
 * of log records created by the execution of a benchmark.
 */
uint4_t LogFactory::nextLength(uint4_t type) {
	boost::random::discrete_distribution<> dd_length(stats.probLengthIndex[type]);
	int lengthIndex = dd_length(gen);
	uint4_t length = stats.lengthIndexToLength[type][lengthIndex];
	return length;
}

/**
 * Used to generated a page_id following a Zipfian Distribution of (80,20).
 * Generate page_id in the range [0, max_page_id] (at least I hope so).
 */
int LogFactory::nextZipf() {
	if(generatedCount >= INCR_TH) {
		max_page_id *= INCR_RATIO;
	}
	/*resize prev_lsn*/
	prev_lsn.resize(max_page_id, lsn_t::null);

	double h = 0.2;
	int r = (int) ((max_page_id+1) * pow(dDist(gen), (log(h)/log(1-h))));

	return max_page_id - r; /* Try to favor higher page ids */
}