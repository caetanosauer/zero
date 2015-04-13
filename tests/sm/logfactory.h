#ifndef LOGFACTORY_H
#define LOGFACTORY_H

#include "w_defines.h"

#define SM_SOURCE
#include "sm_int_1.h"

#include <sstream>
#include <boost/random.hpp>

#include "stats.h"

class LogFactory {
private:
	/* fake_logrec_t used to have access to private members of logrec_t. We 
	 * need to change this to reflect Zero log records.
	 */
	class fake_logrec_t {
	public:
		fake_logrec_t();
		const lsn_t&	lsn_ck() const {  return *_lsn_ck(); }
		enum {
			max_sz = 3 * sizeof(generic_page),
			hdr_sz = (
				sizeof(uint2_t) +   // _len
				sizeof(u_char) +    // _type
				sizeof(u_char) +    //  _cat
				sizeof(tid_t) +     // _tid
				sizeof(shpid_t) +   // _shpid
				sizeof(vid_t) +     // _vid
				sizeof(uint2_t) +   // _page_tag
				sizeof(snum_t) +    // _snum
				sizeof(lsn_t) +     // _prev // ctns possibly 4 extra
				sizeof(lsn_t) +     // _prev_page
				0
			)
		};

		enum {
			data_sz = max_sz - (hdr_sz + sizeof(lsn_t))
		};

		uint2_t		_len;
		u_char      _type;
		u_char      _cat;
		shpid_t     _shpid;
		tid_t       _tid;
		vid_t       _vid;
		uint2_t     _page_tag;
		snum_t      _snum;
		lsn_t       _prev;
		lsn_t       _prev_page;
		char       _data[data_sz + sizeof(lsn_t)];

		lsn_t*	_lsn_ck() const {
			size_t lo_offset = _len - (hdr_sz + sizeof(lsn_t));
		    w_assert3(alignon(_data+lo_offset, 8));
		    lsn_t *where = (lsn_t*)(_data+lo_offset);
		    return where;
		}
	};

	Stats stats;

	/* Every INCR_TH records generated, increase the max_page_id by a ratio 
	 * of INCR_RATIO.
	 */
	const unsigned INCR_TH;
	const unsigned INCR_RATIO;

    bool sorted;
	unsigned max_page_id;
	vector<lsn_t> prev_lsn;	/* Keep track of previous log record on same page */
	uint4_t generatedCount;	/* Keep track of how many log records were generated */
	lsn_t nextLSN;

	boost::random::mt19937 gen;	/* Random Number Generator */
	boost::random::uniform_real_distribution<double> dDist; //[min,max)

	unsigned nextZipf();

public:
	/* max_page_id = 233220, similar to a TPCC with SF=10 (~2GB with 8KB pages).
	 * th(increase_threshold) = increase database every 1000 log records generated.
	 * ratio = increment on number of pages at the threshold above
	 */
	LogFactory(
                bool sorted = false, // generates sorted log archive
                unsigned max_page_id = 233220,
                unsigned th = 100,
                unsigned increment = 10
        );
	~LogFactory();

	//void open(lsn_t endLSN); SHOULD THIS BE IMPLEMENTED?
    bool next(fake_logrec_t*& lr);
    lsn_t getNextLSN() { return nextLSN; }

};

#endif
