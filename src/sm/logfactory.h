#ifndef LOGFACTORY_H
#define LOGFACTORY_H

#include "w_defines.h"

#define SM_SOURCE
#include "sm_int_1.h"

#include <map>
#include <sstream>
#include <boost/random.hpp>

class LogFactory {
private:
	class fake_logrec_t {
	public:
		fake_logrec_t();
		const lsn_t&	lsn_ck() const {  return *_lsn_ck(); }

                /*
                 * TODO: describe in a comment why we need these fake logrecs
                 * TODO: can't we just use the enums from logrec_T? e.g. logrec_t::max_sz
                 */
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


	/* Every incr_th records generated, increase the maxPageId by a ratio of incr_ratio */
	const int BLK_SIZE;
	const int INCR_TH;
	const int INCR_RATIO;
	int max_page_id;

	//boost::random::random_device rd; //If non-determinist numbers are needed
	boost::random::mt19937 rng;	/* Random Number Generator */
	boost::random::uniform_real_distribution<double> dDist; //[min,max)
	boost::random::uniform_int_distribution<int>* iDist; //[min,max]

	/* Keeps track of how many log records were generated */
	int generatedCount;

	int currentFile;
	int currentLSN;

	char* record;
	char* block;
	char* block_buffer;
	int block_currentPos;

	map<int, int> typeToRecords;				// < type , #LogRecords >
	map<int, int> cummulativeRecordsToType;		// < cumulative#LogRecords , type >
	map<pair<int,int>, int> lengthStats;		// < <type,cumulative#LogRecordsByType> , length >

	int loadRecordStats(string fileName);
	int nextType();
	int nextLength(int type);
	int nextZipf();


public:
	LogFactory(int blk_size, string file_name, int max_page_id, int th, int ratio);
	~LogFactory();

	char* nextBlock();
	void nextRecord(char* addr);

	void printLogRecord(const logrec_t& lr);
	void printRecordStats();
};

#endif
