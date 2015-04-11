#ifndef LOGFACTORY_H
#define LOGFACTORY_H

#include "w_defines.h"

#define SM_SOURCE
#include "sm_int_1.h"

#include <sstream>
#include <boost/random.hpp>

class LogFactory {
private:
	/* Every incr_th records generated, increase the maxPageId by a ratio of incr_ratio */
	const unsigned INCR_TH;
	const float INCR_RATIO;

        bool sorted;
	unsigned max_page_id;
	vector<lsn_t> prev_lsn;	/* Keep track of previous log record on same page */
	uint4_t generatedCount;	/* Keep track of how many log records were generated */
	uint4_t currentFile;
	uint4_t currentLSN;

	boost::random::mt19937 gen;	/* Random Number Generator */
	boost::random::uniform_real_distribution<double> dDist; //[min,max)
	boost::random::discrete_distribution<> dd_type;

	unsigned nextType();
	unsigned nextLength(uint4_t type);
	unsigned nextZipf();

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

	/* Stats of TPCC, SF=10, 4 threads, 90s */
	class TPCC_Stats {
	public:
                /* probType[i] = probability of type i */
		vector<float> probType;
                /* probLengthIndex[i][j] = probability of length with index j for type i */
		vector<vector<float> > probLengthIndex;
                /* lengthIndexToLength[i][j] = translate length index j of type i
                 * to actual length */
		vector<vector<uint4_t> > lengthIndexToLength;

		 TPCC_Stats() : probType(logrec_t::t_max_logrec,0),
			    probLengthIndex(logrec_t::t_max_logrec, vector<float>(5,0.0)),
			    lengthIndexToLength(logrec_t::t_max_logrec, vector<uint4_t>(5,0)) {
			probType[0] = 0.0001183;
			probType[1] = 0;
			probType[2] = 0;
			probType[3] = 8.60363e-05;
			probType[4] = 0.000440936;
			probType[5] = 8.60363e-05;
			probType[6] = 6.45272e-05;
			probType[7] = 8.60363e-05;
			probType[8] = 1.07545e-05;
			probType[9] = 2.15091e-05;
			probType[10] = 0;
			probType[11] = 0.0455777;
			probType[12] = 5.37727e-05;
			probType[13] = 0;
			probType[14] = 0;
			probType[15] = 0.000150564;
			probType[16] = 0.0413942;
			probType[17] = 0.0412437;
			probType[18] = 0;
			probType[19] = 0;
			probType[20] = 0;
			probType[21] = 0;
			probType[22] = 0;
			probType[23] = 0.00659253;
			probType[24] = 0.00903381;
			probType[25] = 2.15091e-05;
			probType[26] = 0.00112923;
			probType[27] = 0;
			probType[28] = 0;
			probType[29] = 0;
			probType[30] = 0.00730233;
			probType[31] = 0.0146477;
			probType[32] = 0.00244128;
			probType[33] = 0.00903381;
			probType[34] = 0.00149488;
			probType[35] = 0.261529;
			probType[36] = 0;
			probType[37] = 0.295933;
			probType[38] = 0;
			probType[39] = 0;
			probType[40] = 0;
			probType[41] = 0;
			probType[42] = 0.260034;
			probType[43] = 0.00147337;
			probType[44] = 0;
			probType[45] = 0;

			probLengthIndex[0][0] = 0.272727;
			probLengthIndex[0][1] = 0.454545;
			probLengthIndex[0][2] = 0.272727;
			probLengthIndex[3][0] = 1;
			probLengthIndex[4][0] = 0.926829;
			probLengthIndex[4][1] = 0.0243902;
			probLengthIndex[4][2] = 0.0243902;
			probLengthIndex[4][3] = 0.0243902;
			probLengthIndex[5][0] = 0.625;
			probLengthIndex[5][1] = 0.125;
			probLengthIndex[5][2] = 0.125;
			probLengthIndex[5][3] = 0.125;
			probLengthIndex[6][0] = 1;
			probLengthIndex[7][0] = 1;
			probLengthIndex[8][0] = 1;
			probLengthIndex[9][0] = 1;
			probLengthIndex[11][0] = 0.00188768;
			probLengthIndex[11][1] = 0.998112;
			probLengthIndex[12][0] = 1;
			probLengthIndex[15][0] = 1;
			probLengthIndex[16][0] = 1;
			probLengthIndex[17][0] = 1;
			probLengthIndex[23][0] = 1;
			probLengthIndex[24][0] = 1;
			probLengthIndex[25][0] = 1;
			probLengthIndex[26][0] = 1;
			probLengthIndex[30][0] = 1;
			probLengthIndex[31][0] = 0.66373;
			probLengthIndex[31][1] = 0.165932;
			probLengthIndex[31][2] = 0.166667;
			probLengthIndex[31][3] = 0.00293686;
			probLengthIndex[31][4] = 0.000734214;
			probLengthIndex[32][0] = 0.995595;
			probLengthIndex[32][1] = 0.00440529;
			probLengthIndex[33][0] = 0.729762;
			probLengthIndex[33][1] = 0.270238;
			probLengthIndex[34][0] = 0.985611;
			probLengthIndex[34][1] = 0.0143885;
			probLengthIndex[35][0] = 0.848014;
			probLengthIndex[35][1] = 0.0759931;
			probLengthIndex[35][2] = 0.0759931;
			probLengthIndex[37][0] = 0.140386;
			probLengthIndex[37][1] = 0.682196;
			probLengthIndex[37][2] = 0.0722099;
			probLengthIndex[37][3] = 0.0722099;
			probLengthIndex[37][4] = 0.0329978;
			probLengthIndex[42][0] = 0.84714;
			probLengthIndex[42][1] = 0.15286;
			probLengthIndex[43][0] = 1;

			lengthIndexToLength[0][0] = 72;
			lengthIndexToLength[0][1] = 88;
			lengthIndexToLength[0][2] = 80;
			lengthIndexToLength[3][0] = 56;
			lengthIndexToLength[4][0] = 24560;
			lengthIndexToLength[4][1] = 9632;
			lengthIndexToLength[4][2] = 4856;
			lengthIndexToLength[4][3] = 12824;
			lengthIndexToLength[5][0] = 64;
			lengthIndexToLength[5][1] = 160;
			lengthIndexToLength[5][2] = 192;
			lengthIndexToLength[5][3] = 128;
			lengthIndexToLength[6][0] = 320;
			lengthIndexToLength[7][0] = 64;
			lengthIndexToLength[8][0] = 320;
			lengthIndexToLength[9][0] = 320;
			lengthIndexToLength[11][0] = 64;
			lengthIndexToLength[11][1] = 80;
			lengthIndexToLength[12][0] = 56;
			lengthIndexToLength[15][0] = 48;
			lengthIndexToLength[16][0] = 48;
			lengthIndexToLength[17][0] = 48;
			lengthIndexToLength[23][0] = 48;
			lengthIndexToLength[24][0] = 64;
			lengthIndexToLength[25][0] = 64;
			lengthIndexToLength[26][0] = 72;
			lengthIndexToLength[30][0] = 64;
			lengthIndexToLength[31][0] = 856;
			lengthIndexToLength[31][1] = 536;
			lengthIndexToLength[31][2] = 80;
			lengthIndexToLength[31][3] = 776;
			lengthIndexToLength[31][4] = 488;
			lengthIndexToLength[32][0] = 3736;
			lengthIndexToLength[32][1] = 3368;
			lengthIndexToLength[33][0] = 72;
			lengthIndexToLength[33][1] = 80;
			lengthIndexToLength[34][0] = 128;
			lengthIndexToLength[34][1] = 64;
			lengthIndexToLength[35][0] = 128;
			lengthIndexToLength[35][1] = 96;
			lengthIndexToLength[35][2] = 72;
			lengthIndexToLength[37][0] = 280;
			lengthIndexToLength[37][1] = 688;
			lengthIndexToLength[37][2] = 1432;
			lengthIndexToLength[37][3] = 264;
			lengthIndexToLength[37][4] = 80;
			lengthIndexToLength[42][0] = 104;
			lengthIndexToLength[42][1] = 96;
			lengthIndexToLength[43][0] = 104;
		}
	};
	TPCC_Stats stats;

public:
	/*
	 * max_page_id = 233220, similar to a TPCC with SF=10 (~2GB with 8KB pages).
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

	void nextRecord(void* addr);
};

#endif
