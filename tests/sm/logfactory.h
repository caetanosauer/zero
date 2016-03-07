#ifndef LOGFACTORY_H
#define LOGFACTORY_H

#include "w_defines.h"

#define SM_SOURCE
#include "sm_base.h"

#include <boost/random.hpp>

#include "stats.h"

class LogFactory {
public:
    /* max_page_id = 233220, similar to a TPCC with SF=10 (~2GB with 8KB pages).
     * th(increase_threshold) = increase database every 100 log records generated.
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
    bool next(void* addr);
    lsn_t getNextLSN() { return nextLSN; }
    void resetRun();

private:
    /* fake_logrec_t used to have access to private members of logrec_t.
     */
    class shore_fake_logrec_t {
    public:
        enum {
            max_sz = 3 * sizeof(generic_page),
            hdr_sz = (
                sizeof(uint16_t) +   // _len
                sizeof(u_char) +    // _type
                sizeof(u_char) +    //  _cat
                sizeof(tid_t) +     // _tid
                sizeof(PageID) +   // _shpid
                sizeof(uint16_t) +   // _page_tag
                sizeof(StoreID) +    // _snum
                sizeof(lsn_t) +     // _prev // ctns possibly 4 extra
                sizeof(lsn_t) +     // _prev_page
                0
            )
        };

        enum {
            data_sz = max_sz - (hdr_sz + sizeof(lsn_t))
        };

        uint16_t        _len;
        u_char      _type;
        u_char      _cat;
        PageID     _shpid;
        tid_t       _tid;
        uint16_t     _page_tag;
        StoreID      _snum;
        lsn_t       _prev;
        lsn_t       _prev_page;
        char       _data[data_sz + sizeof(lsn_t)];

        const lsn_t     get_lsn_ck() const {
                            lsn_t    tmp = *_lsn_ck();
                            return tmp;
                        }
        void            set_lsn_ck(const lsn_t &lsn_ck) {
                            lsn_t& where = *_lsn_ck();
                            where = lsn_ck;
                        }
    private:
        lsn_t*          _lsn_ck() const {
                            size_t lo_offset = _len - (hdr_sz + sizeof(lsn_t));
                            w_assert3(alignon(_data+lo_offset, 8));
                            lsn_t *where = (lsn_t*)(_data+lo_offset);
                            return where;
                        }
    };

    class fake_logrec_t {
    public:
        #include "logtype_gen.h"
        enum {
            max_sz = 3 * sizeof(generic_page),
            hdr_non_ssx_sz = sizeof(baseLogHeader) + sizeof(xidChainLogHeader),
            hdr_single_sys_xct_sz = sizeof(baseLogHeader),
            max_data_sz = max_sz - hdr_non_ssx_sz - sizeof(lsn_t)
        };

        BOOST_STATIC_ASSERT(hdr_non_ssx_sz == 40);
        BOOST_STATIC_ASSERT(hdr_single_sys_xct_sz == 40 - 16);

        enum category_t {
        t_bad_cat   = 0x00,
        t_status    = 0x01,
        t_undo      = 0x02,
        t_redo      = 0x04,
        t_multi     = 0x08,
        t_logical   = 0x10,
        t_cpsn      = 0x20,
        t_rollback  = 0x40,
        t_single_sys_xct    = 0x80
        };

        baseLogHeader header;
        xidChainLogHeader xidInfo;
        char _data[max_sz - sizeof(baseLogHeader) - sizeof(xidChainLogHeader)];

        const lsn_t          get_lsn_ck() const {
                                    lsn_t    tmp = *_lsn_ck();
                                    return tmp;
                             }
        void                 set_lsn_ck(const lsn_t &lsn_ck) {
                                    // put lsn in last bytes of data
                                    lsn_t& where = *_lsn_ck();
                                    where = lsn_ck;
                             }

    private:
        lsn_t*            _lsn_ck() {
            w_assert3(alignon(header._len, 8));
            char* this_ptr = reinterpret_cast<char*>(this);
            return reinterpret_cast<lsn_t*>(this_ptr + header._len - sizeof(lsn_t));
        }
        const lsn_t*            _lsn_ck() const {
            w_assert3(alignon(header._len, 8));
            const char* this_ptr = reinterpret_cast<const char*>(this);
            return reinterpret_cast<const lsn_t*>(this_ptr + header._len - sizeof(lsn_t));
        }
    };

    Stats stats;

    /* Every INCR_TH records generated, increase the max_page_id by a ratio
     * of INCR_RATIO.
     */
    const unsigned INCR_TH;
    const unsigned INCR_RATIO;

    bool sorted;
    unsigned current_page_id;
    unsigned max_page_id;
    vector<lsn_t> prev_lsn;  /* Keep track of previous log record on same page */
    unsigned generatedCount; /* Keep track of how many log records were generated */
    lsn_t nextLSN;

    boost::random::mt19937 gen;    /* Random Number Generator */
    boost::random::uniform_real_distribution<double> dDist; //[min,max)

    unsigned nextZipf();
    unsigned nextSorted();

    static const uint32_t  factory_version_major;
    static const uint32_t  factory_version_minor;

};

#endif
