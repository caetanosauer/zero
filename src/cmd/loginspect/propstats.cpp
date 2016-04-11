#include "propstats.h"

#include "log_core.h"
#include "log_storage.h"
#include "stnode_page.h"
#include "alloc_cache.h"

class PropStatsHandler : public Handler {
public:
    // Use chkpt object to keep track of dirty pages
    // This allows us to reuse the logic of log analysis
    chkpt_t chkpt;

    size_t psize;

    // Counter of page writes per second
    size_t page_writes;

    // Counter of transaction commits
    size_t commits;

    // Counter of page updates
    size_t updates;

    PropStatsHandler(size_t psize)
        : psize(psize), page_writes(0), commits(0), updates(0)
    {
    }

    virtual void initialize()
    {
        out() << "# dirty_pages redo_length page_writes xct_end updates" << endl;
    }

    virtual void invoke(logrec_t& r)
    {
        // Dump stats on each tick log record
        if (r.type() == logrec_t::t_tick_sec || r.type() == logrec_t::t_tick_msec) {
            dumpStats(r.lsn());
            return;
        }
        // Code copied from chkpt_t::scan_log
        if (r.is_page_update()) {
            // Ignore metadata pages
            if (r.pid() % alloc_cache_t::extent_size == 0 ||
                    r.pid() == stnode_page::stpid)
            {
                return;
            }

            lsn_t lsn = r.lsn();
            w_assert0(r.is_redo());
            chkpt.mark_page_dirty(r.pid(), lsn, lsn);

            if (r.is_multi_page()) {
                w_assert0(r.pid2() != 0);
                chkpt.mark_page_dirty(r.pid2(), lsn, lsn);
            }
            updates++;
        }
        else if (r.type() == logrec_t::t_page_write) {
            char* pos = r.data();

            PageID pid = *((PageID*) pos);
            pos += sizeof(PageID);

            lsn_t clean_lsn = *((lsn_t*) pos);
            pos += sizeof(lsn_t);

            uint32_t count = *((uint32_t*) pos);
            PageID end = pid + count;

            while (pid < end) {
                chkpt.mark_page_clean(pid, clean_lsn);
                pid++;
            }

            page_writes += count;
        }
        else if (r.type() == logrec_t::t_xct_end) {
            commits++;
        }
    }

    void dumpStats(lsn_t lsn)
    {
        // A checkpoint taken with a forward scan does not update rec_lsn
        // correctly, so it will always be the first update on each page. We
        // take min_clean_lsn here since it should be a good approximation
        lsn_t rec_lsn = lsn_t::max;
        for(buf_tab_t::const_iterator it = chkpt.buf_tab.begin();
                it != chkpt.buf_tab.end(); ++it)
        {
            if (it->second.clean_lsn != lsn_t::null && it->second.clean_lsn < rec_lsn) {
                rec_lsn = it->second.clean_lsn;
            }
        }
        if (rec_lsn == lsn_t::max) { rec_lsn = lsn_t::null; }

        size_t redo_length = 0;
        if (rec_lsn.hi() == lsn.hi()) {
            redo_length = lsn.lo() - rec_lsn.lo();
        }
        else if (rec_lsn != lsn_t::null) {
            w_assert0(lsn > rec_lsn);
            size_t rest = lsn.lo() + psize - rec_lsn.lo();
            redo_length = psize * (lsn.hi() - rec_lsn.hi()) + rest;
        }
        ERROUT(<< "min_rec_lsn: " << rec_lsn);

        size_t dirty_page_count = 0;
        for (auto e : chkpt.buf_tab) {
            if (e.second.is_dirty()) { dirty_page_count++; }
        }

        out() << "" << dirty_page_count
            << " " << redo_length / 1048576
            << " " << page_writes
            << " " << commits
            << " " << updates
            << endl;

        page_writes = 0;
        commits = 0;
        updates = 0;
    }
};

class PropHistogramHandler : public Handler {
public:
    // Histogram that counts how many consecutive pages were written
    // in each page write operation
    vector<size_t> consecutive_writes;

    virtual void initialize()
    {
        out() << "# write_size frequency" << endl;
    }

    virtual void invoke(logrec_t& r)
    {
        if (r.type() == logrec_t::t_page_write) {
            char* pos = r.data();
            pos += sizeof(PageID);
            pos += sizeof(lsn_t);
            uint32_t count = *((uint32_t*) pos);

            if (consecutive_writes.size() <= count) {
                consecutive_writes.resize(count + 1);
            }
            consecutive_writes[count]++;
        }
    }

    virtual void finalize()
    {
        for (unsigned i = 0; i < consecutive_writes.size(); i++) {
            out() << i << " " << consecutive_writes[i] << endl;
        }
    };
};

void PropStats::setupOptions()
{
    LogScannerCommand::setupOptions();
    boost::program_options::options_description opt("PropStats Options");
    opt.add_options()
        ("psize", po::value<size_t>(&psize)->default_value(1024*1024*1024),
            "Size of log partitions (to calculate REDO length)")
    ;
    options.add(opt);
}

void PropStats::run()
{
    BaseScanner* s = getScanner();

    PropStatsHandler h1{psize};
    h1.setFileOutput("propstats.txt");
    s->add_handler(&h1);

    PropHistogramHandler h2;
    h2.setFileOutput("writesizes.txt");
    s->add_handler(&h2);

    s->fork();
    s->join();

    delete s;
}

