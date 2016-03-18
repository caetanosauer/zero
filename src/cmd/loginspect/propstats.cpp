#include "propstats.h"

#include "log_core.h"
#include "log_storage.h"

class PropStatsHandler : public Handler {
public:
    // Use chkpt object to keep track of dirty pages
    // This allows us to reuse the logic of log analysis
    chkpt_t chkpt;

    size_t psize;

    // Histogram that counts how many consecutive pages were written
    // in each page write operation
    vector<size_t> consecutive_writes;

    // Counter of page writes per second
    size_t page_writes;

    PropStatsHandler(size_t psize)
        : psize(psize), page_writes(0)
    {}

    virtual void invoke(logrec_t& r)
    {
        // Dump stats on each tick log record
        if (r.type() == logrec_t::t_tick_sec || r.type() == logrec_t::t_tick_msec) {
            dumpStats(r.lsn());
            return;
        }
        // Code copied from chkpt_t::scan_log
        if (r.is_page_update()) {
            lsn_t lsn = r.lsn();
            w_assert0(r.is_redo());
            chkpt.mark_page_dirty(r.pid(), lsn, lsn);

            if (r.is_multi_page()) {
                w_assert0(r.pid2() != 0);
                chkpt.mark_page_dirty(r.pid2(), lsn, lsn);
            }
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

            if (consecutive_writes.size() <= count) {
                consecutive_writes.resize(count + 1);
            }
            consecutive_writes[count]++;
            page_writes += count;
        }
    }

    void dumpStats(lsn_t lsn)
    {
        lsn_t rec_lsn = chkpt.get_min_rec_lsn();

        size_t redo_length = 0;
        if (rec_lsn.hi() == lsn.hi()) {
            redo_length = lsn.lo() - rec_lsn.lo();
        }
        else {
            w_assert0(lsn > rec_lsn);
            size_t rest = lsn.lo() + psize - rec_lsn.lo();
            redo_length = psize * (lsn.hi() - rec_lsn.hi()) + rest;
        }

        size_t dirty_page_count = 0;
        for (auto e : chkpt.buf_tab) {
            if (e.second.is_dirty()) { dirty_page_count++; }
        }

        cout << "dirty_pages " << dirty_page_count
            << " redo_length " << redo_length / 1048576
            << " page_writes " << page_writes
            << endl;

        page_writes = 0;
    }

    virtual void finalize()
    {
        cout << "consecutive_writes ";
        for (unsigned i = 0; i < consecutive_writes.size(); i++) {
            cout << consecutive_writes[i] << " ";
        }
        cout << endl;
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
    PropStatsHandler* h = new PropStatsHandler(psize);
    BaseScanner* s = getScanner();

    s->type_handlers.resize(logrec_t::t_max_logrec);
    s->any_handlers.push_back(h);
    s->fork();
    s->join();

    delete s;
    delete h;
}

