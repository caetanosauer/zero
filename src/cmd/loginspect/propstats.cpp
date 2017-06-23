#include "propstats.h"

#include "log_core.h"
#include "log_storage.h"
#include "stnode_page.h"
#include "alloc_cache.h"
#include "chkpt.h"

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

    // Keep track of updates on each page to determine rec_lsn
    std::unordered_map<PageID, std::vector<lsn_t>> page_lsns;

    // Kep track of pages belonging to static (i.e., non-growing) tables and
    // indixces (TPC-C only!)
    std::unordered_set<PageID> static_pids;

    PropStatsHandler(size_t psize)
        : psize(psize), page_writes(0), commits(0), updates(0)
    {
    }

    virtual void initialize()
    {
        out() << "# dirty_pages redo_length page_writes xct_end updates pages_static" << endl;
    }

    // CS TODO
    static constexpr bool track_rec_lsn = false;

    bool is_static_store(StoreID store)
    {
        /*
         * TPC-C store IDs:
         * 2 - warehouse
         * 3 - district
         * 4 - customer
         * 5 - customer name index
         * 6 - history
         * 7 - new_order
         * 8 - order
         * 9 - order customer_id index
         * 10 - order_line
         * 11 - item
         * 12 - stock
         */
        return !(
            store == 6 ||
            store == 8 ||
            store == 9 ||
            store == 10
            );
    }

    void process_page_write(PageID pid, lsn_t clean_lsn, StoreID store)
    {
        if (track_rec_lsn) {
            // Delete updates with lsn < clean_lsn
            auto& vec = page_lsns[pid];
            size_t i = 0;
            while (i < vec.size()) {
                if (vec[i] >= clean_lsn) { break; }
                i++;
            }

            if (i > 0) {
                vec.erase(vec.begin(), vec.begin() + i);
            }
        }

        chkpt.mark_page_clean(pid, clean_lsn);
    }

    void mark_dirty(PageID pid, lsn_t lsn, StoreID store)
    {
        if (track_rec_lsn) {
            auto& vec = page_lsns[pid];
            vec.push_back(lsn);
        }

        if (is_static_store(store)) {
            static_pids.insert(pid);
        }
        chkpt.mark_page_dirty(pid, lsn, lsn);
    }

    virtual void invoke(logrec_t& r)
    {
        // Dump stats on each tick log record
        if (r.type() == logrec_t::t_tick_sec || r.type() == logrec_t::t_tick_msec) {
            dumpStats(r.lsn());
            return;
        }
        // Code copied from chkpt_t::scan_log
        if (r.is_redo()) {
            // Ignore metadata pages
            if (r.pid() % alloc_cache_t::extent_size == 0 ||
                    r.pid() == stnode_page::stpid)
            {
                return;
            }

            mark_dirty(r.pid(), r.lsn(), r.stid());

            if (r.is_multi_page()) {
                w_assert0(r.pid2() != 0);
                mark_dirty(r.pid2(), r.lsn(), r.stid());
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
                process_page_write(pid, clean_lsn, r.stid());
                pid++;
            }

            page_writes += count;
        }
        else if (r.type() == logrec_t::t_evict_page) {
            PageID pid = *(reinterpret_cast<PageID*>(r.data_ssx()));
            bool was_dirty = *(reinterpret_cast<bool*>(r.data_ssx() + sizeof(PageID)));
            if (!was_dirty) {
                chkpt.buf_tab.erase(pid);
                if (track_rec_lsn) { page_lsns[pid].clear(); }
            }
        }
        else if (r.type() == logrec_t::t_xct_end) {
            commits++;
        }
    }

    void dumpStats(lsn_t lsn)
    {
        lsn_t min_rec_lsn = lsn_t::max;
        if (track_rec_lsn) {
            for (auto e : page_lsns) {
                if (e.second.size() > 0) {
                    lsn_t rec = e.second[0];
                    if (rec < min_rec_lsn) { min_rec_lsn = rec; }
                }
            }
        }
        if (min_rec_lsn == lsn_t::max) { min_rec_lsn = lsn_t::null; }

        size_t redo_length = 0;
        if (min_rec_lsn.hi() == lsn.hi()) {
            redo_length = lsn.lo() - min_rec_lsn.lo();
        }
        else if (min_rec_lsn != lsn_t::null) {
            w_assert0(lsn > min_rec_lsn);
            size_t rest = lsn.lo() + psize - min_rec_lsn.lo();
            redo_length = psize * (lsn.hi() - min_rec_lsn.hi()) + rest;
        }
        // ERROUT(<< "min_rec_lsn: " << min_rec_lsn);

        size_t dirty_page_count = 0;
        size_t pages_static = 0;
        for (auto e : chkpt.buf_tab) {
            if (e.second.is_dirty()) {
                dirty_page_count++;
            }
            if (static_pids.count(e.first) > 0) {
                pages_static++;
            }
        }

        out() << "" << dirty_page_count
            << " " << redo_length / 1048576
            << " " << page_writes
            << " " << commits
            << " " << updates
            << " " << pages_static
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

