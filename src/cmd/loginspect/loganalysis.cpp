#include "loganalysis.h"

#include <fstream>
#include "logarchiver.h"

void LogAnalysis::setupOptions()
{
    LogScannerCommand::setupOptions();
    options.add_options()
        ("full", po::value<bool>(&fullScan)
            ->default_value(false),
            "Perform full log scan to collect dirty page and active \
            transactions, ignoring checkpoints. Useful to check correctness \
            of checkpoint and log analysis")
    ;
}

void LogAnalysis::run()
{
    start_base();
    start_log(logdir);

    // Get active TAs and dirty pages from log analysis
    cout << "Performing log analysis ... ";
    chkpt_t chkpt;
    chkpt.scan_log();
    cout << "done!" << endl;

    cout << "chkpt_t active transactions: " << chkpt.xct_tab.size() << endl;

    cout << '\t';
    for(xct_tab_t::const_iterator it = chkpt.xct_tab.begin();
                            it != chkpt.xct_tab.end(); ++it)
    {
        cout << it->first << " ";
    }
    cout << endl;
    cout << endl;

    cout << "chkpt_t dirty pages: " << chkpt.buf_tab.size() << endl;
    cout << '\t';
    for(buf_tab_t::const_iterator it = chkpt.buf_tab.begin();
                            it != chkpt.buf_tab.end(); ++it)
    {
        cout << it->first << " ";
    }
    cout << endl;
    cout << endl;

    if (fullScan) {
        // Scan whole log to collect list of active TAs
        cout << "Performing log full scan ... ";
        BaseScanner* s = getScanner();
        LogAnalysisHandler h;
        s->any_handlers.push_back(&h);
        s->fork();
        s->join();
        cout << "done!" << endl;

        cout << "Log-scan active transactions: "
            << h.activeTAs.size()
            << " committed " << h.xctCount
            << endl;

        {
            cout << '\t';
            unordered_set<tid_t>::const_iterator it;
            for (it = h.activeTAs.begin(); it != h.activeTAs.end(); it++) {
                cout << *it << " ";
            }
            cout << endl << endl;
        }

        cout << "Log-scan dirty pages: "
            << h.activeTAs.size()
            << " committed " << h.xctCount
            << endl;

        {
            cout << '\t';
            unordered_set<PageID>::const_iterator it;
            for (it = h.dirtyPages.begin(); it != h.dirtyPages.end(); it++) {
                cout << *it << " ";
            }
            cout << endl << endl;
        }

        delete s;
    }

}

LogAnalysisHandler::LogAnalysisHandler()
    : xctCount(0)
{
}

void LogAnalysisHandler::invoke(logrec_t& r)
{
    if (!r.tid().is_null()) {
        if (r.is_page_update() || r.is_cpsn()) {
            activeTAs.insert(r.tid());
        }
    }

    if (r.type() == logrec_t::t_xct_end ||
            r.type() == logrec_t::t_xct_abort)
    {
        activeTAs.erase(r.tid());
        xctCount++;
    }

    if (r.is_page_update()) {
        dirtyPages.insert(r.pid());
        if (r.is_multi_page()) {
            dirtyPages.insert(r.pid2());
        }
    }

    if (r.type() == logrec_t::t_page_write) {
        PageID pid = *((PageID*) r.data());
        uint32_t count = *((uint32_t*) (r.data() + sizeof(PageID)));
        PageID end = pid + count;

        while (pid < end) {
            dirtyPages.erase(pid);
            pid++;
        }
    }
}

void LogAnalysisHandler::finalize()
{
}
