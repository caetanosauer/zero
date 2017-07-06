#include "loganalysis.h"

#include <fstream>
#include "logarchiver.h"
#include "chkpt.h"

void LogAnalysis::setupOptions()
{
    LogScannerCommand::setupOptions();
    options.add_options()
        ("printPages", po::value<bool>(&printPages)
            ->default_value(false)->implicit_value(true),
            "Print dirty page table")
        ("takeChkpt", po::value<bool>(&takeChkpt)
            ->default_value(false)->implicit_value(true),
            "Take checkpoint after log analysis")
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

    cout << "chkpt_t min_rec_lsn: " << chkpt.get_min_rec_lsn() << endl;
    cout << "chkpt_t min_xct_lsn: " << chkpt.get_min_xct_lsn() << endl;
    cout << endl;

    cout << "chkpt_t active transactions: " << chkpt.xct_tab.size() << endl;

    for(xct_tab_t::const_iterator it = chkpt.xct_tab.begin();
                            it != chkpt.xct_tab.end(); ++it)
    {
        cout << it->first << " ";
    }
    cout << endl;
    cout << endl;

    cout << "chkpt_t dirty pages: " << chkpt.buf_tab.size() << endl;

    if (printPages) {
        for(buf_tab_t::const_iterator it = chkpt.buf_tab.begin();
                it != chkpt.buf_tab.end(); ++it)
        {
            cout << it->first << " REC " << it->second.rec_lsn
                << " PAGE " << it->second.page_lsn
                << " CLEAN " << it->second.clean_lsn << endl;
        }
        cout << endl;
        cout << endl;
    }

    if (takeChkpt) {
        smlevel_0::chkpt = new chkpt_m(_options, &chkpt);
        smlevel_0::chkpt->take(&chkpt);
    }

    // CS TODO: this old full scan was incorrect because it did not consider the
    // cleanLSN value when processing page_write log records
//     if (fullScan) {
//         // Scan whole log to collect list of active TAs
//         cout << "Performing log full scan ... ";
//         BaseScanner* s = getScanner();
//         LogAnalysisHandler h;
//         s->add_handler(&h);
//         s->fork();
//         s->join();
//         cout << "done!" << endl;

//         cout << "Log-scan active transactions: "
//             << h.activeTAs.size()
//             << " committed " << h.xctCount
//             << endl;

//         {
//             cout << '\t';
//             unordered_set<tid_t>::const_iterator it;
//             for (it = h.activeTAs.begin(); it != h.activeTAs.end(); it++) {
//                 cout << *it << " ";
//             }
//             cout << endl << endl;
//         }

//         cout << "Log-scan dirty pages: "
//             << h.dirtyPages.size()
//             << endl;

//         {
//             cout << '\t';
//             unordered_set<PageID>::const_iterator it;
//             for (it = h.dirtyPages.begin(); it != h.dirtyPages.end(); it++) {
//                 cout << *it << " " << endl;
//             }
//             cout << endl << endl;
//         }

//         delete s;
//     }

}

LogAnalysisHandler::LogAnalysisHandler()
    : xctCount(0)
{
}

void LogAnalysisHandler::invoke(logrec_t& r)
{
    if (!r.tid() == 0) {
        if (r.is_page_update() || r.is_cpsn()) {
            activeTAs.insert(r.tid());
        }
    }

    if (r.type() == xct_end_log ||
            r.type() == xct_abort_log)
    {
        activeTAs.erase(r.tid());
        xctCount++;
    }

    if (r.is_redo()) {
        dirtyPages.insert(r.pid());
        if (r.is_multi_page()) {
            dirtyPages.insert(r.pid2());
        }
    }

    if (r.type() == page_write_log) {
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
