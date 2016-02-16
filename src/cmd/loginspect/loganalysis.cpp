#include "loganalysis.h"

#include <fstream>
#include "logarchiver.h"

void LogAnalysis::setupOptions()
{
    LogScannerCommand::setupOptions();
}

void LogAnalysis::run()
{
    start_base();
    start_log(logdir);

    // Get active TAs and dirty pages from log analysis
    chkpt_t chkpt;
    chkpt.scan_log();

    // Scan whole log to collect list of active TAs
    BaseScanner* s = getScanner();
    LogAnalysisHandler h;
    s->any_handlers.push_back(&h);
    s->fork();
    s->join();

    // Compare lists
    cout << "Log analysis complete!" << endl;
    cout << "chkpt_t active transactions: "
        << chkpt.xct_tab.size() << endl
        << "chkpt_t dirty pages: "
        << chkpt.buf_tab.size()
        << endl;

    cout << '\t';
    for(xct_tab_t::const_iterator it = chkpt.xct_tab.begin();
                            it != chkpt.xct_tab.end(); ++it)
    {
        cout << it->first << " ";
    }
    cout << endl;

    cout << "Log-scan active transactions: "
        << h.activeTAs.size()
        << " committed " << h.count
        << endl;

    cout << '\t';
    unordered_set<tid_t>::const_iterator it;
    for (it = h.activeTAs.begin(); it != h.activeTAs.end(); it++) {
        cout << *it << " ";
    }
    cout << endl;

    delete s;
}

LogAnalysisHandler::LogAnalysisHandler()
    : count(0)
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
        count++;
    }
}

void LogAnalysisHandler::finalize()
{
}
