#include "verifylog.h"

#include <fstream>
#include "logarchiver.h"

void VerifyLog::setupOptions()
{
    LogScannerCommand::setupOptions();
}

void VerifyLog::run()
{
    BaseScanner* s = getScanner();
    VerifyHandler h(merge);
    if (!merge) {
        s->openFileCallback = std::bind(&VerifyHandler::newFile, &h,
            std::placeholders::_1);
    }
    s->add_handler(&h);
    s->fork();
    s->join();

    delete s;
}

VerifyHandler::VerifyHandler(bool merge)
    : minLSN(lsn_t::null), maxLSN(lsn_t::null), lastLSN(lsn_t::null),
    lastPID(0), count(0), merge(merge)
{
}

void VerifyHandler::newFile(const char* fname)
{
    // minLSN = LogArchiver::ArchiveDirectory::parseLSN(fname, false);
    // maxLSN = LogArchiver::ArchiveDirectory::parseLSN(fname, true);
    lastLSN = lsn_t::null;
    lastPID = 0;
}

lsn_t VerifyHandler::getCurrentPageLSN(PageID pid)
{
    auto it = pageLSNs.find(pid);
    if (it == pageLSNs.end()) {
        return pageLSNs[pid] = lsn_t::null;
    }
    else {
        return pageLSNs[pid];
    }
}

void checkLSN(lsn_t lsn, lsn_t current, lsn_t expected)
{
    if(expected != current) {
        std::cout << "on " << lsn
            << " current is " << current
            << " but should be " << expected
        << std::endl;
    }
    w_assert0(expected == current);
}

void VerifyHandler::invoke(logrec_t& r)
{
    w_assert0(r.valid_header());

    lsn_t lsn = r.lsn();
    PageID pid = r.pid();

    // CS TODO: for some stupid reason, some btree page updates are marked as logical
    // log records, and thus there is no way to exclude logical redo records (such as
    // restore_begin) from this verification -- we can only hope that log chain is
    // correct for pid 0
    if (r.is_redo() && pid != 0) {
        lsn_t currPageLSN = getCurrentPageLSN(pid);
        checkLSN(lsn, currPageLSN, r.page_prev_lsn());
        pageLSNs[pid] = lsn;

        if (r.is_multi_page()) {
            currPageLSN = getCurrentPageLSN(r.pid2());
            checkLSN(lsn, currPageLSN, r.page2_prev_lsn());
            pageLSNs[r.pid2()] = lsn;
        }
    }

    // w_assert0(pid >= lastPID);
    // if (pid == lastPID) {
    //     w_assert0(lsn > lastLSN);
    // }
    // w_assert0(merge || lsn >= minLSN);
    // w_assert0(merge || lsn <= maxLSN);

    lastLSN = lsn;
    lastPID = pid;

    count++;
}

void VerifyHandler::finalize()
{
    cout << "Log verification complete!" << endl;
    cout << "scanned_logrecs " << count << endl;
}
