#ifndef VERIFYLOG_H
#define VERIFYLOG_H

#include <unordered_map>

#include "command.h"
#include "handler.h"

class VerifyLog : public LogScannerCommand
{
public:
    void setupOptions();
    void run();

private:
    bool verify_alloc;
    string dbfile;
};

class VerifyHandler : public Handler {
public:
    VerifyHandler(bool merge);
    virtual ~VerifyHandler() {};

    virtual void invoke(logrec_t& r);
    virtual void finalize();
    virtual void newFile(const char* fname);

private:
    std::unordered_map<PageID, lsn_t> pageLSNs;
    std::unordered_set<PageID> allocatedPages;
    lsn_t minLSN;
    lsn_t maxLSN;
    lsn_t lastLSN;
    PageID lastPID;
    long count;
    bool merge;

    lsn_t getCurrentPageLSN(PageID pid);
    void checkRedo(logrec_t& r, PageID pid, lsn_t lsn, lsn_t prev_lsn);
    void checkAlloc(logrec_t& r);
};

#endif
