#ifndef ARCHSTATS_H
#define ARCHSTATS_H

#include "command.h"
#include "handler.h"

#include "logarchive_index.h"

class ArchStats : public LogScannerCommand
{
public:
    void setupOptions();
    void run();
    void printRunInfo(const RunId&);

private:
    bool printStats;
    bool dumpIndex;
    bool scan;
};

class ArchStatsScanner : public Handler
{
public:
    virtual void invoke(logrec_t& r);

    ArchStatsScanner()
        : started(false), pos(0), prevPos(0)
    {}

private:
    bool started;
    PageID currentPID;
    size_t pos;
    size_t prevPos;
};

#endif
