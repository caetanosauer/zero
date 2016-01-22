#ifndef AGGLOG_H
#define AGGLOG_H

#include "command.h"
#include "handler.h"

#include <vector>
#include <bitset>

class AggLog : public LogScannerCommand {
public:
    void run();
    void setupOptions();

private:
    vector<string> typeStrings;
    string beginType;
    string endType;
    int interval;
};

class AggregateHandler : public Handler {
public:
    AggregateHandler(bitset<logrec_t::t_max_logrec> filter, int interval = 1,
            logrec_t::kind_t begin = logrec_t::t_max_logrec,
            logrec_t::kind_t end = logrec_t::t_max_logrec);
    virtual void invoke(logrec_t& r);
    virtual void finalize();
protected:
    vector<unsigned> counts;
    bitset<logrec_t::t_max_logrec> filter;
    const int interval;
    int currentTick;

    logrec_t::kind_t begin;
    logrec_t::kind_t end;
    bool seenBegin;

    void dumpCounts();
};

#endif
