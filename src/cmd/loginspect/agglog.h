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
    string jsonReply();

private:
    vector<string> typeStrings;
    string beginType;
    string endType;
    string json;
    int interval;
};

class AggregateHandler : public Handler {
public:
    AggregateHandler(bitset<t_max_logrec> filter, int interval = 1,
            kind_t begin = t_max_logrec,
            kind_t end = t_max_logrec);
    virtual void invoke(logrec_t& r);
    virtual void finalize();
    string jsonReply();

protected:
    vector<unsigned> counts;
    bitset<t_max_logrec> filter;
    const int interval;
    int currentTick, jsonResultIndex;
    std::stringstream ssJsonResult[t_max_logrec];

    kind_t begin;
    kind_t end;
    bool seenBegin;

    void dumpCounts();
};

#endif
