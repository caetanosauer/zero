#ifndef XCTLATENCY_H
#define XCTLATENCY_H

#include "command.h"
#include "handler.h"

class XctLatency : public LogScannerCommand {
public:
    void run();
    void setupOptions();

private:
    string beginType;
    string endType;
    int interval;
};

class LatencyHandler : public Handler {
public:
    LatencyHandler(int interval = 1,
            logrec_t::kind_t begin = logrec_t::t_max_logrec,
            logrec_t::kind_t end = logrec_t::t_max_logrec);
    virtual void invoke(logrec_t& r);
    virtual void finalize();
protected:
    const int interval;
    int currentTick;

    logrec_t::kind_t begin;
    logrec_t::kind_t end;
    bool seenBegin;

    unsigned long accum_latency;
    unsigned count;

    void dump();
};

#endif
