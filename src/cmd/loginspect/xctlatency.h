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
            kind_t begin = t_max_logrec,
            kind_t end = t_max_logrec);
    virtual void invoke(logrec_t& r);
    virtual void finalize();
protected:
    const int interval;
    int currentTick;

    kind_t begin;
    kind_t end;
    bool seenBegin;

    unsigned long accum_latency;
    unsigned count;

    void dump();
};

#endif
