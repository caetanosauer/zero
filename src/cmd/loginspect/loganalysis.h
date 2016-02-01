#ifndef LOGANALYSIS_H
#define LOGANALYSIS_H

#include "command.h"
#include "handler.h"

#include <unordered_set>

// Hash function for TID
namespace std {
    template<> struct hash<tid_t> {
        size_t operator()(const tid_t& t) const
        {
            return hash<uint64_t>()(t.as_int64());
        }
    };
}

class LogAnalysis : public LogScannerCommand
{
public:
    void setupOptions();
    void run();
};

class LogAnalysisHandler : public Handler {
public:
    LogAnalysisHandler();
    virtual ~LogAnalysisHandler() {};

    virtual void invoke(logrec_t& r);
    virtual void finalize();

    unordered_set<tid_t> activeTAs;
    size_t count;
};

#endif
