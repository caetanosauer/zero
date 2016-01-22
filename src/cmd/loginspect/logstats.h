#ifndef LOGSTATS_H
#define LOGSTATS_H

#include "command.h"

class LogStats : public LogScannerCommand {
public:
    void usage();
    void run();
    void setupOptions();

protected:
    bool indexOnly;
};

#endif
