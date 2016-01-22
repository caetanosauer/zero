#ifndef LOGPAGESTATS_H
#define LOGPAGESTATS_H

#include "command.h"

class LogPageStats : public LogScannerCommand {
public:
    void usage();
    void run();
    void setupOptions();
};

#endif
