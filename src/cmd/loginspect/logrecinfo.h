#ifndef LOGRECINFO_H
#define LOGRECINFO_H

#include "command.h"

class LogrecInfo : public LogScannerCommand {
public:
    void usage();
    void run();
    void setupOptions();
};

#endif

