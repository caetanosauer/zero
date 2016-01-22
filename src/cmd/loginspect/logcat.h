#ifndef LOGCAT_H
#define LOGCAT_H

#include "command.h"

class LogCat : public LogScannerCommand {
public:
    void usage();
    void run();
    void setupOptions();
};

#endif
