#ifndef PROPSTATS_H
#define PROPSTATS_H

#include "command.h"

class PropStats : public LogScannerCommand {
public:
    void run();
    void setupOptions();

private:
    size_t psize;
};

#endif
