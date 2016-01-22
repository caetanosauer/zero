#ifndef MERGERUNS_H
#define MERGERUNS_H

#include "command.h"

class MergeRuns : public Command
{
public:
    void setupOptions();
    void run();
private:
    string indir;
    string outdir;
    size_t minRunSize;
    size_t maxRunSize;
    size_t fanin;
};

#endif
