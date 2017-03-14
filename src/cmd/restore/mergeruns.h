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
    size_t level;
    size_t fanin;
    size_t bucketSize;
};

#endif
