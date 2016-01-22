#ifndef GENARCHIVE_H
#define GENARCHIVE_H

#include "command.h"

class GenArchive : public Command
{
public:
    void setupOptions();
    void run();
private:
    string logdir;
    string archdir;
    long maxLogSize;
};

#endif
