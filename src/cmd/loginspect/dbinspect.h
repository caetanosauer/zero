#ifndef DBINSPECT_H
#define DBINSPECT_H

#include "command.h"

class DBInspect : public Command {
public:
    void usage();
    void run();
    void setupOptions();
private:
    string file;
};

#endif
