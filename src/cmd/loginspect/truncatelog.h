#ifndef TRUNCATELOG_H
#define TRUNCATELOG_H

#include "command.h"

class TruncateLog : public Command
{
public:
    void setupOptions();
    void run();
private:
    string logdir;
};

#endif
