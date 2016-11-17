#ifndef ADDBACKUP_H
#define ADDBACKUP_H

#include "command.h"

class AddBackup : public Command
{
public:
    void setupOptions();
    void run();

private:
    string logdir;
    string backupPath;
    string lsnString;
};

#endif
