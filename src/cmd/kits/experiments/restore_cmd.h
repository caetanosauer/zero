#ifndef RESTORE_CMD_H
#define RESTORE_CMD_H

#include "kits_cmd.h"

class RestoreCmd : public KitsCommand
{
public:
    virtual void setupOptions();
    virtual void run();

protected:
    unsigned opt_segmentSize;
    bool opt_singlePass;
    bool opt_instant;
    bool opt_evict;
    unsigned opt_failDelay;
    bool opt_crash;
    int opt_crashDelay;
    bool opt_waitForRestore;
    bool opt_offline;
    bool opt_onDemand;
    bool opt_randomOrder;

    bool hasFailed;

    virtual void loadOptions(sm_options& opt);
    virtual void doWork();
};

#endif
