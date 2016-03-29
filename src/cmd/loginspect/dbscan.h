#ifndef DBSCAN_H
#define DBSCAN_H

#include "command.h"

class DBScan : public Command
{
public:
    void setupOptions();
    void run();
protected:
    virtual void handlePage(PageID pid, const generic_page& page, vol_t* vol);
private:
    string dbfile;
};

#endif
