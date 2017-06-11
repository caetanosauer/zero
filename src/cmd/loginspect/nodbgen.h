#ifndef NODBGEN_H
#define NODBGEN_H

#include "command.h"

class NoDBGen : public Command
{
public:
    void setupOptions();
    void run();
protected:
    void handlePage(fixable_page_h& p);
private:
    string dbfile;
    string logdir;
};

#endif
