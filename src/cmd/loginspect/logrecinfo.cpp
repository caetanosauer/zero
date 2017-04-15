#include "logrecinfo.h"

class LogrecInfoHandler : public Handler {
public:
    LogrecInfoHandler()
    {}

    virtual void invoke(logrec_t& r)
    {
        std::cout << r.lsn().hi()
            << '\t' << r.lsn().lo()
            << '\t' << r.pid() << std::endl;
    }
};

void LogrecInfo::setupOptions()
{
    LogScannerCommand::setupOptions();
}

void LogrecInfo::run()
{
    LogrecInfoHandler h;
    BaseScanner* s = getScanner();

    s->add_handler(&h);
    s->fork();
    s->join();

    delete s;
}


