#define private public
#include "logcat.h"
#include "logarchiver.h"
#undef private

class PrintHandler : public Handler {
    virtual void invoke(logrec_t& r)
    {
        std::cout << r << endl;
    }

    virtual void finalize() {};
};

void LogCat::setupOptions()
{
    LogScannerCommand::setupOptions();
}

void LogCat::run()
{
    PrintHandler* h = new PrintHandler();
    BaseScanner* s = getScanner();

    s->type_handlers.resize(logrec_t::t_max_logrec);
    s->any_handlers.push_back(h);
    s->fork();
    s->join();

    delete s;
    delete h;
}

