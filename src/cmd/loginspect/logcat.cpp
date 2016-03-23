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
    PrintHandler h;
    BaseScanner* s = getScanner();

    s->add_handler(&h);
    s->fork();
    s->join();

    delete s;
}

