#include "tracerestore.h"

#include <iostream>

void RestoreTrace::setupOptions()
{
    LogScannerCommand::setupOptions();
}

void RestoreTrace::run()
{
    RestoreTraceHandler h;
    BaseScanner* s = getScanner();
    s->add_handler(&h);
    s->fork();
    s->join();
    delete s;
}

RestoreTraceHandler::RestoreTraceHandler()
    : currentTick(0)
{
}

void RestoreTraceHandler::invoke(logrec_t& r)
{
    if (r.type() == logrec_t::t_tick_sec || r.type() == logrec_t::t_tick_msec) {
        currentTick++;
    }
    else if (r.type() == logrec_t::t_restore_segment) {
        uint32_t segment = *((uint32_t*) r.data_ssx());
        std::cout << currentTick << " " << segment << std::endl;
    }
}
