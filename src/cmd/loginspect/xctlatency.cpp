#include "xctlatency.h"

void XctLatency::setupOptions()
{
    LogScannerCommand::setupOptions();
    boost::program_options::options_description agglog("XctLatency Options");
    agglog.add_options()
        ("interval,i", po::value<int>(&interval)->default_value(1),
            "Size of the aggregation groups in number of ticks (default 1)")
        ("begin,b", po::value<string>(&beginType)->default_value(""),
            "Only begin aggregation once logrec of given type is found")
        ("end,e", po::value<string>(&endType)->default_value(""),
            "Finish aggregation once logrec of given type is found")
    ;
    options.add(agglog);
}

void XctLatency::run()
{
    logrec_t::kind_t begin = logrec_t::t_max_logrec;
    logrec_t::kind_t end = logrec_t::t_max_logrec;

    for (int i = 0; i < logrec_t::t_max_logrec; i++) {
        if (beginType == string(logrec_t::get_type_str((logrec_t::kind_t) i)))
        {
            begin = (logrec_t::kind_t) i;
        }
        if (endType == string(logrec_t::get_type_str((logrec_t::kind_t) i)))
        {
            end = (logrec_t::kind_t) i;
        }
    }

    LatencyHandler h(interval, begin, end);

    BaseScanner* s = getScanner();
    s->add_handler(&h);
    s->fork();
    s->join();
    delete s;
}

LatencyHandler::LatencyHandler(int interval, logrec_t::kind_t begin,
        logrec_t::kind_t end)
    : interval(interval), currentTick(0), begin(begin), end(end),
    seenBegin(false), accum_latency(0), count(0)
{
    assert(interval > 0);

    if (begin == logrec_t::t_max_logrec) {
        seenBegin = true;
    }

    cout << "#xct_latency_in_nsec" << endl;
}

void LatencyHandler::invoke(logrec_t& r)
{
    if (!seenBegin) {
        if (r.type() == begin) {
            seenBegin = true;
        }
        else {
            return;
        }
    }

    if (r.type() == end) {
        seenBegin = false;
        return;
    }

    if (r.type() == logrec_t::t_tick_sec || r.type() == logrec_t::t_tick_msec) {
        currentTick++;
        if (currentTick == interval) {
            currentTick = 0;
            dump();
        }
    }
    else if (r.type() == logrec_t::t_xct_latency_dump) {
        unsigned long latency = *((unsigned long*) r.data());
        accum_latency += latency;
        count++;
    }
}

void LatencyHandler::dump()
{
    cout << (count > 0 ? (accum_latency / count) : 0) << endl;
    accum_latency = 0;
    count = 0;
}

void LatencyHandler::finalize()
{
    dump();
}
