#include "agglog.h"

void AggLog::setupOptions()
{
    LogScannerCommand::setupOptions();
    boost::program_options::options_description agglog("AggLog Options");
    agglog.add_options()
        ("type,t", po::value<vector<string> >(&typeStrings)->multitoken(),
            "Log record types to be considered by the aggregator")
        ("interval,i", po::value<int>(&interval)->default_value(1),
            "Size of the aggregation groups in number of ticks (default 1)")
        ("begin,b", po::value<string>(&beginType)->default_value(""),
            "Only begin aggregation once logrec of given type is found")
        ("end,e", po::value<string>(&endType)->default_value(""),
            "Finish aggregation once logrec of given type is found")
    ;
    options.add(agglog);
}

void AggLog::run()
{
    bitset<logrec_t::t_max_logrec> filter;
    filter.reset();

    // set filter bit for all valid logrec types found in the arguments given
    for (int i = 0; i < logrec_t::t_max_logrec; i++) {
        auto it = find(typeStrings.begin(), typeStrings.end(),
                string(logrec_t::get_type_str((logrec_t::kind_t) i)));

        if (it != typeStrings.end()) {
            filter.set(i);
        }
    }

    if (filter.none()) {
        throw runtime_error("AggLog requires at least one valid logrec type");
    }

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

    AggregateHandler h(filter, interval, begin, end);

    // filter must not ignore tick log records
    filter.set(logrec_t::t_tick_sec);
    filter.set(logrec_t::t_tick_msec);

    // filter must not ignore begin and end marks
    if (begin != logrec_t::t_max_logrec) { filter.set(begin); }
    if (end != logrec_t::t_max_logrec) { filter.set(end); }

    BaseScanner* s = getScanner(&filter);
    s->any_handlers.push_back(&h);
    s->fork();
    s->join();
    delete s;
}

AggregateHandler::AggregateHandler(bitset<logrec_t::t_max_logrec> filter,
        int interval, logrec_t::kind_t begin, logrec_t::kind_t end)
    : filter(filter), interval(interval), currentTick(0),
    begin(begin), end(end), seenBegin(false)
{
    assert(interval > 0);
    counts.reserve(logrec_t::t_max_logrec);
    for (size_t i = 0; i < logrec_t::t_max_logrec; i++) {
        counts[i] = 0;
    }

    if (begin == logrec_t::t_max_logrec) {
        seenBegin = true;
    }

    // print header line with type names
    cout << "#";
    for (int i = 0; i < logrec_t::t_max_logrec; i++) {
        if (filter[i]) {
            cout << " " << logrec_t::get_type_str((logrec_t::kind_t) i);
        }
    }
    cout << flushl;
}

void AggregateHandler::invoke(logrec_t& r)
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
            dumpCounts();
        }
    }
    else if (filter[r.type()]) {
        counts[r.type()]++;
    }
}

void AggregateHandler::dumpCounts()
{
    for (size_t i = 0; i < counts.capacity(); i++) {
        if (filter[i]) {
            cout << counts[i] << '\t';
            counts[i] = 0;
        }
    }
    cout << flushl;
}

void AggregateHandler::finalize()
{
    dumpCounts();
}
