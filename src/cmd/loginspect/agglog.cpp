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
    bitset<t_max_logrec> filter;
    filter.reset();

    // set filter bit for all valid logrec types found in the arguments given
    for (int i = 0; i < t_max_logrec; i++) {
        auto it = find(typeStrings.begin(), typeStrings.end(),
                string(logrec_t::get_type_str((kind_t) i)));

        if (it != typeStrings.end()) {
            filter.set(i);
        }
    }

    // if no logrec types given, aggregate all types
    if (filter.none()) {
        for (int i = 0; i < t_max_logrec; i++) {
            filter.set(i);
        }
    }

    kind_t begin = t_max_logrec;
    kind_t end = t_max_logrec;

    for (int i = 0; i < t_max_logrec; i++) {
        if (beginType == string(logrec_t::get_type_str((kind_t) i)))
        {
            begin = (kind_t) i;
        }
        if (endType == string(logrec_t::get_type_str((kind_t) i)))
        {
            end = (kind_t) i;
        }
    }

    AggregateHandler h(filter, interval, begin, end);

    // filter must not ignore tick log records
    filter.set(tick_sec_log);
    filter.set(tick_msec_log);

    // filter must not ignore begin and end marks
    if (begin != t_max_logrec) { filter.set(begin); }
    if (end != t_max_logrec) { filter.set(end); }

    BaseScanner* s = getScanner(&filter);
    s->add_handler(&h);
    s->fork();
    s->join();
    json = h.jsonReply();
    delete s;
}

string AggLog::jsonReply()
{
    return json;
}

AggregateHandler::AggregateHandler(bitset<t_max_logrec> filter,
        int interval, kind_t begin, kind_t end)
    : filter(filter), interval(interval), currentTick(0),
    begin(begin), end(end), seenBegin(false), jsonResultIndex(0)
{
    assert(interval > 0);
    counts.reserve(t_max_logrec);
    for (size_t i = 0; i < t_max_logrec; i++) {
        counts[i] = 0;
    }

    if (begin == t_max_logrec) {
        seenBegin = true;
    }

    // print header line with type names
    cout << "#";
    for (int i = 0; i < t_max_logrec; i++) {
        if (filter[i]) {
            cout << " " << logrec_t::get_type_str((kind_t) i);
            ssJsonResult[jsonResultIndex] << "\"" << logrec_t::get_type_str((kind_t) i) <<  "\" : [";
            jsonResultIndex++;
        }
    }
    cout << endl;
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

    if (r.type() == tick_sec_log || r.type() == tick_msec_log) {
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
            ssJsonResult[i] << counts[i] << ", ";
            counts[i] = 0;
        }
    }
    cout << endl;
}

string AggregateHandler::jsonReply()
{
    string reply("{ ");
    for (int i = 0; i < jsonResultIndex; i++) {
        reply += ssJsonResult[i].str();
        reply[reply.size() - 2] = ']';
        reply += ", ";
    }
    reply[reply.size() - 2] = '}';
    return reply;
}

void AggregateHandler::finalize()
{
    dumpCounts();
}
