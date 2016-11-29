#ifndef EVENTLOG_H
#define EVENTLOG_H

#include "boost/date_time/gregorian/gregorian.hpp"
#include "boost/date_time/posix_time/posix_time.hpp"

class sysevent_timer {
public:

    /*
     * Our timestamps are the number of miliseconds since
     * Jan 1, 2015
     */
    // Defined in logrec.cpp
    static boost::gregorian::date epoch;

    static const unsigned long MSEC_IN_DAY = 86400000;

    static unsigned long timestamp()
    {
        boost::posix_time::ptime t =
            boost::posix_time::microsec_clock::local_time();
        long days = (t.date() - epoch).days();
        long msec = t.time_of_day().total_milliseconds();

        return (unsigned long) days * MSEC_IN_DAY + msec;
    };

    static std::string timestamp_to_str()
    {
        // TODO implement
        return "";
    };
};

#endif
