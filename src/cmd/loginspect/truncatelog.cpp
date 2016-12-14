#include "truncatelog.h"

#include "sm.h"
#include "chkpt.h"

void TruncateLog::setupOptions()
{
    options.add_options()
        ("logdir,l", po::value<string>(&logdir)->required(),
         "Directory containing log to be truncated");
}

void TruncateLog::run()
{
    start_base();
    start_log(logdir);
    start_buffer();
    start_other();

    cout << "Taking checkpoint ... ";
    ss_m::chkpt->take();
    cout << "OK" << endl;

    ss_m::_truncate_log();
}
