#include "eventlog.h"

#include "logdef_gen.cpp"

boost::gregorian::date sysevent_timer::epoch
    = boost::gregorian::date(2015,1,1);

void sysevent::log(logrec_t::kind_t kind)
{
    // this should use TLS allocator, so it's fast
    // (see macro DEFINE_SM_ALLOC in allocator.h and logrec.cpp)
    logrec_t* lr = new logrec_t();
    lr->header._type = kind;
    lr->header._cat = 0 | logrec_t::t_status;
    lr->fill(0, 0);
    W_COERCE(smlevel_0::log->insert(*lr, NULL));
    delete lr;
}

<<<<<<< HEAD
void sysevent::log_page_read(PageID shpid)
{
    logrec_t* lr = new logrec_t();
    lr->header._type = logrec_t::t_page_read;
    lr->header._cat = 0 | logrec_t::t_status;

    memcpy(lr->data(), &shpid, sizeof(PageID));
    lr->fill(0, sizeof(PageID));
    W_COERCE(smlevel_0::log->insert(*lr, NULL));
    delete lr;
}

void sysevent::log_page_write(PageID shpid, uint32_t count)
{
    logrec_t* lr = new logrec_t();
    lr->header._type = logrec_t::t_page_write;
    lr->header._cat = 0 | logrec_t::t_status;

    memcpy(lr->_data, &shpid, sizeof(PageID));
    memcpy(lr->_data + sizeof(PageID), &count, sizeof(uint32_t));
    lr->fill(0, sizeof(PageID) + sizeof(uint32_t));
    W_COERCE(smlevel_0::log->insert(*lr, NULL));
    delete lr;
}
