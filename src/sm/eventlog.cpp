#include "eventlog.h"

#include "logdef_gen.cpp"

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

void sysevent::log_page_read(shpid_t shpid)
{
    logrec_t* lr = new logrec_t();
    lr->header._type = logrec_t::t_page_read;
    lr->header._cat = 0 | logrec_t::t_status;

    memcpy(lr->data(), &shpid, sizeof(shpid_t));
    lr->fill(0, sizeof(shpid_t));
    W_COERCE(smlevel_0::log->insert(*lr, NULL));
    delete lr;
}

void sysevent::log_page_write(shpid_t shpid, uint32_t count)
{
    logrec_t* lr = new logrec_t();
    lr->header._type = logrec_t::t_page_write;
    lr->header._cat = 0 | logrec_t::t_status;

    memcpy(lr->_data, &shpid, sizeof(shpid_t));
    memcpy(lr->_data + sizeof(shpid_t), &count, sizeof(uint32_t));
    lr->fill(0, sizeof(shpid_t) + sizeof(uint32_t));
    W_COERCE(smlevel_0::log->insert(*lr, NULL));
    delete lr;
}
