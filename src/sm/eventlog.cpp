#include "eventlog.h"
#include "log_core.h"

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

void sysevent::log_page_read(PageID shpid, uint32_t count)
{
    logrec_t* lr = new logrec_t();
    lr->header._type = logrec_t::t_page_read;
    lr->header._cat = 0 | logrec_t::t_status;

    memcpy(lr->data(), &shpid, sizeof(PageID));
    memcpy(lr->_data + sizeof(PageID), &count, sizeof(uint32_t));
    lr->fill(0, sizeof(PageID) + sizeof(uint32_t));
    W_COERCE(smlevel_0::log->insert(*lr, NULL));
    delete lr;
}

void sysevent::log_page_write(PageID shpid, lsn_t lsn, uint32_t count)
{
    logrec_t* lr = new logrec_t();
    lr->header._type = logrec_t::t_page_write;
    lr->header._cat = 0 | logrec_t::t_status;

    char* pos = lr->_data;

    memcpy(pos, &shpid, sizeof(PageID));
    pos += sizeof(PageID);

    memcpy(pos, &lsn, sizeof(lsn_t));
    pos += sizeof(lsn_t);

    memcpy(pos, &count, sizeof(uint32_t));
    pos += sizeof(uint32_t);


    lr->fill(0, pos - lr->_data);
    W_COERCE(smlevel_0::log->insert(*lr, NULL));
    delete lr;
}

// METADATA OPERATIONS (CS TODO)
// CS: there are logged with sysevent because the per-page chain cannot be
// generated automatically (with give_logbuf), as the operations are performed
// directly on in-memory data structures instead of buffered pages.

void sysevent::log_alloc_page(PageID pid, lsn_t& prev_page_lsn)
{
    logrec_t* lr = new alloc_page_log(pid);
    lr->set_page_prev_lsn(prev_page_lsn);
    W_COERCE(smlevel_0::log->insert(*lr, &prev_page_lsn));
    delete lr;
}

void sysevent::log_dealloc_page(PageID pid, lsn_t& prev_page_lsn)
{
    logrec_t* lr = new dealloc_page_log(pid);
    lr->set_page_prev_lsn(prev_page_lsn);
    W_COERCE(smlevel_0::log->insert(*lr, &prev_page_lsn));
    delete lr;
}

void sysevent::log_create_store(PageID root, StoreID stid, lsn_t& prev_page_lsn)
{
    logrec_t* lr = new create_store_log(root, stid);
    lr->set_page_prev_lsn(prev_page_lsn);
    W_COERCE(smlevel_0::log->insert(*lr, &prev_page_lsn));
    delete lr;
}

void sysevent::log_append_extent(extent_id_t ext, lsn_t& prev_page_lsn)
{
    logrec_t* lr = new append_extent_log(ext);
    lr->set_page_prev_lsn(prev_page_lsn);
    W_COERCE(smlevel_0::log->insert(*lr, &prev_page_lsn));
    delete lr;
}
