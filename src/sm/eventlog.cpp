#include "eventlog.h"
#include "log_core.h"

boost::gregorian::date sysevent_timer::epoch
    = boost::gregorian::date(2015,1,1);

void sysevent::log(logrec_t::kind_t kind)
{
    // this should use TLS allocator, so it's fast
    // (see macro DEFINE_SM_ALLOC in allocator.h and logrec.cpp)
    logrec_t* lr = new logrec_t();
    lr->init_header(kind);
    lr->init_xct_info();
    W_COERCE(smlevel_0::log->insert(*lr, NULL));
    delete lr;
}

void sysevent::log_page_read(PageID shpid, uint32_t count)
{
    logrec_t* lr = new logrec_t();
    lr->init_header(logrec_t::t_page_read);
    lr->init_xct_info();

    memcpy(lr->data(), &shpid, sizeof(PageID));
    memcpy(lr->_data + sizeof(PageID), &count, sizeof(uint32_t));
    lr->set_size(sizeof(PageID) + sizeof(uint32_t));
    W_COERCE(smlevel_0::log->insert(*lr, NULL));
    delete lr;
}

void sysevent::log_page_write(PageID shpid, lsn_t lsn, uint32_t count)
{
    logrec_t* lr = new logrec_t();
    lr->init_header(logrec_t::t_page_write);
    lr->init_xct_info();

    char* pos = lr->_data;

    memcpy(pos, &shpid, sizeof(PageID));
    pos += sizeof(PageID);

    memcpy(pos, &lsn, sizeof(lsn_t));
    pos += sizeof(lsn_t);

    memcpy(pos, &count, sizeof(uint32_t));
    pos += sizeof(uint32_t);


    lr->set_size(pos - lr->_data);
    W_COERCE(smlevel_0::log->insert(*lr, NULL));
    delete lr;
}

// METADATA OPERATIONS (CS TODO)
// CS: there are logged with sysevent because the per-page chain cannot be
// generated automatically (with give_logbuf), as the operations are performed
// directly on in-memory data structures instead of buffered pages.
//
// CS TODO: GET RID OF THESE!
// Just abstract away the location of the logrec buffer in XctLogger

void sysevent::log_alloc_page(PageID pid, lsn_t& prev_page_lsn)
{
    alloc_page_log* lr = new alloc_page_log();
    lr->init_header(logrec_t::t_alloc_page);
    lr->init_xct_info();
    lr->construct(pid);
    lr->set_page_prev_lsn(prev_page_lsn);
    W_COERCE(smlevel_0::log->insert(*lr, &prev_page_lsn));
    delete lr;
}

void sysevent::log_dealloc_page(PageID pid, lsn_t& prev_page_lsn)
{
    dealloc_page_log* lr = new dealloc_page_log();
    lr->init_header(logrec_t::t_dealloc_page);
    lr->init_xct_info();
    lr->construct(pid);
    lr->set_page_prev_lsn(prev_page_lsn);
    W_COERCE(smlevel_0::log->insert(*lr, &prev_page_lsn));
    delete lr;
}

void sysevent::log_create_store(PageID root, StoreID stid, lsn_t& prev_page_lsn)
{
    create_store_log* lr = new create_store_log();
    lr->init_header(logrec_t::t_create_store);
    lr->init_xct_info();
    lr->construct(root, stid);
    lr->set_page_prev_lsn(prev_page_lsn);
    W_COERCE(smlevel_0::log->insert(*lr, &prev_page_lsn));
    delete lr;
}

void sysevent::log_append_extent(extent_id_t ext, lsn_t& prev_page_lsn)
{
    append_extent_log* lr = new append_extent_log();
    lr->init_header(logrec_t::t_append_extent);
    lr->init_xct_info();
    lr->construct(ext);
    lr->set_page_prev_lsn(prev_page_lsn);
    W_COERCE(smlevel_0::log->insert(*lr, &prev_page_lsn));
    delete lr;
}

void sysevent::log_xct_latency_dump(unsigned long nsec)
{
    xct_latency_dump_log* lr = new xct_latency_dump_log();
    lr->init_header(logrec_t::t_xct_latency_dump);
    lr->init_xct_info();
    lr->construct(nsec);
    W_COERCE(smlevel_0::log->insert(*lr, nullptr));
    delete lr;
}
