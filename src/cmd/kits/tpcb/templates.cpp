#include "tpcb_schema.h"

#include "table_man.cpp"

template class table_man_t<tpcb::branch_t>;
template class table_man_t<tpcb::teller_t>;
template class table_man_t<tpcb::account_t>;
template class table_man_t<tpcb::history_t>;
