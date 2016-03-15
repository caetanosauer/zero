#include "tpcc_schema.h"
#include "sort.h"

#include "table_man.cpp"

template class table_man_t<tpcc::customer_t>;
template class table_man_t<tpcc::district_t>;
template class table_man_t<tpcc::history_t>;
template class table_man_t<tpcc::item_t>;
template class table_man_t<tpcc::new_order_t>;
template class table_man_t<tpcc::order_line_t>;
template class table_man_t<tpcc::order_t>;
template class table_man_t<tpcc::stock_t>;
template class table_man_t<tpcc::warehouse_t>;
template class table_man_t<asc_sort_buffer_t>;
