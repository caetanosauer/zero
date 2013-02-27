#!/bin/sh

# @file reset_db_lsns.sh.sh
# 
# @brief When moving the database or changing the logging mechanism the 
# LSNs of the database files should be reset. The db_load utility is used
# for this purpose, with the -r option.
#
# @author Ippokratis Pandis (ipandis) 

db_load -r lsn TPCC_CUSTOMER.bdb
db_load -r lsn TPCC_DISTRICT.bdb
db_load -r lsn TPCC_HISTORY.bdb
db_load -r lsn TPCC_ITEM.bdb
db_load -r lsn TPCC_NEW_ORDER.bdb
db_load -r lsn TPCC_ORDER.bdb
db_load -r lsn TPCC_ORDERLINE.bdb
db_load -r lsn TPCC_STOCK.bdb
db_load -r lsn TPCC_WAREHOUSE.bdb

echo "TPC-C Database LSNs reset..."

