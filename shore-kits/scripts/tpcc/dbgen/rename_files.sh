#!/bin/sh

# @file rename_files.sh
# 
# @brief The stagedtrx loader (<base_dir>/tests/tpcc_load) needs the flat data
# files with specific filenames. This script simply renames the files generated
# by the dbgen-generate-files.pl tool accordingly.
#
# @author Ippokratis Pandis (ipandis) 

mv 3.dat WAREHOUSE.dat
mv 4.dat DISTRICT.dat
mv 5.dat ITEM.dat
mv 6.dat STOCK.dat
mv 7.dat CUSTOMER.dat
mv 8.dat HISTORY.dat
mv 9.dat ORDER.dat
mv 10.dat ORDERLINE.dat
mv 11.dat NEW_ORDER.dat
