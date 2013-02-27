#!/bin/bash

#@file:   scripts/do-remaining-runs
#@brief:  All the possible power and dbx runs, uncomment those you like to run
#@author: Ippokratis Pandis


echo " "
echo "POWER RUNS"
echo " "

echo " "
echo "BASELINE"
echo " "



# const int XCT_TM1_MIX           = 20;
# const int XCT_TM1_GET_SUB_DATA  = 21;
# const int XCT_TM1_GET_NEW_DEST  = 22;
# const int XCT_TM1_GET_ACC_DATA  = 23;
# const int XCT_TM1_UPD_SUB_DATA  = 24;
# const int XCT_TM1_UPD_LOCATION  = 25;
# const int XCT_TM1_CALL_FWD_MIX  = 26;

echo " "
echo "BASE-TM1-{ALL} POWER"
echo " "
SEQ=(20 21 22 23 24 25 26) ; for trx in ${SEQ[@]}; do ./scripts/powerwrapper.sh EXP tm1-100 300 3 30 $trx 1 80 large cd; done



# const int XCT_TPCB_ACCT_UPDATE = 31;

echo " "
echo "BASE-TPCB POWER"
echo " "
SEQ=(31) ; for trx in ${SEQ[@]}; do ./scripts/powerwrapper.sh EXP tpcb-100 300 3 30 $trx 1 80 large cd; done




# const int XCT_MIX           = 0;
# const int XCT_NEW_ORDER     = 1;
# const int XCT_PAYMENT       = 2;
# const int XCT_ORDER_STATUS  = 3;
# const int XCT_DELIVERY      = 4;
# const int XCT_STOCK_LEVEL   = 5;
# const int XCT_LITTLE_MIX    = 9;

echo " "
echo "BASE-TPCC-{ALL} POWER"
echo " "
SEQ=(0 1 2 3 4 5 9) ; for trx in ${SEQ[@]}; do ./scripts/powerwrapper.sh EXP tpcc-100 1200 3 30 $trx 1 80 large cd; done




# echo " "
# echo "DORA"
# echo " "

# # In DORA different workloads get differnt utilization, due to the intra-transaction parallelism.
# # Therefore we use different client sets per workload

# # The client sets:

# # SMALL        (1 4 8 12 16 20 24 28 32 36 40 44)
# # MEDIUM       (1 4 8 12 16 20 24 28 32 36 40 44 48 52 56 60)
# # LARGE        (1 4 8 12 16 20 24 28 32 36 40 44 48 52 56 60 62 64 68 74)
# # EXTRALARGE   (1 4 8 12 16 20 24 28 32 36 40 44 48 52 56 60 62 64 68 74 78 82 86 90 94 98)


# echo " "
# echo "DORA-TM1 POWER"
# echo " "

# echo "DORA-TM1-{GetSubData,GetAccData,CallFwdMix,UpdLocation} POWER - LARGE"
# SEQ=(221 223 225 226) ; for trx in ${SEQ[@]}; do ./scripts/powerwrapper.sh EXP tm1-100-nl 300 3 30 $trx 1 70 large cd; done

# echo "DORA-TM1-{UpdSubData,GetNewDest,TM1Mix} POWER - SMALL"
# SEQ=(222 224 200) ; for trx in ${SEQ[@]}; do ./scripts/powerwrapper.sh EXP tm1-100-nl 300 3 30 $trx 1 70 small cd; done



# echo " "
# echo "DORA-TPCB POWER"
# echo " "

# echo "DORA-TPCB POWER - SMALL"
# SEQ=(331) ; for trx in ${SEQ[@]}; do ./scripts/powerwrapper.sh EXP tpcb-50-nl 300 3 30 $trx 1 70 small cd; done



# echo " "
# echo "DORA-TPCC POWER"
# echo " "

# echo "DORA-TPCC-{ALL} POWER - SMALL"
# SEQ=(101 102 103 104 105 109 100) ; for trx in ${SEQ[@]}; do ./scripts/powerwrapper.sh EXP tpcc-50-nl 800 3 30 $trx 1 70 small cd; done



echo " "
echo "DBX RUNS"
echo " "

echo " "
echo "BASE-TM1-{ALL} DBX"
echo " "
SEQ=(20 21 22 23 24 25 26) ; for trx in ${SEQ[@]}; do ./scripts/dbxrunwrapper.sh EXPDBX tm1-100 $trx ./scripts/loggerrun.sh 100 30 3 $trx 400 large cd; done


echo " "
echo "BASE-TPCB DBX"
echo " "
SEQ=(31) ; for trx in ${SEQ[@]}; do ./scripts/dbxrunwrapper.sh EXPDBX tpcb-100 $trx ./scripts/loggerrun.sh 100 30 3 $trx 400 large cd; done


echo " "
echo "BASE-TPCC-{ALL} DBX"
echo " "
SEQ=(0 1 2 3 4 5 9) ; for trx in ${SEQ[@]}; do ./scripts/dbxrunwrapper.sh EXPDBX tpcc-100 $trx ./scripts/loggerrun.sh 100 30 3 $trx 1500 large cd; done



# echo " "
# echo "DORA-TM-1 DBX"
# echo " "

# echo "DORA-TM1-{GetSubData,GetAccData,CallFwdMix,UpdLocation} DBX - LARGE"
# SEQ=(221 223 225 226) ; for trx in ${SEQ[@]}; do ./scripts/dbxrunwrapper.sh EXPDBX tm1-100-nl $trx ./scripts/loggerrun.sh 100 30 3 $trx 400 large cd; done

# echo "DORA-TM1-{UpdSubData,GetNewDest,TM1Mix} POWER - SMALL"
# SEQ=(222 224 200) ; for trx in ${SEQ[@]}; do ./scripts/dbxrunwrapper.sh EXPDBX tm1-100-nl $trx ./scripts/loggerrun.sh 100 30 3 $trx 400 small cd; done

# echo " "
# echo "DORA-TPCB DBX - SMALL"
# echo " "
# SEQ=(331) ; for trx in ${SEQ[@]}; do ./scripts/dbxrunwrapper.sh EXPDBX tpcb-50-nl $trx ./scripts/loggerrun.sh 100 30 3 $trx 400 large; done


# echo " "
# echo "DORA-TPCC DBX"
# echo " "

# echo "DORA-TPCC-{ALL} DBX - SMALL"
# SEQ=(101 102 103 104 105 109 100) ; for trx in ${SEQ[@]}; do ./scripts/dbxrunwrapper.sh EXPDBX tpcc-50-nl $trx ./scripts/loggerrun.sh 100 30 3 $trx 800 large; done

