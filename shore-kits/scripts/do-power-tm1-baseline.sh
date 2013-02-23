#!/bin/bash

#@file:   scripts/do-power-tm1-baseline.sh 
#@brief:  Does powerruns for the TM1 transactions for Baseline
#@author: Ippokratis Pandis

echo "for ((trx=20; trx <= 26; trx++)); do ./scripts/powerwrapper.sh EXP base-tm1 1 80 $trx 30 4; done"

for ((trx=20; trx <= 26; trx++)); do ./scripts/powerwrapper.sh EXP base-tm1 1 80 $trx 30 4; done
