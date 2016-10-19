#!/bin/sh

## 
## (c) Copyright 2013-2014, Hewlett-Packard Development Company, LP
## 


usr=`whoami`
rm -rf /dev/shm/$usr/foster/data /dev/shm/$usr/foster/log
cp -r /dev/shm/$usr/backup/data /dev/shm/$usr/backup/log /dev/shm/$usr/foster/
