#!/bin/sh

## 
## (c) Copyright 2013-2014, Hewlett-Packard Development Company, LP
## 


usr=`whoami`

mkdir -p "/dev/shm/$usr/backup/"
rm -rf /dev/shm/$usr/backup/*
mkdir -p "/dev/shm/$usr/foster/log"

cp -r /dev/shm/$usr/foster/data /dev/shm/$usr/foster/log /dev/shm/$usr/backup/
