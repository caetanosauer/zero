#ifndef EXPERIMENTS_ENV_H
#define EXPERIMENTS_ENV_H

#include <pwd.h>
#include <unistd.h>

#include <sys/types.h>

#include <string>

#define LOG_FOLDER getLogFolder()
#define DATA_DEVICE getDataDevice()

//const char* LOG_FOLDER = "/home/hkimura/logs";
//const char* LOG_FOLDER = "/dev/shm/tpcb/log";
// const char* LOG_FOLDER = "/media/SSDVolume";
//const char* DATA_DEVICE = "/dev/shm/tpcb/data";
const char * getLogFolder();
const char * getCLogFolder();
const char * getDataDevice();
const char * getBackupFolder();
const char * getArchiveFolder();


#endif// EXPERIMENTS_ENV_H
