#include "experiments_env.h"

#include <pwd.h>
#include <unistd.h>

#include <sys/types.h>

#include <string>

#include <Lintel/ProgramOptions.hpp>

using namespace std;


string getUserName() {
    //Lifted from KVS
    const uint32_t STRING_BUFFER_SIZE = 2048;
    struct passwd password, *pwptr;
    char string_buffer[STRING_BUFFER_SIZE];
    if(getpwuid_r(getuid(), &password, string_buffer,
                  STRING_BUFFER_SIZE, &pwptr) !=0) {
        //Hmm, can't get user name.
        return "unknown-user";
    }
    return password.pw_name;
}

lintel::ProgramOption<string>
po_log_folder("log-folder", "What directory to log to", "/dev/shm/USER/foster/log");

lintel::ProgramOption<string>
po_clog_folder("clog-folder", "What directory to log to (clog)", "/dev/shm/USER/foster/clog");

const char * getLogFolder(){
    static string retval;
    if (!po_log_folder.used()) {
        if (retval.length()!=0) {
            return retval.c_str();
        }
        retval = "/dev/shm/" + getUserName() + "/foster/log";
        return retval.c_str();
    } else {
        return po_log_folder.get().c_str();
    }
    
}

lintel::ProgramOption<string>
po_archive_folder("archive-folder", "Where to store the log archive",
        "/dev/shm/USER/foster/archive");

const char * getArchiveFolder(){
    static string retval;
    if (!po_archive_folder.used()) {
        if (retval.length()!=0) {
            return retval.c_str();
        }
        retval = "/dev/shm/" + getUserName() + "/foster/archive";
        return retval.c_str();
    } else {
        return po_archive_folder.get().c_str();
    }
    
}

lintel::ProgramOption<std::string>
    po_data_device("data-device", "Backing store to use", "/dev/shm/USER/foster/data");

const char * getDataDevice(){

    static std::string retval;
    if (!po_data_device.used()) {
        if (retval.length()!=0) {
            return retval.c_str();
        }
        retval = "/dev/shm/" + getUserName() + "/foster/data";
        return retval.c_str();
    } else {
        return po_data_device.get().c_str();
    }
}

const char * getCLogFolder(){
    static string retval;
    if (!po_clog_folder.used()) {
        if (retval.length()!=0) {
            return retval.c_str();
        }
        retval = "/dev/shm/" + getUserName() + "/foster/clog";
        return retval.c_str();
    } else {
        return po_clog_folder.get().c_str();
    }
}


lintel::ProgramOption<std::string>
    po_backup_folder("backup-folder", "What directory we store/look-for backup files",
                        "/dev/shm/USER/foster/backup");

const char * getBackupFolder(){
    // ohhh, static holder to return std::string.c_str(). bad idea, we should just pass around
    // std::string. But I'm assimilated by the surrounding code here.
    static std::string retval;
    if (!po_backup_folder.used()) {
        if (retval.length()!=0) {
            return retval.c_str();
        }
        retval = "/dev/shm/" + getUserName() + "/foster/backup";
        return retval.c_str();
    } else {
        return po_backup_folder.get().c_str();
    }
}

