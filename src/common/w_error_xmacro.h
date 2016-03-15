/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */

/**
 * \file w_error_xmacro.h
 * \brief Error code/message definition in X-Macro style.
 *
 * \details
 * To add new errors, append a new line at the end.
 * You can also insert at an arbitrary place, but note that the value of existing
 * error codes will change in that case.  If we store error codes somewhere, it
 * might become an issue.  For now we don't do that anywhere, though.
 * For more details, see w_error.h.
 */

X(fcINTERNAL,                 "Internal error")
X(fcOS,                       "Operating system error ")
X(fcFULL,                     "Container is full")
X(fcOUTOFMEMORY,              "Malloc failed: out of memory ")
X(fcMMAPFAILED,               "Mmap could not map aligned memory")
X(fcNOTIMPLEMENTED,           "Feature is not implemented")

X(stINTERNAL,                 "Internal error")
X(stOS,                       "Operating system error ")
X(stTIMEOUT,                  "Timed out waiting for resource")
X(stBADPATH,                  "Path name cannot be stat()ed")
X(stBADFD,                    "Bad file descriptor for I/O operation, seek, or close")
X(stINUSE,                    "Resource in use")
X(stSHORTIO,                  "Short I/O")
X(stSHORTSEEK,                "Short Seek")

X(eINTERNAL,                  "Internal error")
X(eOS,                        "Operating system error ")
X(eNOTIMPLEMENTED,            "Feature is not implemented")
X(eBADOPTION,                 "Invalid value for SM option")
X(eUSERABORT,                 "User initiated abort")
X(eCRASH,                     "Server told to crash or shutdown immediately")
X(eOUTOFSPACE,                "Out of disk space")
X(eBADSTID,                   "Malformed or invalid store ID")
X(eNOTFOUND,                  "Item not found ")
X(eFRAMENOTFOUND,             "Frame not found in buffer pool ")
X(eBADVOL,                    "Invalid volume ID")
X(eVOLTOOLARGE,               "Volume is too large for device")
X(eDEVTOOLARGE,               "Device is too large for OS file interface")
X(eDEVICEVOLFULL,             "Device cannot hold anymore volumes")
X(eDEVNOTMOUNTED,             "Device is not mounted")
X(eALREADYMOUNTED,            "Device already mounted")
X(eVOLEXISTS,                 "Volume already exists")
X(eBADFORMAT,                 "Volume has bad format")
X(eNVOL,                      "Too many volumes")
X(eEOF,                       "End of scan/record reached")
X(eDUPLICATE,                 "Duplicate entries found")
X(eBADSTOREFLAGS,             "Bad store flags")
X(eSTCACHEFULL,               "Maximum number of stores already created")
X(eNDXNOTEMPTY,               "Index is not empty")
X(eOUTOFLOGSPACE,             "Out of log space ")
X(eOUTOFMEMORY,               "Malloc failed: out of memory ")
X(eRECWONTFIT,                "Record will not fit")
X(eCANTCOMPRESS,              "B-tree page cannot be compressed further")
X(eBADSAVEPOINT,              "Bad save point ")
X(eBADCHECKSUM,               "Page checksum does not match")
X(eBADARGUMENT,               "Bad argument or combination of arguments to function")
X(eTWOTHREAD,                 "Multiple threads not allowed for this operation")
X(eTWOUTHREAD,                "Multiple update threads not allowed for this operation")
X(eNOTRANS,                   "No active transaction")
X(eINTRANS,                   "In active transaction (not allowed for this operation)")
X(eNOABORT,                   "Logging is turned off -- cannot roll back")
X(eISPREPARED,                "Transaction thread is prepared  -- cannot do this operaton")
X(eNOTEXTERN2PC,              "Transaction is not participating in external 2-phase commit ")
X(eTHREADMAPFULL,             "No more bits for thread maps")
X(eLOCKTIMEOUT,               "Lock timeout")
X(eCONDLOCKTIMEOUT,           "Conditional lock timeout. Unconditional retry should follow")
X(eLATCHQFAIL,                "Attempt to acquire/maintain latch in Q mode failed")
X(ePARENTLATCHQFAIL,          "Attempt to fix page in Q mode failed because parent's Q latch failed")
X(eNEEDREALLATCH,             "Need parent and child page fixed in SH or EX modes")
X(eNOTBLOCKED,                "Smthread is not blocked")
X(eDEADLOCK,                  "Deadlock detected")
X(eBADCCLEVEL,                "Unsupported concurrency control level")
X(eRETRY,                     "Retry the request (used internally)")
X(eGOODRETRY,                 "Retry, but had some progress (used internally)")
X(eLOCKRETRY,                 "Retry related to locks (used internally)")
X(eTOOMANYRETRY,              "This IS an error and can be thrown to user")
X(eCANTWHILEACTIVEXCTS,       "Can't do to volume while there are active transactions")
X(eLISTTOOLONG,               "Too many items in list")
X(eLOGVERSIONTOONEW,          "Log created with newer incompatible server")
X(eLOGVERSIONTOOOLD,          "Log created with older incompatible server")
X(eBADMASTERCHKPTFORMAT,      "Bad format for master checkpoint")
X(eLOGSPACEWARN,              "Log space warning (not necessarily out)")
X(eBADCOMPENSATION,           "Log could not apply requested compensation")
X(eDUAUDITFAILED,             "Audit failed for du statistics")
X(ePINACTIVE,                 "Thread has something pinned")
X(eBPFORCEFAILED,             "Could not force all the necessary pages from the buffer pool")
X(eWRITEORDERLOOP,            "Causes a loop in write order dependency")
X(eLIL_TOOMANYVOL_XCT,        "Accessing too many volumes in one transaction")
X(eLIL_TOOMANYST_XCT,         "Accessing too many stores in one transaction")
X(eBF_DIRECTFIX_SWIZZLED_PTR, "Requested a direct page fix with swizzled pointer")
X(eWRONG_PAGE_LSNCHAIN,       "Not the right per-page LSN chain to follow")
X(eNO_PARENT_SPR,             "Parent page needed for Single-Page-Recovery")
X(eBACKUPBUSY,                "Backup is already being taken")
X(eNO_BACKUP_FILE,            "Backup file does not exist")
X(eBACKUP_SHORTSEEK,          "Failed to seek in Backup file")
X(eBACKUP_SHORTIO,            "Failed to read from Backup file")
X(eBFFULL,                    "Buffer pool is full during Recovery operation")
X(eACCESS_CONFLICT,           "User transaction is conflicting with Recovery task on a page access")
X(eBAD_BACKUPPAGE,            "Retrieved page from backup file was incorrect")
X(eVOLFAILED,                 "Volume is failed")

/*
 * CS: The old Shore-MT RC used a simple integer as error code, which allowed
 * any application to define its own codes. This was done in Shore-Kits to
 * represent errors in tables, indices, etc.
 *
 * Since extending error codes is not possible with the new Zero design, I've
 * simply copied the Kits error codes below.
 */
X(se_NOT_FOUND                , "se_NOT_FOUND")
X(se_VOLUME_NOT_FOUND         , "se_VOLUME_NOT_FOUND")
X(se_INDEX_NOT_FOUND          , "se_INDEX_NOT_FOUND")
X(se_TABLE_NOT_FOUND          , "se_TABLE_NOT_FOUND")
X(se_TUPLE_NOT_FOUND          , "se_TUPLE_NOT_FOUND")
X(se_NO_CURRENT_TUPLE         , "se_NO_CURRENT_TUPLE")
X(se_CANNOT_INSERT_TUPLE      , "se_CANNOT_INSERT_TUPLE")
X(se_SCAN_OPEN_ERROR          , "se_SCAN_OPEN_ERROR")
X(se_INCONSISTENT_INDEX       , "se_INCONSISTENT_INDEX")
X(se_OPEN_SCAN_ERROR          , "se_OPEN_SCAN_ERROR")
X(se_LOAD_NOT_EXCLUSIVE       , "se_LOAD_NOT_EXCLUSIVE")
X(se_ERROR_IN_LOAD            , "se_ERROR_IN_LOAD")
X(se_ERROR_IN_IDX_LOAD        , "se_ERROR_IN_IDX_LOAD")
X(se_WRONG_DISK_DATA          , "se_WRONG_DISK_DATA")
X(se_INVALID_INPUT            , "se_INVALID_INPUT")
