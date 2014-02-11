/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager
   
                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne
   
                         All Rights Reserved.
   
   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.
   
   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

/*<std-header orig-src='shore'>

 $Id: sm.cpp,v 1.501 2010/12/17 19:36:26 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#define SM_SOURCE
#define SM_C

#ifdef __GNUG__
class prologue_rc_t;
#endif

#include "w.h"
#include "sm_int_4.h"
#include "chkpt.h"
#include "sm.h"
#include "sm_vtable_enum.h"
#include "prologue.h"
#include "device.h"
#include "vol.h"
#include "bf_tree.h"
#include "crash.h"
#include "restart.h"
#include "sm_options.h"
#include "suppress_unused.h"
#include "backup.h"

#ifdef EXPLICIT_TEMPLATE
template class w_auto_delete_t<SmStoreMetaStats*>;
#endif

bool        smlevel_0::shutdown_clean = true;
bool        smlevel_0::shutting_down = false;

smlevel_0::operating_mode_t 
            smlevel_0::operating_mode = smlevel_0::t_not_started;

            //controlled by AutoTurnOffLogging:
bool        smlevel_0::lock_caching_default = true;
bool        smlevel_0::logging_enabled = true;
bool        smlevel_0::do_prefetch = false;

bool        smlevel_0::statistics_enabled = true;

#ifndef SM_LOG_WARN_EXCEED_PERCENT
#define SM_LOG_WARN_EXCEED_PERCENT 40
#endif
smlevel_0::fileoff_t smlevel_0::log_warn_trigger = 0;
int                  smlevel_0::log_warn_exceed_percent = 
                                    SM_LOG_WARN_EXCEED_PERCENT;
ss_m::LOG_WARN_CALLBACK_FUNC      
                     smlevel_0::log_warn_callback = 0;
ss_m::LOG_ARCHIVED_CALLBACK_FUNC 
                     smlevel_0::log_archived_callback = 0;

// these are set when the logsize option is set
smlevel_0::fileoff_t        smlevel_0::max_logsz = 0;
smlevel_0::fileoff_t        smlevel_0::chkpt_displacement = 0;

// Whenever a change is made to data structures stored on a volume,
// volume_format_version be incremented so that incompatibilities
// will be detected.
//
// Different ALIGNON values are NOT reflected in the version number,
// so it is still possible to create incompatible volumes by changing
// ALIGNON.
//
//  1 = original
//  2 = lid and lgrex indexes contain vid_t
//  3 = lid index no longer contains vid_t
//  4 = added store flags to pages
//  5 = large records no longer contain vid_t
//  6 = volume headers have lvid_t instead of vid_t
//  7 = removed vid_t from sinfo_s (stored in directory index)
//  8 = added special store for 1-page btrees
//  9 = changed prefix for reserved root index entries to SSM_RESERVED
//  10 = extent link changed shape.
//  11 = extent link changed, allowing concurrency in growing a store
//  12 = dir btree contents changed (removed store flag and property)
//  13 = Large volumes : changed size of snum_t and extnum_t
//  14 = Changed size of lsn_t, hence log record headers were rearranged
//       and page headers changed.  Small disk address
//  15 = Same as 14, but with large disk addresses.
//  16 = Align body of page to an eight byte boundary.  This should have 
//       occured in 14, but there are some people using it, so need seperate
//       numbers.
//  17 = Same as 16 but with large disk addresses.   
//  18 = Release 6.0 of the storage manager.  
//       Only large disk addresses, 8-byte alignment, added _hdr_pages to
//       volume header, logical IDs and 1page indexes are deprecated.
//       Assumes 64-bit architecture.
//       No support for older volume formats.

#define        VOLUME_FORMAT        18

uint32_t        smlevel_0::volume_format_version = VOLUME_FORMAT;


/*
 * _being_xct_mutex: Used to prevent xct creation during volume dismount.
 * Its sole purpose is to be sure that we don't have transactions 
 * running while we are  creating or destroying volumes or 
 * mounting/dismounting devices, which are generally 
 * start-up/shut-down operations for a server.
 */

typedef srwlock_t sm_vol_rwlock_t;
// Certain operations have to exclude xcts
static sm_vol_rwlock_t          _begin_xct_mutex;

backup_m* smlevel_0::bk = 0;
device_m* smlevel_0::dev = 0;
io_m* smlevel_0::io = 0;
bf_tree_m* smlevel_0::bf = 0;
log_m* smlevel_0::log = 0;
tid_t *smlevel_0::redo_tid = 0;

lock_m* smlevel_0::lm = 0;

ErrLog*            smlevel_0::errlog;


char smlevel_0::zero_page[page_sz];

chkpt_m* smlevel_1::chkpt = 0;

btree_m* smlevel_2::bt = 0;

lid_m* smlevel_4::lid = 0;

ss_m* smlevel_4::SSM = 0;

/*
 *  Class ss_m code
 */

/*
 *  Order is important!!
 */
int ss_m::_instance_cnt = 0;
//ss_m::param_t ss_m::curr_param;

void ss_m::_set_option_logsize() {
    // the logging system should not be running.  if it is
    // then don't set the option
    if (!_options.get_bool_option("sm_logging", true) || smlevel_0::log) return;

    fileoff_t maxlogsize = fileoff_t(_options.get_int_option("sm_logsize", 10000));

    // The option is in units of KB; convert it to bytes.
    maxlogsize *= 1024;

    // maxlogsize is the user-defined maximum open-log size.
    // Compile-time constants determine the size of a segment,
    // and the open log size is smlevel_0::max_openlog segments,
    // so that means we determine the number of segments per
    // partition thus:
    // max partition size is user max / smlevel_0::max_openlog.
    // max partition size must be an integral multiple of segments
    // plus 1 block. The log manager computes this for us:
    fileoff_t psize = maxlogsize / smlevel_0::max_openlog;

    // convert partition size to partition data size: (remove overhead)
    psize = log_m::partition_size(psize);

    /* Enforce the built-in shore limit that a log partition can only
       be as long as the file address in a lsn_t allows for...  
       This is really the limit of a LSN, since LSNs map 1-1 with disk
       addresses. 
       Also that it can't be larger than the os allows
   */

    if (psize > log_m::max_partition_size()) {
        // we might not be able to do this: 
        fileoff_t tmp = log_m::max_partition_size();
        tmp /= 1024;

        std::cerr << "Partition data size " << psize
                << " exceeds limit (" << log_m::max_partition_size() << ") "
                << " imposed by the size of an lsn."
                << std::endl;
        std::cerr << " Choose a smaller sm_logsize." << std::endl;
        std::cerr << " Maximum is :" << tmp << std::endl;
        W_FATAL(eCRASH);
    }

    if (psize < log_m::min_partition_size()) {
        fileoff_t tmp = fileoff_t(log_m::min_partition_size());
        tmp *= smlevel_0::max_openlog;
        tmp /= 1024;
        std::cerr
            << "Partition data size (" << psize 
            << ") is too small for " << endl
            << " a segment ("  
            << log_m::min_partition_size()   << ")" << endl
            << "Partition data size is computed from sm_logsize;"
            << " minimum sm_logsize is " << tmp << endl;
        W_FATAL(eCRASH);
    }


    // maximum size of all open log files together
    smlevel_0::max_logsz = fileoff_t(psize * smlevel_0::max_openlog);

    // cerr << "Resulting max_logsz " << max_logsz << " bytes" << endl;

    // take check points every 3 log file segments.
    smlevel_0::chkpt_displacement = log_m::segment_size() * 3;
}

/* 
 * NB: reverse function, _make_store_property
 * is defined in dir.cpp -- so far, used only there
 */
ss_m::store_flag_t
ss_m::_make_store_flag(store_property_t property)
{
    store_flag_t flag = st_unallocated;

    switch (property)  {
        case t_regular:
            flag = st_regular;
            break;
        case t_temporary:
            flag = st_tmp;
            break;
        case t_load_file:
            flag = st_load_file;
            break;
        case t_insert_file:
            flag = st_insert_file;
            break;
        case t_bad_storeproperty:
        default:
            W_FATAL_MSG(eINTERNAL, << "bad store property :" << property );
            break;
    }

    return flag;
}


static queue_based_block_lock_t ssm_once_mutex;
ss_m::ss_m(
    const sm_options &options,
    smlevel_0::LOG_WARN_CALLBACK_FUNC callbackwarn /* = NULL */,
    smlevel_0::LOG_ARCHIVED_CALLBACK_FUNC callbackget /* = NULL */
)
    :   _options(options)
{
    _set_option_logsize();
    sthread_t::initialize_sthreads_package();

    // This looks like a candidate for pthread_once(), 
    // but then smsh would not be able to
    // do multiple startups and shutdowns in one process, alas. 
    CRITICAL_SECTION(cs, ssm_once_mutex);
    _construct_once(callbackwarn, callbackget);
}

void
ss_m::_construct_once(
    smlevel_0::LOG_WARN_CALLBACK_FUNC warn,
    smlevel_0::LOG_ARCHIVED_CALLBACK_FUNC get
)
{
    FUNC(ss_m::_construct_once);

    smlevel_0::log_warn_callback  = warn;
    smlevel_0::log_archived_callback  = get;

    // Clear out the fingerprint map for the smthreads.
    // All smthreads created after this will be compared against
    // this map for duplication.
    smthread_t::init_fingerprint_map();

    if (_instance_cnt++)  {
        // errlog might not be null since in this case there was another instance.
        if(errlog) {
            errlog->clog << fatal_prio 
            << "ss_m cannot be instantiated more than once"
             << flushl;
        }
        W_FATAL_MSG(eINTERNAL, << "instantiating sm twice");
    }

    /*
     *  Level 0
     */
    errlog = new ErrLog("ss_m", log_to_unix_file, _options.get_string_option("sm_errlog", "-").c_str());
    if(!errlog) {
        W_FATAL(eOUTOFMEMORY);
    }


    std::string error_loglevel = _options.get_string_option("sm_errlog_level", "error");
    errlog->setloglevel(ErrLog::parse(error_loglevel.c_str()));
    ///////////////////////////////////////////////////////////////
    // Henceforth, all errors can go to ss_m::errlog thus:
    // ss_m::errlog->clog << XXX_prio << ... << flushl;
    // or
    // ss_m::errlog->log(log_XXX, "format...%s..%d..", s, n); NB: no newline
    ///////////////////////////////////////////////////////////////
#if W_DEBUG_LEVEL > 0
	// just to be sure errlog is working
	errlog->clog << debug_prio << "Errlog up and running." << flushl; 
#endif

    w_assert1(page_sz >= 1024);

    /*
     *  Reset flags
     */
    shutting_down = false;
    shutdown_clean = true;

   /*
    * buffer pool size
    */

    int64_t bufpoolsize = _options.get_int_option("sm_bufpoolsize", 8192);
    uint32_t  nbufpages = (bufpoolsize * 1024 - 1) / page_sz + 1;
    if (nbufpages < 10)  {
        errlog->clog << fatal_prio << "ERROR: buffer size ("
             << bufpoolsize
             << "-KB) is too small" << flushl;
        errlog->clog << fatal_prio << "       at least " << 32 * page_sz / 1024
             << "-KB is needed" << flushl;
        W_FATAL(eCRASH);
    }

    // number of page writers
    int32_t npgwriters = _options.get_int_option("sm_num_page_writers", 1);
    if(npgwriters < 0) {
        errlog->clog << fatal_prio << "ERROR: num page writers must be positive : "
             << npgwriters
             << flushl;
        W_FATAL(eCRASH);
    }
    if (npgwriters == 0) {
        npgwriters = 1;
    }

    int64_t cleaner_interval_millisec_min = _options.get_int_option("sm_cleaner_interval_millisec_min", 1000);
    if (cleaner_interval_millisec_min <= 0) {
        cleaner_interval_millisec_min = 1000;
    }

    int64_t cleaner_interval_millisec_max = _options.get_int_option("sm_cleaner_interval_millisec_max", 256000);
    if (cleaner_interval_millisec_max <= 0) {
        cleaner_interval_millisec_max = 256000;
    }

    uint64_t logbufsize = _options.get_int_option("sm_logbufsize", 128 << 10); // at least 128KB
    // pretty big limit -- really, the limit is imposed by the OS's
    // ability to read/write
    if (uint64_t(logbufsize) < (uint64_t) 4 * ss_m::page_sz) {
        errlog->clog << fatal_prio 
        << "Log buf size (sm_logbufsize = " << (int)logbufsize
        << " ) is too small for pages of size " 
        << unsigned(ss_m::page_sz) << " bytes."
        << flushl; 
        errlog->clog << fatal_prio 
        << "Need to hold at least 4 pages ( " << 4 * ss_m::page_sz
        << ")"
        << flushl; 
        W_FATAL(eCRASH);
    }
    if (uint64_t(logbufsize) > uint64_t(max_int4)) {
        errlog->clog << fatal_prio 
        << "Log buf size (sm_logbufsize = " << (int)logbufsize
        << " ) is too big: individual log files can't be large files yet."
        << flushl; 
        W_FATAL(eCRASH);
    }

    /*
     * Now we can create the buffer manager
     */ 
    bool initially_enable_cleaners = _options.get_bool_option("sm_backgroundflush", true);
    bool bufferpool_swizzle = _options.get_bool_option("sm_bufferpool_swizzle", false);
    std::string bufferpool_replacement_policy = _options.get_string_option("sm_bufferpool_replacement_policy", "clock"); // clock or random

    uint32_t cleaner_write_buffer_pages = (uint32_t) _options.get_int_option("sm_cleaner_write_buffer_pages", 64);
    bf = new bf_tree_m(nbufpages, npgwriters, cleaner_interval_millisec_min, cleaner_interval_millisec_max, cleaner_write_buffer_pages, bufferpool_replacement_policy.c_str(), initially_enable_cleaners, bufferpool_swizzle);
    if (! bf) {
        W_FATAL(eOUTOFMEMORY);
    }
    /* just hang onto this until we create thelog manager...*/
    lm = new lock_m(_options.get_int_option("sm_locktablesize", 64000));
    if (! lm)  {
        W_FATAL(eOUTOFMEMORY);
    }

    bk = new backup_m();
    if (! bk) {
        W_FATAL(eOUTOFMEMORY);
    }

    dev = new device_m;
    if (! dev) {
        W_FATAL(eOUTOFMEMORY);
    }

    io = new io_m;
    if (! io) {
        W_FATAL(eOUTOFMEMORY);
    }

    /*
     *  Level 1
     */
    smlevel_0::logging_enabled = _options.get_bool_option("sm_logging", true);
    if (logging_enabled)  
    {
        if(max_logsz < 8*int(logbufsize)) {
          errlog->clog << warning_prio << 
            "WARNING: Log buffer is bigger than 1/8 partition (probably safe to make it smaller)."
                   << flushl;
        }
        std::string logdir = _options.get_string_option("sm_logdir", "");
        if (logdir.empty()) {
            errlog->clog << fatal_prio  << "ERROR: sm_logdir must be set to enable logging." << flushl;
            W_FATAL(eCRASH);
        }
        w_rc_t e = log_m::new_log_m(log,
                     logdir.c_str(),
                     logbufsize, 
                     _options.get_bool_option("sm_reformat_log", false));
        W_COERCE(e);

        int percent = _options.get_int_option("sm_log_warn", 0);

        // log_warn_exceed is %; now convert it to raw # bytes
        // that we must have left at all times. When the space available
        // in the log falls below this, it'll trigger the warning.
        if (percent > 0) {
            smlevel_0::log_warn_trigger  = (long) (
        // max_openlog is a compile-time constant
                log->limit() * max_openlog * 
                (100.0 - (double)smlevel_0::log_warn_exceed_percent) / 100.00);
        }

    } else {
        /* Run without logging at your own risk. */
        errlog->clog << warning_prio << 
        "WARNING: Running without logging! Do so at YOUR OWN RISK. " 
        << flushl;
    }
    
    smlevel_0::statistics_enabled = _options.get_bool_option("sm_statistics", true);

    // start buffer pool cleaner when the log module is ready
    {
        w_rc_t e = bf->init();
        W_COERCE(e);
    }

    DBG(<<"Level 2");
    
    /*
     *  Level 2
     */
    
    bt = new btree_m;
    if (! bt) {
        W_FATAL(eOUTOFMEMORY);
    }
    bt->construct_once();

    DBG(<<"Level 3");
    /*
     *  Level 3
     */
    chkpt = new chkpt_m;
    if (! chkpt)  {
        W_FATAL(eOUTOFMEMORY);
    }

    DBG(<<"Level 4");
    /*
     *  Level 4
     */
    SSM = this;

    lid = new lid_m();
    if (! lid) {
        W_FATAL(eOUTOFMEMORY);
    }

    me()->mark_pin_count();
 
    /*
     * Mount the volumes for recovery.  For now, we automatically
     * mount all volumes.  A better solution would be for restart_m
     * to tell us, after analysis, whether any volumes should be
     * mounted.  If not, we can skip the mount/dismount.
     */

    if (_options.get_bool_option("sm_logging", true))  {
        restart_m restart;
        smlevel_0::redo_tid = restart.redo_tid();
        restart.recover(log->master_lsn());

        {   // contain the scope of dname[]
            // record all the mounted volumes after recovery.
            int num_volumes_mounted = 0;
            int        i;
            char    **dname;
            dname = new char *[max_vols];
            if (!dname) {
                W_FATAL(fcOUTOFMEMORY);
            }
            for (i = 0; i < max_vols; i++) {
                dname[i] = new char[smlevel_0::max_devname+1];
                if (!dname[i]) {
                    W_FATAL(fcOUTOFMEMORY);
                }
            }
            vid_t    *vid = new vid_t[max_vols];
            if (!vid) {
                W_FATAL(fcOUTOFMEMORY);
            }

            W_COERCE( io->get_vols(0, max_vols, dname, vid, num_volumes_mounted) );

            DBG(<<"Dismount all volumes " << num_volumes_mounted);
            // now dismount all of them at the io level, the level where they
            // were mounted during recovery.
            W_COERCE( io->dismount_all(true/*flush*/) );

            // now mount all the volumes properly at the sm level.
            // then dismount them and free temp files only if there
            // are no locks held.
            for (i = 0; i < num_volumes_mounted; i++)  {
                uint vol_cnt;
                rc_t rc;
                DBG(<<"Remount volume " << dname[i]);
                rc =  _mount_dev(dname[i], vol_cnt, vid[i]) ;
                if(rc.is_error()) {
                    ss_m::errlog->clog  << warning_prio
                    << "Volume on device " << dname[i]
                    << " was only partially formatted; cannot be recovered."
                    << flushl;
                } else {
                    W_COERCE( _dismount_dev(dname[i]));
                }
            }
            delete [] vid;
            for (i = 0; i < max_vols; i++) {
                delete [] dname[i];
            }
            delete [] dname;    
        }

        smlevel_0::redo_tid = 0;

    }

    smlevel_0::operating_mode = t_forward_processing;

    // Have the log initialize its reservation accounting.
    if(log) log->activate_reservations();

    // Force the log after recovery.  The background flush threads exist
    // and might be working due to recovery activities.
    // But to avoid interference with their control structure, 
    // we will do this directly.  Take a checkpoint as well.
    if(log) {
        bf->force_until_lsn(log->curr_lsn().data());
        chkpt->wakeup_and_take();
    }    

    me()->check_pin_count(0);

    chkpt->spawn_chkpt_thread();

    do_prefetch = _options.get_bool_option("sm_prefetch", false);
    DBG(<<"constructor done");
}

ss_m::~ss_m()
{
    // This looks like a candidate for pthread_once(), but then smsh 
    // would not be able to
    // do multiple startups and shutdowns in one process, alas. 
    CRITICAL_SECTION(cs, ssm_once_mutex);
    _destruct_once();
}

void
ss_m::_destruct_once()
{
    FUNC(ss_m::~ss_m);

    --_instance_cnt;

    if (_instance_cnt)  {
        if(errlog) {
            errlog->clog << warning_prio << "ss_m::~ss_m() : \n"
             << "\twarning --- destructor called more than once\n"
             << "\tignored" << flushl;
        } else {
            cerr << "ss_m::~ss_m() : \n"
             << "\twarning --- destructor called more than once\n"
             << "\tignored" << endl;
        }
        return;
    }

    // Set shutting_down so that when we disable bg flushing, if the
    // log flush daemon is running, it won't just try to re-activate it.
    shutting_down = true;
    
    // get rid of all non-prepared transactions
    // First... disassociate me from any tx
    if(xct()) {
        me()->detach_xct(xct());
    }
    // now it's safe to do the clean_up
    int nprepared = xct_t::cleanup(false /* don't dispose of prepared xcts */);
    (void) nprepared; // Used only for debugging assert
    if (shutdown_clean) {
        // dismount all volumes which aren't locked by a prepared xct
        // We can't use normal dismounting for the prepared xcts because
        // they would be logged as dismounted. We need to dismount them
        // w/o logging turned on.
        // That happens below.

        W_COERCE( bf->force_all() );
        me()->check_actual_pin_count(0);

        // take a clean checkpoints with the volumes which need 
        // to be remounted and the prepared xcts
        // Note that this force_until_lsn will do a direct bpool scan
        // with serial writes since the background flushing has been
        // disabled
        if(log) bf->force_until_lsn(log->curr_lsn());
    chkpt->wakeup_and_take();

        // from now no more logging and checkpoints will be done
        chkpt->retire_chkpt_thread();

        W_COERCE( dev->dismount_all() );
    } else {
        /* still have to close the files, but don't log since not clean !!! */

        // from now no more logging and checkpoints will be done
        chkpt->retire_chkpt_thread();

        log_m* saved_log = log;
        log = 0;                // turn off logging

        W_COERCE( dev->dismount_all() );

        log = saved_log;            // turn on logging
    }
    nprepared = xct_t::cleanup(true /* now dispose of prepared xcts */);
    w_assert1(nprepared == 0);
    w_assert1(xct_t::num_active_xcts() == 0);

    lm->assert_empty(); // no locks should be left

    /*
     *  Level 4
     */
    delete lid; lid=0;

    /*
     *  Level 3
     */
    delete chkpt; chkpt = 0; // NOTE : not level 3 now, but
    // has been retired

    /*
     *  Level 2
     */
    bt->destruct_once();
    delete bt; bt = 0; // btree manager

    /*
     *  Level 1
     */


    // delete the lock manager
    delete lm; lm = 0; 

    if(log) {
        log->shutdown(); // log joins any subsidiary threads
        // We do not delete the log now; shutdown takes care of that. delete log;
    }
    log = 0;

    delete io; io = 0; // io manager
    delete dev; dev = 0; // device manager
    {
        w_rc_t e = bf->destroy();
        W_COERCE (e);
    }
    delete bf; bf = 0; // destroy buffer manager last because io/dev are flushing them!
    delete bk; bk = 0;
    /*
     *  Level 0
     */
    if (errlog) {
        delete errlog; errlog = 0;
    }

    /*
     *  free buffer pool memory
     */
     w_rc_t        e;
     char        *unused;
     e = smthread_t::set_bufsize(0, unused);
     if (e.is_error())  {
        cerr << "ss_m: Warning: set_bufsize(0):" << endl << e << endl;
     }
}

void ss_m::set_shutdown_flag(bool clean)
{
    shutdown_clean = clean;
}

/*--------------------------------------------------------------*
 *  ss_m::begin_xct()                                *
 *
 *\details
 *
 * You cannot start a transaction while any thread is :
 * - mounting or unmounting a device, or
 * - creating or destroying a volume.
 *--------------------------------------------------------------*/
rc_t 
ss_m::begin_xct(
        sm_stats_info_t*             _stats, // allocated by caller
        timeout_in_ms timeout)
{
    SM_PROLOGUE_RC(ss_m::begin_xct, not_in_xct, read_only,  0);
    tid_t tid;
    W_DO(_begin_xct(_stats, tid, timeout));
    return RCOK;
}
rc_t 
ss_m::begin_xct(timeout_in_ms timeout)
{
    SM_PROLOGUE_RC(ss_m::begin_xct, not_in_xct, read_only,  0);
    tid_t tid;
    W_DO(_begin_xct(0, tid, timeout));
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::begin_xct() - for Markos' tests                       *
 *--------------------------------------------------------------*/
rc_t
ss_m::begin_xct(tid_t& tid, timeout_in_ms timeout)
{
    SM_PROLOGUE_RC(ss_m::begin_xct, not_in_xct,  read_only, 0);
    W_DO(_begin_xct(0, tid, timeout));
    return RCOK;
}

rc_t ss_m::begin_sys_xct(bool single_log_sys_xct, bool deferred_ssx,
    sm_stats_info_t *stats, timeout_in_ms timeout)
{
    tid_t tid;
    W_DO (_begin_xct(stats, tid, timeout, true, single_log_sys_xct, deferred_ssx));
    return RCOK;
}


/*--------------------------------------------------------------*
 *  ss_m::commit_xct()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::commit_xct(sm_stats_info_t*& _stats, bool lazy,
                 lsn_t* plastlsn)
{
    SM_PROLOGUE_RC(ss_m::commit_xct, commitable_xct, read_write, 0);

    W_DO(_commit_xct(_stats, lazy, plastlsn));
    prologue.no_longer_in_xct();

    return RCOK;
}

rc_t
ss_m::commit_sys_xct()
{
    sm_stats_info_t *_stats = NULL; 
    W_DO(_commit_xct(_stats, true, NULL)); // always lazy commit
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::commit_xct_group()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::commit_xct_group(xct_t *list[], int listlen)
{
    W_DO(_commit_xct_group(list, listlen));
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::commit_xct()                                          *
 *--------------------------------------------------------------*/
rc_t
ss_m::commit_xct(bool lazy, lsn_t* plastlsn)
{
    SM_PROLOGUE_RC(ss_m::commit_xct, commitable_xct, read_write, 0);

    sm_stats_info_t*             _stats=0; 
    W_DO(_commit_xct(_stats,lazy,plastlsn));
    prologue.no_longer_in_xct();
    /*
     * throw away the _stats, since user isn't harvesting... 
     */
    delete _stats;

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::abort_xct()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::abort_xct(sm_stats_info_t*&             _stats)
{
    SM_PROLOGUE_RC(ss_m::abort_xct, abortable_xct, read_write, 0);

    // Temp removed for debugging purposes only
    // want to see what happens if the abort proceeds (scripts/alloc.10)
    bool was_sys_xct = xct() && xct()->is_sys_xct();
    W_DO(_abort_xct(_stats));
    if (!was_sys_xct) { // system transaction might be nested
        prologue.no_longer_in_xct();
    }

    return RCOK;
}
rc_t
ss_m::abort_xct()
{
    SM_PROLOGUE_RC(ss_m::abort_xct, abortable_xct, read_write, 0);
    sm_stats_info_t*             _stats=0;

    W_DO(_abort_xct(_stats));
    /*
     * throw away _stats, since user is not harvesting them
     */
    delete _stats;
    prologue.no_longer_in_xct();

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::save_work()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::save_work(sm_save_point_t& sp)
{
    // For now, consider this a read/write operation since you
    // wouldn't be doing this unless you intended to write and
    // possibly roll back.
    SM_PROLOGUE_RC(ss_m::save_work, in_xct, read_write, 0);
    W_DO( _save_work(sp) );
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::rollback_work()                            *
 *--------------------------------------------------------------*/
rc_t
ss_m::rollback_work(const sm_save_point_t& sp)
{
    SM_PROLOGUE_RC(ss_m::rollback_work, in_xct, read_write, 0);
    W_DO( _rollback_work(sp) );
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::num_active_xcts()                            *
 *--------------------------------------------------------------*/
uint32_t 
ss_m::num_active_xcts()
{
    return xct_t::num_active_xcts();
}
/*--------------------------------------------------------------*
 *  ss_m::tid_to_xct()                                *
 *--------------------------------------------------------------*/
xct_t* ss_m::tid_to_xct(const tid_t& tid)
{
    return xct_t::look_up(tid);
}

/*--------------------------------------------------------------*
 *  ss_m::xct_to_tid()                                *
 *--------------------------------------------------------------*/
tid_t ss_m::xct_to_tid(const xct_t* x)
{
    w_assert0(x != NULL);
    return x->tid();
}

/*--------------------------------------------------------------*
 *  ss_m::dump_xcts()                                           *
 *--------------------------------------------------------------*/
rc_t ss_m::dump_xcts(ostream& o)
{
    xct_t::dump(o);
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::state_xct()                                *
 *--------------------------------------------------------------*/
ss_m::xct_state_t ss_m::state_xct(const xct_t* x)
{
    w_assert3(x != NULL);
    return x->state();
}

smlevel_0::fileoff_t ss_m::xct_log_space_needed()
{
    w_assert3(xct() != NULL);
    return xct()->get_log_space_used();
}

rc_t ss_m::xct_reserve_log_space(fileoff_t amt) {
    w_assert3(xct() != NULL);
    return xct()->wait_for_log_space(amt);
}

/*--------------------------------------------------------------*
 *  ss_m::chain_xct()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::chain_xct( sm_stats_info_t*&  _stats, bool lazy)
{
    SM_PROLOGUE_RC(ss_m::chain_xct, commitable_xct, read_write, 0);
    W_DO( _chain_xct(_stats, lazy) );
    return RCOK;
}
rc_t
ss_m::chain_xct(bool lazy)
{
    SM_PROLOGUE_RC(ss_m::chain_xct, commitable_xct, read_write, 0);
    sm_stats_info_t        *_stats = 0;
    W_DO( _chain_xct(_stats, lazy) );
    /*
     * throw away the _stats, since user isn't harvesting... 
     */
    delete _stats;
    return RCOK;
}


/*--------------------------------------------------------------*
 *  ss_m::checkpoint()                                        
 *  For debugging, smsh
 *--------------------------------------------------------------*/
rc_t
ss_m::checkpoint()
{
    // Just kick the chkpt thread
    chkpt->wakeup_and_take();
    return RCOK;
}

rc_t ss_m::force_buffers() {
    return bf->force_all();
}

rc_t ss_m::force_volume(volid_t vol) {
    return bf->force_volume(vol);
}

/*--------------------------------------------------------------*
 *  ss_m::dump_buffers()                            *
 *  For debugging, smsh
 *--------------------------------------------------------------*/
rc_t
ss_m::dump_buffers(ostream &o)
{
    bf->debug_dump(o);
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::config_info()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::config_info(sm_config_info_t& info) {
    info.page_size = ss_m::page_sz;

    //however, fixable_page_h.space.acquire aligns() the whole mess (hdr + record)
    //which rounds up the space needed, so.... we have to figure that in
    //here: round up then subtract one aligned entity.
    // 
    // OK, now that _data is already aligned, we don't have to
    // lose those 4 bytes.
    info.lg_rec_page_space = btree_page::data_sz;
    info.buffer_pool_size = bf->get_block_cnt() * ss_m::page_sz / 1024;
    info.max_btree_entry_size  = btree_m::max_entry_size();
    info.exts_on_page  = 0;
    info.pages_per_ext = smlevel_0::ext_sz;

    info.logging  = (ss_m::log != 0);

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::set_disk_delay()                            *
 *--------------------------------------------------------------*/
rc_t
ss_m::set_disk_delay(u_int milli_sec)
{
    W_DO(io_m::set_disk_delay(milli_sec));
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::start_log_corruption()                        *
 *--------------------------------------------------------------*/
rc_t
ss_m::start_log_corruption()
{
    SM_PROLOGUE_RC(ss_m::start_log_corruption, in_xct, read_write, 0);
    if(log) {
        // flush current log buffer since all future logs will be
        // corrupted.
        errlog->clog << emerg_prio << "Starting Log Corruption" << flushl;
        log->start_log_corruption();
    }
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::sync_log()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::sync_log(bool block)
{
    return log? log->flush_all(block) : RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::flush_until()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::flush_until(lsn_t& anlsn, bool block)
{
  return log->flush(anlsn, block);
}

/*--------------------------------------------------------------*
 *  ss_m::get_curr_lsn()                            *
 *--------------------------------------------------------------*/
rc_t
ss_m::get_curr_lsn(lsn_t& anlsn)
{
  anlsn = log->curr_lsn();
  return (RCOK);
}

/*--------------------------------------------------------------*
 *  ss_m::get_durable_lsn()                            *
 *--------------------------------------------------------------*/
rc_t
ss_m::get_durable_lsn(lsn_t& anlsn)
{
  anlsn = log->durable_lsn();
  return (RCOK);
}

void ss_m::dump_page_lsn_chain(std::ostream &o) {
    dump_page_lsn_chain(o, lpid_t::null, lsn_t::max);
}
void ss_m::dump_page_lsn_chain(std::ostream &o, const lpid_t &pid) {
    dump_page_lsn_chain(o, pid, lsn_t::max);
}
void ss_m::dump_page_lsn_chain(std::ostream &o, const lpid_t &pid, const lsn_t &max_lsn) {
    log->dump_page_lsn_chain(o, pid, max_lsn);
}

/*--------------------------------------------------------------*
 *  DEVICE and VOLUME MANAGEMENT                        *
 *--------------------------------------------------------------*/

/*--------------------------------------------------------------*
 *  ss_m::format_dev()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::format_dev(const char* device, smksize_t size_in_KB, bool force)
{
     // SM_PROLOGUE_RC(ss_m::format_dev, not_in_xct, 0);
    FUNC(ss_m::format_dev);                             

    if(size_in_KB > sthread_t::max_os_file_size / 1024) {
        return RC(eDEVTOOLARGE);
    }
    {
        prologue_rc_t prologue(prologue_rc_t::not_in_xct,  
                                prologue_rc_t::read_write,0); 
        if (prologue.error_occurred()) return prologue.rc();

        bool result = dev->is_mounted(device);
        if(result) {
            return RC(eALREADYMOUNTED);
        }
        DBG( << "already mounted=" << result );

        W_DO(vol_t::format_dev(device, 
                /* XXX possible loss of bits */
                shpid_t(size_in_KB/(page_sz/1024)), force));
    }
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::mount_dev()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::mount_dev(const char* device, u_int& vol_cnt, devid_t& devid, vid_t local_vid)
{
    SM_PROLOGUE_RC(ss_m::mount_dev, not_in_xct, read_only, 0);

    spinlock_write_critical_section cs(&_begin_xct_mutex);

    // do the real work of the mount
    W_DO(_mount_dev(device, vol_cnt, local_vid));

    // this is a hack to get the device number.  _mount_dev()
    // should probably return it.
    devid = devid_t(device);
    w_assert3(devid != devid_t::null);
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::dismount_dev()                            *
 *                                                              *
 *  only allow this if there are no active XCTs                 *
 *--------------------------------------------------------------*/
rc_t
ss_m::dismount_dev(const char* device)
{
    SM_PROLOGUE_RC(ss_m::dismount_dev, not_in_xct, read_only, 0);

    spinlock_write_critical_section cs(&_begin_xct_mutex);

    if (xct_t::num_active_xcts())  {
        fprintf(stderr, "Active transactions: %d : cannot dismount %s\n", 
                xct_t::num_active_xcts(), device);
        return RC(eCANTWHILEACTIVEXCTS);
    }  else  {
        W_DO( _dismount_dev(device) );
    }

    // take a checkpoint to record the dismount
    chkpt->take();

    DBG(<<"dismount_dev ok");

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::dismount_all()                            *
 *                                                              *
 *  Only allow this if there are no active XCTs                 *
 *--------------------------------------------------------------*/
rc_t
ss_m::dismount_all()
{
    SM_PROLOGUE_RC(ss_m::dismount_all, not_in_xct, read_only, 0);
    
    spinlock_write_critical_section cs(&_begin_xct_mutex);

    // of course a transaction could start immediately after this...
    // we don't protect against that.
    if (xct_t::num_active_xcts())  {
        fprintf(stderr, 
        "Active transactions: %d : cannot dismount_all\n", 
        xct_t::num_active_xcts());
        return RC(eCANTWHILEACTIVEXCTS);
    }

    // take a checkpoint to record the dismounts
    chkpt->take();

    // dismount is protected by _begin_xct_mutex, actually....
    W_DO( io->dismount_all_dev() );

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::list_devices()                            *
 *--------------------------------------------------------------*/
rc_t
ss_m::list_devices(const char**& dev_list, devid_t*& devid_list, u_int& dev_cnt)
{
    SM_PROLOGUE_RC(ss_m::list_devices, not_in_xct,  read_only,0);
    W_DO(io->list_devices(dev_list, devid_list, dev_cnt));
    return RCOK;
}

rc_t
ss_m::list_volumes(const char* device, 
        lvid_t*& lvid_list, 
        u_int& lvid_cnt)
{
    SM_PROLOGUE_RC(ss_m::list_volumes, can_be_in_xct, read_only, 0);
    lvid_cnt = 0;
    lvid_list = NULL;

    // for now there is only on lvid possible, but later there will
    // be multiple volumes on a device
    lvid_t lvid;
    W_DO(io->get_lvid(device, lvid));
    if (lvid != lvid_t::null) {
        lvid_list = new lvid_t[1];
        lvid_list[0] = lvid;
        if (lvid_list == NULL) return RC(eOUTOFMEMORY);
        lvid_cnt = 1;
    }
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::get_device_quota()                            *
 *--------------------------------------------------------------*/
rc_t
ss_m::get_device_quota(const char* device, smksize_t& quota_KB, smksize_t& quota_used_KB)
{
    SM_PROLOGUE_RC(ss_m::get_device_quota, can_be_in_xct, read_only, 0);
    W_DO(io->get_device_quota(device, quota_KB, quota_used_KB));
    return RCOK;
}

rc_t
ss_m::generate_new_lvid(lvid_t& lvid)
{
    SM_PROLOGUE_RC(ss_m::generate_new_lvid, can_be_in_xct, read_only, 0);
    W_DO(lid->generate_new_volid(lvid));
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::create_vol()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::create_vol(const char* dev_name, const lvid_t& lvid, 
                 smksize_t quota_KB, bool skip_raw_init, vid_t local_vid,
                 const bool apply_fake_io_latency, const int fake_disk_latency)
{
    SM_PROLOGUE_RC(ss_m::create_vol, not_in_xct, read_only, 0);

    spinlock_write_critical_section cs(&_begin_xct_mutex);

    // make sure device is already mounted
    if (!io->is_mounted(dev_name)) return RC(eDEVNOTMOUNTED);

    // make sure volume is not already mounted
    vid_t vid = io->get_vid(lvid);
    if (vid != vid_t::null) return RC(eVOLEXISTS);

    W_DO(_create_vol(dev_name, lvid, quota_KB, skip_raw_init, 
                     apply_fake_io_latency, fake_disk_latency));

    // remount the device so the volume becomes visible
    u_int vol_cnt;
    W_DO(_mount_dev(dev_name, vol_cnt, local_vid));
    w_assert3(vol_cnt > 0);
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::destroy_vol()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::destroy_vol(const lvid_t& lvid)
{
    SM_PROLOGUE_RC(ss_m::destroy_vol, not_in_xct, read_only, 0);

    spinlock_write_critical_section cs(&_begin_xct_mutex);

    if (xct_t::num_active_xcts())  {
        fprintf(stderr, 
            "Active transactions: %d : cannot destroy volume\n", 
            xct_t::num_active_xcts());
        return RC(eCANTWHILEACTIVEXCTS);
    }  else  {
        // find the device name
        vid_t vid = io->get_vid(lvid);

        if (vid == vid_t::null)
            return RC(eBADVOL);
        char *dev_name = new char[smlevel_0::max_devname+1];
        if (!dev_name)
            W_FATAL(fcOUTOFMEMORY);

        w_auto_delete_array_t<char> ad_dev_name(dev_name);
        const char* dev_name_ptr = io->dev_name(vid);
        w_assert1(dev_name_ptr != NULL);
        strncpy(dev_name, dev_name_ptr, smlevel_0::max_devname);
        w_assert3(io->is_mounted(dev_name));

        // remember quota on the device
        smksize_t quota_KB;
        W_DO(dev->quota(dev_name, quota_KB));
        
        // since only one volume on the device, we can destroy the
        // volume by reformatting the device
        // W_DO(_dismount_dev(dev_name));
        // GROT

        /* XXX possible loss of bits */
        W_DO(vol_t::format_dev(dev_name, shpid_t(quota_KB/(page_sz/1024)), true));
        // take a checkpoint to record the destroy (dismount)
        chkpt->take();

        // tell the system about the device again
        u_int vol_cnt;
        W_DO(_mount_dev(dev_name, vol_cnt, vid_t::null));
        w_assert3(vol_cnt == 0);
    }
    return RCOK;
}



/*--------------------------------------------------------------*
 *  ss_m::get_volume_quota()                            *
 *--------------------------------------------------------------*/
rc_t
ss_m::get_volume_quota(const lvid_t& lvid, smksize_t& quota_KB, smksize_t& quota_used_KB)
{
    SM_PROLOGUE_RC(ss_m::get_volume_quota, can_be_in_xct, read_only, 0);
    vid_t vid = io->get_vid(lvid);
    W_DO(io->get_volume_quota(vid, quota_KB, quota_used_KB));
    return RCOK;
}

rc_t ss_m::verify_volume(
    vid_t vid, int hash_bits, verify_volume_result &result)
{
    W_DO(btree_m::verify_volume(vid, hash_bits, result));
    return RCOK;
}

ostream& operator<<(ostream& o, const lpid_t& pid)
{
    return o << "p(" << pid.vol() << '.' << pid.store() << '.' << pid.page << ')';
}

istream& operator>>(istream& i, lpid_t& pid)
{
    char c[6];
    memset(c, 0, sizeof(c));
    i >> c[0] >> c[1] >> pid._stid.vol >> c[2] 
      >> pid._stid.store >> c[3] >> pid.page >> c[4];
    c[5] = '\0';
    if (i)  {
        if (strcmp(c, "p(..)")) {
            i.clear(ios::badbit|i.rdstate());  // error
        }
    }
    return i;
}

#if defined(__GNUC__) && __GNUC_MINOR__ > 6
ostream& operator<<(ostream& o, const smlevel_1::xct_state_t& xct_state)
{
// NOTE: these had better be kept up-to-date wrt the enumeration
// found in sm_int_1.h
    const char* names[] = {"xct_stale", 
                        "xct_active", 
                        "xct_prepared", 
                        "xct_aborting",
                        "xct_chaining", 
                        "xct_committing", 
                        "xct_freeing_space", 
                        "xct_ended"};
    
    o << names[xct_state];
    return o;
}
#endif


/*--------------------------------------------------------------*
 *  ss_m::dump_locks()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::dump_locks(ostream &o)
{
    lm->dump(o);
    return RCOK;
}

rc_t
ss_m::dump_locks() {
  return dump_locks(std::cout);
}



//#ifdef SLI_HOOKS
/*--------------------------------------------------------------*
 *  Enable/Disable Shore-SM features                            *
 *--------------------------------------------------------------*/

void ss_m::set_sli_enabled(bool /* enable */) 
{
    fprintf(stdout, "SLI not supported\n");
    //lm->set_sli_enabled(enable);
    //TODO: SHORE-KITS-API
    assert(0);
}

void ss_m::set_elr_enabled(bool /* enable */) 
{
    fprintf(stdout, "ELR not supported\n");
    //xct_t::set_elr_enabled(enable);
    //TODO: SHORE-KITS-API
    assert(0);
}

rc_t ss_m::set_log_features(char const* /* features */) 
{
    fprintf(stdout, "Aether not integrated\n");
    return (RCOK);
    //return log->set_log_features(features);
    //TODO: SHORE-KITS-API
    assert(0);
}

char const* ss_m::get_log_features() 
{
    fprintf(stdout, "Aether not integrated\n");
    return ("NOT-IMPL");
    //return log->get_log_features();
    //TODO: SHORE-KITS-API
    assert(0);
}
//#endif

lil_global_table* ss_m::get_lil_global_table() {
    if (lm) {
        return lm->get_lil_global_table();
    } else {
        return NULL;
    }
}

/*--------------------------------------------------------------*
 *  ss_m::lock()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::lock(const lockid_t& n, const okvl_mode& m,
           bool check_only, timeout_in_ms timeout)
{
    SM_PROLOGUE_RC(ss_m::lock, in_xct, read_only, 0);
    W_DO( lm->lock(n, m, check_only, timeout) );
    return RCOK;
}

rc_t
ss_m::lock(const stid_t& n, const okvl_mode& m,
           bool check_only, timeout_in_ms timeout)
{
    SUPPRESS_UNUSED_4(n, m, check_only, timeout);
    //TODO: SHORE-KITS-API
    //Why stid_t??? Shore-MT doesn't support this function signature 
    assert(0);
    SUPPRESS_NON_RETURN(rc_t);
}


/*--------------------------------------------------------------*
 *  ss_m::unlock()                                *
 *--------------------------------------------------------------*/
/*rc_t
ss_m::unlock(const lockid_t& n)
{
    SM_PROLOGUE_RC(ss_m::unlock, in_xct, read_only, 0);
    W_DO( lm->unlock(n) );
    return RCOK;
}
*/

/*
rc_t
ss_m::query_lock(const lockid_t& n, lock_mode_t& m)
{
    SM_PROLOGUE_RC(ss_m::query_lock, in_xct, read_only, 0);
    W_DO( lm->query(n, m, xct()->tid()) );

    return RCOK;
}
*/

/*****************************************************************
 * Internal/physical-ID version of all the storage operations
 *****************************************************************/

/*--------------------------------------------------------------*
 *  ss_m::_begin_xct(sm_stats_info_t *_stats, timeout_in_ms timeout) *
 *
 * @param[in] _stats  If called by begin_xct without a _stats, then _stats is NULL here.
 *                    If not null, the transaction is instrumented.
 *                    The stats structure may be returned to the 
 *                    client through the appropriate version of 
 *                    commit_xct, abort_xct, prepare_xct, or chain_xct.
 *--------------------------------------------------------------*/
rc_t
ss_m::_begin_xct(sm_stats_info_t *_stats, tid_t& tid, timeout_in_ms timeout, bool sys_xct,
    bool single_log_sys_xct, bool deferred_ssx)
{
    w_assert1(!single_log_sys_xct || sys_xct); // SSX is always system-transaction
    w_assert1(!deferred_ssx || single_log_sys_xct); // deferred SSX is always SSX

    // system transaction can be a nested transaction, so
    // xct() could be non-NULL
    if (!sys_xct && xct() != NULL) {
        return RC (eINTRANS);
    }

    xct_t* x;
    if (sys_xct) {
        x = xct();
        if (single_log_sys_xct && x) {
            // in this case, we don't need an independent transaction object.
            // we just piggy back on the outer transaction
            w_assert0(x->is_piggy_backed_single_log_sys_xct() == false); // ssx can't be nested by ssx
            x->set_piggy_backed_single_log_sys_xct(true);
            tid = x->tid();
            return RCOK;
        }
        // system transaction doesn't need synchronization with create_vol etc
        // TODO might need to reconsider. but really needs this change now
        x = xct_t::new_xct(_stats, timeout, sys_xct, single_log_sys_xct, deferred_ssx);
    } else {
        spinlock_read_critical_section cs(&_begin_xct_mutex);
        x = xct_t::new_xct(_stats, timeout, sys_xct);
    }

    if (!x) 
        return RC(eOUTOFMEMORY);

    w_assert3(xct() == x);
    w_assert3(x->state() == xct_t::xct_active);
    tid = x->tid();

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_commit_xct()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::_commit_xct(sm_stats_info_t*& _stats, bool lazy,
                  lsn_t* plastlsn)
{
    w_assert3(xct() != 0);
    xct_t& x = *xct();
    DBG(<<"commit " << ((char *)lazy?" LAZY":"") << x );
    
    if (x.is_piggy_backed_single_log_sys_xct()) {
        // then, commit() does nothing
        x.set_piggy_backed_single_log_sys_xct(false); // but resets the flag
        return RCOK;
    }

    w_assert3(x.state()==xct_active);

    W_DO( x.commit(lazy,plastlsn) );

    if(x.is_instrumented()) {
        _stats = x.steal_stats();
        _stats->compute();
    }
    bool was_sys_xct W_IFDEBUG3(= x.is_sys_xct());
    xct_t::destroy_xct(&x);
    w_assert3(was_sys_xct || xct() == 0);

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_commit_xct_group( xct_t *list[], len)                *
 *--------------------------------------------------------------*/

rc_t
ss_m::_commit_xct_group(xct_t *list[], int listlen)
{
    // We don't care what, if any, xct is attached
    xct_t* x = xct();
    if(x) me()->detach_xct(x);

    DBG(<<"commit group " );

    // 1) verify either all are participating in 2pc
    // in same way (not, prepared, not prepared)
    // Some may be read-only
    // 2) do the first part of the commit for each one.
    // 3) write the group-commit log record.
    // (TODO: we should remove the read-only xcts from this list)
    //
    int participating=0;
    for(int i=0; i < listlen; i++) {
        // verify list
        x = list[i];
        w_assert3(x->state()==xct_active);
    }
    if(participating > 0 && participating < listlen) {
        // some transaction is not participating in external 2-phase commit 
        // but others are. Don't delete any xcts.
        // Leave it up to the server to decide how to deal with this; it's
        // a server error.
        return RC(eNOTEXTERN2PC);
    }

    for(int i=0; i < listlen; i++) {
        x = list[i];
        /*
         * Do a partial commit -- all but logging the
         * commit and freeing the locks.
         */
        me()->attach_xct(x);
        {
        SM_PROLOGUE_RC(ss_m::mount_dev, commitable_xct, read_write, 0);
        W_DO( x->commit_as_group_member() );
        }
        w_assert1(me()->xct() == NULL);

        if(x->is_instrumented()) {
            // remove the stats, delete them
            sm_stats_info_t* _stats = x->steal_stats();
            delete _stats;
        }
    }

    // Write group commit record
    // Failure here requires that the server abort them individually.
    // I don't know why the compiler won't convert from a
    // non-const to a const xct_t * list.
    W_DO(xct_t::group_commit((const xct_t **)list, listlen));

    // Destroy the xcts
    for(int i=0; i < listlen; i++) {
        /*
         *  Free all locks for each transaction
         */
        x = list[i];
        w_assert1(me()->xct() == NULL);
        me()->attach_xct(x);
        W_DO(x->commit_free_locks());
        me()->detach_xct(x);
        xct_t::destroy_xct(x);
    }
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_chain_xct()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::_chain_xct(
        sm_stats_info_t*&  _stats, /* pass in a new one, get back the old */
        bool lazy)
{
    sm_stats_info_t*  new_stats = _stats;
    w_assert3(xct() != 0);
    xct_t* x = xct();

    W_DO( x->chain(lazy) );
    w_assert3(xct() == x);
    if(x->is_instrumented()) {
        _stats = x->steal_stats();
        _stats->compute();
    }
    x->give_stats(new_stats);

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_abort_xct()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::_abort_xct(sm_stats_info_t*&             _stats)
{
    w_assert3(xct() != 0);
    xct_t& x = *xct();
    
    // if this is "piggy-backed" ssx, just end the status
    if (x.is_piggy_backed_single_log_sys_xct()) {
        x.set_piggy_backed_single_log_sys_xct(false);
        return RCOK;
    }
    
    bool was_sys_xct W_IFDEBUG3(= x.is_sys_xct());

    W_DO( x.abort(true /* save _stats structure */) );
    if(x.is_instrumented()) {
        _stats = x.steal_stats();
        _stats->compute();
    }

    xct_t::destroy_xct(&x);
    w_assert3(was_sys_xct || xct() == 0);

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::save_work()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::_save_work(sm_save_point_t& sp)
{
    w_assert3(xct() != 0);
    xct_t* x = xct();

    W_DO(x->save_point(sp));
    sp._tid = x->tid();
#if W_DEBUG_LEVEL > 4
    {
        w_ostrstream s;
        s << "save_point @ " << (void *)(&sp)
            << " " << sp
            << " created for tid " << x->tid();
        fprintf(stderr,  "%s\n", s.c_str());
    }
#endif
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::rollback_work()                            *
 *--------------------------------------------------------------*/
rc_t
ss_m::_rollback_work(const sm_save_point_t& sp)
{
    w_assert3(xct() != 0);
    xct_t* x = xct();
#if W_DEBUG_LEVEL > 4
    {
        w_ostrstream s;
        s << "rollback_work for " << (void *)(&sp)
            << " " << sp
            << " in tid " << x->tid();
        fprintf(stderr,  "%s\n", s.c_str());
    }
#endif
    if (sp._tid != x->tid())  {
        return RC(eBADSAVEPOINT);
    }
    W_DO( x->rollback(sp) );
    return RCOK;
}

rc_t
ss_m::_mount_dev(const char* device, u_int& vol_cnt, vid_t local_vid)
{
    vid_t vid;
    DBG(<<"_mount_dev " << device);

    // inform device_m about the device
    W_DO(io->mount_dev(device, vol_cnt));
    if (vol_cnt == 0) return RCOK;

    DBG(<<"_mount_dev vol count " << vol_cnt );
    // make sure volumes on the dev are not already mounted
    lvid_t lvid;
    W_DO(io->get_lvid(device, lvid));
    vid = io->get_vid(lvid);
    if (vid != vid_t::null) {
                // already mounted
                return RCOK;
    }

    if (local_vid == vid_t::null) {
        W_DO(io->get_new_vid(vid));
    } else {
        if (io->is_mounted(local_vid)) {
            // vid already in use
            return RC(eBADVOL);
        }
        vid = local_vid;
    }

    W_DO(io->mount(device, vid));
    // take a checkpoint to record the mount
    chkpt->take();

    return RCOK;
}

rc_t
ss_m::_dismount_dev(const char* device)
{
    vid_t        vid;
    lvid_t       lvid;
    rc_t         rc;

    DBG(<<"dismount_dev");
    W_DO(io->get_lvid(device, lvid));
    DBG(<<"dismount_dev" << lvid);
    if (lvid != lvid_t::null) {
        vid = io->get_vid(lvid);
        DBG(<<"dismount_dev" << vid);
        if (vid == vid_t::null) return RC(eDEVNOTMOUNTED);
    }

    W_DO( io->dismount_dev(device) );

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_create_vol()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::_create_vol(const char* dev_name, const lvid_t& lvid, 
                  smksize_t quota_KB, bool skip_raw_init,
                  const bool apply_fake_io_latency, 
                  const int fake_disk_latency)
{
    vid_t tmp_vid;
    W_DO(io->get_new_vid(tmp_vid));
    DBG(<<"got new vid " << tmp_vid 
        << " formatting " << dev_name);

    W_DO(vol_t::format_vol(dev_name, lvid, tmp_vid,
        /* XXX possible loss of bits */
       shpid_t(quota_KB/(page_sz/1024)), skip_raw_init));

    DBG(<<"vid " << tmp_vid  << " mounting " << dev_name);
    W_DO(io->mount(dev_name, tmp_vid, apply_fake_io_latency, fake_disk_latency));
    DBG(<<" mount done " << dev_name << " tmp_vid " << tmp_vid);
    DBG(<<" dismounting volume");
    W_DO(io->dismount(tmp_vid));
    
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::get_du_statistics()        DU DF
 *--------------------------------------------------------------*/
rc_t
ss_m::get_du_statistics(vid_t vid, sm_du_stats_t& du, bool audit)
{
    SM_PROLOGUE_RC(ss_m::get_du_statistics, in_xct, read_only, 0);
    W_DO(_get_du_statistics(vid, du, audit));
    return RCOK;
}


/*--------------------------------------------------------------*
 *  ss_m::get_du_statistics()        DU DF                    *    
 *--------------------------------------------------------------*/
rc_t
ss_m::get_du_statistics(const stid_t& stid, sm_du_stats_t& du, bool audit)
{
    SM_PROLOGUE_RC(ss_m::get_du_statistics, in_xct, read_only, 0);
    W_DO(_get_du_statistics(stid, du, audit));
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_get_du_statistics()        DU DF                    *    
 *--------------------------------------------------------------*/
rc_t
ss_m::_get_du_statistics( const stid_t& stpgid, sm_du_stats_t& du, bool audit)
{
    // TODO this should take S lock, not IS
    lpid_t root_pid;
    W_DO(open_store(stpgid, root_pid));

    btree_stats_t btree_stats;
    W_DO( bt->get_du_statistics(root_pid, btree_stats, audit));
    if (audit) {
        W_DO(btree_stats.audit());
    }
    du.btree.add(btree_stats);
    du.btree_cnt++;
    return RCOK;
}


/*--------------------------------------------------------------*
 *  ss_m::_get_du_statistics()  DU DF                           *
 *--------------------------------------------------------------*/
rc_t
ss_m::_get_du_statistics(vid_t vid, sm_du_stats_t& du, bool audit)
{
    /*
     * Cannot call this during recovery, even for 
     * debugging purposes
     */
    if(smlevel_0::in_recovery()) {
        return RCOK;
    }
    W_DO(lm->intent_vol_lock(vid, audit ? okvl_mode::S : okvl_mode::IS));
    sm_du_stats_t new_stats;

    /*********************************************************
     * First get stats on all the special stores in the volume.
     *********************************************************/

    stid_t stid;

    /**************************************************
     * Now get stats on every other store on the volume
     **************************************************/

    rc_t rc;
    // get du stats on every store
    for (stid_t s(vid, 0); s.store < stnode_page_h::max; s.store++) {
        DBG(<<"look at store " << s);
        
        store_flag_t flags;
        rc = io->get_store_flags(s, flags);
        if (rc.is_error()) {
            if (rc.err_num() == eBADSTID) {
                DBG(<<"skipping bad STID " << s );
                continue;  // skip any stores that don't exist
            } else {
                return rc;
            }
        }
        DBG(<<" getting stats for store " << s << " flags=" << flags);
        rc = _get_du_statistics(s, new_stats, audit);
        if (rc.is_error()) {
            if (rc.err_num() == eBADSTID) {
                DBG(<<"skipping large object or missing store " << s );
                continue;  // skip any stores that don't show
                           // up in the directory index
                           // in this case it this means stores for
                           // large object pages
            } else {
                return rc;
            }
        }
        DBG(<<"end for loop with s=" << s );
    }

    W_DO( io->get_du_statistics(vid, new_stats.volume_hdr, audit));

    if (audit) {
        W_DO(new_stats.audit());
    }
    du.add(new_stats);

    return RCOK;
}



/*--------------------------------------------------------------*
 *  ss_m::{enable,disable,set}_fake_disk_latency()              *        
 *--------------------------------------------------------------*/
rc_t 
ss_m::enable_fake_disk_latency(vid_t vid)
{
  SM_PROLOGUE_RC(ss_m::enable_fake_disk_latency, not_in_xct, read_only, 0);
  W_DO( io->enable_fake_disk_latency(vid) );
  return RCOK;
}

rc_t 
ss_m::disable_fake_disk_latency(vid_t vid)
{
  SM_PROLOGUE_RC(ss_m::disable_fake_disk_latency, not_in_xct, read_only, 0);
  W_DO( io->disable_fake_disk_latency(vid) );
  return RCOK;
}

rc_t 
ss_m::set_fake_disk_latency(vid_t vid, const int adelay)
{
  SM_PROLOGUE_RC(ss_m::set_fake_disk_latency, not_in_xct, read_only, 0);
  W_DO( io->set_fake_disk_latency(vid,adelay) );
  return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::get_volume_meta_stats()                                *        
 *--------------------------------------------------------------*/
rc_t
ss_m::get_volume_meta_stats(vid_t vid, SmVolumeMetaStats& volume_stats, concurrency_t cc)
{
    SM_PROLOGUE_RC(ss_m::get_volume_meta_stats, in_xct, read_only, 0);
    W_DO( _get_volume_meta_stats(vid, volume_stats, cc) );
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_get_volume_meta_stats()                                *        
 *--------------------------------------------------------------*/
rc_t
ss_m::_get_volume_meta_stats(vid_t vid, SmVolumeMetaStats& volume_stats, concurrency_t cc)
{
    if (cc == t_cc_vol)  {
        W_DO(lm->intent_vol_lock(vid, okvl_mode::S));
    }  else if (cc != t_cc_none)  {
        return RC(eBADCCLEVEL);
    }

    W_DO( io->get_volume_meta_stats(vid, volume_stats) );

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::gather_xct_stats()                            *
 *  Add the stats from this thread into the per-xct stats structure
 *  and return a copy in the given struct _stats.
 *  If reset==true,  clear the per-xct copy.
 *  Doing this has the side-effect of clearing the per-thread copy.
 *--------------------------------------------------------------*/
rc_t
ss_m::gather_xct_stats(sm_stats_info_t& _stats, bool reset)
{
    // Use commitable_xct to ensure exactly 1 thread attached for
    // clean collection of all stats,
    // even those that read-only threads would increment.
    //
    SM_PROLOGUE_RC(ss_m::gather_xct_stats, commitable_xct, read_only, 0);

    w_assert3(xct() != 0);
    xct_t& x = *xct();

    if(x.is_instrumented()) {
        DBGTHRD(<<"instrumented, reset= " << reset );
        // detach_xct adds the per-thread stats to the xct's stats,
        // then clears the per-thread stats so that
        // the next time some stats from this thread are gathered like this
        // into an xct, they aren't duplicated.
        // They are added to the global_stats before they are cleared, so 
        // they don't get lost entirely.
        me()->detach_xct(&x); 
        me()->attach_xct(&x);

        // Copy out the stats structure stored for this xct.
        _stats = x.const_stats_ref(); 

        if(reset) {
            DBGTHRD(<<"clearing stats " );
            // clear
            // NOTE!!!!!!!!!!!!!!!!!  NOT THREAD-SAFE:
            x.clear_stats();
        }
#ifdef COMMENT
        /* help debugging sort stuff -see also code in bf.cpp  */
        {
            // print -grot
            extern int bffix_SH[];
            extern int bffix_EX[];
        FIXME: THIS CODE IS ROTTEN AND OUT OF DATE WITH tag_t!!!
            static const char *names[] = {
                "t_bad_p",
                "t_alloc_p",
                "t_stnode_p",
                "t_btree_p",
                "none"
                };
            cout << "PAGE FIXES " <<endl;
            for (int i=0; i<=14; i++) {
                    cout  << names[i] << "="  
                        << '\t' << bffix_SH[i] << "+" 
                    << '\t' << bffix_EX[i] << "=" 
                    << '\t' << bffix_EX[i] + bffix_SH[i]
                     << endl;

            }
            int sumSH=0, sumEX=0;
            for (int i=0; i<=14; i++) {
                    sumSH += bffix_SH[i];
                    sumEX += bffix_EX[i];
            }
            cout  << "TOTALS" << "="  
                        << '\t' << sumSH<< "+" 
                    << '\t' << sumEX << "=" 
                    << '\t' << sumSH+sumEX
                     << endl;
        }
        if(reset) {
            extern int bffix_SH[];
            extern int bffix_EX[];
            for (int i=0; i<=14; i++) {
                bffix_SH[i] = 0;
                bffix_EX[i] = 0;
            }
        }
#endif /* COMMENT */
    } else {
        DBGTHRD(<<"xct not instrumented");
    }

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::gather_stats()                            *
 *  NOTE: the client is assumed to pass in a copy that's not
 *  referenced by any other threads right now.
 *  Resetting is not an option. Clients have to gather twice, then
 *  subtract.
 *  NOTE: you do not have to be in a transaction to call this.
 *--------------------------------------------------------------*/
rc_t
ss_m::gather_stats(sm_stats_info_t& _stats)
{
    class GatherSmthreadStats : public SmthreadFunc
    {
    public:
        GatherSmthreadStats(sm_stats_info_t &s) : _stats(s)
        {
            new (&_stats) sm_stats_info_t; // clear the stats
            // by invoking the constructor.
        };
        void operator()(const smthread_t& t)
        {
            t.add_from_TL_stats(_stats);
        }
        void compute() { _stats.compute(); }
    private:
        sm_stats_info_t &_stats;
    } F(_stats);

    //Gather all the threads' statistics into the copy given by
    //the client.
    smthread_t::for_each_smthread(F);
    // F.compute();

    // Now add in the global stats.
    // Global stats contain all the per-thread stats that were collected
    // before a per-thread stats structure was cleared. 
    // (This happens when per-xct stats get gathered for instrumented xcts.)
    add_from_global_stats(_stats); // from finished threads and cleared stats
	_stats.compute();
    return RCOK;
}

#if W_DEBUG_LEVEL > 0
extern void dump_all_sm_stats();
void dump_all_sm_stats()
{
    static sm_stats_info_t s;
    W_COERCE(ss_m::gather_stats(s));
    w_ostrstream o;
    o << s << endl; 
    fprintf(stderr, "%s\n", o.c_str());
}
#endif

ostream &
operator<<(ostream &o, const sm_stats_info_t &s) 
{
    o << s.bfht;
    o << s.sm;
    return o;
}


/*--------------------------------------------------------------*
 *  ss_m::get_store_info()                            *
 *--------------------------------------------------------------*/
rc_t
ss_m::get_store_info(
    const stid_t&           stpgid, 
    sm_store_info_t&        info)
{
    SM_PROLOGUE_RC(ss_m::get_store_info, in_xct, read_only, 0);
    W_DO(_get_store_info(stpgid, info));
    return RCOK;
}


ostream&
operator<<(ostream& o, smlevel_3::sm_store_property_t p)
{
    if (p == smlevel_3::t_regular)                o << "regular";
    if (p == smlevel_3::t_temporary)                o << "temporary";
    if (p == smlevel_3::t_load_file)                o << "load_file";
    if (p == smlevel_3::t_insert_file)                o << "insert_file";
    if (p == smlevel_3::t_bad_storeproperty)        o << "bad_storeproperty";
    if (p & !(smlevel_3::t_regular
                | smlevel_3::t_temporary
                | smlevel_3::t_load_file
                | smlevel_3::t_insert_file
                | smlevel_3::t_bad_storeproperty))  {
        o << "unknown_property";
        w_assert3(1);
    }
    return o;
}

ostream&
operator<<(ostream& o, smlevel_0::store_flag_t flag) {
    if (flag == smlevel_0::st_unallocated)  o << "|unallocated";
    if (flag & smlevel_0::st_regular)       o << "|regular";
    if (flag & smlevel_0::st_tmp)           o << "|tmp";
    if (flag & smlevel_0::st_load_file)     o << "|load_file";
    if (flag & smlevel_0::st_insert_file)   o << "|insert_file";
    if (flag & smlevel_0::st_empty)         o << "|empty";
    if (flag & !(smlevel_0::st_unallocated
                | smlevel_0::st_regular
                | smlevel_0::st_tmp
                | smlevel_0::st_load_file 
                | smlevel_0::st_insert_file 
                | smlevel_0::st_empty))  {
        o << "|unknown";
        w_assert3(1);
    }

    return o << "|";
}

ostream& 
operator<<(ostream& o, const smlevel_0::store_operation_t op)
{
    const char *names[] = {"delete_store", 
                        "create_store", 
                        "set_deleting", 
                        "set_store_flags", 
                        "set_root"};

    if (op <= smlevel_0::t_set_root)  {
        return o << names[op];
    }  
    // else:
    w_assert3(1);
    return o << "unknown";
}

ostream& 
operator<<(ostream& o, const smlevel_0::store_deleting_t value)
{
    const char *names[] = { "not_deleting_store", 
                        "deleting_store", 
                        "store_freeing_exts", 
                        "unknown_deleting"};
    
    if (value <= smlevel_0::t_unknown_deleting)  {
        return o << names[value];
    }  
    // else:
    w_assert3(1);
    return o << "unknown_deleting_store_value";
}

rc_t         
ss_m::log_file_was_archived(const char * logfile)
{
    if(log) return log->file_was_archived(logfile);
    // should be a programming error to get here!
    return RCOK;
}


extern "C" {
/* Debugger-callable functions to dump various SM tables. */

    void        sm_dumplocks()
    {
        if (smlevel_0::lm) {
                W_IGNORE(ss_m::dump_locks(cout));
        }
        else
                cout << "no smlevel_0::lm" << endl;
        cout << flush;
    }

    void   sm_dumpxcts()
    {
        W_IGNORE(ss_m::dump_xcts(cout));
        cout << flush;
    }

    void        sm_dumpbuffers()
    {
        W_IGNORE(ss_m::dump_buffers(cout));
        cout << flush;
    }
}

/*
 * descend to io_m to check the disk containing the given volume
 */
w_rc_t ss_m::dump_vol_store_info(const vid_t &vid)
{
    SM_PROLOGUE_RC(ss_m::dump_vol_store_info, in_xct, read_only,  0);
    return io_m::check_disk(vid); 
}


w_rc_t 
ss_m::log_message(const char * const msg)
{
    SM_PROLOGUE_RC(ss_m::log_message, in_xct, read_write,  0);
    w_ostrstream out;
    out <<  msg << ends;
    return log_comment(out.c_str());
}
