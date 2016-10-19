/*
 * (c) Copyright 2013-2014, Hewlett-Packard Development Company, LP
 */

#ifndef EXPERIMENTS_TPCC_H
#define EXPERIMENTS_TPCC_H

/**
 * \file tpcc.h
 * \brief Common codes for TPC-C programs.
 */
#include <iostream>

#define SM_SOURCE

#include "sm_vas.h"
#include "btcursor.h"
#include "btree.h"
#include "smthread.h"

#include "w_key.h"

#include "xct.h"
#include "sthread.h"
#include "lock_x.h"
#include "logrec.h"

#include "tpcc_schema.h"
#include "tpcc_rnd.h"

namespace tpcc { /// Thread classes declaration BEGIN
    class driver_thread_t;
    class worker_thread_t;
    class archiver_control_thread_t;

    /** How much information to write out to stdout. */
    enum verbose_enum {
        /** nothing. */
        VERBOSE_NONE = 0,
        /** only batched information. */
        VERBOSE_STANDARD,
        /** write out something per transaction. */
        VERBOSE_DETAIL,
        /** write out detailed debug information. */
        VERBOSE_TRACE,
    };
    verbose_enum get_verbose_level();

    /**
     * \brief Main thread class for TPCC experiments.
     * \details
     * Each experiment should override this class and invoke fire_experiments() to start experiments.
     */
    class driver_thread_t : public smthread_t {
        public:
        driver_thread_t ();
        virtual ~driver_thread_t() {}

        void run();
        rc_t run_actual();
        int  return_value() const { return retval; }
        int fire_experiments();

        /** returns the root page ID of the given store. */
        const PageID& get_root_pid ( StoreID stid );

        uint32_t get_max_warehouse_id() const { return max_warehouse_id;}
        uint32_t get_max_district_id() const { return max_district_id;}
        uint32_t get_max_customer_id() const { return max_customer_id;}

        uint32_t get_deadlock_counts() const { return deadlock_counts; }
        uint32_t increment_deadlock_counts() { return ++deadlock_counts; }
        uint32_t get_toomanyretry_counts() const { return toomanyretry_counts; }
        uint32_t increment_toomanyretry_counts() { return ++toomanyretry_counts; }
        uint32_t get_duplicate_counts() const { return duplicate_counts; }
        uint32_t increment_duplicate_counts() { return ++duplicate_counts; }
        uint32_t get_user_requested_aborts() const { return user_requested_aborts; }
        uint32_t increment_user_requested_aborts() {return ++user_requested_aborts; }

        verbose_enum get_verbose_level() const {return verbose_level;}
        bool is_nolock() const {return nolock;}
        bool is_nolog() const {return nolog;}
        bool is_noswizzling() const {return noswizzling;}
        bool is_pin_numa() const {return pin_numa;}
        const char* get_data_device() const { return data_device; }

        std::vector<worker_thread_t*>& get_workers() { return workers; }

        protected:
        /**
         * Most likely you don't override this. do_more_init is enough.
         * But, tpcc_load does override this to skip mounting.
         */
        virtual rc_t do_init();

        /**
         * Override this if you want custom initialization for the master thread.
         */
        virtual rc_t do_more_init() {return RCOK;}

        /**
         * \brief  Instantiate a worker thread object with the given ID.
         * \details
         * You \b must override this to instantiate your own worker thread class.
         * Your worker thread class must inherit worker_thread_t.
         */
        virtual worker_thread_t* new_worker_thread(int32_t worker_id) = 0;

        rc_t create_table ( const char *name, StoreID &stid );
        rc_t create_table_expect_stnum(const char *name, StoreID &stid, unsigned expected_stnum);

        void empty_dir ( const char *folder_name );

        /** pre-load the table into bufferpool by reading all pages. */
        rc_t read_table ( uint stnum );

        int retval;

        // properties set by program parameters
        const char* log_folder;
        const char* clog_folder;
        const char* data_device;
        const char* backup_folder;
        const char* archive_folder;
        uint32_t    disk_quota_in_kb;
        verbose_enum verbose_level;

        // these properties are initialized in do_init().
        uint32_t max_warehouse_id;
        uint32_t max_district_id;
        uint32_t max_customer_id;
        uint64_t max_history_id;

        /** whether this is a data loading program, skipping actual experiments. */
        bool data_load;

        /** whether to read all pages to warm up bufferpool before starting. */
        bool preload_tables;

        /** whether to terminate the system without waiting for log flushers. */
        bool dirty_shutdown;

        /** whether we disable locking. */
        bool nolock;

        /** whether we disable logging. */
        bool nolog;

        /** whether we disable swizzling. */
        bool noswizzling;

        /** whether to pin worker threads to NUMA nodes. */
        bool pin_numa;

        bool archiving;
        bool async_merging;
        int archiver_freq;
        archiver_control_thread_t* arch_thread;

        std::vector<worker_thread_t*> workers;

        // statistics
        uint32_t deadlock_counts;
        uint32_t toomanyretry_counts;
        uint32_t duplicate_counts;
        uint32_t user_requested_aborts;

    private:
        rc_t init_max_warehouse_id();
        rc_t init_max_district_id();
        rc_t init_max_customer_id();
        rc_t init_max_history_id();

        std::map<StoreID, PageID> ROOT_PIDS;
    };

    const uint64_t TLR_RANDOM_SEED = 123456; // TODO this should be a program parameter

    /**
     * \brief The worker thread to run transactions in the experiment.
     */
    class worker_thread_t : public smthread_t {
        public:
        worker_thread_t (uint32_t worker_id_arg, driver_thread_t* driver_arg)
            : worker_id(worker_id_arg), driver(driver_arg),
                last_rc (RCOK), current_xct(NULL), in_xct(0),
                rnd(TLR_RANDOM_SEED + worker_id_arg) {}
        virtual ~worker_thread_t() {}

        void run();
        const rc_t& get_last_rc() const {return last_rc;}

        /** this may or may not be skewed. */
        uint32_t get_random_district_id();
        /** this may or may not be skewed. */
        uint32_t get_random_warehouse_id();
        /** use this if you always want uniform random. */
        uint32_t get_uniform_random_district_id();
        /** use this if you always want uniform random. */
        uint32_t get_uniform_random_warehouse_id();

        protected:
        /** unique ID of this worker from 0 to #workers-1. */
        const uint32_t worker_id;

        /** the parent thread that launched this worker. */
        driver_thread_t* driver;

        /** the last result code this worker observed. */
        rc_t last_rc;

        /** The transaction currently active in this thread. */
        xct_t* current_xct;

        /**
         * Count of already-committed xct this thread is conveying
         * for log-chaining (if chained, >0).
         */
        uint32_t in_xct;

        /** thread local random. */
        tlr_t rnd;

        /**
         * Override this if you want additional initialization for the worker thread.
         */
        virtual rc_t init_worker() { return RCOK; }

        /**
         * You \b must override this to implement the actual transactions.
         */
        virtual rc_t run_worker() = 0;

        /**
         * Open a new transaction to this thread.
         */
        rc_t open_xct(smlevel_0::concurrency_t concurrency = smlevel_0::t_cc_keyrange,
                       xct_t::elr_mode_t elr_mode = xct_t::elr_clv);

        /**
         * Close the currently active transaction, possibly log-chaining it.
         * @param[in] xct_result this function aborts the xct if this value refers some error,
         * commits otherwise (RCOK).
         * @param[in] max_log_chains max number of transactions to batch log-flush together
         * @param[in] lazy_commit whether to skip log-flush
         */
        rc_t close_xct(const rc_t &xct_result, uint32_t max_log_chains = 0, bool lazy_commit = false);
    };

    class archiver_control_thread_t : public smthread_t
    {
    private:
        int freq;
        bool merging;
        lintel::Atomic<bool> active;
    public:
        archiver_control_thread_t(int freq, bool merging)
            : freq(freq), merging(merging), active(true)
        {}

        void stop() {
            active = false;
        }

        // CS: copied from my Shore-Kits code
        virtual void run() {
            int ticker = 0;
            cout << "Activating archiver thread" << endl;
            while(active) {
                /**
                 * CS: sleep one second F times rather than sleeping F seconds.
                 * This allows to thread to finish after one second at most
                 * once it is deactivated.
                 */
                for (int i = 0; i < freq; i++) {
                    ::sleep(1);
                    ticker++;
                    if (!active) break;
                }
                /*
                 * Also send signals to wake up archiver and merger daemons.
                 * Wait is set to false, because this is just a mechanism to
                 * make sure that the daemons are always running (up to a 1 sec
                 * window). (TODO) In the future, those daemons should be controlled
                 * by system activity.
                 */
                ss_m::activate_archiver();
                if (ticker >= freq * 2) {
                    ss_m::activate_merger();
                }
            }
        }
    };

} /// Thread classes declaration END

namespace tpcc { /// misc common functions BEGIN
    template<class T>
    inline void zero_clear(T &t) {
        ::memset(&t, 0, sizeof(T));
    }

    std::string get_current_time_string();

    /**
    * TPC-C Lastname Function.
    * @param[in] num  non-uniform random number
    * @param[out] name last name string
    */
    void generate_lastname ( int32_t num, char *name );

    StoreID get_stid(stnum_enum stnum);
    uint32_t get_first_store_id();
    uint32_t get_our_volume_id();

    /**
    * Update one record with the given key.
    * If the key doesn't exist, this returns an error code.
    */
    template<class DATA>
    rc_t update(stnum_enum stnum, const w_keystr_t& key, const DATA &new_data) {
        vec_t vec(&new_data, sizeof(new_data));
        return ss_m::update_assoc(get_stid(stnum), key, vec);
    }
    /**
    * Overwrite a part of record with the given key.
    * If the key doesn't exist, this returns an error code.
    */
    inline rc_t overwrite(stnum_enum stnum, const w_keystr_t& key, const void* el,
                   size_t offset, size_t elen) {
        return ss_m::overwrite_assoc(get_stid(stnum), key,
                                     reinterpret_cast<const char*>(el), offset, elen);
    }
/**
* \def BYTEOFFSET(dat, col)
* \brief Helper to calculate offset of specific column.
*/
#define BYTEOFFSET(dat, col) \
    (reinterpret_cast<const char*>(&(dat . col)) - reinterpret_cast<const char*>(&dat))
/**
* \def OVERWRITE_COLUMN(stnum, key, dat, col)
* \brief Shorthand for using overwrite() on specific column.
*/
#define OVERWRITE_COLUMN(stnum, key, dat, col) \
    overwrite(stnum, key, &(dat . col), BYTEOFFSET(dat, col), sizeof(dat . col))
/**
* \def OVERWRITE_2COLUMNS(stnum, key, dat, col1, col2)
* \brief To overwrite 2 successive columns.
*/
#define OVERWRITE_2COLUMNS(stnum, key, dat, col1, col2) \
    overwrite(stnum, key, &(dat . col1), BYTEOFFSET(dat, col1),\
              sizeof(dat . col1) + sizeof(dat . col2))
/**
* \def IS_SUCCESSIVE_2COLUMNS(dat, col1, col2)
* \brief For assertion.
*/
#define IS_SUCCESSIVE_2COLUMNS(dat, col1, col2) \
    (BYTEOFFSET(dat, col1) + sizeof(dat . col1) == BYTEOFFSET(dat, col2))

    /**
    * Insert one record with the given key.
    * If the key already exists, this returns an error code.
    */
    template<class DATA>
    rc_t insert(stnum_enum stnum, const w_keystr_t& key, const DATA &new_data) {
        vec_t vec(&new_data, sizeof(new_data));
        return ss_m::create_assoc (get_stid(stnum), key, vec);
    }
    /**
    * Overload for a key-only table/index.
    */
    inline rc_t insert(stnum_enum stnum, const w_keystr_t& key) {
        vec_t vec(&key, 0); // whatever
        return ss_m::create_assoc (get_stid(stnum), key, vec);
    }
} /// misc common functions END

#endif // EXPERIMENTS_TPCC_H
