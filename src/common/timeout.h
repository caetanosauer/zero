#ifndef TIMEOUT_T
#define TIMEOUT_T

/**\enum timeout_t
 * \brief Special values for int.
 *
 * \details sthreads package recognizes 2 WAIT_* values:
 * == WAIT_IMMEDIATE
 * and != WAIT_IMMEDIATE.
 *
 * If it's not WAIT_IMMEDIATE, it's assumed to be
 * a positive integer (milliseconds) used for the
 * select timeout.
 * WAIT_IMMEDIATE: no wait
 * WAIT_FOREVER:   may block indefinitely
 * The user of the thread (e.g., sm) had better
 * convert timeout that are negative values (WAIT_* below)
 * to something >= 0 before calling block().
 *
 * All other WAIT_* values other than WAIT_IMMEDIATE
 * are handled by sm layer:
 * WAIT_SPECIFIED_BY_THREAD: pick up a int from the smthread.
 * WAIT_SPECIFIED_BY_XCT: pick up a int from the transaction.
 * Anything else: not legitimate.
 *
 * \sa int
 */
struct timeout_t {
    static constexpr int WAIT_IMMEDIATE     = 0;
    static constexpr int WAIT_FOREVER     = -1;
    static constexpr int WAIT_SPECIFIED_BY_THREAD     = -4; // used by lock manager
    static constexpr int WAIT_SPECIFIED_BY_XCT = -5; // used by lock manager
    // CS: I guess the NOT_USED value is only for threads that never acquire
    // any locks? And neither latches?
    static constexpr int WAIT_NOT_USED = -6; // indicates last negative number used by sthreads
};

#endif
