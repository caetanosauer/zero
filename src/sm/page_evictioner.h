#ifndef PAGE_EVICTIONER_H
#define PAGE_EVICTIONER_H

#include "smthread.h"
#include "sm_options.h"
#include "lsn.h"
#include "bf_hashtable.h"
#include "allocator.h"
#include "generic_page.h"
#include "bf_tree_cb.h"

#include "worker_thread.h"

typedef uint32_t clk_idx;

class bf_tree_m;
class generic_page;
struct bf_tree_cb_t;

/*!\class   page_evictioner_base
 * \brief   Page Eviction Algorithm RANDOM (latched)
 *
 * \details Basic class for page eviction. Implements some functionality
 *          required for every page evictioner (need to inherit from this
 *          class) and implements a RANDOM page eviction.
 *          The RANDOM page eviction strategy does not collect any statistics
 *          about page references but just iterates over the buffer frames
 *          until it finds a page which can be latched in exclusive mode
 *          without waiting for another thread releasing the frame's latch.
 *          Therefore this replacement strategy is called "latched".
 *
 * \author  Caetano Sauer
 */
class page_evictioner_base : public worker_thread_t {
public:
    /*!\fn      page_evictioner_base(bf_tree_m* bufferpool, const sm_options& options)
     * \brief   Constructor for page_evictioner_base
     * \details This instantiates a page evictioner that uses the RANDOM algorithm
     *          to select victims for replacement. It will serve the specified
     *          \c bufferpool but won't use the specified \c options as this
     *          page replacement strategy doesn't need any further parameters.
     *
     * @param bufferpool The bf_tree_m the constructed page evictioner is used to
     *                   select pages for eviction for.
     * @param options    The options passed to the program on startup.
     */
    page_evictioner_base(bf_tree_m* bufferpool, const sm_options& options);
    
    /*!\fn      ~page_evictioner_base()
     * \brief   Destructor for page_evictioner_base
     * \details Destroys this instance.
     */
    virtual ~page_evictioner_base();
    
    /*!\fn      hit_ref(bf_idx idx)
     * \brief   Updates the eviction statistics on page hit
     * \details As RANDOM page eviction doesn't require any statistics, this function
     *          does nothing.
     *
     * @param idx The frame of the \link _bufferpool \endlink that was fixed with a
     *            page hit.
     */
    virtual void            hit_ref(bf_idx idx);
    
    /*!\fn      unfix_ref(bf_idx idx)
     * \brief   Updates the eviction statistics on page unfix
     * \details As RANDOM page eviction doesn't require any statistics, this function
     *          does nothing.
     *
     * @param idx The frame of the \link _bufferpool \endlink that was unfixed.
     */
    virtual void            unfix_ref(bf_idx idx);
    
    /*!\fn      miss_ref(bf_idx b_idx, PageID pid)
     * \brief   Updates the eviction statistics on page miss
     * \details As RANDOM page eviction doesn't require any statistics, this function
     *          does nothing.
     *
     * @param b_idx The frame of the \link _bufferpool \endlink that was fixed with a
     *              page miss.
     * @param pid   The \link PageID \endlink of the \link generic_page \endlink that was
     *              loaded into the buffer frame.
     */
    virtual void            miss_ref(bf_idx b_idx, PageID pid);
    
    /*!\fn      used_ref(bf_idx idx)
     * \brief   Updates the eviction statistics of used pages during eviction
     * \details As RANDOM page eviction doesn't require any statistics, this function
     *          does nothing.
     *
     * @param idx The frame of the \link _bufferpool \endlink that was picked for
     *            eviction while it was fixed.
     */
    virtual void            used_ref(bf_idx idx);
    
    /*!\fn      dirty_ref(bf_idx idx)
     * \brief   Updates the eviction statistics of dirty pages during eviction
     * \details As RANDOM page eviction doesn't require any statistics, this function
     *          does nothing.
     *
     * @param idx The frame of the \link _bufferpool \endlink that was picked for
     *            eviction while the contained page is dirty.
     */
    virtual void            dirty_ref(bf_idx idx);
    
    /*!\fn      block_ref(bf_idx idx)
     * \brief   Updates the eviction statistics of pages that cannot be evicted at all
     * \details As RANDOM page eviction doesn't require any statistics, this function
     *          does nothing.
     *
     * @param idx The frame of the \link _bufferpool \endlink that contains a page that
     *            cannot be evicted at all.
     */
    virtual void            block_ref(bf_idx idx);
    
    /*!\fn      swizzle_ref(bf_idx idx)
     * \brief   Updates the eviction statistics of pages containing swizzled pointers during eviction
     * \details As RANDOM page eviction doesn't require any statistics, this function
     *          does nothing.
     *
     * @param idx The frame of the \link _bufferpool \endlink that was picked for
     *            eviction while containing a page with swizzled pointers.
     */
    virtual void            swizzle_ref(bf_idx idx);
    
    /*!\fn      unbuffered(bf_idx idx)
     * \brief   Updates the eviction statistics on explicit eviction
     * \details As RANDOM page eviction doesn't require any statistics, this function
     *          does nothing.
     *
     * @param idx The frame of the \link _bufferpool \endlink that is freed explicitly.
     */
    virtual void            unbuffered(bf_idx idx);


protected:
    /*!\var     _bufferpool
     * \bried   The bufferpool for which this evictioner is responsible for
     * \details This evictioner expects to be used for the eviction of pages from
     *          the bufferpool referenced here.
     */
    bf_tree_m*                  _bufferpool;
    
    /*!\var     _swizzling_enabled
     * \brief   Pointer Swizzling in the bufferpool
     * \details Set if the \link _bufferpool \endlink uses pointer swizzling for page references.
     */
    bool                        _swizzling_enabled;
    
    /*!\var     _maintain_emlsn
     * \brief   EMLSN-update by evictioner
     * \details Set if the parent's EMLSN should be updated during eviction.
     */
    bool                        _maintain_emlsn;
    
    
    /*!\fn      pick_victim()
     * \brief   Selects a page to be evicted from the \link _bufferpool \endlink
     * \details This method uses the RANDOM algorithm to select one buffer frame which
     *          is expected to be used the furthest in the future (with the currently
     *          cached page). It acquires a LATCH_EX to prohibit the usage of the
     *          frame as the content of the buffer frame will definitely change.
     *
     * @return The buffer frame that can be freed or \c 0 if no eviction victim could
     *         be found.
     */
    virtual bf_idx          pick_victim();
    
    /*!\fn      evict_page(bf_idx idx, PageID &evicted_page)
     * \brief   Prepares a page for eviction
     * \details Checks a buffer frame if it can be freed (in use, contained page
     *          not pinned, etc.). If it can be freed, the checked buffer frame is
     *          latched in exclusive mode after the execution of this function.
     *
     * @param idx          The buffer frame that should be freed. If it can be freed,
     *                     it gets latched exclusively by this function.
     * @param evicted_page Returns the page in the selected buffer frame \c idx.
     * @return             Returns \c true if the page can be evicted and \c false
     *                     if some property prevents the eviction of that page.
     */
    bool evict_page(bf_idx idx, PageID &evicted_page);


private:
    /*!\var     EVICT_BATCH_RATIO
     * \brief   Ratio of buffer frames freed as batch
     * \details When eviction is triggered, _about_ this number of buffer frames will
     *          be freed at once. If this amount of frames is already free, the
     *          eviction does nothing and goes back to sleep. Given as a ratio of the
     *          buffer size (currently 1%).
     */
    const float EVICT_BATCH_RATIO = 0.01;
    
    /*!\var     _current_frame
     * \brief   Last buffer frame examined
     * \details The buffer frame index of the last frame that was checked by RANDOM
     *          page replacement.
     */
    bf_idx                      _current_frame;
    
    
    /*!\fn      unswizzle_and_update_emlsn(bf_idx idx)
     * \brief   Unswizzles the pointer in the parent page and updates the EMLSN of that
     *          page
     * \details In case swizziling is enabled, it will unswizzle the parent point.
     *          Additionally, it will update the parent EMLSN.
     *          This two operations are kept in a single method because both require
     *          looking up the parent, latching, etc., so we save some work.
     *
     * @param idx The buffer frame index where the page that gets evicted can be found.
     * @return    \c true if the .
     */
    bool unswizzle_and_update_emlsn(bf_idx idx);
    
    /*!\fn      do_work()
     * \brief   Function evicting pages in the eviciton thread
     * \details Runs in the eviction thread (executed when the eviction thread gets woken
     *          up and when terminated it terminates the eviction thread) and evicts pages
     *          as long as there are not that many buffer frames free as defined in
     *          \link EVICT_BATCH_RATIO \endlink.
     */
    virtual void do_work();
};

/*!\class   page_evictioner_gclock
 * \brief   Page Eviction Algorithm GCLOCK
 *
 * \details Page replacement algorithm GCLOCK as presented in
 *          <A HREF="http://doi.org/10.1145/320263.320276">"Sequentiality and
 *          Prefetching in Database Systems"</A> by Alan Jay Smith.
 *          To use this page eviction algorithm, the only thing to do is
 *          to set the parameter \c sm_evict_policy to \c gclock when
 *          starting the \c zapps. To set the k-parameter (i in the original
 *          paper), the parameter \c sm_bufferpool_gclock_k is offered by
 *          \c zapps. The default value is 10.
 *          On construction, this page evictioner needs to be connected to
 *          a bufferpool bf_tree_m for which this will serve. The bufferpool
 *          needs to call hit_ref(bf_idx idx) on every page hit and
 *          pick_victim() needs to be called to get a page to evict from the
 *          bufferpool.
 *
 * \author  Lucas Lersch
 */
class page_evictioner_gclock : public page_evictioner_base {
public:
    /*!\fn      page_evictioner_gclock(bf_tree_m* bufferpool, const sm_options& options)
     * \brief   Constructor for page_evictioner_gclock
     * \details This instantiates a page evictioner that uses the GCLOCK algorithm
     *          to select victims for replacement. It will serve the specified
     *          \c bufferpool and it will use the \c sm_bufferpool_gclock_k
     *          parameter from the \c options to specify \link _k \endlink (default
     *          value is 10).
     *          It also initializes the \link _counts \endlink array of referenced
     *          counters and it initializes the clock hand \link _current_frame \endlink
     *          to the invalid frame 0 which gets fixed during the first execution of
     *          \link pick_victim() \endlink.
     *
     * @param bufferpool The bf_tree_m the constructed page evictioner is used to
     *                   select pages for eviction for.
     * @param options    The options passed to the program on startup.
     */
    page_evictioner_gclock(bf_tree_m* bufferpool, const sm_options& options);
    
    /*!\fn      ~page_evictioner_gclock()
     * \brief   Destructor for page_evictioner_gclock
     * \details Destroys this instance and its \link _counts \endlink array of
     *          referenced counters.
     */
    virtual ~page_evictioner_gclock();
    
    /*!\fn      hit_ref(bf_idx idx)
     * \brief   Updates the eviction statistics on page hit
     * \details Sets the referenced counter of the specified buffer frame \c idx to
     *          the value specified in \link _k \endlink.
     *
     * @param idx The frame of the \link _bufferpool \endlink that was fixed with a
     *            page hit.
     */
    virtual void            hit_ref(bf_idx idx);
    
    /*!\fn      unfix_ref(bf_idx idx)
     * \brief   Updates the eviction statistics on page unfix
     * \details Sets the referenced counter of the specified buffer frame \c idx to
     *          the value specified in \link _k \endlink as this page was still used
     *          until this point in time.
     *
     * @param idx The frame of the \link _bufferpool \endlink that was unfixed.
     */
    virtual void            unfix_ref(bf_idx idx);
    
    /*!\fn      miss_ref(bf_idx b_idx, PageID pid)
     * \brief   Updates the eviction statistics on page miss
     * \details There are three situations leading to empty buffer frames that require
     *          an initialized referenced counter when used the next time:
     *            - Buffer frame wasn't used since the startup: Referenced counters
     *              are initialized with 0 when a \link page_evictioner_gclock \endlink
     *              is constructed.
     *            - Buffer frame was freed explicitly: Therefore the function
     *              \link bf_tree_m::_add_free_block(bf_idx idx) \endlink was called. If
     *              the function was called from within \link page_evictioner_base::do_work() \endlink
     *              it is redundant to initialize the referenced counter here (see last
     *              case) but if another method called it, it is required as the reference
     *              counter could have any value.
     *            - The Buffer frame was freed by the evictioner: This only happens when
     *              the referenced counter of the frame is 0.
     *          Therefore, no action is required during a page miss as the initial value
     *          of the referenced counter is always already set.
     *
     * @param b_idx The frame of the \link _bufferpool \endlink that was fixed with a
     *              page miss.
     * @param pid   The \link PageID \endlink of the \link generic_page \endlink that was
     *              loaded into the buffer frame.
     */
    virtual void            miss_ref(bf_idx b_idx, PageID pid);
    
    /*!\fn      used_ref(bf_idx idx)
     * \brief   Updates the eviction statistics of used pages during eviction
     * \details As GCLOCK logs page usage in its statistics, the referenced counter of a page
     *          which is encountered used needs to be handled like page hits.
     *          When a page is fixed while its referenced counter is 0, it is picked for
     *          eviction during each circulation of the clock hand. But the eviction fails
     *          as long as it is fixed and therefore the incrementing of the referenced
     *          counter delays the next time this page is picked for eviction and therefore
     *          this probably speeds up the eviction.
     *
     * @param idx The frame of the \link _bufferpool \endlink that had a referenced counter
     *            of 0 while it was fixed.
     */
    virtual void            used_ref(bf_idx idx);
    
    /*!\fn      dirty_ref(bf_idx idx)
     * \brief   Updates the eviction statistics of dirty pages during eviction
     * \details As a dirty page shouldn't be picked for eviction until it is cleaned, it
     *          should be excluded from the eviction to increase the performance of the
     *          eviction but that is not implemented yet.
     *
     * @param idx The frame of the \link _bufferpool \endlink that had a referenced counter
     *            of 0 while the contained page is dirty.
     */
    virtual void            dirty_ref(bf_idx idx);
    
    /*!\fn      block_ref(bf_idx idx)
     * \brief   Updates the eviction statistics of pages that cannot be evicted at all
     * \details As some pages are not allowed to be evicted at all (will never be allowed),
     *          those are excluded from the eviction by setting the referenced value to
     *          a large value.
     *
     * @param idx The frame of the \link _bufferpool \endlink that contains a page that
     *            cannot be evicted at all.
     */
    virtual void            block_ref(bf_idx idx);
    
    /*!\fn      swizzle_ref(bf_idx idx)
     * \brief   Updates the eviction statistics of pages containing swizzled pointers during eviction
     * \details As a page containing swizzled pointers shouldn't be picked for eviction until the
     *          pointers are unswizzled, it should be excluded from the eviction to increase the
     *          performance of the eviction but that is not implemented yet.
     *
     * @param idx The frame of the \link _bufferpool \endlink that had a referenced counter
     *            of 0 while containing a page with swizzled pointers.
     */
    virtual void            swizzle_ref(bf_idx idx);
    
    /*!\fn      unbuffered(bf_idx idx)
     * \brief   Updates the eviction statistics on explicit eviction
     * \details When a page is evicted explicitly, the referenced counter of the corresponding frame
     *          might be greater than 0 and therefore this function initializes the counter for
     *          this case.
     *
     * @param idx The frame of the \link _bufferpool \endlink that is freed explicitly.
     */
    virtual void            unbuffered(bf_idx idx);


protected:
    /*!\fn      pick_victim()
     * \brief   Selects a page to be evicted from the \link _bufferpool \endlink
     * \details This method uses the GCLOCK algorithm to select one buffer frame which
     *          is expected to be used the furthest in the future (with the currently
     *          cached page). It acquires a LATCH_EX to prohibit the usage of the
     *          frame as the content of the buffer frame will definitely change.
     *
     * @return The buffer frame that can be freed or \c 0 if no eviction victim could
     *         be found.
     */
    virtual bf_idx          pick_victim();


private:
    /*!\var     _k
     * \brief   k-parameter (set referenced counter to that value)
     * \details The k-parameter (i in the original paper) of the algorithm. When a
     *          page is referenced, its referenced counter is set to this value.
     */
    uint16_t            _k;
    
    /*!\var     _counts
     * \brief   Referenced counters per buffer frame
     * \details One referenced counter per buffer frame set to \link _k \endlink on
     *          page hits and decremented during the execution of
     *          \link pick_victim() \endlink.
     */
    uint16_t*           _counts;
    
    /*!\var     _current_frame
     * \brief   Clock hand into \link _counts \endlink
     * \details Represents the clock hand pointing to the buffer frame that was
     *          checked last during the most recent execution of
     *          \link pick_victim() \endlink (evicted last).
     */
    bf_idx              _current_frame;
};

template<class key>
class hashtable_queue;
template<class key, class value>
class multi_clock;

/*!\class   page_evictioner_car
 * \brief   Page Eviction Algorithm CAR
 *
 * \details Page replacement algorithm CAR as presented in
 *          <A HREF="http://www-cs.stanford.edu/~sbansal/pubs/fast04.pdf">
 *          "CAR: Clock with Adaptive Replacement"</A> by Sorav Bansal and
 *          Dharmendra S. Modha.
 *          To use this page eviction algorithm, the only thing to do is
 *          to set the parameter \c sm_evict_policy to \c car when
 *          starting the \c zapps. Other parameters aren't needed as this
 *          page replacement algorithm is self-tuning.
 *          On construction, this page evictioner needs to be connected to
 *          a bufferpool bf_tree_m for which this will serve. The bufferpool
 *          needs to call ref(bf_idx idx) on every page hit,
 *          miss_ref(bf_idx b_idx, PageID pid) on every page miss and
 *          pick_victim() needs to be called to get a page to evict from the
 *          bufferpool.
 *
 * \author  Max Gilbert
 */
class page_evictioner_car : public page_evictioner_base {
public:
    /*!\fn      page_evictioner_car(bf_tree_m *bufferpool, const sm_options &options)
     * \brief   Constructor for page_evictioner_car
     * \details This instantiates a page evictioner that uses the CAR algorithm
     *          to select victims for replacement. It will serve the specified
     *          \c bufferpool but won't use the specified \c options as this
     *          page replacement strategy doesn't need any "magic" parameters.
     *
     * @param bufferpool The bf_tree_m the constructed page evictioner is used to
     *                   select pages for eviction for.
     * @param options    The options passed to the program on startup.
     */
    page_evictioner_car(bf_tree_m *bufferpool, const sm_options &options);
    
    /*!\fn      ~page_evictioner_car()
     * \brief   Destructor for page_evictioner_car
     * \details Destroys this instance including the \link _lock \endlink.
     */
    virtual             ~page_evictioner_car();
    
    /*!\fn      ref(bf_idx idx)
     * \brief   Updates the eviction statistics on page hit
     * \details As a page currently fixed cannot be evicted, setting the referenced bit of
     *          the corresponding buffer frame is not required to prevent its eviction. We
     *          therefore set the referenced bit not during the page fix but during the
     *          page unfix.
     *
     * @param idx The frame of the \link _bufferpool \endlink that was fixed with a
     *            page hit.
     */
    virtual void        hit_ref(bf_idx idx);
    
    /*!\fn      unfix_ref(bf_idx idx)
     * \brief   Updates the eviction statistics on page unfix
     * \details Sets the referenced bit of the specified buffer frame. This prevents
     *          the evictioner to evict this page during the next circulation of the
     *          corresponding clock.
     *
     * @param idx The frame of the \link _bufferpool \endlink that was unfixed.
     */
    virtual void            unfix_ref(bf_idx idx);
    
    /*!\fn      miss_ref(bf_idx b_idx, PageID pid)
     * \brief   Updates the eviction statistics on page miss
     * \details Classifies the specified buffer frame to be in clock \f$T_1\f$ or
     *          \f$T_2\f$ based on the membership of the referenced page in either
     *          \f$B_1\f$, \f$B_2\f$ of none of the LRU-lists. It also removes entries
     *          from the LRU-lists \f$B_1\f$ or \f$B_2\f$ if needed. The referenced
     *          bit of the specified buffer frame will be unset.
     *
     * @param b_idx The frame of the \link _bufferpool \endlink where the fixed page
     *              is cached in.
     * @param pid   The \c PageID of the fixed page.
     */
    virtual void        miss_ref(bf_idx b_idx, PageID pid);
    
    /*!\fn      used_ref(bf_idx idx)
     * \brief   Updates the eviction statistics of used pages during eviction
     * \details As CAR logs page fixes in specific time intervals, a page fixed for
     *          a longer timespan must not set the corresponding referenced bit as this
     *          would be recognized as repeated usage and therefore the page would be
     *          promoted to \f$T_2\f$.
     *
     * @param idx The frame of the \link _bufferpool \endlink that was picked for
     *            eviction while it was fixed.
     */
    virtual void        used_ref(bf_idx idx);
    
    /*!\fn      dirty_ref(bf_idx idx)
     * \brief   Updates the eviction statistics of dirty pages during eviction
     * \details As a dirty page shouldn't be picked for eviction until it is cleaned, it
     *          should be excluded from the eviction to increase the performance of the
     *          eviction but that is not implemented yet.
     *
     * @param idx The frame of the \link _bufferpool \endlink that was picked for
     *            eviction while the contained page is dirty.
     */
    virtual void        dirty_ref(bf_idx idx);
    
    /*!\fn      block_ref(bf_idx idx)
     * \brief   Updates the eviction statistics of pages that cannot be evicted at all
     * \details As some pages are not allowed to be evicted at all (will never be allowed),
     *          those should be excluded from the eviction but that is not implemented
     *          yet.
     *
     * @param idx The frame of the \link _bufferpool \endlink that contains a page that
     *            cannot be evicted at all.
     */
    virtual void        block_ref(bf_idx idx);
    
    /*!\fn      swizzle_ref(bf_idx idx)
     * \brief   Updates the eviction statistics of pages containing swizzled pointers during eviction
     * \details As a page containing swizzled pointers shouldn't be picked for eviction until the
     *          pointers are unswizzled, it should be excluded from the eviction to increase the
     *          performance of the eviction but that is not implemented yet.
     *
     * @param idx The frame of the \link _bufferpool \endlink that was picked for
     *            eviction while containing a page with swizzled pointers.
     */
    virtual void        swizzle_ref(bf_idx idx);
    
    /*!\fn      unbuffered(bf_idx idx)
     * \brief   Updates the eviction statistics on explicit eviction
     * \details When a page is evicted explicitly, the corresponding buffer frame index
     *          is removed from the clock \f$T_1\f$ or \f$T_2\f$.
     *
     * @param idx The frame of the \link _bufferpool \endlink that is freed explicitly.
     */
    virtual void        unbuffered(bf_idx idx);


protected:
    /*!\fn      pick_victim()
     * \brief   Selects a page to be evicted from the \link _bufferpool \endlink
     * \details This method uses the CAR algorithm to select one buffer frame which
     *          is expected to be used the furthest in the future (with the currently
     *          cached page). It acquires a LATCH_EX to prohibit the usage of the
     *          frame as the content of the buffer frame will definitely change.
     *
     * @return The buffer frame that can be freed.
     */
    bf_idx              pick_victim();


protected:
    /*!\var     _clocks
     * \brief   Clocks f$T_1\f$ and \f$T_2\f$
     * \details Represents the clocks \f$T_1\f$ and \f$T_2\f$ which contain
     *          eviction-specific metadata of the pages that are inside the bufferpool.
     *          Therefore there needs to be two clocks in the multi_clock and the size
     *          of the clock equals the size of the bufferpool. As the CAR algorithm
     *          only stores a referenced bit, the value stored for each index is of
     *          Boolean type. And as the internal operation of multi_clock needs an
     *          invalid index (as well as a range of indexes starting from 0), the used
     *          invalid index is 0 which isn't used in the bufferpool as well.
     */
    multi_clock<bf_idx, bool>*      _clocks;
    
    /*!\var     _b1
     * \brief   LRU-list \f$B_1\f$
     * \details Represents the LRU-list \f$B_1\f$ which contains the PageIDs of pages
     *          evicted from \f$T_1\f$.
     */
    hashtable_queue<PageID>*        _b1;
    
    /*!\var     _b2
     * \brief   LRU-list \f$B_2\f$
     * \details Represents the LRU-list \f$B_2\f$ which contains the PageIDs of pages
     *          evicted from \f$T_2\f$.
     */
    hashtable_queue<PageID>*        _b2;
    
    /*!\var     _p
     * \brief   Parameter \f$p\f$
     * \details Represents the parameter \f$p\f$ which acts as a target size of \f$T_1\f$.
     */
    u_int32_t                       _p;
    
    /*!\var     _c
     * \brief   Parameter \f$c\f$
     * \details The number of buffer frames in the bufferpool \link _bufferpool \endlink.
     */
    u_int32_t                       _c;
    
    /*!\var     _hand_movement
     * \brief   Clock hand movements in current circulation
     * \details The combined number of movements of the clock hands of \f$T_1\f$ and
     *          \f$T_2\f$. Is reset after \link _c \endlink movements.
     */
    bf_idx                          _hand_movement;
    
    /*!\var     _lock
     * \brief   Latch of \link _clocks \endlink, \link _b1 \endlink and \link _b2 \endlink
     * \details As the data structures \link _clocks \endlink, \link _b1 \endlink and
     *          \link _b2 \endlink aren't thread-safe and as the
     *          \link pick_victim() \endlink and the \link miss_ref(bf_idx, PageID) \endlink
     *          methods might change those data structures concurrently, this lock needs
     *          to be acquired by those methods. The \link ref() \endlink method is only
     *          called with the corresponding buffer frame latched and the access is also
     *          only atomic and therefore this method doesn't need to acquire this lock
     *          for its changes.
     */
    pthread_mutex_t                 _lock;
    
    /*!\enum    clock_index
     * \brief   Clock names
     * \details Contains constants that map the names of the clocks used by the CAR
     *          algorithm to the indexes used by the \link _clocks \endlink data structure.
     */
    enum clock_index {
        T_1 = 0,
        T_2 = 1
    };
};

/*!\class   hashtable_queue
 * \brief   Queue with Direct Access
 * \details Represents a queue of keys with direct access using the keys. It
 *          offers the usual queue semantics where entries are inserted at
 *          the back of the queue and where entries are removed from the
 *          front of it. But it also offers the possibility to remove a
 *          specified element from somewhere within the queue. The data type
 *          of the entries is specified using the template parameter. Each
 *          value contained in the queue needs to be unique and inserts of
 *          duplicate keys are prevented.
 *          The computational complexity of the direct access as well as
 *          removal and insertion with queue semantics depends on the
 *          implementation of std::unordered_map, as this class is used for
 *          that. The space complexity also depends on the implementation of
 *          std::unordered_map where \c Key has a size of the \c key
 *          template parameter and where \c T has double the size of the
 *          \c key template parameter.
 *
 * \note    Could also be implemented using \c Boost.MultiIndex .
 *
 * @tparam key The data type of the entries stored in this data structure.
 *
 * \author Max Gilbert
 */
template<class key>
class hashtable_queue {
private:
    /*!\class   key_pair
     * \brief   A pair of keys for the implementation of a queue as a
     *          doubly-linked list.
     * \details Instances of this class can be used to represent entries of
     *          a doubly-linked list which only stores the pointer without
     *          any other value.
     *
     * \author  Max Gilbert
     */
    class key_pair {
    public:
        /*!\fn      key_pair()
         * \brief   Constructor for an empty pair of keys
         * \details This constructor instantiates a \link key_pair \endlink without setting
         *          the members \link _previous \endlink and \link _next \endlink.
         */
        key_pair() {}
        
        /*!\fn      key_pair(key previous, key next)
         * \brief   Constructor a pair of keys with initial values
         * \details This constructor instantiates a \link key_pair \endlink and
         *          initializes the members \link _previous \endlink and
         *          \link _next \endlink as specified.
         *
         * @param previous The initial value of \link _previous \endlink.
         * @param next     The initial value of \link _next \endlink.
         */
        key_pair(key previous, key next) {
            this->_previous = previous;
            this->_next = next;
        }
        
        /*!\fn      ~key_pair()
         * \brief   Destructor of a pair of keys
         * \details As this class doesn't allocate memory dynamically, this destructor
         *          doesn't do anything.
         */
        virtual ~key_pair() {}
        
        /*!\var     _previous
         * \brief   The previous element of this element
         * \details The key of the previous element with regard to the queue order.
         *          The previous element is closer to the front of the queue and
         *          was therefore inserted earlier and will get removed later. If
         *          this element represents the front of the queue, this member
         *          variable will contain an invalid key.
         */
        key     _previous;
        
        /*!\var     _next
         * \brief   The next element of this element
         * \details The key of the next element with regard to the queue order.
         *          The next element is closer to the back of the queue and
         *          was therefore inserted later and will get removed earlier. If
         *          this element represents the back of the queue, this member
         *          variable will contain an invalid key.
         */
        key     _next;
    };
    
    /*!\var     _direct_access_queue
     * \brief   Maps from keys to their queue entry
     * \details Allows direct access to specific elements of the queue and stores
     *          the inner queue elements. Every access on queue elements happens
     *          through the interface of this data structure but this doesn't
     *          directly support the access with queue semantics.
     *          The \c key represents an queue entry and the \c key_pair
     *          which is mapped to that \c key stores the information about
     *          previous and next \c key in the queue.
     *
     * \see key_pair
     */
    std::unordered_map<key, key_pair>*      _direct_access_queue;
    
    /*!\var     _back
     * \brief   Element at the back
     * \details Stores the \c key of the element at the back of the queue. This
     *          element was inserted most recently and it will be removed the furthest
     *          in the future (regarding queue semantics). This element doesn't have
     *          a next element but the previous element can be accesses using
     *          \link _direct_access_queue \endlink.
     */
    key                                     _back;
    
    /*!\var     _front
     * \brief   Element at the front
     * \details Stores the \c key of the element at the front of the queue. This
     *          element was inserted least recently and it will be removed next
     *          (regarding queue semantics). This element doesn't have a previous
     *          element but the next element can be accesses using
     *          \link _direct_access_queue \endlink.
     */
    key                                     _front;
    
    
    /*!\var     _invalid_key
     * \brief   Invalid (Unused) \c key
     * \details This specifies an invalid \c key which can be used to mark that
     *          an element in the queue doesn't have a previous or next element. It
     *          can also be used to mark that there is no back or front of the queue
     *          when there is no queue. This should have the semantics of \c null for
     *          the specified \c key template parameter therefore a natural choice
     *          of a this for the case that \c key is a pointer would be \c null.
     */
    key                                     _invalid_key;


public:
    /*!\fn      hashtable_queue(key invalid_key)
     * \brief   Constructor of a Queue with Direct Access
     * \details Creates a new instance of \link hashtable_queue \endlink with the specified
     *          \link _invalid_key \endlink. This \c invalid_key has the semantics of
     *          \c null for the data stucture and therefore the initialized queue uses this
     *          value for mark the emptiness of the queue.
     *
     * @param invalid_key A key used when a \c null -key is required.
     */
    hashtable_queue(key invalid_key);
    
    /*!\fn      ~hashtable_queue()
     * \brief   Destructor of a Queue with Direct Access
     * \details Destructs this instance of \link hashtable_queue \endlink including the
     *          dynamically allocated memory used for the data.
     */
    virtual          ~hashtable_queue();
    
    /*!\fn      contains(key k)
     * \brief   Entry with given key contained
     * \details Searches the Queue with Direct Access for the given key and the return value
     *          gives information about if the key could be found.
     *
     * @param k The key that should be searched for in the \link hashtable_queue \endlink.
     * @return  \c true if this \link hashtable_queue \endlink contains an entry with
     *          \c k as key, \c false else.
     */
    bool             contains(key k);
    
    /*!\fn      push(key k)
     * \brief   Add the key to the queue
     * \details Adds an entry to the back of the queue. Every entry that was added to the
     *          queue before will be removed from the queue before \c k.
     *
     * @param k The key that is added to the queue.
     * @return  \c true if the key could be added successfully, \c false if
     *          it was already contained in the queue.
     */
    bool             push(key k);
    
    /*!\fn      pop()
     * \brief   Removes the next key from the queue
     * \details Removes an entry from the front of the queue. The removed entry was the
     *          entry that was added the furthest in the past.
     *
     * @return  \c true if the key could be removed successfully, \c false if
     *          the queue was already empty.
     */
    bool             pop();
    
    /*!\fn      remove(key k)
     * \brief   Removes a specific key from the queue
     * \details Removes the specified key \c k from the queue using the hash table over
     *          the queue entries if the key \c k was contained in the queue. The entry
     *          behind the key \c k (inserted immediately after it) will now be removed
     *          after the entry that was in front of \c k is removed.
     *
     * @param k The key to remove from the queue.
     * @return  \c true if the key \c k could be removed from the queue sucessfully,
     *          \c false if the key was not contained in the queue.
     */
    bool             remove(key k);
    
    /*!\fn      length()
     * \brief   Number of entries in the queue
     * \details Returns the number of entries (keys) that are contained in the queue.
     *
     * @return The number of entries contained in the queue.
     */
    inline u_int32_t length();
};

/*!\class   multi_clock
 * \brief   Multiple Clocks with a Common Set of Entries
 * \details Represents multiple clocks of key-value pairs using one common set of entries.
 *          The total size of the clocks (number of key-value pairs) is fixed but the sizes
 *          of the different clocks are variable and limiting those is not supported. The
 *          keys are stored implicitly as array indices for the values and therefore it
 *          works best when the domain of the keys is very limited.
 *          Each clock has an index (starting from 0) which is required when working with
 *          it. It is possible to add an entry at the tail of a clock and to remove one from
 *          its head. It is also possible to get the key or get/set the value of the entry
 *          where the clock hand of a clock points to. In addition to the typical interface
 *          of a single clock, it is possible to swap one entry from one clock's head to
 *          another clock's tail.
 *          The computational complexity of the methods of this class is in
 *          \f$\mathcal{O}\left(1\right)\f$ and the space complexity of this class is in
 *          \f$\mathcal{O}\left(n\right)\f$ regarding the key range.
 *
 * @tparam key   The data type of the key of the key-value pairs where each key is unique
 *               within one instance of this data structure.
 * @tparam value The data type of the value of the key-value pairs where each value
 *               instance corresponds to a key.
 */
template<class key, class value>
class multi_clock {
public:
    /*!\typedef clk_idx
     * \brief   Data type of clock indexes
     * \details The datatype used to index the specific clocks.
     */
    typedef clk_idx u_int32_t;


private:
    /*!\class   index_pair
     * \brief   Pair of keys
     * \details Pairs of keys used to create a linked list of those.
     */
    class index_pair {
    public:
        /*!\fn      index_pair()
         * \brief   Constructor for an empty pair of keys
         * \details This constructor instantiates an \link index_pair \endlink without
         *          setting the members \link _before \endlink and \link _after \endlink.
         */
        index_pair() {};
        
        /*!\fn      index_pair(key before, key after)
         * \brief   Constructor of a pair of keys with initial values
         * \details This constructor instantiates an \link index_pair \endlink and
         *          initializes the members \link _before \endlink and
         *          \link _after \endlink as specified.
         *
         * @param before The initial value of \link _before \endlink.
         * @param after  The initial value of \link _after \endlink.
         */
        index_pair(key before, key after) {
            this->_before = before;
            this->_after = after;
        };
        
        /*!\var     _before
         * \brief   Key before this key
         * \details The key that is closer to the tail of the clock. It was visited by
         *          the clock hand before this value (specified by the index of this
         *          elements within the array it is stored in).
         */
        key     _before;
        
        /*!\var     _after
         * \brief   Key after this key
         * \details The key that is closer to the head of the clock. It gets visited by
         *          the clock hand after this value (specified by the index of this
         *          elements within the array it is stored in).
         */
        key     _after;
    };
    
    /*!\var     _clocksize
     * \brief   Number of entries the clocks can hold
     * \details Contains the number of key-value pairs that can be stored in the clocks
     *          combined. When this \link multi_clock \endlink is initialized, it allocates
     *          memory to hold this many entries. This also specifies the highest key
     *          that is allowed in the clocks (\c _clocksize \c - \c 1 ).
     */
    key                             _clocksize;
    
    /*!\var     _values   
     * \brief   Values
     * \details Holds the values corresponding the keys. The corresponding key is the index
     *          of this array.
     */
    value*                          _values;
    
    /*!\var     _clocks   
     * \brief   Clocks
     * \details Contains the doubly linked, circular lists representing the clocks (every
     *          not empty clock is contained in here). The \link index_pair \endlink
     *          stored at index \c i contains the indexes within the same clock after
     *          \c i and before \c i .
     */
    index_pair*                     _clocks;
    
    /*!\var     _invalid_index   
     * \brief   Invalid (Unused) \c index
     * \details This specifies an invalid \c key which can be used to mark that
     *          a clock is empty and therefore the clock hand points to this value.
     *          This should have the semantics of \c null for the specified \c key
     *          template parameter therefore a natural choice of a this for the case
     *          that \c key is a pointer would be \c nullptr.
     */
    key                             _invalid_index;
    
    /*!\var     _clock_membership   
     * \brief   Membership of indexes to clocks
     * \details This array specifies for each index in the domain to which clock it
     *          belongs. If an index is not part of a clock, the
     *          \link _invalid_clock_index \endlink is used.
     */
    clk_idx*                        _clock_membership;
    
    /*!\var     _clocknumber   
     * \brief   Number of clocks
     * \details Contains the total number of clocks contained in this
     *          \link multi_clock \endlink an therefore it specifies the highest valid
     *          \link clk_idx \endlink, the number of clock \link _hands \endlink etc.
     *          The actual number of clocks might be smaller as some clocks can be
     *          empty.
     */
    clk_idx                         _clocknumber;
    
    /*!\var     _hands   
     * \brief   Clock hands
     * \details Contains the clock hands of the clocks. Therefore it contains the index
     *          of each clock's head. If a clock is empty, this contains the
     *          \link _invalid_index \endlink.
     */
    key*                            _hands;
    
    /*!\var     _sizes   
     * \brief   Number of elements in the clocks
     * \details Contains for each clock the number of elements this clock currently has.
     */
    key*                            _sizes;
    
    /*!\var     _invalid_clock_index   
     * \brief   Invalid (Unused) clock index
     * \details This specifies an invalid \c clock index which can be used to mark inside
     *          \link _clock_membership \endlink that an index does not belong to any
     *          clock. This should have the semantics of \c null for \link clk_idx \endlink
     *          and is equal to \link _clocknumber \endlink (greatest clock index plus 1).
     */
    clk_idx                         _invalid_clock_index;


public:
    /*!\fn      multi_clock(key clocksize, u_int32_t clocknumber, key invalid_index)
     * \brief   Constructor of Multiple Clocks with a Common Set of Entries
     * \details Constructs a new \link multi_clock \endlink with a specified combined capacity
     *          of the clocks, a specified number of (initially empty) clocks and with an
     *          \link _invalid_index \endlink corresponding to the \link key \endlink data
     *          type. The \c clocksize also specifies the range of the indexes. This constructor
     *          allocates the memory to store \c clocksize entries.
     * 
     * @param clocksize     The range of the clock indexes and the combined size of the clocks.
     * @param clocknumber   The number of clocks maintained by this \link multi_clock \endlink
     * @param invalid_index The \link key \endlink value with the semantics of \c null .
     */
    multi_clock(key clocksize, u_int32_t clocknumber, key invalid_index);
    
    /*!\fn      ~multi_clock()
     * \brief   Destructor of Multiple Clocks with a Common Set of Entries
     * \details Destructs this instance of \link multi_clock \endlink and deallocates the
     *          memory used to store the clocks.
     */
    virtual         ~multi_clock();
    
    /*!\fn      get_head(clk_idx clock, value &head_value)
     * \brief   Get the value of the entry where the clock hand of the specified
     *          clock points to
     * \details Returns the value of the head of the specified clock.
     * 
     * @param clock      The clock whose head's value should be returned.
     * @param head_value The value of the head of the specified clock (return parameter).
     * @return           \c false if the specified clock does not exist or if it is empty,
     *                   \c true else.
     */
    bool            get_head(const clk_idx clock, value &head_value);
    
    /*!\fn      set_head(clk_idx clock, value new_value)
     * \brief   Set the value of the entry where the clock hand of the specified
     *          clock points to
     * \details Sets the value of the head of the specified clock to the specified value.
     * 
     * @param clock     The clock whose head's value should be set.
     * @param new_value The new value of the head of the specified clock.
     * @return          \c false if the specified clock does not exist or if it is empty,
     *                  \c true else.
     */
    bool            set_head(const clk_idx clock, const value new_value);
    
    /*!\fn      get_head_index(clk_idx clock, key &head_index)
     * \brief   Get the index of the entry where the clock hand of the specified
     *          clock points to
     * \details Returns the index of the head of the specified clock.
     * 
     * @param clock      The clock whose head should be returned.
     * @param head_index The index of the head of the specified clock (return parameter).
     * @return           \c false if the specified clock does not exist or if it is empty,
     *                   \c true else.
     */
    bool            get_head_index(const clk_idx clock, key &head_index);
    
    /*!\fn      move_head(clk_idx clock)
     * \brief   Move the clock hand forward
     * \details Moves the tail entry of the specified clock before the head of the same
     *          clock. Therefore the previous tail entry becomes the new head entry. The
     *          previous head will become the element \link index_pair._before \endlink
     *          the new head and the new tail will be the element
     *          \link index_pair._after \endlink the new head.
     * 
     * @param clock The clock whose clock hand should be moved.
     * @return      \c true if the specified clock index is valid and if the clock is not
     *              empty, \c false else.
     */
    bool            move_head(const clk_idx clock);
    
    /*!\fn      add_tail(clk_idx clock, key index)
     * \brief   Make the specified index the tail of the specified clock
     * \details Adds a new entry with the specified index to the tail of the specified
     *          clock. The new entry will be the tail of the clock and the previous tail
     *          entry will be \link index_pair._before \endlink the new entry. Adding a
     *          new entry is only possible if the index is not already contained inside
     *          any clock of the same \link multi_clock \endlink.
     * 
     * @param clock The clock where the new entry should be added to at the tail.
     * @param index The index of the new entry.
     * @return      \c true if the \c clock index is valid and if the new entry's
     *              \c index is valid and \c false else.
     */
    bool            add_tail(const clk_idx clock, const key index);
    
    /*!\fn      add_before(const key inside, const key new_entry)
     * \brief   Add the specified index before another index in an arbitrary clock
     * \details Adds a new entry with the specified index \c new_entry in the clock
     *          before the entry \c inside. The entry the was before \c inside
     *          before will be \link index_pair._before \endlink \c new_entry. Adding a
     *          new entry is only possible if the index is not already contained inside
     *          any clock of the same \link multi_clock \endlink.
     *
     * @param inside    This index will be the entry after the new entry.
     * @param new_entry This is the index of the new entry.
     * @return          \c true if \c inside was contained in any clock and if
     *                  \c new_entry was not, \c false else
     */
    bool            add_before(const key inside, const key new_entry);
    
    /*!\fn      add_after(const key inside, const key new_entry)
     * \brief   Add the specified index after another index in an arbitrary clock
     * \details Adds a new entry with the specified index \c new_entry in the clock
     *          after the entry \c inside. The entry the was after \c inside
     *          before will be \link index_pair._after \endlink \c new_entry. Adding a
     *          new entry is only possible if the index is not already contained inside
     *          any clock of the same \link multi_clock \endlink.
     *
     * @param inside    This index will be the entry before the new entry.
     * @param new_entry This is the index of the new entry.
     * @return          \c true if \c inside was contained in any clock and if
     *                  \c new_entry was not, \c false else
     */
    bool            add_after(const key inside, const key new_entry);
    
    /*!\fn      remove_head(clk_idx clock, key &removed_index)
     * \brief   Remove the head entry from the specified clock
     * \details Removes the entry at the head of the specified clock from that clock.
     *          The new head of the clock will be the entry after the removed entry
     *          and therefore the clock hand will point to that index.
     * 
     * @param clock         The index of the clock whose head entry will be removed.
     * @param removed_index The index of the entry that was removed (return parameter).
     * @return              \c true if the specified clock exists and it is not empty
     *                      , \c false else.
     */
    bool            remove_head(const clk_idx clock, key &removed_index);
    
    /*!\fn      remove(key &index)
     * \brief   Remove the specified entry from any clock
     * \details Removed the specified entry from any clock. The entry before this entry
     *          will be before the entry after the specfied entry and the entry after
     *          this entry will be after the entry before the specfied entry.
     * 
     * @param index The index of the entry that gets removed.
     * @return      \c true if the specified index is valid and contained in any clock,
     *              \c false else.
     */
    bool            remove(key &index);
    
    /*!\fn      switch_head_to_tail(clk_idx source, clk_idx destination, key &moved_index)
     * \brief   Moves an entry from the head of one clock to the tail of another one
     * \details Removes the index at the head of the \c source clock and adds it as tail
     *          of the \c destination clock.
     * 
     * @param source      The index of the clock whose head gets moved. The head will be
     *                    removed from this clock.
     * @param destination The index of the clock where the moved entry gets added to the tail.
     * @param moved_index The index of the entry that was moved from one clock to another
     *                    (return parameter).
     * @return            \c true if the \c source clock exists, if it is not empty and if
     *                    the \c destination clock exists, \c false else.
     */
    bool            switch_head_to_tail(const clk_idx source, const clk_idx destination, key &moved_index);
    
    /*!\fn      size_of(clk_idx clock)
     * \brief   Returns the number of entries in the specified clock
     * \details Returns the number of entries that is currently contained in the specified
     *          clock, if it exists.
     * 
     * @param clock The clock whose current size gets returned.
     * @return      Number of entries in the specified \c clock or \0 if the clock does not exist.
     */
    inline key      size_of(const clk_idx clock) {
        return valid_clock_index(clock) * _sizes[clock];
    }
    
    /*!\fn      empty(const clk_idx clock)
     * \brief   Returns \c true if the specified clock is empty
     * \details Returns \c true if the specified clock currently contains entries, if it exists.
     *
     * @param clock The clock whose emptiness gets returned.
     * @return      \c true if the specified clock exists and contains entries, \c false else.
     */
    inline bool     empty(const clk_idx clock) {
        return size_of(clock) == 0;
    }
    
    /*!\fn      valid_index(const key index)
     * \brief   Returns \c true if the specified index is valid
     * \details Returns \c true if the specified index is valid in this \link multi_clock \endlink.
     *
     * @param index The index whose validity is checked.
     * @return      \c true if the specified index is valid.
     */
    inline bool     valid_index(const key index) {
        return index != _invalid_index && index >= 0 && index <= _clocksize - 1;
    }
    
    /*!\fn      contained_index(const key index)
     * \brief   Returns \c true if the specified index is contained in any clock
     * \details Returns \c true if the specified index is valid in this \link multi_clock \endlink
     *          and if it is contained in any clock within this \link multi_clock \endlink.
     *
     * @param index The index whose clock membership is checked.
     * @return      \c true if the specified index is contained in any clock.
     */
    inline bool     contained_index(const key index) {
        return valid_index(index) && valid_clock_index(_clock_membership[index]);
    }
    
    /*!\fn      valid_clock_index(const clk_idx clock_index)
     * \brief   Returns \c true if the specified clock exists
     * \details Returns \c true if the specified clock exists in this \link multi_clock \endlink.
     *
     * @param clock_index The index of the clock whose existence is checked.
     * @return            \c true if the specified clock exists.
     */
    inline bool     valid_clock_index(const clk_idx clock_index) {
        return clock_index >= 0 && clock_index <= _clocknumber - 1;
    }
    
    /*!\fn      get(key index)
     * \brief   Returns a reference to the value that corresponds to the specified index
     * \details Returns a reference to the value that corresponds to the specified index,
     *          independent of the membership of that index to any clock.
     * 
     * @param index The index whose value gets returned.
     * @return      A reference to the value corresponding the specified index if this index
     *              is valid inside this \link multi_clock \endlink or a reference to the value
     *              of the \link _invalid_index \endlink, else.
     */
    inline value&   get(const key index) {
        return valid_index(index) * _values[index]
             + !valid_index(index) * _values[_invalid_index];
    }
    
    /*!\fn      set(key index, value new_value)
     * \brief   Sets the value that corresponds to the specified index
     * \details Sets the value that corresponds to the specified index, independent of the
     *          membership of that index to any clock.
     *
     * @param index     The index whose value gets set.
     * @param new_value The new value for the specified index if this index is valid inside this
     *                  \link multi_clock \endlink or for the \link _invalid_index \endlink, else.
     */
    inline void     set(const key index, value const new_value) {
        _values[valid_index(index) * index
              + !valid_index(index) * _invalid_index] = new_value;
    }
    
    /*!\fn      operator[](key index)
     * \brief   Returns a reference to the value that corresponds to the specified index
     * \details Returns a reference to the value that corresponds to the specified index,
     *          independent of the membership of that index to any clock.
     * 
     * @param index The index whose value gets returned.
     * @return      A reference to the value corresponding the specified index if this index
     *              is valid inside this \link multi_clock \endlink or a reference to the value
     *              of the \link _invalid_index \endlink, else.
     */
    inline value&   operator[](const key index) {
        return valid_index(index) * _values[index]
             + !valid_index(index) * _values[_invalid_index];
    }
};

#endif
