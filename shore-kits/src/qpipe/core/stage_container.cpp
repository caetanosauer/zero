/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-kits -- Benchmark implementations for Shore-MT
   
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

#include "qpipe/core/stage_container.h"
#include "qpipe/core/dispatcher.h"
#include "util.h"

#include <cstdio>
#include <cstring>

/* constants */

#define TRACE_MERGING TRACE_RESOURCE_RELEASE|0
#define TRACE_DEQUEUE TRACE_RESOURCE_RELEASE|0


ENTER_NAMESPACE(qpipe);


static const bool ALWAYS_TRY_OSP_INSTEAD_OF_WORKER_CREATE = true;
const unsigned int stage_container_t::NEXT_TUPLE_UNINITIALIZED = 0;
const unsigned int stage_container_t::NEXT_TUPLE_INITIAL_VALUE = 1;



// the "STOP" exception. Simply indicates that the stage should stop
// (not necessarily an "error"). Thrown by the adaptor's output(),
// caught by the container.
struct stop_exception { };

EXIT_NAMESPACE(qpipe);
template<>
inline void
guard<qpipe::dispatcher_t::worker_releaser_t>::
   action(qpipe::dispatcher_t::worker_releaser_t* ptr) 
{
    qpipe::dispatcher_t::releaser_release(ptr);
}
ENTER_NAMESPACE(qpipe);



void* stage_container_t::static_run_stage_wrapper(stage_t* stage,
                                                  stage_adaptor_t* adaptor)
{
    adaptor->run_stage(stage);
    delete stage;
    delete adaptor;
    return NULL;
}



// container methods

/**
 *  @brief Stage constructor.
 *
 *  @param container_name The name of this stage container. Useful for
 *  printing debug information. The constructor will create a copy of
 *  this string, so the caller should deallocate it if necessary.
 */
stage_container_t::stage_container_t(const c_str &container_name,
				     stage_factory_t* stage_maker, int active_count, int max_count)
    : _container_lock(thread_mutex_create()),
      _container_queue_nonempty(thread_cond_create()),
      _container_name(container_name), _stage_maker(stage_maker),
      _pool(active_count),
      _max_threads((max_count > active_count)? max_count : std::max(10, active_count * 4)),
      _next_thread(0),
      _rp(&_container_lock._lock, 0, container_name)
{
}



/**
 *  @brief Stage container destructor. Should only be invoked after a
 *  shutdown() has successfully returned and no more worker threads
 *  are in the system. We will delete every entry of the container
 *  queue.
 */
stage_container_t::~stage_container_t(void) 
{
    // There should be no worker threads accessing the packet queue when
    // this function is called. Otherwise, we get race conditions and
    // invalid memory accesses.
  
    // destroy synch vars    
    thread_mutex_destroy(_container_lock);
    thread_cond_destroy(_container_queue_nonempty);
}


/**
 * A wrapper to allow us to run multiple threads off of one stage
 * container. It might be possible to make stage_container_t inherit
 * from thread_t and instantiate it directly multiple times, but
 * thread-local state like the RNG would be too risky to use.
 */
struct stage_thread : public thread_t 
{
    stage_container_t* _sc;
    stage_thread(const c_str &name, stage_container_t* sc)
        : thread_t(name), _sc(sc)
    {        
    }

    virtual void work() {
        _sc->run();
    }
};



/**
 *  THE CALLER MUST BE HOLDING THE _container_lock MUTEX.
 */
void stage_container_t::container_queue_enqueue_no_merge(packet_list_t* packets) {
    _container_queue.push_back(packets);
    thread_cond_signal(_container_queue_nonempty);
}



/**
 *  @brief Helper function used to add the specified packet as a new
 *  element in the _container_queue and signal a waiting worker
 *  thread.
 *
 *  THE CALLER MUST BE HOLDING THE _container_lock MUTEX.
 */
void stage_container_t::container_queue_enqueue_no_merge(packet_t* packet) 
{
    packet_list_t* packets = new packet_list_t();
    packets->push_back(packet);
    container_queue_enqueue_no_merge(packets);
}



/**
 *  @brief Helper function used to remove the next packet in this
 *  container queue. If no packets are available, wait for one to
 *  appear.
 *
 *  THE CALLER MUST BE HOLDING THE _container_lock MUTEX.
 */
packet_list_t* stage_container_t::container_queue_dequeue() {

    // wait for a packet to appear
    while ( _container_queue.empty() ) {
	thread_cond_wait( _container_queue_nonempty, _container_lock );
    }

    // remove available packet
    packet_list_t* plist = _container_queue.front();
    _container_queue.pop_front();
  
    return plist;
}



/**
 *  @brief Create another worker thread.
 *
 *  THE CALLER MUST BE HOLDING THE _container_lock MUTEX.
 */

void stage_container_t::create_worker() 
{    
    // create another worker thread
    _next_thread++;
    c_str thread_name("%s_THREAD_%d", _container_name.data(), _next_thread);
    thread_t* thread = new stage_thread(thread_name, this);

    TRACE(TRACE_DEBUG, "Creating thread %s\n", thread_name.data());

#ifdef USE_SMTHREAD_AS_BASE
    thread->fork();
#else
    thread_create(thread, &_pool);
#endif

    // notify resource pool
    _rp.notify_capacity_increase(1);
}



void stage_container_t::reserve(int n) 
{
    critical_section_t cs(_container_lock);
    _reserve(n);
}


/**
 *  @brief Reserve the specified number of workers.
 *
 *  @param n The number of workers.
 *
 *  THE CALLER MUST BE HOLDING THE _container_lock MUTEX.
 */
void stage_container_t::_reserve(int n) {
    

    assert(n > 0);


    // Make sure we are not asking for something we can't satisfy even
    // with the maximum number of threads.
    assert(n <= _max_threads);


    // See if we can satisfy the request with existing, unreserved
    // threads.
    int curr_capacity = _rp.get_capacity();
    int curr_reserved = _rp.get_reserved();
    if ((curr_capacity - curr_reserved) >= n) {
        /* we can get the resources we want without waiting! */
        _rp.reserve(n);
        return;
    }
    
    
    // We need to bump up the number of existing threads (monitored by
    // the capacity of our resource pool) to at least n to avoid
    // deadlock.
    while (curr_capacity < n) {
        create_worker();
        curr_capacity++;
    }
    

    // At this point, we can reserve n safely without risking
    // deadlock.


    // It is now a policy decision whether to wait for existing,
    // reserved threads to free up (using resource_pool_t::reserve) or
    // increase the number of existing threads further to avoid
    // blocking).


    // Eager approach... create as many threads as we can before
    // blocking...
    while (curr_capacity < _max_threads) {
        
        int unreserved = curr_capacity - curr_reserved;
        if (unreserved >= n) {
            /* we can get the resources we want without waiting! */
            break;
        }

        create_worker();
        curr_capacity++;
    }


    /* Either we have enough or we have hit _max_threads... */
    _rp.reserve(n);
    
    
    // * * * END CRITICAL SECTION * * *
};



/**
 *  @brief Unreserve the specified number of workers.
 *
 *  @param n The number of workers.
 *
 *  THE CALLER MUST NOT BE HOLDING THE _container_lock MUTEX.
 */
void stage_container_t::unreserve(int n) {

    assert(n > 0);

    // * * * BEGIN CRITICAL SECTION * * *
    critical_section_t cs(_container_lock);
    
    _rp.unreserve(n);    
    // * * * END CRITICAL SECTION * * *
}



/**
 *  @brief Send the specified packet to this container. We will try to
 *  merge the packet into a running stage or into an already enqueued
 *  packet packet list. If we fail, we will wrap the packet in a
 *  packet_list_t and insert the list as a new entry in the container
 *  queue.
 *
 *  Merging will fail if the packet is already marked as non-mergeable
 *  (if its is_merge_enabled() method returns false). It will also
 *  fail if there are no "similar" packets (1) currently being
 *  processed by any stage AND (2) currently enqueued in the container
 *  queue.
 *
 *  @param packet The packet to send to this stage.
 *
 *  THE CALLER MUST NOT BE HOLDING THE _container_lock MUTEX.
 */
void stage_container_t::enqueue(packet_t* packet) {


    assert(packet != NULL);
    bool unreserve = packet->unreserve_worker_on_completion();
    guard<dispatcher_t::worker_releaser_t> wr = dispatcher_t::releaser_acquire();
    packet->declare_worker_needs(wr);
    

    /* The caller should have reserved a worker thread before the
       enqueue operation is performed. If we manage to merge, we will
       unreserve that worker. */


    // * * * BEGIN CRITICAL SECTION * * *
    critical_section_t cs(_container_lock);


    // check for non-mergeable packets
    if (!packet->is_merge_enabled())  {
	// We are forcing the new packet to not merge with others.
	container_queue_enqueue_no_merge(packet);
        if (TRACE_MERGING)
            TRACE(TRACE_ALWAYS, "%s merging disabled\n",
                  packet->_packet_id.data());
	return;
	// * * * END CRITICAL SECTION * * *
    }


    /* UTILIZATION AWARE WORK SHARING:

       We want to avoid work sharing when there are idle system
       resources. _pool.max_active contains the number of worker
       threads allowed to run at any time (approximately the number of
       system resources). We want to share when the number of active
       (non-idle) workers is greater than this amount.
    */
    if(ALWAYS_TRY_OSP_INSTEAD_OF_WORKER_CREATE
       || (_rp.get_non_idle() >= _pool._max_active)) {
        
        /* Try merging with packets in merge_candidates before they
           disappear or become non-mergeable. */
        list<stage_adaptor_t*>::iterator sit = _container_current_stages.begin();
        for( ; sit != _container_current_stages.end(); ++sit) {

            // try to merge with this stage
            stage_container_t::stage_adaptor_t* ad = *sit;

            stage_container_t::merge_t ret = ad->try_merge(packet);
            if (ret == stage_container_t::MERGE_SUCCESS_RELEASE_RESOURCES) {
                // * * * END CRITICAL SECTION * * *
                cs.exit();
                if (unreserve)
                    wr->release_resources();
                return;
            }
            if (ret == stage_container_t::MERGE_SUCCESS_HOLD_RESOURCES)
                // * * * END CRITICAL SECTION * * *
                return;
        }


        // If we are here, we could not merge with any of the running
        // stages. Don't give up. We can still try merging with a packet in
        // the container_queue.
        ContainerQueue::iterator cit = _container_queue.begin();
        for ( ; cit != _container_queue.end(); ++cit) {
	
            packet_list_t* cq_plist = *cit;
            packet_t* cq_packet = cq_plist->front();


            // No need to acquire queue_pack's merge mutex. No other
            // thread can touch it until we release _container_lock.
    
            // We need to check queue_pack's mergeability flag since some
            // packets are non-mergeable from the beginning. Don't need to
            // grab its merge_mutex because its mergeability status could
            // not have changed while it was in the queue.
            if ( cq_packet->is_mergeable(packet) ) {
                // add this packet to the list of already merged packets
                // in the container queue
                cq_plist->push_back(packet);

                // * * * END CRITICAL SECTION * * *
                cs.exit();
                
                /* need to exit critical section before we unreserve */
                if (unreserve)
                    wr->release_resources();
                
                return;
            }
        }
    }

    if (TRACE_MERGING)
        TRACE(TRACE_ALWAYS, "%s could not be merged\n",
              packet->_packet_id.data());
    
    
    // No work sharing detected. We can now give up and insert the new
    // packet into the stage_queue.
    container_queue_enqueue_no_merge(packet);
    // * * * END CRITICAL SECTION * * *
};



/**
 *  @brief Worker threads for this stage should invoke this
 *  function. It will return when the stage shuts down.
 *
 *  THE CALLER MUST NOT BE HOLDING THE _container_lock MUTEX.
 */
void stage_container_t::run() {

    while (1) {
	

	// Wait for a packet to become available. We release the
	// _container_lock in container_queue_dequeue() if we end up
	// actually waiting.

	critical_section_t cs(_container_lock);
        // * * * BEGIN CRITICAL SECTION * * *

	packet_list_t* packets = container_queue_dequeue();

	// error checking
	assert( packets != NULL );
	assert( !packets->empty() );
        if (TRACE_DEQUEUE) {
            packet_t* head_packet = *(packets->begin());
            TRACE(TRACE_ALWAYS, "Processing %s\n",
                  head_packet->_packet_id.data());
        }


        // Construct an adaptor to work with. If this is expensive, we
        // can construct the adaptor before the dequeue and invoke
        // some init() function to initialize the adaptor with the
        // packet list.
	stage_adaptor_t
            adaptor(this,
                    packets,
                    packets->front()->_output_filter->input_tuple_size());

        
	// Add new stage to the container's list of active stages. It
	// is better to release the container lock and reacquire it
	// here since stage construction can take a long time.
        _container_current_stages.push_back(&adaptor);

        /* Becomes non-idle. Note that we don't become non-idle in
           this method. We do it in cleanup() since we must do it
           before deciding whether to unreserve ourselves. */
        _rp.notify_non_idle();

        // * * * END CRITICAL SECTION * * *
	cs.exit();

        
        // create stage
        guard<stage_t> stage = _stage_maker->create_stage();
        adaptor.run_stage(stage);

	
        // remove active stage
	critical_section_t cs_remove_active_stage(_container_lock);
        // * * * BEGIN CRITICAL SECTION * * *
        _container_current_stages.remove(&adaptor);
        /* should have marked ourselves non-idle in cleanup */
        // * * * END CRITICAL SECTION * * *
	cs_remove_active_stage.exit();
        
        
	// TODO: check for container shutdown
    }
}




// stage adaptor methods


/**
 * @brief Stage adaptor constructor.
 *
 *  THE CALLER DOES NOT NEED TO BE HOLDING THE _container_lock
 *  MUTEX. Holding the lock creates a longer critical section, but
 *  increases the chances of seeing sharing since a packet list is
 *  either in the container queue or in an current stage.
 */
stage_container_t::stage_adaptor_t::stage_adaptor_t(stage_container_t* container,
                                                    packet_list_t* packet_list,
                                                    size_t tuple_size)
    : adaptor_t(page::alloc(tuple_size)),
      _stage_adaptor_lock(thread_mutex_create()),
      _container(container),
      _packet_list(packet_list),
      _next_tuple(NEXT_TUPLE_INITIAL_VALUE),
      _still_accepting_packets(true),
      _contains_late_merger(false),
      _cancelled(false)
{
    
    assert( !packet_list->empty() );
    
    packet_list_t::iterator it;
    
    // We only need one packet to provide us with inputs. We
    // will make the first packet in the list a "primary"
    // packet. We can destroy the packet subtrees in the other
    // packets in the list since they will NEVER be used.
    it = packet_list->begin();
    _packet = *it;

    // Record next_tuple field in ALL packets, even the
    // primary.
    for (it = packet_list->begin(); it != packet_list->end(); ++it) {
        packet_t* packet = *it;
        packet->_next_tuple_on_merge = NEXT_TUPLE_INITIAL_VALUE;
	packet->output_buffer()->writer_init();
    }
}



/**
 *  @brief Try to merge the specified packet into this stage. This
 *  function assumes that packet has its mergeable flag set to
 *  true.
 *
 *  This method relies on the stage's current state (whether
 *  _stage_accepting_packets is true) as well as the
 *  is_mergeable() method used to compare two packets.
 *
 *  @param packet The packet to try and merge.
 *
 *  @return true if the merge was successful. false otherwise.
 *
 *  THE CALLER MUST BE HOLDING THE _container_lock MUTEX. THE CALLER
 *  MUST NOT BE HOLDING THE _stage_adaptor_lock MUTEX FOR THIS
 *  ADAPTOR.
 */
stage_container_t::merge_t stage_container_t::stage_adaptor_t::try_merge(packet_t* packet) {
    
    // * * * BEGIN CRITICAL SECTION * * *
    critical_section_t cs(_stage_adaptor_lock);


    if ( !_still_accepting_packets ) {
	// stage not longer in a state where it can accept new packets
        return stage_container_t::MERGE_FAILED;
        // * * * END CRITICAL SECTION * * *	
    }

    // The _still_accepting_packets flag cannot change since we are
    // holding the _stage_adaptor_lock. We can also safely access
    // _packet for the same reason.
 

    // Mergeability is an equality relation. We can check whether
    // packet is similar to the packets in this stage by simply
    // comparing it with the stage's primary packet.
    if ( !_packet->is_mergeable(packet) ) {
	// packet cannot share work with this stage
	return stage_container_t::MERGE_FAILED;
	// * * * END CRITICAL SECTION * * *
    }
    
    
    /* packet was merged with this existing stage */
    stage_container_t::merge_t ret;

    // If we are here, we detected work sharing!
    _packet_list->push_front(packet);
    packet->_next_tuple_on_merge = _next_tuple;
    if ((_next_tuple == NEXT_TUPLE_INITIAL_VALUE) || _contains_late_merger)
        /* Either we will be done when the primary packet finishes or
           there is already a late merger within the packet chain. In
           the latter case, the late merger has enough worker threads
           reserved for both of us. */
        ret = stage_container_t::MERGE_SUCCESS_RELEASE_RESOURCES;
    else {
        /* We are are a late merger (and we are the first late
           merger). We will be needing our worker threads... */
        _contains_late_merger = true;
        ret = stage_container_t::MERGE_SUCCESS_HOLD_RESOURCES;
    }
    
    // init the writer tid in case this packet is already going. If
    // not, the proper value will be set by the adaptor's constructor.
    packet->output_buffer()->writer_init();

    // * * * END CRITICAL SECTION * * *
    cs.exit();
    
    TRACE(TRACE_WORK_SHARING, "%s merged into %s. next_tuple_on_merge = %d\n",
	  packet->_packet_id.data(),
	  packet->_packet_id.data(),
	  packet->_next_tuple_on_merge);

    
    return ret;
}



/**
 *  @brief Outputs a page of tuples to this stage's packet set. The
 *  caller retains ownership of the page.
 *
 *  THE CALLER SHOULD NOT BE HOLDING THE _container_lock
 *  MUTEX. Holding it should not cause deadlock but it is unnecessary
 *  to hold it. THE CALLER MUST NOT BE HOLDING THE _stage_adaptor_lock
 *  MUTEX FOR THIS ADAPTOR.
 *
 *  COMPLICATED SYNCHRONIZATION ALERT: We will release
 *  stage_adaptor_lock here after recording the head and tail of the
 *  list. We do this with the assumption that the only operations done
 *  to this list by other threads are prepend (push_front)
 *  operations. These should not interfere with us.
 */
void stage_container_t::stage_adaptor_t::output_page(page* p) {

    packet_list_t::iterator it, end;
    unsigned int next_tuple;
    

    critical_section_t cs(_stage_adaptor_lock);
    // * * * BEGIN CRITICAL SECTION * * *
    it  = _packet_list->begin();
    end = _packet_list->end();
    _next_tuple += p->tuple_count();
    next_tuple = _next_tuple;
    // * * * END CRITICAL SECTION * * *
    cs.exit();

    
    // Any new packets which merge after this point will not 
    // receive this page.
    

    page::iterator pend = p->end();
    bool packets_remaining = false;
    while (it != end) {


        packet_t* curr_packet = *it;
	tuple_fifo* output_buffer = curr_packet->output_buffer();
	tuple_filter_t* output_filter = curr_packet->_output_filter;
        bool terminate_curr_packet = false;
        try {
            
            // Drain all tuples in output page into the current packet's
            // output buffer.
            page::iterator page_it = p->begin();
            while(page_it != pend) {

                // apply current packet's filter to this tuple
                tuple_t in_tup = page_it.advance();
                if(output_filter->select(in_tup)) {

                    // this tuple selected by filter!

                    // allocate space in the output buffer and project into it
                    tuple_t out_tup = output_buffer->allocate();
                    output_filter->project(out_tup, in_tup);
                }
            }
            

            // If this packet has run more than once, it may have received
            // all the tuples it needs. Check for this case.
            if ( next_tuple == curr_packet->_next_tuple_needed ) {
                
                 // This packet has received all tuples it needs! Another
                // reason to terminate this packet!
                terminate_curr_packet = true;
            }
        

        } catch(TerminatedBufferException &e) {
            // The consumer of the current packet's output
            // buffer has terminated the buffer! No need to
            // continue iterating over output page.
            TRACE(TRACE_ALWAYS,
                  "Caught TerminatedBufferException. Terminating current packet.\n");
            terminate_curr_packet = true;
        }
        
        
        // check for packet termination
        if (terminate_curr_packet) {
            // Finishing up a stage packet is tricky. We must treat
            // terminating the primary packet as a special case. The
            // good news is that the finish_packet() method handle all
            // special cases for us.
            finish_packet(curr_packet);
            it = _packet_list->erase(it);
            continue;
        }
 

        ++it;
        packets_remaining = true;
    }
    
    
    // no packets that need tuples?
    if ( !packets_remaining )
        throw stop_exception();
}



/**
 *  @brief Send EOF to the packet's output buffer. Delete the buffer
 *  if the consumer has already terminated it. If packet is not the
 *  primary packet for this stage, destroy its subpackets and delete
 *  it.
 *
 *  @param packet The packet to terminate.
 *
 *  THE CALLER DON'T NEED TO BE HOLDING EITHER THE _container_lock
 *  MUTEX OR THE _stage_adaptor_lock MUTEX.
 */
void stage_container_t::stage_adaptor_t::finish_packet(packet_t* packet) {

    // packet output buffer
    guard<tuple_fifo> output_buffer = packet->release_output_buffer();
    if ( output_buffer->send_eof() )
        // Consumer has not already terminated this buffer! It is now
        // responsible for deleting it.
        output_buffer.release();

    // packet input buffer(s)
    if ( packet != _packet ) {
        // since we are not the primary, can happily destroy packet
        // subtrees
        delete packet;
    }
}



/**
 *  @brief When a worker thread dequeues a new packet list from the
 *  container queue, it should create a stage_adaptor_t around that
 *  list, create a stage to work with, and invoke this method with the
 *  stage. This function invokes the stage's process() method with
 *  itself and "cleans up" the stage's packet list when process()
 *  returns.
 *
 *  @param stage The stage providing us with a process().
 *
 *  THE CALLER DOES NOT NEED TO BE HOLDING THE _container_lock
 *  MUTEX. THE CALLER MUST NOT BE HOLDING THE _stage_adaptor_lock
 *  MUTEX.
 */
void stage_container_t::stage_adaptor_t::run_stage(stage_t* stage) {


    // error checking
    assert( stage != NULL );

    
    // run stage-specific processing function
    bool error = false;
    try {
        stage->init(this);
        stage->process();
        flush();
    } catch(stop_exception &) {
        // no error
        TRACE(TRACE_DEBUG, "process() ended early\n");
    } catch(QPipeException &qe) {
        // error!
        TRACE(TRACE_ALWAYS, "process() encountered an error: %s\n", qe.what());
        error = true;
    } catch (...) {
        TRACE(TRACE_ALWAYS, "Caught unrecognized exception\n");
        assert(false);
    }

    // if we are still accepting packets, stop now
    stop_accepting_packets();
    if(error)
        abort_queries();
    else
        cleanup();

}



/**
 *  @brief Cleanup after successful processing of a stage.
 *
 *  Walk through packet list. Invoke finish_packet() on packets that
 *  merged when _next_tuple == NEXT_TUPLE_INITIAL_VALUE. Erase these
 *  packets from the packet list.
 *
 *  Take the remain packet list (of "unfinished" packets) and
 *  re-enqueue it.
 *
 *  Invoke terminate_inputs() on the primary packet and delete it.
 *
 *  THE CALLER DOES NOT NEED TO BE HOLDING THE _container_lock MUTEX
 *  OR THE _stage_adaptor_lock MUTEX.
 *
 *  COMPLICATED SYNCHRONIZATION ALERT: We will walk through the packet
 *  list without holding the _stage_adaptor_lock. We do this with the
 *  assumption that our caller has invoked
 *  stop_acceping_packets(). Since this disables any further merging
 *  into this stage, we can assume that no one else is touching our
 *  packet list.
 */
void stage_container_t::stage_adaptor_t::cleanup() {

    // walk through our packet list
    packet_list_t::iterator it;
    for (it = _packet_list->begin(); it != _packet_list->end(); ) {
        
	packet_t* curr_packet = *it;

        // Check for all packets that have been with this stage from
        // the beginning. If we haven't invoked finish_packet() on the
        // primary yet, we're going to do it now.
        if ( curr_packet->_next_tuple_on_merge == NEXT_TUPLE_INITIAL_VALUE ) {

            // The packet is finished. If it is not the primary
            // packet, finish_packet() will delete it.
            finish_packet(curr_packet);

            it = _packet_list->erase(it);
            continue;
        }
        
        // The packet is not finished. Simply update its progress
        // counter(s). The worker thread that picks up this packet
        // list should set the _stage_next_tuple_on_merge fields to
        // NEXT_TUPLE_INITIAL_VALUE.
        curr_packet->_next_tuple_needed =
            curr_packet->_next_tuple_on_merge;
        curr_packet->_next_tuple_on_merge = NEXT_TUPLE_UNINITIALIZED;
        ++it;
    }


    critical_section_t cs(_container->_container_lock);
    // * * * BEGIN CRITICAL SECTION * * *

    
    /* We will return and be able to process more packets. We can
       unreserve ourself from the container. Remember to drop
       non-idle count before this! */
    _container->_rp.notify_idle();
    if (_packet->unreserve_worker_on_completion())
        _container->_rp.unreserve(1);


    // Re-enqueue incomplete packets if we have them
    if ( _packet_list->empty() )
	delete _packet_list;
    else
        _container->container_queue_enqueue_no_merge(_packet_list);

    
    // * * * END CRITICAL SECTION * * *
    cs.exit();

    
    // TODO Terminate inputs of primary packet. finish_packet()
    // already took care of its output buffer.
    _packet_list = NULL;
    assert(_packet != NULL);
    delete _packet;
    _packet = NULL;
}



/**
 *  @brief Cleanup after unsuccessful processing of a stage.
 *
 *  Walk through packet list. For each non-primary packet, invoke
 *  terminate() on its output buffer, invoke destroy_subpackets() to
 *  destroy its inputs, and delete the packet.
 *
 *  Invoke terminate() on the primary packet's output buffer, invoke
 *  terminate_inputs() to terminate its input buffers, and delete it.
 *
 *  THE CALLER DOES NOT NEED TO BE HOLDING THE _container_lock MUTEX
 *  OR THE _stage_adaptor_lock MUTEX.
 *
 *  COMPLICATED SYNCHRONIZATION ALERT: We will walk through the packet
 *  list without holding the _stage_adaptor_lock. We do this with the
 *  assumption that our caller has invoked
 *  stop_acceping_packets(). Since this disables any further merging
 *  into this stage, we can assume that no one else is touching our
 *  packet list.
 */
void stage_container_t::stage_adaptor_t::abort_queries() {

    TRACE(TRACE_ALWAYS, "Aborting query: %s", _packet->_packet_id.data());

    // handle non-primary packets in packet list
    packet_list_t::iterator it;
    for (it = _packet_list->begin(); it != _packet_list->end(); ++it) {
        
	packet_t* curr_packet = *it;
        if ( curr_packet != _packet ) {
            // packet is non-primary
            delete curr_packet;
        }
    }

    delete _packet_list;
    _packet_list = NULL;
    
    // handle primary packet
    delete _packet;
    _packet = NULL;
}



EXIT_NAMESPACE(qpipe);
