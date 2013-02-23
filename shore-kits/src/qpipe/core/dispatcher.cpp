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

#include "util.h"
#include "qpipe/core/dispatcher.h"

#include <cstdio>
#include <cstring>
#include <map>

using std::map;



ENTER_NAMESPACE(qpipe);


dispatcher_t* dispatcher_t::_instance = NULL;
pthread_mutex_t dispatcher_t::_instance_lock = thread_mutex_create();


dispatcher_t::dispatcher_t() 
{ 
}



dispatcher_t::~dispatcher_t() 
{
  TRACE(TRACE_DEBUG, "Need to destroy nodes and keys\n");
}



/**
 *  @brief THIS FUNCTION IS NOT THREAD-SAFE. It should not have to be
 *  since stages should register themselves in their constructors and
 *  their constructors should execute in the context of the root
 *  thread.
 */
void dispatcher_t::_register_stage_container(const c_str &packet_type,
                                             stage_container_t* sc,
                                             bool osp_enabled)
{
  // We may eventually want multiple stages willing to perform
  // SORT's. But then we need policy/cost model to determine which
  // SORT stage to use when. For now, restrict to one stage per type.
  if ( _scdir[packet_type] != NULL )
    THROW2(DispatcherException,
           "Trying to register duplicate stage for type %s\n",
           packet_type.data());

  _scdir[packet_type] = sc;
  _ospdir[packet_type] = osp_enabled;
}



/**
 *  @brief THIS FUNCTION IS NOT THREAD-SAFE IF MAP LOOKUP IS NOT
 *  THREAD SAFE.
 */
void dispatcher_t::_dispatch_packet(packet_t* packet) {

  stage_container_t* sc = _scdir[packet->_packet_type];
  if (sc == NULL)
    THROW2(DispatcherException, 
           "Packet type %s unregistered\n", packet->_packet_type.data());
  sc->enqueue(packet);
}



/**
 *  @brief THIS FUNCTION IS NOT THREAD-SAFE IF MAP LOOKUP IS NOT
 *  THREAD SAFE.
 */
bool dispatcher_t::_is_osp_enabled_for_type(const c_str& type) 
{  
  /* make sure type is registered */
  stage_container_t* sc = _scdir[type];
  if (sc == NULL) {
    THROW2(DispatcherException, 
           "Packet type %s unregistered\n", type.data());
  }

  return _ospdir[type];
}



/**
 *  @brief Acquire the required number of worker threads.
 *
 *  THIS FUNCTION IS NOT THREAD-SAFE IF MAP LOOKUP IS NOT THREAD SAFE.
 */
void dispatcher_t::_reserve_workers(const c_str& type, int n) 
{
  
  stage_container_t* sc = _scdir[type];
  if (sc == NULL) {
    THROW2(DispatcherException,
           "Type %s unregistered\n", type.data());
  }
  sc->reserve(n);
}



/**
 *  @brief Release the specified number of worker threads.
 *
 *  THIS FUNCTION IS NOT THREAD-SAFE IF MAP LOOKUP IS NOT THREAD SAFE.
 */
void dispatcher_t::_unreserve_workers(const c_str& type, int n) 
{  
  stage_container_t* sc = _scdir[type];
  if (sc == NULL)
    THROW2(DispatcherException,
           "Type %s unregistered\n", type.data());
  sc->unreserve(n);
}



dispatcher_t::worker_reserver_t* dispatcher_t::reserver_acquire() 
{
  return new worker_reserver_t(instance());
}



void dispatcher_t::reserver_release(worker_reserver_t* wr) 
{
  delete wr;
}



dispatcher_t::worker_releaser_t* dispatcher_t::releaser_acquire() {
  return new worker_releaser_t(instance());
}



void dispatcher_t::releaser_release(worker_releaser_t* wr) 
{
  delete wr;
}



void dispatcher_t::worker_reserver_t::acquire_resources() 
{  
  map<c_str, int>::iterator it;
  for (it = _worker_needs.begin(); it != _worker_needs.end(); ++it) {
    int n = it->second;
    if (n > 0) {
        TRACE(TRACE_DEBUG, "Reserving %d %s workers\n", n, it->first.data());
        _dispatcher->_reserve_workers(it->first, n);
    }
  }
}



/* wrapper which we can use inside packet.h */
bool is_osp_enabled_for_type(const c_str& packet_type)
{
  return dispatcher_t::is_osp_enabled_for_type(packet_type);
}

// Set osp at run time on a per-packet-type basis.
// Used for the VLDB07 shared/unshared execution predictive model.
void dispatcher_t::_set_osp_for_type(const c_str& packet_type, bool osp_switch) 
{
      _ospdir[packet_type] = osp_switch;
}
void set_osp_for_type(const c_str& packet_type, bool osp_switch) 
{
      dispatcher_t::set_osp_for_type(packet_type, osp_switch);
}


EXIT_NAMESPACE(qpipe);
