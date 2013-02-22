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

#ifndef __QPIPE_DISPATCHER_H
#define __QPIPE_DISPATCHER_H

#include "qpipe/core/tuple.h"
#include "qpipe/core/packet.h"
#include "qpipe/core/stage_container.h"
#include "util/resource_declare.h"
#include "util/resource_releaser.h"
#include <map>

using std::map;


ENTER_NAMESPACE(qpipe);


DEFINE_EXCEPTION(DispatcherException);


/* exported constants */

#define TRACE_RESOURCE_RELEASE 0
static const int DISPATCHER_NUM_STATIC_HASH_MAP_BUCKETS = 64;


/* exported datatypes */

/**
 *  @brief QPIPE dispatcher that dispatches all packets. All stages
 *  should register themselves with the dispatcher on startup. This
 *  registration should all be done before creating any additional
 *  threads.
 *
 *  Currently, the dispatcher is a singleton, but we provide static
 *  wrappers around our methods to avoid littering the code with
 *  dispatcher_t::instance() calls.
 */

class dispatcher_t {

protected:


    // stage directory
    map<c_str, stage_container_t*> _scdir;
    map<c_str, bool> _ospdir;
    
 
    dispatcher_t();
    ~dispatcher_t();

   
    // methods
    void _register_stage_container(const c_str& packet_type,
                                   stage_container_t* sc, bool osp);
    void _dispatch_packet(packet_t* packet);
    void _reserve_workers(const c_str& type, int n);
    void _unreserve_workers(const c_str& type, int n);
    bool _is_osp_enabled_for_type(const c_str& packet_type);
    
    // Used for the VLDB07 shared/unshared execution predictive model.
    void _set_osp_for_type(const c_str& packet_type, bool osp_switch);


    static pthread_mutex_t _instance_lock;
    static dispatcher_t*   _instance;


    static dispatcher_t* instance() {
        
        critical_section_t cs(_instance_lock);
        if ( _instance == NULL )
            _instance = new dispatcher_t();

        // TODO Move over to a static init() function that is
        // guaranteed to be called by the root thread on startup. Then
        // this function can simply assert that _instance has been
        // initialized.
	return _instance;
    }


public:


    // exported datatypes
    class worker_reserver_t;
    class worker_releaser_t;


    // accessors to global instance...
    static void init() {
        _instance = new dispatcher_t();
    }


    static void register_stage_container(const c_str &packet_type,
                                         stage_container_t* sc, bool osp_enabled) {
        instance()->_register_stage_container(packet_type, sc, osp_enabled);
    }

    static bool is_osp_enabled_for_type(const c_str &packet_type) {
        return instance()->_is_osp_enabled_for_type(packet_type);
    }

       

    /* packet dispatching */
    template<template<class> class Guard>
    static void dispatch_packet(Guard<packet_t> &packet) {
        dispatch_packet(packet.release());
    }
    static void dispatch_packet(packet_t* packet) {
        instance()->_dispatch_packet(packet);
    }

    // Used for the VLDB07 shared/unshared execution predictive model.
    static void set_osp_for_type(const c_str& packet_type, bool osp_switch) {
        return instance()->_set_osp_for_type(packet_type, osp_switch);
    }

    /* worker thread methods */
    static worker_reserver_t* reserver_acquire();
    static void reserver_release(worker_reserver_t* wr);
    static worker_releaser_t* releaser_acquire();
    static void releaser_release(worker_releaser_t* wr);
};



class dispatcher_t::worker_reserver_t : public resource_declare_t
{

private:

    dispatcher_t*   _dispatcher;
    map<c_str, int> _worker_needs;
    
public:

    worker_reserver_t (dispatcher_t* dispatcher)
        : _dispatcher(dispatcher)
    {
    }
    
    virtual void declare(const c_str& name, int count) {
        int curr_needs = _worker_needs[name];
        _worker_needs[name] = curr_needs + count;
    }
    
    void acquire_resources();
};



class dispatcher_t::worker_releaser_t : public resource_declare_t, public resource_releaser_t
{

private:

    dispatcher_t*   _dispatcher;
    map<c_str, int> _worker_needs;
    
public:

    worker_releaser_t (dispatcher_t* dispatcher)
        : _dispatcher(dispatcher)
    {
    }
    
    virtual void declare(const c_str& name, int count) {
        int curr_needs = _worker_needs[name];
        _worker_needs[name] = curr_needs + count;
    }
    
    void release_resources() {
        map<c_str, int>::iterator it;
        for (it = _worker_needs.begin(); it != _worker_needs.end(); ++it) {
            int n = it->second;
            if (n > 0) {
                TRACE(TRACE_RESOURCE_RELEASE & TRACE_ALWAYS,
                      "Releasing %d %s workers\n", n, it->first.data());
                _dispatcher->_unreserve_workers(it->first, n);
            }
        }
    }
};


EXIT_NAMESPACE(qpipe);


#endif
