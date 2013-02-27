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

/** @file:   register_stage_containers.cpp
 *
 *  @brief:  Registers (initiates) containers of the various stages used
 *
 */

#ifndef __QPIPE_REGISTER_STAGE_CONTAINERS_H
#define __QPIPE_REGISTER_STAGE_CONTAINERS_H

#include "qpipe/core.h"

ENTER_NAMESPACE(qpipe);

template <class Stage>
void register_stage(int worker_threads=10, bool osp=true) 
{
    stage_container_t* sc;
    c_str name("%s_CONTAINER", Stage::DEFAULT_STAGE_NAME.data());
    sc = new stage_container_t(name, new stage_factory<Stage>, worker_threads);
    dispatcher_t::register_stage_container(Stage::stage_packet_t::PACKET_TYPE.data(), sc, osp);
}

void register_stage_containers();


EXIT_NAMESPACE(qpipe);

#endif // __QPIPE_REGISTER_STAGE_CONTAINERS_H
