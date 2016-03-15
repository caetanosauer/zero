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

/** @file:   stl_pool.cpp
 * 
 *  @brief:  Pool for the Pool allocator
 * 
 *  @author: Ippokratis Pandis, Nov 2008
 *
 *  @note:   Taken from: http://www.sjbrown.co.uk/2004/05/01/pooled-allocators-for-the-stl/
 */

#include "util/stl_pool.h"
#include "util/trace.h"


Pool::Pool( size_t granularity, size_t size ) 
  : m_granularity( granularity ), m_size( size ), m_used( 0 ), m_overflow( 0 )
{
  if( m_size > 0 )
    {
      m_storage = new char[m_size*granularity];
      m_slots = new void*[m_size];

      for( size_t i = 0; i < m_size; ++i )
        m_slots[i] = reinterpret_cast<void*>( m_storage.get() + i*granularity );
    }
}

Pool::~Pool()
{
  // IP: give it 3 secs to clean
  for (int i=0; i<3; ++i) {
    if ((m_used == 0) && (m_overflow == 0)) {
      break;
    }
    std::cout << "~" << std::endl;
    sleep(1);
  }
  assert( m_used == 0 && m_overflow == 0 && "can't destroy a pool with outstanding allocations" );
  //zstd::cout << m_overflow << std::endl;
}
	
void* Pool::Allocate()
{
  if( m_used < m_size )
    {
      return m_slots[m_used++];
    }
  else
    {
      ++m_overflow;
      TRACE( TRACE_TRX_FLOW, "Overflow (%d)\n", m_overflow);
      return reinterpret_cast<void*>( new char[m_granularity] );
    }
}

void Pool::Deallocate( void* block )
{
  assert( block && "null pointer argument" );
  if( IsFromPool( block ) )
    {
      assert( m_used > 0 && "internal error" );
      m_slots[--m_used] = block;
    }
  else
    {
      assert( m_overflow > 0 && "internal error" );
      delete[] reinterpret_cast<char*>( block );
      --m_overflow;
    }
}

