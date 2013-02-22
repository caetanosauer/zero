
/** @file static_hash_map_struct.h
 *
 *  @brief Exports the internal representation of a
 *  static_hash_map_t datatype. This file should be included in a
 *  module for the sole purpose of statically allocating tables
 *  (i.e. as globals).
 *
 *  Modules should only include this file IF AND ONLY IF they need to
 *  statically and/or initialize static_hash_maps_t's. Once that is
 *  done, this module should continue to use the functions provided in
 *  static_hash_map.h to manipulate the data structure.
 *
 *  @author Naju Mancheril
 *
 *  @bug None known.
 */

#ifndef _STATIC_HASH_MAP_STRUCT_H
#define _STATIC_HASH_MAP_STRUCT_H

#include <sys/types.h>




/* exported structures */


/* It would be nice to provide static initializers here, but every
   hash node in a hash table must be initialized. Since there are a
   variable number of these, I'm not sure how it can be done. */



#endif
