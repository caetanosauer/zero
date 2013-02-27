
/** @file static_list_struct.h
 *
 *  @brief Exports the internal representation of a static_list_t
 *  datatype. This file should be included in a module for the sole
 *  purpose of statically allocating lists (i.e. as
 *  globals).
 *
 *  Any module that needs the static_list_t definition and prototypes
 *  should include static_list.h directly.
 *
 *  Modules should only include this file IF AND ONLY IF they need to
 *  statically allocate and/or initialize static_list_s
 *  structures. Once that is done, this module should continue to use
 *  the functions provided in static_list.h to manipulate the data
 *  structure.
 *
 *  @author Naju Mancheril (ngm)
 *
 *  @brief Adapted from Java list implementation by Ruoran (Roy) Liu.
 *
 *  @bug See static_list.c.
 */
#ifndef _STATIC_LIST_STRUCT_H
#define _STATIC_LIST_STRUCT_H

#include "util/static_list_node_struct.h" /* for static_list_node_s
                                             structure */




/* exported structures */


/** @struct static_list_s
 *
 *  @brief This structure represents our linked list. We expose it
 *  here so that linked lists may be statically allocated. Once an
 *  instance is allocated, it should be passed around as a pointer (a
 *  static_list_t).
 */
struct static_list_s
{
  /** This node acts is a "dummy" element in our list. It is only here
      to simplify bookkeeping. */
  struct static_list_node_s dummy;
};




/* exported initializers */


/** @def STATIC_LIST_INITIALIZER
 *
 *  @brief Static initializer for a static_list_s structure. Can be
 *  used to initialize global and static variables.
 *
 *  @param this Address of the structure we are trying to initialize.
 *
 *  This initializer should be used to initialize a variable x in the
 *  following way:
 *
 *  struct static_list_s sl = STATIC_LIST_INITIALIZER(&sl);
 */
#define STATIC_LIST_INITIALIZER(this) { STATIC_LIST_NODE_INITIALIZER(&(this)->dummy,NULL) }




#endif
