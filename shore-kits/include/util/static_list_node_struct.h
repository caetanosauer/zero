
/** @file static_list_node_struct.h
 *
 *  @brief Exports the internal static_list_node_s structure. This
 *  file should be included in a module for the sole purpose of
 *  statically allocating list nodes (i.e. as globals).
 *
 *  Any module that needs the static_list_node_t definition and
 *  prototypes should include static_list_node.h directly.
 *
 *  Modules should only include this file IF AND ONLY IF they need to
 *  statically allocate and/or initialize static_list_node_s
 *  structures. Once that is done, this module should continue to use
 *  the functions provided in static_list_node.h to manipulate the
 *  data structure.
 *
 *  @author Naju Mancheril (ngm)
 *
 *  @brief Adapted from Java list implementation by Ruoran (Roy) Liu.
 *
 *  @bug See static_list_node.c.
 */
#ifndef _STATIC_LIST_NODE_STRUCT_H
#define _STATIC_LIST_NODE_STRUCT_H




/* exported structures */


/** @struct static_list_node_s
 *
 *  @brief This structure represents our linked list node. We expose
 *  it here so that linked list nodes may be statically
 *  allocated. Once an instance is allocated, it should be passed
 *  around as a pointer (a static_list_node_t).
 */
struct static_list_node_s
{
  /** The previous node in the list. */
  struct static_list_node_s* prev;

  /** The next node in the list. */
  struct static_list_node_s* next;

  /** Finally, we have our node data. */
  void* payload;
};




/* exported initializers */


/** @def STATIC_LIST_NODE_INITIALIZER
 *
 *  @brief Static initializer for a static_list_node_s structure. Can
 *  be used to initialize global and static variables.
 *
 *  @param this Address of the structure we are trying to initialize.
 *
 *  This initializer should be used to initialize a variable x in the
 *  following way:
 *
 *  struct static_list_node_s n = STATIC_LIST_NODE_INITIALIZER(&n,payload);
 */
#define STATIC_LIST_NODE_INITIALIZER(this,payload) { (this), (this), (payload) }




#endif
