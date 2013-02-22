
/** @file static_list_node.h
 *
 *  @brief Exports simple functions to manipulate static_list_node_t
 *  instances. static_list_node_t instances should always be used in
 *  the context of some static_list_t, but there are some applications
 *  that occasionally need direct access to the contents of these
 *  nodes. A common example is an initializer for a data structure
 *  that contains (and needs to initialize) static_list_node_s
 *  structures.
 *
 *  To create a static_list_node_s structure, please include
 *  static_list_struct.h. It contains the internal representation of
 *  this data structure. However, once an instance is created, use the
 *  functions provided below to manipulate the internal fields; this
 *  will keep the data structure in a consistant state.
 *
 *  @author Naju Mancheril (ngm)
 *
 *  @brief Adapted from Java list implementation by Ruoran (Roy) Liu
 *
 *  @bug See static_list_node.h.
 */
#ifndef _STATIC_LIST_NODE_H
#define _STATIC_LIST_NODE_H



/* exported datatypes */


/** @typedef static_list_node_t
 *
 *  @brief This is our static linked list node datatype. Modules that
 *  need access to the list representation should include
 *  static_list_node_struct.h.
 */
typedef struct static_list_node_s* static_list_node_t;



/* exported functions */


void  static_list_node_init_empty(static_list_node_t node);
void  static_list_node_init(static_list_node_t node, void* payload);
void* static_list_node_get_payload(static_list_node_t node);
static_list_node_t static_list_node_get_prev(static_list_node_t node);
static_list_node_t static_list_node_get_next(static_list_node_t node);
void  static_list_node_set_payload(static_list_node_t node, void* payload);
void  static_list_node_insert_after(static_list_node_t insert_after_this,
				    static_list_node_t node_to_insert);
void  static_list_node_insert_before(static_list_node_t insert_before_this,
				     static_list_node_t node_to_insert);
int   static_list_node_is_singleton(static_list_node_t node);
void  static_list_node_cut(static_list_node_t node);



#endif


