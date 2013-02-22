
/** @file static_list.h
 *
 *  @brief List implementation with no internal dynamic memory
 *  allocation. The static_list_t datatype was written to separate the
 *  rules of list management from the rules of allocating and freeing
 *  nodes. This implementation is not synchronized.
 *
 *  To create a static_list_s structure, please include
 *  static_list_struct.h. It contains the internal representation of
 *  this data structure. However, once an instance is created, use the
 *  functions provided below to manipulate the internal fields; this
 *  will keep the data structure in a consistant state.
 *
 *  @author Naju Mancheril (ngm)
 *
 *  @brief Adapted from Java list implementation by Ruoran (Roy) Liu
 *
 *  @bug See static_list.c.
 */
#ifndef _STATIC_LIST_H
#define _STATIC_LIST_H

#include "util/static_list_node.h" /* for static_list_node_t datatype */



/* exported datatypes */


/** @typedef static_list_t
 *
 *  @brief This is our static linked list datatype. Modules that need
 *  access to the list representation should include
 *  static_list_struct.h.
 */
typedef struct static_list_s* static_list_t;



/* exported functions */

/* Avoiding the const keyword with payload pointers since it makes it
   difficult to hand back objects. Anyone that removes a payload must
   also treat it as const. Using const also prevents us from allowing
   functors that modify payloads passed to map. */


/* initialize */

void static_list_init           (static_list_t list);


/* insert */

void static_list_prepend        (static_list_t list, void* value, static_list_node_t node);
void static_list_append         (static_list_t list, void* value, static_list_node_t node);


/* remove */

void static_list_remove_node    (static_list_t list, static_list_node_t node);
int  static_list_remove_head    (static_list_t list, void** result, static_list_node_t* node);
int  static_list_remove_tail    (static_list_t list, void** result, static_list_node_t* node);


/* accessors */

int static_list_get_head        (static_list_t list, void** result);
int static_list_get_tail        (static_list_t list, void** result);
int static_list_is_empty        (static_list_t list);


/* map, process */

void static_list_map            (static_list_t list, void* (*map) (void*,void*), void* map_state);
void static_list_map_reverse    (static_list_t list, void* (*map) (void*,void*), void* map_state);
void static_list_process        (static_list_t list, int (*process) (void*,void*), void* processing_state);
void static_list_process_reverse(static_list_t list, int (*process) (void*,void*), void* processing_state);



#endif
