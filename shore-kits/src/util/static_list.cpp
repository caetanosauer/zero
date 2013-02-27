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

/** @file static_list.c
 *
 *  @brief List implementation with no internal dynamic memory
 *  allocation. The static_list_t datatype was written to separate the
 *  rules of list management from the rules of allocating and freeing
 *  nodes. If necessary, another datatype may be defined to wrap
 *  static_list_t instances with a particular allocator. This
 *  implementation is not synchronized.
 *
 *  @author Naju Mancheril (ngm)
 *
 *  @brief Adapted from Java list implementation by Roy Liu
 *
 *  This file prototypes various methods used to manipulate a linked
 *  list. Nodes which are inserted or removed are not freed since this
 *  implementation assumes they are statically allocated (globals or
 *  stack variables).
 *
 *  @bug None known.
 */

#include <stdlib.h>             /* for NULL */
#include "util/static_list.h"   /* for prototypes */
#include "util/static_list_struct.h" /* for static_list_s structure definition */
#include "util/static_list_node.h"   /* for prototypes */


/* definitions of exported functions */


/** 
 *  @brief List initializer.
 *
 *  @return void
 */

void static_list_init(static_list_t list)
{
    /* initialize */
    static_list_node_init_empty( &list->dummy );
}



/** 
 *  @brief Prepend an element to the list.
 *
 *  @param list The list to update.
 *
 *  @param value The element to insert.
 *
 *  @param node The node to use for the new element.
 *
 *  @return void
 */

void static_list_prepend(static_list_t list, void* value, static_list_node_t node)
{
    static_list_node_init( node, value );
    static_list_node_insert_after( &list->dummy, node );
}



/** 
 *  @brief Append an element to the list.
 *
 *  @param list The list to update.
 *
 *  @param value The element to insert.
 *
 *  @param node The node to use for the new element.
 *
 *  @return void
 */

void static_list_append(static_list_t list, void* value, static_list_node_t node)
{
    static_list_node_init( node, value );
    static_list_node_insert_before( &list->dummy, node );
}



/** 
 *  @brief Remove the specified element from the list. If the element
 *  is not in the list, this results are undefined.
 *
 *  @param list The list to update.
 *
 *  @param node The node to remove.
 *
 *  @return 0 on successful remove. -1 on empty list.
 */

void static_list_remove_node(static_list_t list, static_list_node_t node)
{
    /* cannot actually detect that node is not in the list */
    list = list;
    static_list_node_cut(node);  
}



/** 
 *  @brief Remove first element of list.
 *
 *  @param list The list to update.
 *
 *  @param result On successful remove, the payload will be stored
 *  here.
 *
 *  @param node If not NULL, then on successful remove, the node used
 *  to store the payload will be stored here.
 *
 *  @return 0 on successful remove. -1 on empty list.
 */

int static_list_remove_head(static_list_t list, void** result, static_list_node_t* node)
{
    /* empty list case */
    if ( static_list_is_empty(list) )
        return -1;


    /* cut the first node out of the list */
    static_list_node_t removed_node = static_list_node_get_next( &list->dummy );
    static_list_node_cut( removed_node );


    /* "return" payload and node */
    *result = static_list_node_get_payload(removed_node);
    if ( node != NULL )
        *node = removed_node;
  
    return 0;
}



/** 
 *  @brief Remove last element in a list.
 *
 *  @param list The list to update.
 *
 *  @param result On successful remove, the payload will be stored
 *  here.
 *
 *  @param node If not NULL, then on successful remove, the node used
 *  to store the payload will be stored here.
 *
 *  @return 0 on successful remove. -1 on empty list.
 */

int static_list_remove_tail(static_list_t list, void** result, static_list_node_t* node)
{
    /* empty list case */
    if ( static_list_is_empty(list) )
        return -1;
  

    /* cut the last node out of the list */
    static_list_node_t removed_node = static_list_node_get_prev( &list->dummy );
    static_list_node_cut( removed_node );


    /* "return" payload and node */
    *result = static_list_node_get_payload( removed_node );
    if ( node != NULL )
        *node = removed_node;
  
    return 0;
}



/** 
 *  @brief Get the first element of the list. The list is not
 *  modified.
 *
 *  @param list The list to get the head of.
 *
 *  @param result If the list is not empty, the head is stored here.
 *
 *  @return 0 on success. -1 on empty list.
 */

int static_list_get_head(static_list_t list, void** result)
{
    /* empty list case */
    if ( static_list_is_empty(list) )
        return -1;

    static_list_node_t node = static_list_node_get_next( &list->dummy );
    *result = static_list_node_get_payload(node);

    return 0;
}



/** 
 *  @brief Get the payload stored at the tail of the list. The list is
 *  not modified.
 *
 *  @param list The list to get the tail of.
 *
 *  @param result If the list is not empty, the tail is stored here.
 *
 *  @return 0 on success. -1 on empty list.
 */

int static_list_get_tail(static_list_t list, void** result)
{
    /* empty list case */
    if ( static_list_is_empty(list) )
        return -1;

    static_list_node_t node = static_list_node_get_prev( &list->dummy );
    *result = static_list_node_get_payload(node);
  
    return 0;
}



/** 
 *  @brief Check whether the specified list is empty.
 *
 *  @param list The list to check.
 *
 *  @return Unlike most other functions in this module, this function
 *  returns a boolean value. It returns 1 (true) if the list is empty
 *  and 0 (false) otherwise.
 */

int static_list_is_empty(static_list_t list)
{
    /* empty list contains only "dummy" head node */
    return static_list_node_is_singleton( &list->dummy );
}



/** 
 *  @brief Map the given function onto the list elements. In other
 *  words, each node element n will be replaced with
 *  map(n,map_state).
 *
 *  @param list The list to map.
 *
 *  @param map The mapping function.
 *
 *  @param map_state The use of this parameter is
 *  map-function-specific. It is passed to the map function as the
 *  list is traversed.
 *
 *  @return void
 */

void static_list_map(static_list_t list, 
                     void* (*map) (void*,void*), 
                     void* map_state)
{

    /* grab first node in list */
    static_list_node_t curr = static_list_node_get_next( &list->dummy );
  
    /* traverse */
    while ( curr != &list->dummy )
        {
            /* isolate current node */
            void* payload = static_list_node_get_payload(curr);
    
            /* apply map function to this node */
            void* updated_payload = map(payload, map_state);
            static_list_node_set_payload(curr, updated_payload);
    
            /* continue to next node */
            curr = static_list_node_get_next(curr);
        }
}



/** 
 *  @brief Similar to static_list_map, except the nodes are traversed
 *  in reverse order.
 *
 *  @param list The list to map.
 *
 *  @param map The mapping function.
 *
 *  @param map_state The use of this parameter is
 *  map-function-specific. It is passed to the map function as the
 *  list is traversed.
 *
 *  @return void
 */

void static_list_map_reverse(static_list_t list, 
                             void* (*map) (void*,void*), 
                             void* map_state)
{

    /* grab last node in list */
    static_list_node_t curr = static_list_node_get_prev( &list->dummy );
  
    /* traverse */
    while ( curr != &list->dummy )
        {
            /* isolate current node */
            void* payload = static_list_node_get_payload(curr);
    
            /* apply map function to this node */
            void* updated_payload = map(payload, map_state);
            static_list_node_set_payload(curr, updated_payload);
    
            /* continue to next node */
            curr = static_list_node_get_prev(curr);
        }
}



/** 
 *  @brief Process the given function over the list elements. In other
 *  words, process(n,processing_state) will be called on each node
 *  element n until it has either processed all list elements, or been
 *  directed to stop. The elements are processed in order from the
 *  head to the tail. This function can be used to implement a find().
 *
 *  @param list The list to process.
 *
 *  @param process The processing function. Return 1 to continue
 *  traversing the list. 0 to stop early.
 *
 *  @param processing_state The use of this parameter is
 *  process-function-specific. It is passed to the process function as
 *  the list is traversed.
 *
 *  @return void
 */

void static_list_process(static_list_t list, 
                         int (*process) (void*,void*), 
                         void* processing_state)
{
  
    /* grab first node in list */
    static_list_node_t curr = static_list_node_get_next( &list->dummy );
  
    /* traverse */
    while ( curr != &list->dummy )
        {
            /* isolate current node */
            void* payload = static_list_node_get_payload(curr);
    
            /* apply processing function to this node */
            int keep_going = process(payload, processing_state);
            if ( !keep_going ) break; /* done */

            /* continue to next node */
            curr = static_list_node_get_next(curr);
        }
}



/** 
 *  @brief Similar to static_list_process, except the nodes are
 *  traversed in reverse order.
 *
 *  @param list The list to process.
 *
 *  @param process The processing function. Return 1 to continue
 *  traversing the list. 0 to stop early.
 *
 *  @param processing_state The use of this parameter is
 *  process-function-specific. It is passed to the process function as
 *  the list is traversed.
 *
 *  @return void
 */

void static_list_process_reverse(static_list_t list, 
                                 int (*process) (void*,void*), 
                                 void* processing_state)
{
  
    /* grab last node in list */
    static_list_node_t curr = static_list_node_get_prev( &list->dummy );
  
    /* traverse */
    while ( curr != &list->dummy )
        {
            /* isolate current node */
            void* payload = static_list_node_get_payload(curr);
    
            /* apply processing function to this node */
            int keep_going = process(payload, processing_state);
            if ( !keep_going ) break; /* done */
    
            /* continue to next node */
            curr = static_list_node_get_prev(curr);
        }
}
