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

/** @file static_list_node.c
 *
 *  @brief List node implementation with no internal dynamic memory
 *  allocation. The static_list_node_t datatype was written to provide
 *  a linked list node that could be used by various list
 *  implementations. This implementation is not synchronized.
 *
 *  @author Naju Mancheril (ngm)
 *
 *  @brief Adapted from Java list implementation by Roy Liu
 *
 *  This file prototypes various methods used to manipulate a linked
 *  list node.
 *
 *  @bug None known.
 */

#include <stdlib.h>                  /* for NULL */
#include "util/static_list_node.h"   /* for static_list_node_t datatype */
#include "util/static_list_node_struct.h" /* for static_list_node_s structure */

/* exported functions */


/** 
 *  @brief Initialize a list node to a singleton (a node connected
 *  only to itself). Initialize the node payload to NULL.
 *
 *  @param node The list node.
 *
 *  @return void
 */

void static_list_node_init_empty(static_list_node_t node)
{
    node->next = node->prev = node;
    node->payload = NULL;
}



/** 
 *  @brief Initialize a list node to a singleton (a node connected
 *  only to itself).
 *
 *  @param node The list node.
 *
 *  @param payload The list node payload will be set to this.
 *
 *  @return void
 */

void static_list_node_init(static_list_node_t node, void* payload)
{
    static_list_node_init_empty(node);
    node->payload = payload;
}



/** 
 *  @brief Extract the payload stored within a list node.
 *
 *  @param node The list node.
 *
 *  @return The payload stored at this node.
 */

void* static_list_node_get_payload(static_list_node_t node)
{
    return node->payload;
}



/** 
 *  @brief A list node can be thought of as a node in a doubly linked
 *  list. This function extracts the previous node in the "list".
 *
 *  @param node The list node.
 *
 *  @return The previous node linked to us.
 */

static_list_node_t static_list_node_get_prev(static_list_node_t node)
{
    return node->prev;
}



/** 
 *  @brief A list node can be thought of as a node in a doubly linked
 *  list. This function extracts the next node in the "list".
 *
 *  @param node The list node.
 *
 *  @return The next node linked to us.
 */

static_list_node_t static_list_node_get_next(static_list_node_t node)
{
    return node->next;
}



/** 
 *  @brief Set the payload in this node.
 *
 *  @param node The list node.
 *
 *  @param payload The payload to insert into this node.
 *
 *  @return void
 */

void static_list_node_set_payload(static_list_node_t node, void* payload)
{
    node->payload = payload;
}



/** 
 *  @brief A list node can be thought of as a node in a doubly linked
 *  list. This function inserts a node new "after" some node in a
 *  list.
 *
 *  @param insert_after_this The new node will be linked with this
 *  node. It will be linked so it appears as the "next" node after
 *  this one.
 *
 *  @param node_to_insert The new node.
 *
 *  @return void
 */

void static_list_node_insert_after(static_list_node_t insert_after_this,
                                   static_list_node_t node_to_insert)
{
    /* fix pointers in new node */
    node_to_insert->next = insert_after_this->next;
    node_to_insert->prev = insert_after_this;
  
    /* fix pointers in other nodes */
    insert_after_this->next = node_to_insert;
    node_to_insert->next->prev = node_to_insert;
}



/** 
 *  @brief A list node can be thought of as a node in a doubly linked
 *  list. This function inserts a node new "before" some node in a
 *  list.
 *
 *  @param insert_before_this The new node will be linked with this
 *  node. It will be linked so it appears as the "previous" node
 *  before this one.
 *
 *  @param node_to_insert The new node.
 *
 *  @return void
 */

void static_list_node_insert_before(static_list_node_t insert_before_this,
                                    static_list_node_t node_to_insert)
{
    /* fix pointers in new node */
    node_to_insert->prev = insert_before_this->prev;
    node_to_insert->next = insert_before_this;
  
    /* fix pointers in other nodes */
    insert_before_this->prev = node_to_insert;
    node_to_insert->prev->next = node_to_insert;
}



/** 
 *  @brief A list node can be thought of as a node in a doubly linked
 *  list. This function checks whether this "list" stores more than
 *  one node. It assumes that a list cannot contain duplicate entries.
 *
 *  @param node The node to check.
 *
 *  @return 1 if this node is just linked to itseld. 0 if linked to
 *  any other nodes.
 */

int static_list_node_is_singleton(static_list_node_t node)
{
    return node->next == node;
}



/** 
 *  @brief A list node can be thought of as a node in a doubly linked
 *  list. This function removes the specified node from its list,
 *  patching the list as necessary.
 *
 *  @param node The node to cut out.
 *
 *  @return 0 on successful remove. -1 if called on a singleton.
 */

void static_list_node_cut(static_list_node_t node)
{
    /* patch the list */
    node->next->prev = node->prev;
    node->prev->next = node->next;

    /* fix pointers in removed node */
    node->next = node->prev = node;
}
