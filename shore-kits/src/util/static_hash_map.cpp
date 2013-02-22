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

/** @file static_hash_map.c
 *
 *  @brief Hash map implementation with no internal dynamic memory
 *  allocation. The table is created at a fixed size. Separate
 *  chaining is used to deal with collisions. This implementation is
 *  not synchronized.
 *
 *  To create a struct static_hash_map_s structure, please include
 *  static_hash_map_struct.h. It contains the internal
 *  representation of this data structure. However, once an instance
 *  is created, use the functions provided below to manipulate the
 *  internal fields; this will keep the data structure in a consistant
 *  state.
 *
 *  @author Naju Mancheril
 *
 *  @bug None known.
 */

#include <cstdlib>                        /* for NULL */
#include "util/static_hash_map.h"         /* for prototypes */
#include "util/static_hash_map_struct.h"  /* for prototypes */


/* internal helper functions */

static void* static_hash_node_get_key(static_hash_node_t node);
static void* static_hash_node_get_value(static_hash_node_t node);
static static_hash_node_t static_hash_node_get_next(static_hash_node_t node);
static void static_hash_node_insert_after(static_hash_node_t insert_after_this,
					  static_hash_node_t node_to_insert);
static void static_hash_node_cut(static_hash_node_t node);


/* definitions of exported functions */


/** 
 *  @brief Hash table initializer
 *
 *  @param ht The hash table to initialize.
 *
 *  @param table_entries The array of hash nodes to start the table
 *  with. The static hash map deals with collisions by chaining other
 *  nodes off these initial buckets.
 *
 *  @param table_size The number of hash nodes in table_entries.
 *
 *  @param hf The hash function to be used by this table. The table
 *  will apply this function to keys to determine a table index.
 *
 *  @param comparator The comparator used to compare two keys in the
 *  table. Really only used as an equality comparator. Should return 0
 *  when the two parameters are equal and nonzero otherwise.
 *
 *  @return void
 */
void static_hash_map_init(static_hash_map_t ht,
			  struct static_hash_node_s* table_entries,
			  size_t table_size,
			  size_t (*hf) (const void* key),
			  int    (*comparator) (const void* key1, const void* key2) )
{
    ht->table_entries = table_entries;
    ht->table_size = table_size;
    ht->hf = hf;
    ht->comparator = comparator;

    size_t i;
    for (i = 0; i < table_size; i++)
        {
            /* Initialize table nodes to point to NULL keys and values */
            /* This is just so we know that they are in a deterministic
               state. We don't treat NULL's as special values. */
            static_hash_node_init( &table_entries[i], NULL, NULL );
        }
}


/** 
 *  @brief Insert an element into the hash table. Does not check for
 *  duplicates.
 *
 *  @param ht The hash table.
 *
 *  @param key The key of the entry we will be inserting.
 *
 *  @param value The value of the entry we will be inserting.
 *
 *  @param node The node to use to store this entry.
 *
 *  @return void
 */
void static_hash_map_insert(static_hash_map_t ht,
			    const void* key,
			    const void* value,
			    static_hash_node_t node)
{
    unsigned long hash_code = ht->hf(key);
    unsigned long hash_index = hash_code % ht->table_size;
  
    static_hash_node_init( node, key, value );
  
    /* design design: assume that inserted data will be probed soon */
    /* insert done at the beginning of the chain */
    static_hash_node_insert_after( &ht->table_entries[hash_index], node );
}


/** 
 *  @brief Probe the hash map to see if the specified element exists.
 *
 *  @param ht The hash table.
 *
 *  @param key The key of the entry we ware searching for.
 *
 *  @param value If not NULL and the specified entry is found, the
 *  value of the key-value pair will be stored here.
 *
 *  @param node If not NULL and the specified entry is found, the node
 *  used to store the key-value pair will be stored here.
 *
 *  @return 0 if the specified element is found. Negative value
 *  otherwise.
 */
int static_hash_map_find(static_hash_map_t ht,
			 const void* key,
			 void** value,
			 static_hash_node_t* node)
{
    unsigned long hash_code = ht->hf(key);
    unsigned long hash_index = hash_code % ht->table_size;
  
    /* walk down the bucket chain, looking for a match */
  
    static_hash_node_t head =
        &ht->table_entries[ hash_index ];
  
    static_hash_node_t curr =
        static_hash_node_get_next( head );

    while ( curr != head )
        {
            void* node_key =
                static_hash_node_get_key( curr );
    
            if ( !ht->comparator( node_key, key ) )
                {
                    /* found a match! */
                    if ( value != NULL )
                        *value = static_hash_node_get_value( curr );
                    if ( node != NULL )
                        *node = curr;

                    return 0;
                }

            curr = static_hash_node_get_next( curr );
        }


    /* element not found */
    return -1;
}


/** 
 *  @brief Lookup for the specified element in the hash map. If it
 *  exists, remove it.
 *
 *  @param ht The hash table.
 *
 *  @param key The key of the entry we will be removing.
 *
 *  @param value If not NULL and the specified entry is removed, the
 *  value of the key-value pair will be stored here.
 *
 *  @param node If not NULL and the specified entry is removed, the
 *  node used to store the key-value pair will be stored here.
 *
 *  @return 0 on successful remove. Negative value if the specified
 *  element is not found.
 */
int static_hash_map_remove(static_hash_map_t ht,
			   const void* key,
			   void** value,
			   static_hash_node_t* node)
{

    void* entry_value;
    static_hash_node_t entry_node;
    if ( !static_hash_map_find( ht, key, &entry_value, &entry_node ) )
        {
            /* element found */
            static_hash_map_cut( ht, entry_node );
    
            if ( value != NULL )
                *value = entry_value;

            if ( node != NULL )
                *node = entry_node;

            return 0;
        }

    /* element not found */
    return -1;
}


/** 
 *  @brief Cut the specified node from the hash map. The behavior of
 *  this function is undefined if the specified node is not in the has
 *  map.
 *
 *  @param ht The hash table.
 *
 *  @param node The node to remove from the hash map.
 *
 *  @return void
 */
void static_hash_map_cut(static_hash_map_t ht, static_hash_node_t node)
{
    ht = ht;
    static_hash_node_cut( node );
}


/* static hash node functions */


/** 
 *  @brief Initialize a hash node to a singleton (a node connected
 *  only to itself). Initialize the node payload to the specified key,
 *  pointer pair.
 *
 *  @param node The hash node.
 *
 *  @param key The key to store in this node.
 *
 *  @param value The value to store in this node.
 *
 *  @return void
 */
void static_hash_node_init(static_hash_node_t node, const void* key, const void* value)
{
    node->next = node->prev = node;
    node->key = key;
    node->value = value;
}


/** 
 *  @brief Extract the key stored at this node.
 *
 *  @param node The list node.
 *
 *  @return The key stored here.
 */
static void* static_hash_node_get_key(static_hash_node_t node)
{
    return (void*)node->key;
}


/** 
 *  @brief Extract the value stored at this node.
 *
 *  @param node The list node.
 *
 *  @return The value stored here.
 */
static void* static_hash_node_get_value(static_hash_node_t node)
{
    return (void*)node->value;
}


/** 
 *  @brief A hash node can be thought of as a node in a doubly linked
 *  list. This function extracts the next node in the "list".
 *
 *  @param node The hash node.
 *
 *  @return The next node linked to us.
 */
static static_hash_node_t static_hash_node_get_next(static_hash_node_t node)
{
    return node->next;
}


/** 
 *  @brief A hash node can be thought of as a node in a doubly linked
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
static void static_hash_node_insert_after(static_hash_node_t insert_after_this,
					  static_hash_node_t node_to_insert)
{
    /* fix pointers in new node */
    node_to_insert->next = insert_after_this->next;
    node_to_insert->prev = insert_after_this;
  
    /* fix pointers in other nodes */
    insert_after_this->next = node_to_insert;
    node_to_insert->next->prev = node_to_insert;
}


/** 
 *  @brief A hash node can be thought of as a node in a doubly linked
 *  list. This function removes the specified node from its list,
 *  patching the list as necessary.
 *
 *  @param node The node to cut out.
 *
 *  @return 0 on successful remove. -1 if called on a singleton.
 */
static void static_hash_node_cut(static_hash_node_t node)
{
    /* patch the list */
    node->next->prev = node->prev;
    node->prev->next = node->next;

    /* fix pointers in removed node */
    node->next = node->prev = node;
}
