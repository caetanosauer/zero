#ifndef BTREE_IMPL_DEBUG_H
#define BTREE_IMPL_DEBUG_H

#include "w_defines.h"

/**
 * Moved debug-related flag definitions from btee_impl.cpp.
 * Include this file in each btree_impl_xxx.cpp.
 */ 

#if W_DEBUG_LEVEL > 2
#define BTREE_LOG_COMMENT_ON 1
#else
#define BTREE_LOG_COMMENT_ON 0
#endif

#if W_DEBUG_LEVEL > 3
/* change these at will */
#        define print_sanity false
#        define print_split false
#        define print_traverse false
#        define print_ptraverse false
#        define print_wholetree false
#        define print_remove false
#        define print_propagate false
#else
/* don't change these */
#        define print_sanity false
#        define print_wholetreee false
#        define print_traverse false
#        define print_ptraverse false
#        define print_remove false
#        define print_propagate false
#endif 


#endif //BTREE_IMPL_DEBUG_H
