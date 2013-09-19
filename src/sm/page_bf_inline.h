/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef PAGE_BF_INLINE_H
#define PAGE_BF_INLINE_H

// bufferpool-related inline methods for fixable_page_h.
// these methods are small and frequently called, thus inlined.

// also, they are separated from page.h because these implementations
// have more dependency that need to be inlined (eg bf_tree_m).
// To hide unnecessary details from the caller except when the caller
// actually uses these methods, I separated them to this file.

// also inline bf_tree_m methods.
//#include "bf_tree_inline.h"
//#include "fixable_page_h.h"
//#include "sm_int_0.h"


#endif // PAGE_BF_INLINE_H

