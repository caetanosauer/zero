/*<std-header orig-src='shore' incl-file-exclusion='W_HEAP_H'>

 $Id: w_heap.h,v 1.13 2010/05/26 01:20:25 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#ifndef W_HEAP_H
#define W_HEAP_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include "w_base.h"


/**\brief General-purpose heap.
 *
 * This class implements a general purpose heap.
 * The element type T and the ordering function (via class Cmp)
 * are passed in as parameters to the Heap template.
 *
 * This heap sifts the LARGEST elements to the top and the
 * SMALLEST elements to the bottom, according tho the comparison
 * function, Cmp.gt().  Thus the "heap property" is that
 * value(Parent(i)) gt value(i) for each element i in the heap.
 *
 * An element anywhere in the heap can be changed (by an outsider-
 * not by the heap template) and the heap property can be restored,
 * but he who changes the element must know whether the element's
 * key value increased or decreased.  Since we're sifting "large"
 * elements to the top of the heap, if the element's key value increases,
 * the element must be sifted UP, and if its value decreases, it must
 * be sifted down.  The methods IncreaseN and DecreaseN
 * handle these cases.
 *
 * The heap has the following worst case run times:
 *
 *   - create initial heap        O(n)
 *   - retrieve first element     O(1)
 *   - retrieve second element    O(1)
 *   - replace first element      O(log n)
 *   - delete first element       O(log n)
 *   - add new element            O(log n)
 *   - increment/decrement key for any element O(log n)
 *
 * The comparison class, Cmp, must have the member functions
 *   gt (T* left, T* right) returns true iff left > right
 *
 * RemoveFirst always removes the largest element.
 *
 ****************************************************************************/


template <class T, class Cmp>
class Heap
{
    public:
                        Heap(const Cmp& cmpFunction,
                                int initialNumElements = 32);
                        ~Heap();

                        /**\brief How many elements in the heap? */
        int             NumElements() const;

                        /**\brief Take top/largest element off */
        T               RemoveFirst();

                        /**\brief Remove element from the middle */
        T               RemoveN(int i);

                        /**\brief Put in heap and recompute to restore heap property */
        void            AddElement(const T& elem);

                        /**\brief Put in heap but do NOT recompute to restore heap property */
        void            AddElementDontHeapify(const T& elem);
                        /**\brief Recompute to restore heap property */
        void            Heapify();

                        /**\brief Get reference to top of heap (do not remove) */
        T&              First();

                        /**\brief Get reference arbitrary element (do not remove) */
        T&              Value(int i);

                        /**\brief Get reference element just below top (do not remove) */
        const T&        Second() const;

                        /**\brief Inform heap that I replaced the top: recompute, but
                         * more efficient than Heapify() */
        void            ReplacedFirst();

                        /**\brief Inform heap that element \e i must rise in heap
                         * to restore heap property. More efficient than general
                         * Heapify() */
        void            IncreasedN(int i);

                        /**\brief Inform heap that element \e i must drop in heap
                         * to restore heap property. More efficient than general
                         * Heapify() */
        void            DecreasedN(int i);

                        /**\brief Check heap property */
        void            CheckHeap() const;

                        /**\brief Dump heap */
        void            Print(ostream& out) const;

                        /**\brief Check heap property from given
                         * root to the bottom */
        bool            HeapProperty(int root) const;

    protected:
        int                numElements;
        int                maxNumElements;
        T*                 elements;
        const Cmp&         cmp;

        int                LeftChild(int i) const;
        int                RightChild(int i) const;
        int                RightSibling(int i) const;
        int                Parent(int i) const;

        void                PrintRoot(ostream& out, int rootElem, int indentLevel) const;
        void                CheckHeapRoot(int rootElem) const;
                                // check heap property for entire heap
        bool                HeapProperty(int lower, int upper) const; // check
                                // heap property from lower->upper, inclusive

        void                SiftDown(int rootElem); // fix heap from rootElem
                                // down to bottom of heap
        void                SiftUp(int rootElem); // fix heap from rootElem
                                // to top of heap

        void                GrowElements(int newSize); // accommodate more
};


// Mappings between children and parent array indices

template<class T, class Cmp>
inline int Heap<T, Cmp>::LeftChild(int i) const
{
    return 2 * i + 1;
}


template<class T, class Cmp>
inline int Heap<T, Cmp>::RightChild(int i) const
{
    return 2 * i + 2;
}


template<class T, class Cmp>
inline int Heap<T, Cmp>::RightSibling(int i) const
{
    return i + 1;
}


template<class T, class Cmp>
inline int Heap<T, Cmp>::Parent(int i) const
{
    return (i - 1) / 2;
}


// short inlined functions

template<class T, class Cmp>
inline void Heap<T, Cmp>::CheckHeap() const
{
    w_assert9(HeapProperty(0));
}


template<class T, class Cmp>
inline int Heap<T, Cmp>::NumElements() const
{
    return numElements;
}


template<class T, class Cmp>
inline void Heap<T, Cmp>::Print(ostream& out) const
{
    PrintRoot(out, 0, 0);
}


template<class T, class Cmp>
inline void Heap<T, Cmp>::ReplacedFirst()
{
    SiftDown(0);
}

template<class T, class Cmp>
inline void Heap<T, Cmp>::IncreasedN(int n)
{
    SiftUp(n);
}

template<class T, class Cmp>
inline void Heap<T, Cmp>::DecreasedN(int n)
{
    SiftDown(n);
}


template<class T, class Cmp>
Heap<T, Cmp>::Heap(const Cmp& cmpFunction, int initialNumElements)
:
    numElements(0),
    maxNumElements(initialNumElements),
    elements(0),
    cmp(cmpFunction)
{
    elements = new T[initialNumElements];
}


template<class T, class Cmp>
Heap<T, Cmp>::~Heap()
{
    delete[] elements;
}


// removes and returns the first element.  maintains heap property.
// runs in O(log n).
template<class T, class Cmp>
T Heap<T, Cmp>::RemoveN(int i)
{
    w_assert9(numElements > 0);
    T temp = elements[i];
    bool smaller = cmp.gt(elements[i], elements[--numElements]);
    elements[i] = elements[numElements];
    if(smaller) {
        SiftDown(i);
    } else {
        SiftUp(i);
    }
    return temp;
}

template<class T, class Cmp>
T Heap<T, Cmp>::RemoveFirst()
{
    // return RemoveN(0);
    // This is pretty inefficient in comparisons if only
    // because we know that when removing the top element,
    // we're removing the largest one. So we include
    // an optimized version of RemoveN(i)
    T temp = elements[0];
    elements[0] = elements[--numElements];
    SiftDown(0);
    return temp;
}


// returns the First element in the heap.
// runs in O(1).

template<class T, class Cmp>
T& Heap<T, Cmp>::First()
{
    w_assert9(numElements > 0);
    return elements[0];
}

template<class T, class Cmp>
T& Heap<T, Cmp>::Value(int i)
{
    w_assert9(i < numElements && i >= 0);
    return elements[i];
}

// return the element which would become First if RemoveFirst() were called.
// runs in O(1)

template<class T, class Cmp>
const T& Heap<T, Cmp>::Second() const
{
    w_assert9(numElements > 1);
    int second = LeftChild(0);
    if (RightSibling(second) < numElements)  {
        if (cmp.gt(elements[RightSibling(second)], elements[second]))  {
            second = RightSibling(second);
        }
    }
    return elements[second];
}


// adds a new element to the heap, maintaining the heap property.
// runs in O(log n)

template<class T, class Cmp>
void Heap<T, Cmp>::AddElement(const T& elem)
{
    if (numElements >= maxNumElements)  {
        GrowElements(2 * maxNumElements);
    }

    int rootElem = numElements++; // get a new slot
    while (rootElem > 0)  {
        if (cmp.gt(elem, elements[Parent(rootElem)]))  {
            elements[rootElem] = elements[Parent(rootElem)];
            rootElem = Parent(rootElem);
        } else {
            break;
        }
    }
    elements[rootElem] = elem;
}


// use this to fill the heap initially.
// proceed with a call to Heapify() when all elements are added.

template<class T, class Cmp>
void Heap<T, Cmp>::AddElementDontHeapify(const T& elem)
{
    if (numElements >= maxNumElements)  {
        GrowElements(2 * maxNumElements);
    }
    elements[numElements++] = elem;
}


// recreates the heap condition.
//
// assumes the subtrees of rootElem are heaps and that
// only rootElem violates the heap condition.
//
// starts at the rootElem and stops if there is a valid heap.
// otherwise it sifts the rootElem down into the tree of the
// larger of the children.
//
// this runs in O(log n).

template<class T, class Cmp>
void Heap<T, Cmp>::SiftDown(int rootElem)
{
    /*
     * Assumes: both children are legitimate heaps
     */
    if(LeftChild(rootElem) < numElements) {
        w_assert9(HeapProperty(LeftChild(rootElem)));
    }

    if (numElements > 1)  {
        const T rootValue = elements[rootElem];
        while (rootElem <= Parent(numElements - 1)) {
            w_assert9(LeftChild(rootElem) < numElements);

            // find sibling with larger value
            // Tends to sift down the larger side
            int child = LeftChild(rootElem);
            if (RightSibling(child) < numElements)  {
                if (cmp.gt(elements[RightSibling(child)], elements[child]))  {
                    child = RightSibling(child);
                }
            }

            // If the larger child is larger than the root, swap the root
            // with the larger child.
            if (cmp.gt(elements[child], rootValue))  {
                elements[rootElem] = elements[child];                // need to swap
                rootElem = child;
            }  else  {
                break;                                                        // done
            }
        }
        elements[rootElem] = rootValue;                                // complete swap
    }

    /*
     * Now: legit heap from the root on down
     * Not necessarily for the whole heap, since
     * we might be in the middle/early stages of
     * Heapify-ing a new (unordered) heap.
     */
    w_assert9(HeapProperty(rootElem));
}

template<class T, class Cmp>
void Heap<T, Cmp>::SiftUp(int rootElem)
{
    /*
     * Assumes: legit heap on down to just above rootElem
     */
    if(rootElem > 0) {
        w_assert9(HeapProperty(0, Parent(rootElem)));
    }

    if(rootElem > 0) {
        const T rootValue = elements[rootElem];
        int         i = rootElem;
        while (i > 0) {
            if (cmp.gt(rootValue, elements[Parent(i)]))  {
                // Swap : move down parent
                elements[i] = elements[Parent(i)];
                i = Parent(i);
            } else {
                break;
            }
        }
        elements[i] = rootValue;                                // complete swap
    }
    /*
     * Now: legit down to and including root Elem
     */
    w_assert9(HeapProperty(0, rootElem));
}


// creates a heap out of the unordered elements.
//
// start at bottom of the heap and form heaps out of all the
// elements which have one child, then form heaps out of
// these subheaps until the root of the tree is reached.
//
// this runs in O(n).

template<class T, class Cmp>
void Heap<T, Cmp>::Heapify()
{
    if (NumElements() > 0)  {
        for (int i = Parent(NumElements() - 1); i >= 0; --i)  {
            SiftDown(i);
        }
#if W_DEBUG_LEVEL > 2
        CheckHeap();
#endif
    }
}


// Changes the elements array size.

template<class T, class Cmp>
void Heap<T, Cmp>::GrowElements(int newSize)
{
    w_assert9(newSize >= numElements);

    T* newElements = new T[newSize];
    for (int i = 0; i < numElements; ++i)  {
        newElements[i] = elements[i];
    }
    delete[] elements;
    elements = newElements;
    maxNumElements = newSize;
}


// Verifies the heap condition of the elements array.

template<class T, class Cmp>
void Heap<T, Cmp>::CheckHeapRoot(int rootElem) const
{
    if (rootElem < numElements)  {
        if (LeftChild(rootElem) < numElements)  {
            w_assert1(!cmp.gt(elements[LeftChild(rootElem)],
                elements[rootElem]));
            CheckHeapRoot(LeftChild(rootElem));
        }
        if (RightChild(rootElem) < numElements)  {
            w_assert1(!cmp.gt(elements[RightChild(rootElem)],
                elements[rootElem]));
            CheckHeapRoot(RightChild(rootElem));
        }
    }
}

template<class T, class Cmp>
bool Heap<T, Cmp>::HeapProperty(int root) const
{
    return HeapProperty(root, numElements);
}

// Check heap property from [top, bottom) i.e, NOT INCLUDING bottom
// but including top.

template<class T, class Cmp>
bool Heap<T, Cmp>::HeapProperty(int top, int bottom) const
{
   int last = (bottom > numElements) ? numElements : bottom;

   for(int i= LeftChild(top); i<last; i++) {
        if( cmp.gt( elements[i] , elements[Parent(i)] ) ) return false;
   }
   return true;
}


// Prints out the heap in a rotated tree format.

template<class T, class Cmp>
void Heap<T, Cmp>::PrintRoot(ostream& out, int rootElem, int indentLevel) const
{
    if (rootElem < NumElements())  {
        cout << elements[rootElem] << endl;
        PrintRoot(out, LeftChild(rootElem), indentLevel + 1);
        for (int i = 0; i < indentLevel; i++)  {
            out << ' ';
        }
        PrintRoot(out, RightChild(rootElem), indentLevel + 1);
    }
}

/*<std-footer incl-file-exclusion='W_HEAP_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
