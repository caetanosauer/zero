#include "w_defines.h"
#include "w_heap.h"
#include "w.h"
#include <cstring>
#include "../nullbuf.h"
#include "gtest/gtest.h"

#if W_DEBUG_LEVEL > 3
#define vout std::cout
// std::ostream vout (&std::cout);
#else // W_DEBUG_LEVEL
nullbuf null_obj;
std::ostream vout (&null_obj);
#endif // W_DEBUG_LEVEL

template<class T> class CmpLessFunction
{
    public:
        bool                gt(const T& x, const T& y) const;
        bool                cmp(const T& x, const T& y) const;
};


template<class T> class CmpGreaterFunction
{
    public:
        bool                gt(const T& x, const T& y) const;
        bool                cmp(const T& x, const T& y) const;
};


template<class T>
inline bool CmpLessFunction<T>::gt(const T& x, const T& y) const
{
    return x < y;
}
template<class T>
inline bool CmpLessFunction<T>::cmp(const T& x, const T& y) const
{
    return (x < y) ? 1 : (x==y)? 0 : -1;
}


template<class T>
inline bool CmpGreaterFunction<T>::gt(const T& x, const T& y) const
{
    return x > y;
}
template<class T>
inline bool CmpGreaterFunction<T>::cmp(const T& x, const T& y) const
{
    return (x > y) ? 1 : (x==y)? 0 : -1;
}



#define BY_SMALLEST_HEAP_ORDER
#ifdef BY_SMALLEST_HEAP_ORDER
typedef CmpLessFunction<char> CmpFunction;
#else
typedef CmpGreaterFunction<char> CmpFunction;
#endif

typedef Heap<char, CmpFunction> CharHeap;

#ifdef EXPLICIT_TEMPLATE
template class CmpLessFunction<char>;
template class CmpGreaterFunction<char>;
template class Heap<char, CmpLessFunction<char> >;
template class Heap<char, CmpGreaterFunction<char> >;
#endif

void ReplaceAllHeadsWithLargest(CharHeap& heap)
{
    char second = 0;
    for (int i = 0; i < heap.NumElements(); ++i)  {
        if (i > 1)  {
            second = heap.Second();
        }
        heap.First() = '|';
        heap.ReplacedFirst();
        if (i > 1)  {
            EXPECT_EQ(second, heap.First());
        }
        heap.CheckHeap();
        vout << "\n\n-------------------\n\n";
        heap.Print(vout);
    }
}


void ReplaceAllHeadsWithSmallest(CharHeap& heap)
{
    char second = 0;
    for (int i = 0; i < heap.NumElements(); ++i)  {
        if (i > 1)  {
            second = heap.Second();
        }
        heap.First() = '!';
        heap.ReplacedFirst();
        if (i > 1)  {
            EXPECT_EQ(second, heap.First());
        }
        heap.CheckHeap();
        vout << "\n\n-------------------\n\n";
        heap.Print(vout);
    }
}


TEST(HeapTest, All) {
    char sentence[] = "the quick brown fox jumped over the lazy dogs.";
    int maxArraySize = strlen(sentence);
    char *array;

    CmpFunction cmp;

    // test building, second and replace for a variable number of elements
    for (int arraySize = 0; arraySize < maxArraySize; ++arraySize)  {
        CharHeap heap(cmp);

        vout << "\n\n ========== TEST # " << arraySize << " =========== \n\n";
        array = new char[arraySize + 1];
        strncpy(array, sentence, arraySize);
        array[arraySize] = 0;
        for (int i = 0; i < arraySize; i++)  {
            if (array[i] == ' ')  {
                array[i] = '_';
            }
            heap.AddElementDontHeapify(array[i]);
        }
        vout << "\n\n ========== TEST # P1." << arraySize << " =========== \n\n";
        heap.Print(vout);
        heap.Heapify();
        vout << "\n\n ========== TEST # P2." << arraySize << " =========== \n\n";
        heap.Print(vout);
#ifdef BY_SMALLEST_HEAP_ORDER
        ReplaceAllHeadsWithLargest(heap);
#else
        ReplaceAllHeadsWithSmallest(heap);
#endif
        vout << "\n\n ========== TEST # P3." << arraySize << " =========== \n\n";
        heap.Print(vout);
        delete[] array;
    }

    // test building, second, and replace when all are equal
    {
        CharHeap heap(cmp);
        char array2[] = "AAAAAAAAAAAAAAAAAAAAAAA";
        for (unsigned int i = 0; i < strlen(array2); ++i)  {
            heap.AddElementDontHeapify(array2[i]);
        }
        vout << "\n\n ========== TEST # P4. =========== \n\n";
        heap.Print(vout);
        heap.Heapify();
        heap.CheckHeap();
        vout << "\n\n ========== TEST # P5. =========== \n\n";
        heap.Print(vout);
#ifdef BY_SMALLEST_HEAP_ORDER
        ReplaceAllHeadsWithLargest(heap);
#else
        ReplaceAllHeadsWithSmallest(heap);
#endif
    }

    // test removing from a heap
    {
        int i;
        CharHeap heap(cmp);
        for (i = 0; i < maxArraySize; ++i)  {
            heap.AddElementDontHeapify(sentence[i]);
        }
        heap.Heapify();
        vout << "\n\n*******************\n\n";
        for (i = 0; i < maxArraySize; ++i)  {
            vout << heap.RemoveFirst();
        }
        vout << endl;
        w_assert1(heap.NumElements() == 0);

        for (i = 0; i < maxArraySize; ++i)  {
            heap.AddElement(sentence[i]);
            heap.CheckHeap();
        }
        vout << "\n\n*******************\n\n";
        for (i = 0; i < maxArraySize; ++i)  {
            vout << heap.RemoveFirst();
        }
        vout << endl;
    }
}

