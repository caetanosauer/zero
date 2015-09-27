#ifndef LINTEL_ATOMIC_COUNTER_HPP
#define LINTEL_ATOMIC_COUNTER_HPP

/** @file
    \brief Header file for lintel::Atomic class
*/

//See the following for explanations why things are done the way they are
//http://www.hpl.hp.com/personal/Hans_Boehm/c++mm/threadsintro.html
//http://www.justsoftwaresolutions.co.uk/threading/intel-memory-ordering-and-c++-memory-model.html
//http://gcc.gnu.org/bugzilla/show_bug.cgi?id=17884
//
//asms could be more explictit - like xaddb xaddw xaddl xaddq, but gasm
//uses correct one automatically, since it knows argument size. If your asm doesn't, use %z0
//asm volatile + memory clobber is the only available compiler barrier
//without it, gcc generates asm which is closer to __sync_add_and_fetch() builtin, but there
//is nothing we can do since there is no other compiler barrier.

// Choose exactly one of
//#define LINTEL_USE_STD_ATOMICS 1
//#define LINTEL_USE_GCC_BUILTIN_SYNC_ATOMICS 1
#define LINTEL_USE_GCC_ASM_ATOMICS 1

#if (LINTEL_USE_STD_ATOMICS + LINTEL_USE_GCC_BUILTIN_SYNC_ATOMICS + LINTEL_USE_GCC_ASM_ATOMICS != 1)
#    error Choose exactly one of the above
#endif

#if defined(__GNUC__) && (__GNUC__ * 100 + __GNUC_MINOR__ >= 406) // need at least gcc 4.6
#   define LINTEL_HAS_STD_ATOMICS 1
#endif

#if defined(__GNUC__) && (__GNUC__ * 100 + __GNUC_MINOR__ < 407) // need at least gcc 4.7
#   if defined(LINTEL_USE_STD_ATOMICS)
#      error gcc-4.6 + std::atomic = bug (http://gcc.gnu.org/ml/gcc-bugs/2012-10/msg00158.html)
#   endif
#endif

#if defined(__GNUC__) && (__GNUC__ * 100 + __GNUC_MINOR__ >= 401) // need at least gcc 4.1
#    if defined(__i386__) // Pure __i386__ does not have a __sync_add_and_fetch that can return a value.
#        if defined(__i486__) || defined(__i586__) || defined(__i686__)
#            define LINTEL_HAS_GCC_BUILTIN_SYNC_ATOMICS 1
#            define LINTEL_HAS_GCC_ASM_ATOMICS 1
#        endif
#    elif defined(__x86_64) || defined(__x86_64__) //(__i386__ is not defined on x86_64) 
#        define LINTEL_HAS_GCC_BUILTIN_SYNC_ATOMICS 1
#        define LINTEL_HAS_GCC_ASM_ATOMICS 1
#    else // Assume all non i386 archs have it
#        define LINTEL_HAS_GCC_BUILTIN_SYNC_ATOMICS 1
#    endif
#else
#    if defined(LINTEL_USE_GCC_BUILTIN_SYNC_ATOMICS)
#        error detected platform does not have __sync_* primitives
#    endif 
#endif

#if defined(LINTEL_USE_GCC_BUILTIN_SYNC_ATOMICS) && not defined(LINTEL_HAS_GCC_BUILTIN_SYNC_ATOMICS) 
    #error detected platform does not have __sync builtins
#endif

#if defined(LINTEL_USE_STD_ATOMICS) && not defined(LINTEL_HAS_STD_ATOMICS) 
    #error detected platform does not have std::atomic<>
#endif

#if defined(LINTEL_USE_GCC_ASM_ATOMICS) && not defined(LINTEL_HAS_GCC_ASM_ATOMICS) 
    #error detected platform does not have asm atomics
#endif

#if defined (LINTEL_USE_STD_ATOMICS)
#    include <atomic>
#endif

namespace lintel {

#if defined (LINTEL_USE_STD_ATOMICS)
#    define LINTEL_ATOMIC_FETCH(op, var, amount)  ::std::atomic_fetch_##op(var, amount)
#    define LINTEL_ATOMIC_LOAD(ptr) ::std::atomic_load(ptr)
#    define LINTEL_ATOMIC_STORE(ptr, val) ::std::atomic_store(ptr, val)
#    define LINTEL_ATOMIC_EXCHANGE(ptr, val) ::std::atomic_exchange(ptr, val)
#    define LINTEL_COMPARE_EXCHANGE(current, expected, desired) ::std::atomic_compare_exchange_strong(current, expected, desired)
#    define LINTEL_ATOMIC_THREAD_FENCE(order) ::std::atomic_thread_fence(order)
#elif defined (LINTEL_USE_GCC_BUILTIN_SYNC_ATOMICS)
#    define LINTEL_ATOMIC_FETCH(op, var, amount)  ::__sync_fetch_and_##op(var, amount)
//unnesessary preceding barrier in gcc 4.6 std::atomic<>, but not here. fixed in gcc 4.7
#    define LINTEL_ATOMIC_LOAD(ptr) ({__typeof(*(ptr)) t=*(ptr); ::__sync_synchronize(); t;})
//BUG in gcc 4.6 std::atomic<> (missing preceding barrier), but not here. fixed in gcc 4.7
#    define LINTEL_ATOMIC_STORE(ptr, val) {::__sync_synchronize(); *(ptr)=val; ::__sync_synchronize();}
#    define LINTEL_ATOMIC_EXCHANGE(ptr, val) ::__sync_lock_test_and_set(ptr, val)
#    define LINTEL_COMPARE_EXCHANGE(current, expected, desired) ({__typeof(*(expected)) val=*(expected); val==(*(expected)=::__sync_val_compare_and_swap(current, val, desired));})
#    define LINTEL_ATOMIC_THREAD_FENCE(order) {::__sync_synchronize();(void)order;}
#elif defined (LINTEL_USE_GCC_ASM_ATOMICS)
#    define LINTEL_ATOMIC_FETCH(op, var, amount)  lintel::x86Gcc_atomic_fetch_##op(var, amount)
#    define LINTEL_ATOMIC_LOAD(ptr) lintel::x86Gcc_atomic_load(ptr)
#    define LINTEL_ATOMIC_STORE(ptr, val) lintel::x86Gcc_atomic_store(ptr, val)
#    define LINTEL_ATOMIC_EXCHANGE(ptr, val) lintel::x86Gcc_atomic_exchange(ptr, val)
#    define LINTEL_COMPARE_EXCHANGE(current, expected, desired) lintel::x86Gcc_compare_exchange(current, expected, desired)
#    define LINTEL_ATOMIC_THREAD_FENCE(order) lintel::x86Gcc_atomic_thread_fence(order)
#endif

#if defined (LINTEL_USE_STD_ATOMICS)
  using ::std::memory_order;
  using ::std::memory_order_relaxed;
  using ::std::memory_order_consume;
  using ::std::memory_order_acquire;
  using ::std::memory_order_release;
  using ::std::memory_order_acq_rel;
  using ::std::memory_order_seq_cst;
#else 
  /// Enumeration for memory_order
  typedef enum memory_order {
    memory_order_relaxed, memory_order_consume, memory_order_acquire,
    memory_order_release, memory_order_acq_rel, memory_order_seq_cst
  } memory_order;
#endif

#if defined(LINTEL_USE_GCC_ASM_ATOMICS)

    template<typename T>
    static inline T x86Gcc_atomic_load(const T *counter) {
      T v;
      asm volatile ("mov %1, %0": "=r" (v) : "m" (*counter) );
      asm volatile ("":::"memory"); //compiler barrier
      return v;
    }
    template<typename T>
    static inline void x86Gcc_atomic_store(T *counter, T v) {
      asm volatile ("xchg %1, %0": "+m" (*counter), "+r" (v)::"memory");
    }
    template<typename T>
    static inline T x86Gcc_atomic_exchange(T *counter, T v) {
      asm volatile ("xchg %1, %0": "+m" (*counter), "+r" (v)::"memory");
      return v;
    }
    template<typename T>
    static inline bool x86Gcc_compare_exchange(T* current, T* expected, T desired) {
      bool result;
      asm volatile ("lock; cmpxchg %3,%0\n\t"
	   "setz %2\n\t"
	   : "+m" (*current), "+a" (*expected), "=rm"(result)
	   : "r" (desired)
	   : "memory");
      return result;
    }

    template<typename T>
    static inline T x86Gcc_atomic_fetch_add(T *counter, T v) {
      asm volatile ("lock xadd %1, %0": "+m" (*counter), "+r" (v)::"memory");
      return v;
    }
    template<typename T>
    static inline T x86Gcc_atomic_fetch_sub(T *counter, T v) {
      return x86Gcc_atomic_fetch_add<T>(counter, -v);
    }
    template<typename T>
    static inline T x86Gcc_atomic_fetch_or(T *counter, T v) {
        T expected = LINTEL_ATOMIC_LOAD(counter);
	T desired;
	do {
	    desired = expected | v;
	} while (!x86Gcc_compare_exchange(counter, &expected, desired));
	return expected;
    }
    template<typename T>
    static inline T x86Gcc_atomic_fetch_and(T *counter, T v) {
        T expected = LINTEL_ATOMIC_LOAD(counter);
	T desired;
	do {
	    desired = expected & v;
	} while (!x86Gcc_compare_exchange(counter, &expected, desired));
	return expected;
    }
    template<typename T>
    static inline T x86Gcc_atomic_fetch_xor(T *counter, T v) {
        T expected = LINTEL_ATOMIC_LOAD(counter);
	T desired;
	do {
	    desired = expected ^ v;
	} while (!x86Gcc_compare_exchange(counter, &expected, desired));
	return expected;
    }

    static inline void x86Gcc_atomic_thread_fence(memory_order order)
    {
        // Unlike gcc we don't issue fences unnesessary for Atomic<>.
        // If you issue nontemporal SSE asms yourself, you have to
        // issue your own fences yourself as well.
        switch(order) {
	case memory_order_acquire:
	case memory_order_consume:
	    // gcc 4.6 issues lfence, which is never needed for current Atomic
	    asm volatile ("#lfence":::"memory");
	    break;
	case memory_order_release:
	    // gcc 4.6 issues sfence, which is never needed for current Atomic
	    asm volatile ("#sfence":::"memory");
	    break;
	case memory_order_acq_rel:
	    // gcc 4.6 issues mfence, which is not needed for current Atomic
	    asm volatile ("#mfence":::"memory");
	case memory_order_seq_cst:
	    // what gcc does. StoreLoad barrier.
	    asm volatile ("mfence":::"memory");
	    //asm volatile ("lock; orl $0, (%%rsp)":::"memory"); // faster on x86 <= Westmere
	    break;
        case memory_order_relaxed:
	    break; // do nothing
      }
    }

#endif

    inline void atomic_thread_fence(memory_order order)
    { LINTEL_ATOMIC_THREAD_FENCE(order); }

    inline void atomic_signal_fence(memory_order)
    { asm volatile ("":::"memory"); }

    /// \brief An atomic counter that avoids using locks.
    ///
    /// Encapsulates an atomic counter.  Be wary when using it so as
    /// to not inadvertently introduce a race--the value may change at
    /// any time outside of the defined operations.
    ///
    /// If you really need the current value, then fetchThenAdd(0).
    /// Beware that the returned value may immediatly become out of
    /// date!
    template<class T>
    class Atomic {
    public:
        Atomic() { counter = 0; } /*=default*/ // Should be initialized, 
						// for good measure especially
						// on older compilers

        explicit Atomic(T counter) : counter(counter) { }

        /// Increments the counter and then returns the value
        /// C11/C++11 have no such function. Use prefix operator++() instead
        T incThenFetch() { return ++*this; }

	/// Decrements the counter and then returns the value
        /// C11/C++11 have no such function. Use prefix operator--() instead
	T decThenFetch() { return --*this; }

        /// Adds amount to the counter and then returns the value
        /// C11/C++11 have no such function. Use operator += instead
        T addThenFetch(T amount) { return *this+=amount; }

        /// Returns true if the counter is zero
        /// C11/C++11 have no such function. Use operator T() instead
        bool isZero() const { return !load(); }

        operator T() const { return load(); }

        T load() const {
	    return LINTEL_ATOMIC_LOAD(&counter);
        }
        void store(T t) {
	    LINTEL_ATOMIC_STORE(&counter, t);
        }

        /// Assignement
        T operator=(T amount) { store(amount); return amount; }

        T exchange(T t) {
	    return LINTEL_ATOMIC_EXCHANGE(&counter, t);
        }

        bool compare_exchange_strong(T * expected, T desired)
        { return LINTEL_COMPARE_EXCHANGE(&counter, expected, desired); }

        T fetch_add(T amount) { return LINTEL_ATOMIC_FETCH(add, &counter, amount); }
        T fetch_sub(T amount) { return LINTEL_ATOMIC_FETCH(sub, &counter, amount); }
        T fetch_or (T amount) { return LINTEL_ATOMIC_FETCH(or , &counter, amount); }
        T fetch_and(T amount) { return LINTEL_ATOMIC_FETCH(and, &counter, amount); }
        T fetch_xor(T amount) { return LINTEL_ATOMIC_FETCH(xor, &counter, amount); }

        T operator +=(T amount) { return fetch_add(amount) + amount; }
        T operator -=(T amount) { return fetch_sub(amount) - amount; }
        T operator |=(T amount) { return fetch_or (amount) | amount; }
        T operator &=(T amount) { return fetch_and(amount) & amount; }
        T operator ^=(T amount) { return fetch_xor(amount) ^ amount; }

        T operator++(int) { return this->fetch_add(1);  } //suffix
        T operator--(int) { return this->fetch_sub(1);  }
        T operator++(   ) { return this->fetch_add(1)+1;} //prefix
        T operator--(   ) { return this->fetch_sub(1)-1;}

    private:
        /// Copy construction is forbidden.
        Atomic(const Atomic&); //=delete

        /// Copy assignment is forbidden.
        Atomic& operator=(const Atomic&); // = delete;

#if defined (LINTEL_USE_STD_ATOMICS)
        std::atomic<T> counter;
#elif defined (LINTEL_USE_GCC_BUILTIN_SYNC_ATOMICS) || defined (LINTEL_USE_GCC_ASM_ATOMICS)
        T counter; // gcc will always align it because it ignores packed attribute on non-POD fields
#endif
    };

    typedef Atomic<int> AtomicCounter; //for backward compatibility. Remove when no longer needed.

#if defined(__GNUC__) && (__GNUC__ * 100 + __GNUC_MINOR__ > 404) 
// Below pragmas trigger ICE for GCC 4.4 and below;
#pragma GCC push_options
#pragma GCC optimize ("no-strict-aliasing")
#endif

    namespace unsafe {
        // These are not "Type-Based Alias Analysis" (TBAA) safe. Undefined behaviour. use -fno-strict-aliasing
        // to somewhat constrain compiler, but it is still unsafe. for example
        // struct S { short x; int a; } __attribute__((packed)) s;
        // assert (__alignof__(s.a) != 4);
        // lintel::unsafe::atomic_load(&s.a); //FAIL, not atomic!
 
        template<typename T> T atomic_load(const T* object)
	{ return reinterpret_cast<const Atomic<T>*>(object)->load(); }

        template<typename T, typename C> void atomic_store(T* object, C desired)
	{ reinterpret_cast<Atomic<T>*>(object)->store(static_cast<T>(desired)); }

        template<typename T, typename C> T atomic_exchange(T* object, C desired)
	{ return reinterpret_cast<Atomic<T>*>(object)->exchange(static_cast<T>(desired)); }

        template<typename T, typename C> bool atomic_compare_exchange_strong(T* object, T* expected, C desired)
	{ return reinterpret_cast<Atomic<T>*>(object)->compare_exchange_strong(expected, static_cast<T>(desired)); }

        template<typename T, typename C> T atomic_fetch_add(T* object, C operand)
	{ return reinterpret_cast<Atomic<T>*>(object)->fetch_add(static_cast<T>(operand)); }
        template<typename T, typename C> T atomic_fetch_sub(T* object, C operand)
	{ return reinterpret_cast<Atomic<T>*>(object)->fetch_sub(static_cast<T>(operand)); }
        template<typename T, typename C> T atomic_fetch_or(T* object, C operand)
	{ return reinterpret_cast<Atomic<T>*>(object)->fetch_or(static_cast<T>(operand)); }
        template<typename T, typename C> T atomic_fetch_and(T* object, C operand)
	{ return reinterpret_cast<Atomic<T>*>(object)->fetch_and(static_cast<T>(operand)); }
        template<typename T, typename C> T atomic_fetch_xor(T* object, C operand)
	{ return reinterpret_cast<Atomic<T>*>(object)->fetch_xor(static_cast<T>(operand)); }
    }

#if defined(__GNUC__) && (__GNUC__ * 100 + __GNUC_MINOR__ > 404) 
#pragma GCC pop_options
#endif
} // namespace lintel

#endif
