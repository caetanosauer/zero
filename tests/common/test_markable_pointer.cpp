#include "w_markable_pointer.h"
#include <AtomicCounter.hpp>
#include "gtest/gtest.h"
#include "local_random.h"

struct Foo {
    Foo() : id(0), released_count(0) {}
    uint32_t id;
    uint32_t released_count;
    MarkablePointer<Foo> next;
};
typedef MarkablePointer<Foo> FooPtr;

void do_test(Foo* pointer, uint64_t int_value, bool real_pointer, uint32_t id) {
    SCOPED_TRACE(testing::Message() << "original=0x" << std::hex << int_value << std::dec
        << ", real?=" << real_pointer);
    uint32_t real_counter = 0;
    if (real_pointer) {
        pointer->released_count = 0;
        pointer->id = id;
    }
    {
        FooPtr ptr (pointer, false);
        EXPECT_FALSE(ptr.is_marked());
        EXPECT_FALSE(ptr.is_null());
        EXPECT_EQ(0, ptr.get_aba_stamp());
        EXPECT_EQ(pointer, ptr.get_pointer());
        if (real_pointer) {
            EXPECT_EQ(id, ptr->id);
            ++ptr->released_count;
            EXPECT_EQ(++real_counter, ptr->released_count);
        }

        ptr.set_mark(true);
        EXPECT_TRUE(ptr.is_marked());
        EXPECT_FALSE(ptr.is_null());
        EXPECT_EQ(0, ptr.get_aba_stamp());
        EXPECT_EQ(pointer, ptr.get_pointer());
        if (real_pointer) {
            EXPECT_EQ(id, ptr->id);
            ++ptr->released_count;
            EXPECT_EQ(++real_counter, ptr->released_count);
        }
    }
    {
        FooPtr ptr (pointer, true);
        EXPECT_TRUE(ptr.is_marked());
        EXPECT_FALSE(ptr.is_null());
        EXPECT_EQ(0, ptr.get_aba_stamp());
        EXPECT_EQ(pointer, ptr.get_pointer());
        if (real_pointer) {
            EXPECT_EQ(id, ptr->id);
            ++ptr->released_count;
            EXPECT_EQ(++real_counter, ptr->released_count);
        }
    }

    {
        FooPtr ptr (pointer, false);
        EXPECT_TRUE(ptr.atomic_cas(pointer, pointer, false, true, 0, 1));
        EXPECT_TRUE(ptr.is_marked());
        EXPECT_FALSE(ptr.is_null());
        EXPECT_EQ(1, ptr.get_aba_stamp());
        EXPECT_EQ(pointer, ptr.get_pointer());
        if (real_pointer) {
            EXPECT_EQ(id, ptr->id);
            ++ptr->released_count;
            EXPECT_EQ(++real_counter, ptr->released_count);
        }

        EXPECT_FALSE(ptr.atomic_cas(pointer, pointer, false, true, 0, 1));
        EXPECT_FALSE(ptr.atomic_cas(pointer, pointer, false, true, 1, 2));
        EXPECT_FALSE(ptr.atomic_cas(pointer, pointer, true, false, 0, 1));
        if (real_pointer) {
            EXPECT_EQ(id, ptr->id);
            ++ptr->released_count;
            EXPECT_EQ(++real_counter, ptr->released_count);
        }

        EXPECT_TRUE(ptr.atomic_cas(pointer, pointer, true, false, 1, 2));
        EXPECT_FALSE(ptr.is_marked());
        EXPECT_FALSE(ptr.is_null());
        EXPECT_EQ(2, ptr.get_aba_stamp());
        EXPECT_EQ(pointer, ptr.get_pointer());
        if (real_pointer) {
            EXPECT_EQ(id, ptr->id);
            ++ptr->released_count;
            EXPECT_EQ(++real_counter, ptr->released_count);
        }

        EXPECT_TRUE(ptr.atomic_cas(pointer, pointer, false, false, 2, 0x3780));
        EXPECT_FALSE(ptr.is_marked());
        EXPECT_FALSE(ptr.is_null());
        EXPECT_EQ(0x3780, ptr.get_aba_stamp());
        EXPECT_EQ(pointer, ptr.get_pointer());
        if (real_pointer) {
            EXPECT_EQ(id, ptr->id);
            ++ptr->released_count;
            EXPECT_EQ(++real_counter, ptr->released_count);
        }

        EXPECT_TRUE(ptr.atomic_cas(pointer, pointer, false, false, 0x3780, 0xBCDE));
        EXPECT_FALSE(ptr.is_marked());
        EXPECT_FALSE(ptr.is_null());
        EXPECT_EQ(0xBCDE, ptr.get_aba_stamp());
        EXPECT_EQ(pointer, ptr.get_pointer());
        if (real_pointer) {
            EXPECT_EQ(id, ptr->id);
            ++ptr->released_count;
            EXPECT_EQ(++real_counter, ptr->released_count);
        }

        EXPECT_TRUE(ptr.atomic_cas(pointer, pointer, false, false, 0xBCDE, 32));
        EXPECT_FALSE(ptr.is_marked());
        EXPECT_FALSE(ptr.is_null());
        EXPECT_EQ(32, ptr.get_aba_stamp());
        EXPECT_EQ(pointer, ptr.get_pointer());
        if (real_pointer) {
            EXPECT_EQ(id, ptr->id);
            ++ptr->released_count;
            EXPECT_EQ(++real_counter, ptr->released_count);
        }
    }
    {
        Foo     foo;
        const uint32_t FOO_ID = 43215;
        foo.id = FOO_ID;
        FooPtr  ptr (pointer, false);
        EXPECT_TRUE(ptr.atomic_cas(pointer, &foo, false, false, 0, 0xE3F1));
        EXPECT_FALSE(ptr.is_marked());
        EXPECT_FALSE(ptr.is_null());
        EXPECT_EQ(0xE3F1, ptr.get_aba_stamp());
        EXPECT_EQ(&foo, ptr.get_pointer());

        EXPECT_EQ(FOO_ID, ptr->id);
        ++ptr->released_count;
        EXPECT_EQ((uint32_t) 1, ptr->released_count);

        EXPECT_FALSE(ptr.atomic_cas(&foo, pointer, false, false, 0, 1));
        EXPECT_EQ(FOO_ID, ptr->id);
        ++ptr->released_count;
        EXPECT_EQ((uint32_t) 2, ptr->released_count);
        EXPECT_EQ(&foo, ptr.get_pointer());

        EXPECT_TRUE(ptr.atomic_cas(&foo, pointer, false, false, 0xE3F1, 1));
        if (real_pointer) {
            EXPECT_EQ(id, ptr->id);
            ++ptr->released_count;
            EXPECT_EQ(++real_counter, ptr->released_count);
        }
        EXPECT_EQ(pointer, ptr.get_pointer());

        EXPECT_TRUE(ptr.atomic_cas(pointer, pointer, false, true, 1, 0xA431));
        EXPECT_TRUE(ptr.is_marked());
        EXPECT_FALSE(ptr.is_null());
        EXPECT_EQ(0xA431, ptr.get_aba_stamp());
        EXPECT_EQ(pointer, ptr.get_pointer());
        if (real_pointer) {
            EXPECT_EQ(id, ptr->id);
            ++ptr->released_count;
            EXPECT_EQ(++real_counter, ptr->released_count);
        }
    }
}
void do_ptr_test(Foo* value, uint32_t id) {
    do_test(value, FooPtr::cast_to_int(value), true, id);
}
void do_int_test(uint64_t value) {
    do_test(FooPtr::cast_to_ptr(static_cast<uintptr_t>(value)), value, false, 0);
}

TEST(MarkablePointerTest, Null) {
    FooPtr ptr;
    EXPECT_FALSE(ptr.is_marked());
    EXPECT_TRUE(ptr.is_null());
    EXPECT_EQ(0, ptr.get_aba_stamp());

    EXPECT_TRUE(ptr.atomic_cas(NULL, NULL, false, true, 0, 1));
    EXPECT_TRUE(ptr.is_marked());
    EXPECT_TRUE(ptr.is_null());
    EXPECT_EQ(1, ptr.get_aba_stamp());
}

TEST(MarkablePointerTest, Int) {
    tlr_t random(123);
    for (int i = 0; i < 1000; ++i) {
        uint64_t val = (static_cast<uint64_t>(random.nextInt32()) << 32) | random.nextInt32();
        // First 16 bits and last 3 bits are never used as pointer to a class.
        val &= 0x0000FFFFFFFFFFF8LL;
        do_int_test(val);
    }
}

TEST(MarkablePointerTest, Pointer) {
    const size_t COUNT = 1000;
    Foo* pointers[COUNT];
    for (size_t i = 0; i < COUNT; ++i) {
        pointers[i] = new Foo();
        do_ptr_test(pointers[i], i);
    }
    for (size_t i = 0; i < COUNT; ++i) {
        delete pointers[i];
    }
}

TEST(MarkablePointerTest, Array) {
    const size_t COUNT = 1000;
    Foo* pointers = new Foo[COUNT];
    for (size_t i = 0; i < COUNT; ++i) {
        do_ptr_test(pointers + i, i);
    }
    delete[] pointers;
}
