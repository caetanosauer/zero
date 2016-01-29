#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include "xct.h"

btree_test_env *test_env;

StoreID   stor = 900;

void dump(lockid_t &l)
{
    vout << "\t store()=" << l.store() << endl;
}

TEST (LockidTest, Create){
    const char     *keybuf = "Admiral Richard E. Byrd";
    const char     *keybuf2= "Most of the confusion in the world comes from not knowing how little we need.";

    vout << "Sources: " << endl
        <<  "\t store " << stor << endl
        ;
    {
        lockid_t l(stor, (const unsigned char*) keybuf, strlen(keybuf));
        vout << "Key lock1 " << l << endl;
        vout << "}" << endl;
    }
    {
        lockid_t l(stor, (const unsigned char*) keybuf2, strlen(keybuf2));
        vout << "Key lock2 " << l << endl;
        vout << "}" << endl;
    }
}

TEST (LockidTest, SameKey){
    const char *buf = "Don't think, Feel";
    {
        lockid_t l(stor, (const unsigned char*) buf, ::strlen(buf));
        lockid_t l2(stor, (const unsigned char*) buf, ::strlen(buf));
        EXPECT_EQ(l.hash(), l2.hash());
    }
    {
        StoreID   stor2(10);
        lockid_t l(stor, (const unsigned char*) buf, ::strlen(buf));
        lockid_t l2(stor2, (const unsigned char*) buf, ::strlen(buf));
        EXPECT_NE(l.hash(), l2.hash());
    }
}

TEST (LockidTest, CollisionQualitySequence){
    std::set<uint32_t> observed;
    char buf[8];
    buf [0] = 'k';
    buf [1] = 'e';
    buf [2] = 'y';
    const int COUNT = 100000;
    for (int i = 0; i < COUNT; ++i) {
        buf[3] = '0' + (i / 10000);
        buf[4] = '0' + ((i / 1000) % 10);
        buf[5] = '0' + ((i / 100) % 10);
        buf[6] = '0' + ((i / 10) % 10);
        buf[7] = '0' + ((i / 1) % 10);
        lockid_t l(stor, (const unsigned char*) buf, 8);
        observed.insert(l.hash());
    }
    double collision_rate = (COUNT - observed.size()) / (double) COUNT;
    std::cout << "Seq: distinct hashes " << observed.size()
        << " out of " << COUNT
        << "(collision rate=" << collision_rate * 100.0f << "%)" << endl;
    EXPECT_LT (collision_rate, 0.00005f);
}

TEST (LockidTest, CollisionQualityRandom){
    std::set<uint32_t> observed;
    std::set<uint32_t> used_randoms;
    char buf[10];
    buf [0] = 'k';
    buf [1] = 'e';
    buf [2] = 'y';
    buf [3] = 'z';
    buf [8] = 'b';
    buf [9] = 'c';
    const int COUNT = 100000;

    ::srand(1233); // use fixed seed for repeatability
    for (int i = 0; i < COUNT; ++i) {
        uint32_t val;
        while (true) {
            val = ((uint32_t) ::rand() << 16) + ::rand();
            if (used_randoms.find(val) == used_randoms.end()) {
                used_randoms.insert(val);
                break;
            }
        }
        // *reinterpret_cast<uint32_t*>(buf + 4) = val; this causes Bus Error on solaris, so:
        ::memcpy (buf + 4, &val, sizeof(uint32_t));
        lockid_t l(stor, (const unsigned char*) buf, 10);
        if (observed.find(l.hash()) != observed.end()) {
            std::cout << "collide!" << val << endl;
        }
        observed.insert(l.hash());
    }
    double collision_rate = (COUNT - observed.size()) / (double) COUNT;
    std::cout << "Rnd: distinct hashes " << observed.size()
        << " out of " << COUNT
        << "(collision rate=" << collision_rate * 100.0f << "%)" << endl;
    EXPECT_LT (collision_rate, 0.00005f);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}

