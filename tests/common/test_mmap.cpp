#include "w_defines.h"
#include "w.h"
#include "sthread.h"
#include "gtest/gtest.h"

#define num 7
#define MB 1024*1024
const size_t sizes[num] =  
{
    24*MB,
    48*MB,
    11*MB,
    1*MB,
        70*MB,
        1,
    25
};


// Returns NULL in case of error
char * trymap(
    char *buf,
    size_t size
) 
{
#ifdef HAVE_HUGETLBFS
    w_rc_t    e;
    e = sthread_t::set_hugetlbfs_path(HUGETLBFS_PATH);
    if (e.is_error()) W_COERCE(e);
#endif
    w_rc_t rc = sthread_t::set_bufsize(size, buf, false);
    EXPECT_FALSE(rc.is_error()) << "Test failed with size " << size  << " : rc= "  << rc;
    if (rc.is_error()) {
        return NULL;
    }
    return buf;
}

void test_write(char *b, size_t s)
{
     // Make sure we can write each address
     // Since it's SM_PAGESIZE-aligned, we should be able
     // to write as integers
     long sz = s;
     long numpages = sz/SM_PAGESIZE;
     int intsperpage = int(SM_PAGESIZE/sizeof(int));
     for (int p=0; p< numpages; p++)
     {
         // to catch some bug:
         EXPECT_EQ(numpages, long(s)/SM_PAGESIZE);
         EXPECT_LT(p, int(s/SM_PAGESIZE));
         for (int i=0; i< intsperpage; i++)
         {
            int  off = p*SM_PAGESIZE + i*sizeof(int);
            int *target = (int *)(b+off);
            *target = 0;
         }
     }
     cout << "test_write size= " << s << " success" << endl;
}

int test(size_t input_size)
{

    cout << "input size " << int(input_size) << endl;
    char *b1 = trymap(0, input_size);
    if(b1) { 
        // success
        test_write(b1, input_size);
        int err = sthread_t::do_unmap();
        EXPECT_EQ(err, 0);
        return 0;
    }
    else
    {
      // failure
      return 1;
    }
}



TEST(MMapTest, All) {
    for(int i=0; i < num; i++)
    {
       cout << "-------------------------------------" << sizes[i] << endl;
       EXPECT_EQ(test(sizes[i]), 0);
    }
}

