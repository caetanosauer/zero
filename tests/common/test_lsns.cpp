#include "w_defines.h"

#include <cstddef>
#include "w.h"
#include "lsn.h"
#include <stdint.h>
#include "gtest/gtest.h"

TEST (LsnTest, All) {
    for(int i=-2; i < 4; i++)
    {
        cout  << endl;
        for(int j=-2; j<4; j++)
        {
            lsn_t a(i,j);    

            EXPECT_EQ (a.valid(), i != 0);

            uint32_t correct_file = i >= 0 ? i : (1 << 16) + i;
            EXPECT_EQ (a.file(), correct_file);
            
            uint64_t correct_rba = j >= 0 ? j : (1L << 48L) + j;
            EXPECT_EQ (a.rba(), (int64_t) correct_rba);

            uint64_t correct_mask = (1L << 48L) - 1L;
            EXPECT_EQ (a.mask(), correct_mask);
        }
    }
}
