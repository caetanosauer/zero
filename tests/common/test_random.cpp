#include "w_defines.h"

#include "w_stream.h"
#include "rand48.h"
#include "gtest/gtest.h"

rand48 generator;

TEST(RandomTest, All) {
    int i;
    for(i=1; i<25; i++) {
        cout << "i= "<< i << " ";
        cout << generator.rand() ;
        cout <<endl;
    }
    for(i=1; i<25; i++) {
        cout << "i= "<< i << " ";
        W_FORM2(cout, ("%10.10f ",generator.drand()) );
        cout <<endl;
    }
    for(i=1; i<25; i++) {
        cout << "i= "<< i << " ";
        W_FORM2(cout, ("%d (0->25) ",generator.randn(25)) );
        cout <<endl;
    }
    cout << "done." << endl;
}

