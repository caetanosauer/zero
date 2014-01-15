#include "w_defines.h"
#include "w.h"
#include "gtest/gtest.h"

#include <iostream>

TEST(ErrcodesTest, All) {
    std::stringstream buf;
    const char *correct =
"Foundation Classes:65536:Internal error\n"
"Foundation Classes:65537:Operating system error \n"
"Foundation Classes:65538:Container is full\n"
"Foundation Classes:65539:Container is empty\n"
"Foundation Classes:65540:Malloc failed : out of memory \n"
"Foundation Classes:65541:Mmap could not map aligned memory\n"
"Foundation Classes:65542:Item not found \n"
"Foundation Classes:65543:Feature is not implemented\n"
"Foundation Classes:65544:unknown error code\n"
"Foundation Classes:65545:Assertion Failed\n"
"Foundation Classes:65546:User-requested rollback\n";
    
    w_error_t::print(buf);
    EXPECT_STREQ(buf.str().c_str(), correct);
}

