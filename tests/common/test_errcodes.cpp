#include "w_defines.h"
#include "w.h"
#include "option.h"
#include "gtest/gtest.h"

#include <iostream>

option_group_t t(2); // causes error codes for options to
    // be included.

#ifdef __GNUC__
typedef w_auto_delete_array_t<char> gcc_kludge_1;
typedef w_list_i<option_t, unsafe_list_dummy_lock_t>             gcc_kludge_0;
#endif /* __GNUC__*/

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
"Foundation Classes:65546:User-requested rollback\n"
"Options Package:131072:Illegal option description line\n"
"Options Package:131073:Illegal option class name\n"
"Options Package:131074:Option class name too long\n"
"Options Package:131075:Too many option class levels\n"
"Options Package:131076:Option name is not unique\n"
"Options Package:131077:Unknown option name\n"
"Options Package:131078:Unknown option class name\n"
"Options Package:131079:Bad syntax in configuration file\n"
"Options Package:131080:Bad option value\n"
"Options Package:131081:A required option was not set\n";
    
    w_error_t::print(buf);
    EXPECT_STREQ(buf.str().c_str(), correct);
}

