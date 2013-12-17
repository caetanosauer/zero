include(../testutil.cmake)

set (the_libraries gtest_main)

ENABLE_TESTING()

X_ADD_TESTCASE(test_fancylatches "${the_libraries}")

