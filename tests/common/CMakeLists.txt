include(../testutil.cmake)

set (the_libraries common fc sthread atomic_ops gtest_main)

ENABLE_TESTING()

X_ADD_TESTCASE(test_bitmap "${the_libraries}")
X_ADD_TESTCASE(test_errcodes "${the_libraries}")
X_ADD_TESTCASE(test_kvl "${the_libraries}")
X_ADD_TESTCASE(test_latch "${the_libraries}")
X_ADD_TESTCASE(test_lsns "${the_libraries}")
X_ADD_TESTCASE(test_opaque "${the_libraries}")
X_ADD_TESTCASE(test_snumt "${the_libraries}")
X_ADD_TESTCASE(test_vectors "${the_libraries}")
