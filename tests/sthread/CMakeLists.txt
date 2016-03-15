include(../testutil.cmake)

set (the_libraries common gtest_main)

ENABLE_TESTING()

X_ADD_TESTCASE(test_errlog "${the_libraries}")
X_ADD_TESTCASE(test_mmap "${the_libraries}")
X_ADD_TESTCASE(test_pthread "${the_libraries}")
X_ADD_TESTCASE(test_thread1 "${the_libraries}")
X_ADD_TESTCASE(test_thread2 "${the_libraries}")
X_ADD_TESTCASE(test_thread3 "${the_libraries}")
X_ADD_TESTCASE(test_thread4 "${the_libraries}")
