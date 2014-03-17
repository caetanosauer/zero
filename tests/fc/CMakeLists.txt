INCLUDE_DIRECTORIES (
  ${CMAKE_CURRENT_BINARY_DIR}
)

include(../testutil.cmake)

set (the_libraries fc gtest_main)

ENABLE_TESTING()

X_ADD_TESTCASE(test_bitvector "${the_libraries}")
X_ADD_TESTCASE(test_container "${the_libraries}")
X_ADD_TESTCASE(test_cuckoo "${the_libraries}")
X_ADD_TESTCASE(test_darray "${the_libraries}")
X_ADD_TESTCASE(test_endian "${the_libraries}")
set (all_test_libraries common fc sthread gtest_main)
X_ADD_TESTCASE(test_gc_pool_forest "${all_test_libraries}")
X_ADD_TESTCASE(test_hash "${the_libraries}")
X_ADD_TESTCASE(test_heap "${the_libraries}")
X_ADD_TESTCASE(test_key_t gtest_main)
X_ADD_TESTCASE(test_list "${the_libraries}")
X_ADD_TESTCASE(test_lockfree_list "${all_test_libraries}")
# X_ADD_TESTCASE(test_lockfree_queue "${all_test_libraries}")
X_ADD_TESTCASE(test_markable_pointer "${the_libraries}")
X_ADD_TESTCASE(test_memblock "${the_libraries}") #FIXME fails on ubuntu 12 due to limitations of gtest with expected crashes in MT environment
X_ADD_TESTCASE(test_ntime "${the_libraries}")
X_ADD_TESTCASE(test_rand "${the_libraries}")
X_ADD_TESTCASE(test_random "${the_libraries}")
X_ADD_TESTCASE(test_rc "${the_libraries}")
X_ADD_TESTCASE(test_ss "${the_libraries}")
X_ADD_TESTCASE(test_w_okvl "${the_libraries}")

INCLUDE_DIRECTORIES (${CMAKE_CURRENT_BINARY_DIR}) 
set(TEST_STAT_GENFILES_FILES
  test_stat_collect_enum_gen.h
  test_stat_dec_gen.cpp
  test_stat_msg_gen.h
  test_stat_struct_gen.h
  test_stat_collect_gen.cpp
  test_stat_inc_gen.cpp
  test_stat_out_gen.cpp)
add_custom_command(OUTPUT ${TEST_STAT_GENFILES_FILES}
  COMMAND perl ${CMAKE_SOURCE_DIR}/tools/stats.pl ${CMAKE_CURRENT_SOURCE_DIR}/test_stat.dat ${COPY_GENERATED_FILES}
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/test_stat.dat
)
add_custom_target(TEST_STAT_GENFILES DEPENDS ${TEST_STAT_GENFILES_FILES})

X_ADD_TESTCASE(test_stat "${the_libraries}")
add_dependencies(test_stat TEST_STAT_GENFILES)
