include(../testutil.cmake)

set (the_libraries fc atomic_ops gtest_main)

ENABLE_TESTING()

X_ADD_TESTCASE(test_bitvector "${the_libraries}")
X_ADD_TESTCASE(test_container "${the_libraries}")
X_ADD_TESTCASE(test_cuckoo "${the_libraries}")
X_ADD_TESTCASE(test_darray "${the_libraries}")
X_ADD_TESTCASE(test_endian "${the_libraries}")
X_ADD_TESTCASE(test_hash "${the_libraries}")
X_ADD_TESTCASE(test_heap "${the_libraries}")
X_ADD_TESTCASE(test_key_t gtest_main)
X_ADD_TESTCASE(test_list "${the_libraries}")
X_ADD_TESTCASE(test_memblock "${the_libraries}")
X_ADD_TESTCASE(test_ntime "${the_libraries}")
X_ADD_TESTCASE(test_rand "${the_libraries}")
X_ADD_TESTCASE(test_random "${the_libraries}")
X_ADD_TESTCASE(test_rc "${the_libraries}")
X_ADD_TESTCASE(test_ss "${the_libraries}")

# generate stat header files with tools/stats.pl
INCLUDE_DIRECTORIES (${CMAKE_CURRENT_BINARY_DIR}) 
set(TEST_STAT_GENFILES_FILES
  test_stat_collect_enum_gen.h
  test_stat_dec_gen.cpp
  test_stat_msg_gen.h
  test_stat_struct_gen.h
  test_stat_collect_gen.cpp
  test_stat_inc_gen.cpp
  test_stat_out_gen.cpp)
foreach(_file ${TEST_STAT_GENFILES_FILES})
    add_custom_command(OUTPUT ${_file}
        COMMAND perl ${CMAKE_SOURCE_DIR}/tools/stats.pl ${CMAKE_CURRENT_SOURCE_DIR}/test_stat.dat
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/test_stat.dat
    )
endforeach()
add_custom_target(TEST_STAT_GENFILES DEPENDS ${TEST_STAT_GENFILES_FILES})

X_ADD_TESTCASE(test_stat "${the_libraries}")
add_dependencies(test_stat TEST_STAT_GENFILES)
