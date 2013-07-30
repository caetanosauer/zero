# CMake function to help build testcases.
# Include this file from the CMakeLists.txt
FUNCTION (X_ADD_GTEST targetname)
    ADD_TEST(${targetname} ${targetname} --gtest_output=xml:test-reports/result_${targetname}.xml)
ENDFUNCTION()

# To pick up these xml files from Jenkins,
# Set "**/test-reports/*.xml" as the report file filter.

# CMake function to add compile target, linking, ADD_TEST directives.
FUNCTION (X_ADD_TESTCASE targetname the_libraries)
    add_executable(${targetname} ${CMAKE_CURRENT_SOURCE_DIR}/${targetname}.cpp)
    target_link_libraries(${targetname} ${the_libraries})
    X_ADD_GTEST(${targetname})
ENDFUNCTION()

# CMake function to add both swizzled and non-swizzled targets, linking, ADD_TEST directives.
FUNCTION (X_ADD_SM_TESTCASE targetname the_libraries special_compile_def)
    add_executable(${targetname} ${CMAKE_CURRENT_SOURCE_DIR}/${targetname}.cpp)
    target_link_libraries(${targetname} ${the_libraries})
    X_ADD_GTEST(${targetname})
    set_property(TARGET ${targetname}  PROPERTY COMPILE_DEFINITIONS ${special_compile_def})

    add_executable(${targetname}_${special_compile_def} ${CMAKE_CURRENT_SOURCE_DIR}/${targetname}.cpp)
    target_link_libraries(${targetname}_${special_compile_def} ${the_libraries})
    X_ADD_GTEST(${targetname}_${special_compile_def})
ENDFUNCTION()

