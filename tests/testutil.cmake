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
