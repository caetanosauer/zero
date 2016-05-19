include(../testutil.cmake)

ENABLE_TESTING()

add_library(btree_test_env STATIC ${CMAKE_CURRENT_SOURCE_DIR}/btree_test_env.cpp)
target_link_libraries(btree_test_env gtest common sm)

# CS: think of better solution -- see comment at the bottom (TODO)
# add_library(btree_test_env_plog STATIC ${CMAKE_CURRENT_SOURCE_DIR}/btree_test_env.cpp)
# target_link_libraries(btree_test_env_plog gtest common fc sthread sm_plog)

add_library(logfactory STATIC ${CMAKE_CURRENT_SOURCE_DIR}/logfactory.cpp ${CMAKE_CURRENT_SOURCE_DIR}/stats.cpp)
target_link_libraries(logfactory btree_test_env)

#################################################
# To add alternate configurations to be tested,
# edit both ../testutil.cmake and also btree_test_env.h
#################################################

X_ADD_TESTCASE(test_alloc btree_test_env)
X_ADD_TESTCASE(test_bf_tree btree_test_env)
#X_ADD_TESTCASE(test_fix_with_Q btree_test_env)   # should fail for now until have QSX latch integrated
X_ADD_TESTCASE(test_btree_create btree_test_env)
X_ADD_TESTCASE(test_btree_cursor btree_test_env)
X_ADD_TESTCASE(test_btree_basic btree_test_env)
X_ADD_TESTCASE(test_btree_ghost btree_test_env)
X_ADD_TESTCASE(test_btree_keytrunc btree_test_env)
# X_ADD_TESTCASE(test_btree_merge btree_test_env)
X_ADD_TESTCASE(test_btree_rollback btree_test_env)
X_ADD_TESTCASE(test_btree_verify btree_test_env)
X_ADD_TESTCASE(test_btree_page_h btree_test_env)
X_ADD_TESTCASE(test_chain_xct btree_test_env)
X_ADD_TESTCASE(test_checksum btree_test_env)
X_ADD_TESTCASE(test_crash btree_test_env)                       # Serial and traditional recovery test suite
X_ADD_TESTCASE(test_restart btree_test_env)                     # Serial and traditional recovery test suite
                                                                # normally disabled so the code does not get compiled or executed during functional test run
X_ADD_TESTCASE(test_deadlock btree_test_env)
X_ADD_TESTCASE(test_emlsn btree_test_env)
X_ADD_TESTCASE(test_elr btree_test_env)
X_ADD_TESTCASE(test_intent_lock btree_test_env)
X_ADD_TESTCASE(test_lockid btree_test_env)
X_ADD_TESTCASE(test_lock_cache btree_test_env)
X_ADD_TESTCASE(test_page_lsn_chain btree_test_env)
X_ADD_TESTCASE(test_spr btree_test_env)
X_ADD_TESTCASE(test_lock_okvl btree_test_env)
X_ADD_TESTCASE(test_lock_raw btree_test_env)
X_ADD_TESTCASE(test_log_lsn_tracker btree_test_env)
X_ADD_TESTCASE(test_sys_xct btree_test_env)
X_ADD_TESTCASE(test_insert_many btree_test_env)
X_ADD_TESTCASE(test_btree_insert_100K btree_test_env)

X_ADD_TESTCASE(test_logarchiver logfactory)
X_ADD_TESTCASE(test_logfactory logfactory)
X_ADD_TESTCASE(test_checkpoint btree_test_env)
X_ADD_TESTCASE(test_cleaner btree_test_env)
X_ADD_TESTCASE(test_mem_mgmt btree_test_env)
X_ADD_TESTCASE(test_ringbuffer btree_test_env)
X_ADD_TESTCASE(test_restore btree_test_env)

SET(cmd_LIBS zapps_base loginspect kits restore sm)

X_ADD_TESTCASE(stress_carray sm)
X_ADD_TESTCASE(stress_cleaner "${cmd_LIBS}")
X_ADD_TESTCASE(stress_btree "${cmd_LIBS}")

# CS: this makes sm_plog be compiled even if EXCLUDE_FROM_ALL is set (TODO fix it)
# X_ADD_TESTCASE(test_plog btree_test_env_plog)
