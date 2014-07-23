include(../testutil.cmake)

ENABLE_TESTING()

add_library(btree_test_env STATIC ${CMAKE_CURRENT_SOURCE_DIR}/btree_test_env.cpp)
target_link_libraries(btree_test_env gtest common fc sthread sm)

#################################################
# To add alternate configurations to be tested,
# edit both ../testutil.cmake and also btree_test_env.h
#################################################

X_ADD_TESTCASE(test_alloc btree_test_env)
X_ADD_TESTCASE(test_backup btree_test_env)
X_ADD_TESTCASE(test_bf_tree btree_test_env)
#X_ADD_TESTCASE(test_fix_with_Q btree_test_env)   # should fail for now until have QSX latch integrated
X_ADD_TESTCASE(test_btree_create btree_test_env)
X_ADD_TESTCASE(test_btree_cursor btree_test_env)
X_ADD_TESTCASE(test_btree_basic btree_test_env)
X_ADD_TESTCASE(test_btree_ghost btree_test_env)
X_ADD_TESTCASE(test_btree_keytrunc btree_test_env)
X_ADD_TESTCASE(test_btree_merge btree_test_env)
X_ADD_TESTCASE(test_btree_rollback btree_test_env)
X_ADD_TESTCASE(test_btree_verify btree_test_env)
X_ADD_TESTCASE(test_btree_page_h btree_test_env)
X_ADD_TESTCASE(test_chain_xct btree_test_env)
X_ADD_TESTCASE(test_checksum btree_test_env)
X_ADD_TESTCASE(test_crash btree_test_env)               # Serial and traditional recovery test suite
X_ADD_TESTCASE(test_restart btree_test_env)             # Serial and traditional recovery test suite
X_ADD_TESTCASE(test_restart_bugs btree_test_env)	# Serial and traditional recovery test suite containing tests failing because of bugs
#X_ADD_TESTCASE(test_concurrent_restart btree_test_env)  # Concurrent recovery test suite
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
X_ADD_TESTCASE(test_btree_durability btree_test_env)



