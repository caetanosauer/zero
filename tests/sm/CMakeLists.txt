include(../testutil.cmake)

ENABLE_TESTING()

add_library(btree_test_env STATIC ${CMAKE_CURRENT_SOURCE_DIR}/btree_test_env.cpp)
target_link_libraries(btree_test_env gtest common fc sthread sm)

#################################################
#
# To add special definitions for features to test,
# edit btree_test_env.h and add #definition
# to the #ifdef ALTERNATIVE_FEATURES (around line 22)
#
#################################################

X_ADD_TESTCASE(test_alloc btree_test_env ALTERNATIVE_FEATURES)
X_ADD_TESTCASE(test_bf_tree btree_test_env ALTERNATIVE_FEATURES)
X_ADD_TESTCASE(test_btree_create btree_test_env ALTERNATIVE_FEATURES)
X_ADD_TESTCASE(test_btree_cursor btree_test_env ALTERNATIVE_FEATURES)
X_ADD_TESTCASE(test_btree_basic btree_test_env ALTERNATIVE_FEATURES)
X_ADD_TESTCASE(test_btree_ghost btree_test_env ALTERNATIVE_FEATURES)
X_ADD_TESTCASE(test_btree_keytrunc btree_test_env ALTERNATIVE_FEATURES)
X_ADD_TESTCASE(test_btree_merge btree_test_env ALTERNATIVE_FEATURES)
X_ADD_TESTCASE(test_btree_rollback btree_test_env ALTERNATIVE_FEATURES)
X_ADD_TESTCASE(test_btree_verify btree_test_env ALTERNATIVE_FEATURES)
X_ADD_TESTCASE(test_btree_p btree_test_env ALTERNATIVE_FEATURES)
X_ADD_TESTCASE(test_chain_xct btree_test_env ALTERNATIVE_FEATURES)
X_ADD_TESTCASE(test_checksum btree_test_env ALTERNATIVE_FEATURES)
X_ADD_TESTCASE(test_crash btree_test_env ALTERNATIVE_FEATURES)
X_ADD_TESTCASE(test_deadlock btree_test_env ALTERNATIVE_FEATURES)
X_ADD_TESTCASE(test_elr btree_test_env ALTERNATIVE_FEATURES)
X_ADD_TESTCASE(test_intent_lock btree_test_env ALTERNATIVE_FEATURES)
X_ADD_TESTCASE(test_lockid btree_test_env ALTERNATIVE_FEATURES)
X_ADD_TESTCASE(test_lock_cache btree_test_env ALTERNATIVE_FEATURES)
X_ADD_TESTCASE(test_sys_xct btree_test_env ALTERNATIVE_FEATURES)
X_ADD_TESTCASE(test_insert_many btree_test_env ALTERNATIVE_FEATURES)
X_ADD_TESTCASE(test_btree_insert_100K btree_test_env ALTERNATIVE_FEATURES)
X_ADD_TESTCASE(test_btree_durability btree_test_env ALTERNATIVE_FEATURES)
