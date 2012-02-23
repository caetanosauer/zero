#include "btree_test_env.h"
#include "page_s.h"
#include "bf.h"
#include "bf_core.h"
#include "btree.h"
#include "btree_p.h"
#include "btree_impl.h"
#include "page.h"
#include "log.h"
#include "w_error.h"

#include "bf_fixed.h"
#include "bf_tree_cb.h"
#include "bf_tree.h"

#include <vector>

btree_test_env *test_env;
/**
 * Unit test for new bufferpool for B-tree pages (bf_tree_m).
 */

TEST (TreeBufferpoolTest, AlignmentCheck) {
    cout << "sizeof(bf_tree_cb_t)=" << sizeof(bf_tree_cb_t) << endl;
    EXPECT_EQ ((uint)0, sizeof(bf_tree_cb_t) % 8);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
