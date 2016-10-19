add_library(tpcc ${CMAKE_CURRENT_SOURCE_DIR}/tpcc.cpp)
target_link_libraries(tpcc ${EXPERIMENT_LIBRARIES})

add_library(tpcc_NOSWIZZLING ${CMAKE_CURRENT_SOURCE_DIR}/tpcc.cpp)
target_link_libraries(tpcc_NOSWIZZLING ${EXPERIMENT_LIBRARIES_NOSWIZZLING})

add_library(tpcc_MAINMEMORY ${CMAKE_CURRENT_SOURCE_DIR}/tpcc.cpp)
target_link_libraries(tpcc_MAINMEMORY ${EXPERIMENT_LIBRARIES_MAINMEMORY})

# CS: new plog target (delete to get old behavior)
add_library(tpcc_plog ${CMAKE_CURRENT_SOURCE_DIR}/tpcc.cpp)
target_link_libraries(tpcc_plog ${EXPERIMENT_LIBRARIES_PLOG})

# example doesn't need 3 versions
add_executable(tpcc_example ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_example.cpp)
target_link_libraries(tpcc_example ${EXPERIMENT_LIBRARIES} tpcc)

# load doesn't need 3 versions
add_executable(tpcc_load ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_load.cpp)
target_link_libraries(tpcc_load ${EXPERIMENT_LIBRARIES} tpcc)

set(tpcc_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_full.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_delivery.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_order_status.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_neworder.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_payment.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_stock_level.cpp)

add_executable(tpcc_full ${tpcc_SRCS})
target_link_libraries(tpcc_full ${EXPERIMENT_LIBRARIES} tpcc)

add_executable(tpcc_full_NOSWIZZLING ${tpcc_SRCS})
target_link_libraries(tpcc_full_NOSWIZZLING ${EXPERIMENT_LIBRARIES_NOSWIZZLING} tpcc_NOSWIZZLING)

add_executable(tpcc_full_MAINMEMORY ${tpcc_SRCS})
target_link_libraries(tpcc_full_MAINMEMORY ${EXPERIMENT_LIBRARIES_MAINMEMORY} tpcc_MAINMEMORY)

add_executable(tpcc_full_plog ${tpcc_SRCS})
target_link_libraries(tpcc_full_plog ${EXPERIMENT_LIBRARIES_PLOG} tpcc_plog)

# copy the script files to build folder for easier use.
configure_file(run_full.sh run_full.sh COPYONLY)
configure_file(run_plog.sh run_plog.sh COPYONLY)
configure_file(run_full_w100.sh run_full_w100.sh COPYONLY)
configure_file(restore_backup.sh restore_backup.sh COPYONLY)
configure_file(take_backup.sh take_backup.sh COPYONLY)
configure_file(summarize_results.rb summarize_results.rb COPYONLY)
configure_file(benchmark.rb benchmark.rb COPYONLY)

# tpcc with direct IO
configure_file(run_full_dio.sh run_full_dio.sh COPYONLY)
