add_library(tpcc ${CMAKE_CURRENT_SOURCE_DIR}/tpcc.cpp)
target_link_libraries(tpcc ${EXPERIMENT_LIBRARIES})

add_executable(tpcc_example ${CMAKE_CURRENT_SOURCE_DIR}/tpcc_example.cpp)
target_link_libraries(tpcc_example ${EXPERIMENT_LIBRARIES} tpcc)

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
