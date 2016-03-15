set(kits_SRCS
    # BASIC TABLE INFRASTRUCTURE
    ${CMAKE_CURRENT_SOURCE_DIR}/field.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/index_desc.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/row.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/table_man.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/table_desc.cpp

    # KITS SM INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/shore_env.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/shore_client.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/reqs.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/daemons.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/skewer.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/thread.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/trx_worker.cpp

    # UTILITIES
    ${CMAKE_CURRENT_SOURCE_DIR}/mersenne.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/trace.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/stl_pool.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/random_input.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/confparser.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/shore_worker.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/sort.cpp

    # TPC-B
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcb/tpcb_client.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcb/tpcb_env.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcb/tpcb_input.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcb/tpcb_schema.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcb/tpcb_schema_man.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcb/tpcb_xct.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcb/templates.cpp

    # TPC-C
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcc/tpcc_client.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcc/tpcc_env.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcc/tpcc_input.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcc/tpcc_schema.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcc/tpcc_schema_man.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcc/tpcc_xct.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcc/tpcc_random.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcc/templates.cpp

    # KITS ZAPPS INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/kits_cmd.cpp
    )


# copy shore.conf file to /src/
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/shore.conf
        ${CMAKE_CURRENT_BINARY_DIR}/../shore.conf COPYONLY)

add_subdirectory(experiments)

add_library (kits ${kits_SRCS})

target_link_libraries(kits experiments)
