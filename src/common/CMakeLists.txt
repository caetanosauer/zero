
set(common_STAT_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/basics.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bitmap.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/devid_t.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/latch.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lid_t.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/kvl_t.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lsn.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/option.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/regerror.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/regcomp.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/regexec.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/regfree.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/regex_posix.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/stid_t.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tid_t.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/vec_mkchunk.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/vec_t.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/vid_t.cpp)

add_library(common STATIC ${common_STAT_SRCS})
#target_link_libraries (common atomic_ops)

set (OPT_ERROR_GENFILES_H opt_errmsg_gen.h opt_einfo_gen.h opt_error_def_gen.h opt_einfo_bakw_gen.h)
add_custom_command(OUTPUT ${OPT_ERROR_GENFILES_H}
  COMMAND perl ${CMAKE_SOURCE_DIR}/tools/errors.pl -d ${CMAKE_CURRENT_SOURCE_DIR}/opt_error.dat ${COPY_GENERATED_FILES}
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/opt_error.dat
  )
    set (OPT_ERROR_GENFILES_B_H ${OPT_ERROR_GENFILES_B_H} ${CMAKE_CURRENT_BINARY_DIR}/${_file})
add_custom_target(OPT_ERROR_GENFILES DEPENDS ${OPT_ERROR_GENFILES_H})
add_dependencies(common OPT_ERROR_GENFILES)


set (GENFILES ${OPT_ERROR_GENFILES_B_H})
########### install files ###############

install(FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/basics.h
  ${CMAKE_CURRENT_SOURCE_DIR}/bitmap.h
  ${CMAKE_CURRENT_SOURCE_DIR}/devid_t.h
  ${CMAKE_CURRENT_SOURCE_DIR}/lsn.h
  ${CMAKE_CURRENT_SOURCE_DIR}/hostname.h
  ${CMAKE_CURRENT_SOURCE_DIR}/kvl_t.h
  ${CMAKE_CURRENT_SOURCE_DIR}/latch.h
  ${CMAKE_CURRENT_SOURCE_DIR}/lid_t.h
  ${CMAKE_CURRENT_SOURCE_DIR}/option.h
  ${CMAKE_CURRENT_SOURCE_DIR}/regcomp_i.h
  ${CMAKE_CURRENT_SOURCE_DIR}/regerror_i.h
  ${CMAKE_CURRENT_SOURCE_DIR}/regex.h
  ${CMAKE_CURRENT_SOURCE_DIR}/regex2.h
  ${CMAKE_CURRENT_SOURCE_DIR}/regex_cclass.h
  ${CMAKE_CURRENT_SOURCE_DIR}/regex_cname.h
  ${CMAKE_CURRENT_SOURCE_DIR}/regex_engine_i.h
  ${CMAKE_CURRENT_SOURCE_DIR}/regex_posix.h
  ${CMAKE_CURRENT_SOURCE_DIR}/regex_utils.h
  ${CMAKE_CURRENT_SOURCE_DIR}/sm_vas.h
  ${CMAKE_CURRENT_SOURCE_DIR}/stid_t.h
  ${CMAKE_CURRENT_SOURCE_DIR}/tid_t.h
  ${CMAKE_CURRENT_SOURCE_DIR}/umemcmp.h
  ${CMAKE_CURRENT_SOURCE_DIR}/vec_t.h
  ${CMAKE_CURRENT_SOURCE_DIR}/vid_t.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_opaque.h
  DESTINATION include)

INSTALL_GENERATED_H(${GENFILES})
