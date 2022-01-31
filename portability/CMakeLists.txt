include_directories(${CMAKE_CURRENT_SOURCE_DIR})

set(tokuportability_srcs
  file
  portability
  toku_path
  toku_pthread
  toku_time
  )

if (NOT BUILD_FOR_LINUX_KERNEL_MODULE)
  ## The source code does not have main function
  ## It is not supposed to compiled into an executable
  list(APPEND tokuportability_srcs)
  add_library(${LIBTOKUPORTABILITY} SHARED ${tokuportability_srcs})
  target_link_libraries(${LIBTOKUPORTABILITY} LINK_PUBLIC ${CMAKE_THREAD_LIBS_INIT} ${EXTRA_SYSTEM_LIBS})
  maybe_add_gcov_to_libraries(${LIBTOKUPORTABILITY})
  set_property(TARGET ${LIBTOKUPORTABILITY} APPEND PROPERTY COMPILE_DEFINITIONS _GNU_SOURCE)
  add_dependencies(${LIBTOKUPORTABILITY} install_tdb_h generate_config_h)
endif (NOT BUILD_FOR_LINUX_KERNEL_MODULE)

add_library(tokuportability_static_conv STATIC ${tokuportability_srcs})
set_target_properties(tokuportability_static_conv PROPERTIES POSITION_INDEPENDENT_CODE ${STATIC_PIC})
add_dependencies(tokuportability_static_conv install_tdb_h generate_config_h)

set(tokuportability_source_libs tokuportability_static_conv ${CMAKE_THREAD_LIBS_INIT} ${EXTRA_SYSTEM_LIBS})

maybe_add_gcov_to_libraries(tokuportability_static_conv)
set_property(TARGET tokuportability_static_conv APPEND PROPERTY COMPILE_DEFINITIONS _GNU_SOURCE)

set_property(SOURCE file memory portability toku_rwlock APPEND PROPERTY
  COMPILE_DEFINITIONS TOKU_ALLOW_DEPRECATED=1)

add_subdirectory(kernel_tests)
