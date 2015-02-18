set(util_srcs
  frwlock
  kibbutz
  mempool
  partitioned_counter
  threadpool
  )

if (NOT BUILD_FOR_LINUX_KERNEL_MODULE)
  add_library(util SHARED ${util_srcs})
  maybe_add_gcov_to_libraries(util)
  target_link_libraries(util LINK_PRIVATE ${LIBTOKUPORTABILITY})
  target_link_libraries(util LINK_PUBLIC ${CMAKE_THREAD_LIBS_INIT} ${EXTRA_SYSTEM_LIBS})
  add_dependencies(util install_tdb_h)
endif (NOT BUILD_FOR_LINUX_KERNEL_MODULE)

add_library(util_static STATIC ${util_srcs})
maybe_add_gcov_to_libraries(util_static)
set_target_properties(util_static PROPERTIES POSITION_INDEPENDENT_CODE ${STATIC_PIC})
add_dependencies(util_static install_tdb_h)

# detect when we are being built as a subproject
if (NOT DEFINED MYSQL_PROJECT_NAME_DOCSTRING)
  install(
    FILES partitioned_counter.h
    DESTINATION include
    )
endif ()

if (NOT BUILD_FOR_LINUX_KERNEL_MODULE)
  add_subdirectory(tests)
else () 
  add_subdirectory(kernel_tests)
endif (NOT BUILD_FOR_LINUX_KERNEL_MODULE)
