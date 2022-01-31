set(util_srcs
  kibbutz
  mempool
  partitioned_counter
  threadpool
  )

if (NOT BUILD_FOR_LINUX_KERNEL_MODULE)
  # Use frwlock_user is compiled for user space
  list(APPEND util_srcs frwlock_user)
  add_library(util SHARED ${util_srcs})
  maybe_add_gcov_to_libraries(util)
  target_link_libraries(util LINK_PRIVATE ${LIBTOKUPORTABILITY})
  target_link_libraries(util LINK_PUBLIC ${CMAKE_THREAD_LIBS_INIT} ${EXTRA_SYSTEM_LIBS})
  add_dependencies(util install_tdb_h)
else ()
  list(APPEND util_srcs frwlock)
endif (NOT BUILD_FOR_LINUX_KERNEL_MODULE)

add_library(util_static STATIC ${util_srcs})
maybe_add_gcov_to_libraries(util_static)
set_target_properties(util_static PROPERTIES POSITION_INDEPENDENT_CODE ${STATIC_PIC})
add_dependencies(util_static install_tdb_h)

add_subdirectory(kernel_tests)
