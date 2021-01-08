include_directories(${CMAKE_CURRENT_SOURCE_DIR})

set(tokuportability_srcs
  huge_page_detection
  file
  os_malloc
  portability
  toku_crash
  toku_path
  toku_pthread
  toku_time
  )

if (NOT BUILD_FOR_LINUX_KERNEL_MODULE)
  add_library(${LIBTOKUPORTABILITY} SHARED ${tokuportability_srcs})
  target_link_libraries(${LIBTOKUPORTABILITY} LINK_PRIVATE jemalloc)
  target_link_libraries(${LIBTOKUPORTABILITY} LINK_PUBLIC ${CMAKE_THREAD_LIBS_INIT} ${EXTRA_SYSTEM_LIBS})
  maybe_add_gcov_to_libraries(${LIBTOKUPORTABILITY})
  set_property(TARGET ${LIBTOKUPORTABILITY} APPEND PROPERTY COMPILE_DEFINITIONS _GNU_SOURCE)
endif (NOT BUILD_FOR_LINUX_KERNEL_MODULE)

add_library(tokuportability_static_conv STATIC ${tokuportability_srcs})
set_target_properties(tokuportability_static_conv PROPERTIES POSITION_INDEPENDENT_CODE ${STATIC_PIC})

add_dependencies(tokuportability_static_conv build_jemalloc)
set(tokuportability_source_libs tokuportability_static_conv jemalloc ${CMAKE_THREAD_LIBS_INIT} ${EXTRA_SYSTEM_LIBS})
toku_merge_static_libs(${LIBTOKUPORTABILITY}_static ${LIBTOKUPORTABILITY}_static "${tokuportability_source_libs}")

maybe_add_gcov_to_libraries(tokuportability_static_conv)
set_property(TARGET tokuportability_static_conv APPEND PROPERTY COMPILE_DEFINITIONS _GNU_SOURCE)

set_property(SOURCE file memory os_malloc portability toku_rwlock APPEND PROPERTY
  COMPILE_DEFINITIONS TOKU_ALLOW_DEPRECATED=1)

# detect when we are being built as a subproject
if (NOT DEFINED MYSQL_PROJECT_NAME_DOCSTRING)
  install(
    FILES toku_os_types.h toku_time.h
    DESTINATION include
    )
  if (BUILD_FOR_LINUX_KERNEL_MODULE)
    install(
      TARGETS ${LIBTOKUPORTABILITY}_static
      DESTINATION lib
      )
  else ()
    install(
      TARGETS ${LIBTOKUPORTABILITY} ${LIBTOKUPORTABILITY}_static
      DESTINATION lib
      )
  endif (BUILD_FOR_LINUX_KERNEL_MODULE)
endif ()

if (NOT BUILD_FOR_LINUX_KERNEL_MODULE)
  add_subdirectory(tests)
else ()
  add_subdirectory(kernel_tests)
endif (NOT BUILD_FOR_LINUX_KERNEL_MODULE)
