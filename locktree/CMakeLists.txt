include_directories(${CMAKE_CURRENT_SOURCE_DIR})

set(locktree_srcs
    locktree
    manager
    lock_request
    txnid_set
    range_buffer
    keyrange
    wfg
    )

## make the shared library for tests
if (NOT BUILD_FOR_LINUX_KERNEL_MODULE)
  add_library(locktree SHARED ${locktree_srcs})
  add_dependencies(locktree install_tdb_h)
  # The locktree uses the standard portability layer and also the ybt 
  # functions from the ft layer. Maybe one day the ybt functions will be
  # elsewhere.
  target_link_libraries(locktree LINK_PRIVATE ft ${LIBTOKUPORTABILITY})
  maybe_add_gcov_to_libraries(locktree)
endif (NOT BUILD_FOR_LINUX_KERNEL_MODULE)

## make the real library, it's going to go into libtokudb.so so it needs
## to be PIC
add_library(locktree_static STATIC ${locktree_srcs})
set_target_properties(locktree_static PROPERTIES POSITION_INDEPENDENT_CODE ${STATIC_PIC})
add_dependencies(locktree_static install_tdb_h)
maybe_add_gcov_to_libraries(locktree_static)

if (NOT BUILD_FOR_LINUX_KERNEL_MODULE)
  add_subdirectory(tests)
else()
  add_subdirectory(kernel_tests)
endif (NOT BUILD_FOR_LINUX_KERNEL_MODULE)
