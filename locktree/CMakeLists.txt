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

## make the real library, it's going to go into libtokudb.so so it needs
## to be PIC
add_library(locktree_static STATIC ${locktree_srcs})
set_target_properties(locktree_static PROPERTIES POSITION_INDEPENDENT_CODE ${STATIC_PIC})
add_dependencies(locktree_static install_tdb_h)
maybe_add_gcov_to_libraries(locktree_static)

add_subdirectory(kernel_tests)
