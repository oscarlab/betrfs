set(tokuportability_kernel_srcs
test-gettime
test-gettimeofday
test-filesystem-sizes
test-stat
test-active-cpus
test-cpu-freq
test-cpu-freq-openlimit17
test-pthread-rwlock-rwr
test-pthread-rwlock-rdlock
test-fair-rwlock
test-flock
test-toku-malloc
test-snprintf
test-memory-status
test-fsync-directory
test-fsync
test-pwrite4g
)

if (NOT BUILD_FOR_LINUX_KERNEL_MODULE)
  list(APPEND tokuportability_kernel_srcs)
endif (NOT BUILD_FOR_LINUX_KERNEL_MODULE)

add_library(tokuportability_kernel_static STATIC ${tokuportability_kernel_srcs})
maybe_add_gcov_to_libraries(tokuportability_kernel_static)
set_target_properties(tokuportability_kernel_static PROPERTIES POSITION_INDEPENDENT_CODE ${STATIC_PIC})
add_dependencies(tokuportability_kernel_static install_tdb_h generate_config_h)
