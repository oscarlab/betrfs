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
#test-flock
test-hugepage
test-toku-malloc
test-snprintf
test-memory-status
test-fsync-directory
test-fsync
test-pwrite4g
)

add_library(tokuportability_kernel_static STATIC ${tokuportability_kernel_srcs})
maybe_add_gcov_to_libraries(tokuportability_static_conv)
set_target_properties(tokuportability_kernel_static PROPERTIES POSITION_INDEPENDENT_CODE ${STATIC_PIC})

