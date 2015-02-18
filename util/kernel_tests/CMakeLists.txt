set(util_kernel_srcs
marked-omt-test
omt-tmpl-test
sort-tmpl-test
test-frwlock-fair-writers
test-rwlock-unfair-writers
test-rwlock-cheapness
test-rwlock
test-kibbutz
threadpool-testrunf
threadpool-test
test_doubly_linked_list
test_circular_buffer
test_partitioned_counter_5833
test_partitioned_counter
)

add_library(util_kernel_static STATIC ${util_kernel_srcs})
maybe_add_gcov_to_libraries(util_static)
set_target_properties(util_kernel_static PROPERTIES POSITION_INDEPENDENT_CODE ${STATIC_PIC})
add_dependencies(util_kernel_static install_tdb_h)

