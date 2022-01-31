include_directories(${CMAKE_CURRENT_SOURCE_DIR} "${CMAKE_CURRENT_BINARY_DIR}/..")

set(tokudb_srcs
  ydb
  ydb_cursor
  ydb_db
  ydb_env_func
  ydb_row_lock
  ydb_txn
  ydb_write
  errors
  loader
  indexer
  indexer-undo-do
  toku_patent
  )

## make the static library
add_library(tokudb_static_conv STATIC ${tokudb_srcs})
add_dependencies(tokudb_static_conv install_tdb_h generate_log_code)
set_target_properties(tokudb_static_conv PROPERTIES POSITION_INDEPENDENT_CODE $(STATIC_PIC))
set(tokudb_source_libs tokudb_static_conv locktree_static ft_static util_static)

## add gcov and define _GNU_SOURCE
maybe_add_gcov_to_libraries(tokudb_static_conv)
set_property(TARGET tokudb_static_conv APPEND PROPERTY COMPILE_DEFINITIONS _GNU_SOURCE)

add_subdirectory(kernel_tests)
