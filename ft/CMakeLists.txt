include_directories(${CMAKE_CURRENT_SOURCE_DIR})

## generate log_code.cc, log_print.cc, log_header.cc
set_source_files_properties(
  "${CMAKE_CURRENT_BINARY_DIR}/log_code"
  "${CMAKE_CURRENT_BINARY_DIR}/log_print"
  "${CMAKE_CURRENT_BINARY_DIR}/log_header.h"
  PROPERTIES GENERATED TRUE)

add_custom_command(
  OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/logformat"
  COMMAND "${CMAKE_CXX_COMPILER}" -Wall "${CMAKE_CURRENT_SOURCE_DIR}/logformat.cc" -o "${CMAKE_CURRENT_BINARY_DIR}/logformat"
  DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/logformat.cc"
  )

add_custom_command(
  OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/log_code.cc"
  OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/log_print.cc"
  OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/log_header.h"
  COMMAND "${CMAKE_CURRENT_BINARY_DIR}/logformat" .
  DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/logformat"
  )
add_custom_target(
  generate_log_code
  DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/log_code.cc" "${CMAKE_CURRENT_BINARY_DIR}/log_print.cc" "${CMAKE_CURRENT_BINARY_DIR}/log_header.h"
  )

set(FT_SOURCES
  background_job_manager
  block_allocator
  block_table
  bndata
  cachetable
  checkpoint
  compress
  dbufio
  fifo
  ft
  ft-cachetable-wrappers
  ft-flusher
  ft-hot-flusher
  ftloader-callback
  ft_msg
  ft_node-serialize
  ft-indirect
  ft-node-deserialize
  ft-ops
  ft-serialize
  ft-test-helpers
  ft-verify
  key
  leafentry
  le-cursor
  logcursor
  logfilemgr
  logger
  log_upgrade
  memarena
  minicron
  omt
  pqueue
  queue
  quicklz
  recover
  rollback
  rollback-apply
  rollback-ct-callbacks
  rollback_log_node_cache
  roll
  sub_block
  txn
  txn_child_manager
  txn_manager
  ule
  x1764
  xids
  ft-slice
  ybt
  "${CMAKE_CURRENT_BINARY_DIR}/log_code"
  "${CMAKE_CURRENT_BINARY_DIR}/log_print"
  )

add_library(ft_static STATIC ${FT_SOURCES})
## we're going to link this into libtokudb.so so it needs to have PIC
set_target_properties(ft_static PROPERTIES POSITION_INDEPENDENT_CODE ${STATIC_PIC})
maybe_add_gcov_to_libraries(ft_static)

## depend on other generated targets
add_dependencies(ft_static install_tdb_h generate_log_code)

## link dependers with zlib
target_link_libraries(ft_static LINK_PRIVATE)

add_subdirectory(kernel_tests)
