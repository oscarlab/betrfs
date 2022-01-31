set_directory_properties(PROPERTIES INCLUDE_DIRECTORIES "")

add_compile_options(-mcmodel=small)
add_compile_options(-fpie)

file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/runcat.sh" "#!/bin/bash
out=$1; shift
exec \"$@\" >$out")

add_executable(make_tdb make_tdb)
set_property(TARGET make_tdb APPEND PROPERTY COMPILE_DEFINITIONS _GNU_SOURCE)
add_custom_command(
  OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/db.h"
  COMMAND bash runcat.sh "${CMAKE_CURRENT_BINARY_DIR}/db.h" $<TARGET_FILE:make_tdb>
  DEPENDS make_tdb)
add_custom_target(install_tdb_h DEPENDS
  "${CMAKE_CURRENT_BINARY_DIR}/db.h")
