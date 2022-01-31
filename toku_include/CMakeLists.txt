configure_file(toku_config.h.in toku_config.h)
add_custom_target(generate_config_h DEPENDS
  "${CMAKE_CURRENT_BINARY_DIR}/toku_config.h")
