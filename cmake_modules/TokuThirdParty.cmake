include(ExternalProject)

if (NOT DEFINED BUILD_FOR_LINUX_KERNEL_MODULE)

  ## add jemalloc with an external project
  set(JEMALLOC_SOURCE_DIR "${TokuDB_SOURCE_DIR}/third_party/jemalloc" CACHE FILEPATH "Where to find jemalloc sources.")
  if (NOT EXISTS "${JEMALLOC_SOURCE_DIR}/configure")
    message(FATAL_ERROR "Can't find jemalloc sources.  Please check them out to ${JEMALLOC_SOURCE_DIR} or modify JEMALLOC_SOURCE_DIR.")
  endif ()
  set(jemalloc_configure_opts "CC=${CMAKE_C_COMPILER}" "--with-jemalloc-prefix=" "--with-private-namespace=tokudb_jemalloc_internal_" "--enable-cc-silence")
  option(JEMALLOC_DEBUG "Build jemalloc with --enable-debug." OFF)
  if (JEMALLOC_DEBUG)
    list(APPEND jemalloc_configure_opts --enable-debug)
  endif ()
  ExternalProject_Add(build_jemalloc
    PREFIX jemalloc
    SOURCE_DIR "${JEMALLOC_SOURCE_DIR}"
    CONFIGURE_COMMAND
    "${JEMALLOC_SOURCE_DIR}/configure" ${jemalloc_configure_opts}
    "--prefix=${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/jemalloc"
    )

  add_library(jemalloc STATIC IMPORTED GLOBAL)
  set_target_properties(jemalloc PROPERTIES IMPORTED_LOCATION
    "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/jemalloc/lib/libjemalloc_pic.a")
  add_dependencies(jemalloc build_jemalloc)
  add_library(jemalloc_nopic STATIC IMPORTED GLOBAL)
  set_target_properties(jemalloc_nopic PROPERTIES IMPORTED_LOCATION
    "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/jemalloc/lib/libjemalloc.a")
  add_dependencies(jemalloc_nopic build_jemalloc)

  # detect when we are being built as a subproject
  if (NOT DEFINED MYSQL_PROJECT_NAME_DOCSTRING)
    install(
      DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/jemalloc/lib"
      DESTINATION .
      )
  endif ()

endif()

if (CMAKE_GENERATOR STREQUAL Ninja)
  ## ninja doesn't understand "$(MAKE)"
  set(SUBMAKE_COMMAND make)
else ()
  ## use "$(MAKE)" for submakes so they can use the jobserver, doesn't
  ## seem to break Xcode...
  set(SUBMAKE_COMMAND $(MAKE))
endif ()
