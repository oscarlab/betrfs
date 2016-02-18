FILE(REMOVE_RECURSE
  "CMakeFiles/build_lzma"
  "CMakeFiles/build_lzma-complete"
  "xz/src/build_lzma-stamp/build_lzma-install"
  "xz/src/build_lzma-stamp/build_lzma-mkdir"
  "xz/src/build_lzma-stamp/build_lzma-download"
  "xz/src/build_lzma-stamp/build_lzma-update"
  "xz/src/build_lzma-stamp/build_lzma-patch"
  "xz/src/build_lzma-stamp/build_lzma-configure"
  "xz/src/build_lzma-stamp/build_lzma-build"
  "xz/src/build_lzma-stamp/build_lzma-reclone_src"
)

# Per-language clean rules from dependency scanning.
FOREACH(lang)
  INCLUDE(CMakeFiles/build_lzma.dir/cmake_clean_${lang}.cmake OPTIONAL)
ENDFOREACH(lang)
