FILE(REMOVE_RECURSE
  "CMakeFiles/build_ctags"
  "../tags"
  "ctags-stamp"
)

# Per-language clean rules from dependency scanning.
FOREACH(lang)
  INCLUDE(CMakeFiles/build_ctags.dir/cmake_clean_${lang}.cmake OPTIONAL)
ENDFOREACH(lang)
