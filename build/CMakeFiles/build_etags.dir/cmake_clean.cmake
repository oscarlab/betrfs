FILE(REMOVE_RECURSE
  "CMakeFiles/build_etags"
  "../TAGS"
  "etags-stamp"
)

# Per-language clean rules from dependency scanning.
FOREACH(lang)
  INCLUDE(CMakeFiles/build_etags.dir/cmake_clean_${lang}.cmake OPTIONAL)
ENDFOREACH(lang)
