FILE(REMOVE_RECURSE
  "CMakeFiles/generate_log_code"
  "log_code.cc"
  "log_print.cc"
  "log_header.h"
  "logformat"
)

# Per-language clean rules from dependency scanning.
FOREACH(lang)
  INCLUDE(CMakeFiles/generate_log_code.dir/cmake_clean_${lang}.cmake OPTIONAL)
ENDFOREACH(lang)
