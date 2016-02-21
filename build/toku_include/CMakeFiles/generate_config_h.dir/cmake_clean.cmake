FILE(REMOVE_RECURSE
  "CMakeFiles/generate_config_h"
)

# Per-language clean rules from dependency scanning.
FOREACH(lang)
  INCLUDE(CMakeFiles/generate_config_h.dir/cmake_clean_${lang}.cmake OPTIONAL)
ENDFOREACH(lang)
