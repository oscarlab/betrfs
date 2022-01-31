The steps below are used to disable page sharing


1. Replace `ON` with `OFF` in the line below in cmake_modules/TokuSetupCompiler.cmake 

option(PAGE_SHARING "Enable Page Sharing." ON)

2. comment the line below in filesystem/Makefile

KBUILD_CFLAGS += -DFT_INDIRECT

3. comment the line below in ftfs/Makefile

KBUILD_CFLAGS += -DFT_INDIRECT


