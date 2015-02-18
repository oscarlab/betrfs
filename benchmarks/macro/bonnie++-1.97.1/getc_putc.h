#ifndef GETC_PUTC_H
#define GETC_PUTC_H

#define FILE_FD 253

#define TEST_FUNC(XACTION, XNAME, XCODE, XRES) \
  if(!quiet) fprintf(stderr, "done\n%s with %s...", XACTION, XNAME); \
  fflush(NULL); \
  dur.reset(); \
  dur.start(); \
  for(i = 0; i < size; i++) \
  { \
    XCODE \
    { \
      fprintf(stderr, "Can't %s!\n", XNAME); \
      return 1; \
    } \
    c++; \
  } \
  XRES = dur.stop();

#define TEST_FUNC_WRITE(XNAME, XCODE, XRES) \
  TEST_FUNC("Writing", XNAME, c = 0x20 + (i & 0x3f); XCODE, XRES)

#define TEST_FUNC_READ(XNAME, XCODE, XRES) \
  TEST_FUNC("Reading", XNAME, XCODE, XRES)


#endif
