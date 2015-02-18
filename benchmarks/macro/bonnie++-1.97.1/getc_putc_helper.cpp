#include "bonnie.h"

#include <unistd.h>
#include <stdlib.h>
#include <cstring>
#include "duration.h"
#include <vector>
#include "getc_putc.h"

int main(int argc, char *argv[])
{
  if(argc != 2)
  {
    fprintf(stderr, "Error - don't run this yourself, run getc_putc!\n");
    return 1;
  }

  int size = atoi(argv[1]);
  bool quiet = false;

  if(argv[1][strlen(argv[1]) - 1] == 'q')
    quiet = true;

  volatile char c;
  int i;
  Duration dur;
  double res[2];

  FILE *fp = fdopen(FILE_FD, "w+");
  if(!fp)
  {
    fprintf(stderr, "Can't reopen for putc.\n");
    return 1;
  }
  if(fseek(fp, 0, SEEK_SET) != 0)
  {
    fprintf(stderr, "Can't seek.\n");
    return 1;
  }
  fflush(NULL);
  TEST_FUNC_WRITE("putc(c, fp) no thread", if(putc(c, fp) == EOF), res[0]);
  if(fseek(fp, 0, SEEK_SET) != 0)
  {
    fprintf(stderr, "Can't seek.\n");
    return 1;
  }
  fflush(NULL);
  TEST_FUNC_READ("getc() no thread", if( (c = getc(fp)) == EOF), res[1]);
  if(fseek(fp, 0, SEEK_SET) != 0)
  {
    fprintf(stderr, "Can't seek.\n");
    return 1;
  }
  fflush(NULL);

  if(write(1, res, sizeof(res)) != sizeof(res))
  {
    fprintf(stderr, "Can't write results to parent process.\n");
    return 1;
  }

  return 0;
}

