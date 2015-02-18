#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <netinet/in.h>

void usage()
{
  fprintf(stderr, "Usage: generate_randfile [-s seed] [-f file] count\n");
  exit(1);
}

int main(int argc, char **argv)
{
  if(argc < 2)
  {
    usage();
  }
  unsigned int seed = getpid() ^ time(NULL);
  FILE *fp = stdout;
  int int_c;
  while(-1 != (int_c = getopt(argc, argv, "s:f:")) )
  {
    switch(char(int_c))
    {
      case '?':
      case ':':
        usage();
      break;
      case 's':
        if(sscanf(optarg, "%u", &seed) != 1)
          usage();
      break;
      case 'f':
        fp = fopen(optarg, "w");
        if(fp == NULL)
          usage();
      break;
    }
  }
  if(optind >= argc)
    usage();
  int count = atoi(argv[optind]);
  srand(seed);
  fprintf(stderr, "Generating %d random numbers with seed %d.\n", count, seed);
  for(int i = 0; i < count; i++)
  {
    unsigned long val = htonl(rand());
    if(fwrite(&val, sizeof(val), 1, fp) != 1)
    {
      fprintf(stderr, "Can't write item %d.\n", i);
      return 1;
    }
  }
  if(fp != stdout)
    fclose(fp);
  return 0;
}
