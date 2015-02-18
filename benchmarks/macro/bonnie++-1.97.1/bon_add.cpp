#include "bonnie.h"
#include <stdio.h>
#include <vector>
#include <string.h>
#include <math.h>

// Maximum number of items expected on a csv line
#define MAX_ITEMS 45
typedef vector<PCCHAR> STR_VEC;

vector<STR_VEC> data;
typedef PCCHAR * PPCCHAR;
PPCCHAR * props;

// Splits a line of text (CSV format) by commas and adds it to the list to
// process later.  Doesn't keep any pointers to the buf...
void read_in(CPCCHAR buf);
// print line in the specified line from columns start..end as a line of a
// HTML table
void print_a_line(int num, int start, int end);
// 0 means don't do colors, 1 means speed, 2 means CPU, 3 means latency
const int vals[MAX_ITEMS] =
  { 0,0,0,0,0,1,2,1,2,1,2,1,2,1,2,1,2,
    0,0,0,0,1,2,1,2,1,2,1,2,1,2,1,2,
    3,3,3,3,3,3,3,3,3,3,3,3 };

void usage()
{
  exit(1);
}

int main(int argc, char **argv)
{
  char buf[1024];

  FILE *fp = NULL;
  if(argc > 1)
  {
    fp = fopen(argv[1], "r");
    if(!fp)
      usage();
  }
  while(fgets(buf, sizeof(buf), fp ? fp : stdin))
  {
    buf[sizeof(buf) - 1] = '\0';
    strtok(buf, "\r\n");
    read_in(buf);
  }

  printf("%s", data[0][0]);
  int i;
  for(i = 1; i < MAX_ITEMS; i++)
  {
    switch(vals[i])
    {
    case 0:
      printf(",%s", data[0][i]);
    break;
    case 1:
    case 2:
      {
        int sum = 0, tmp = 0;
        for(int j = 0; j < data.size(); j++)
        {
          if(sscanf(data[j][i], "%d", &tmp) != 1)
          {
            sum = 0;
            j = data.size();
          }
          sum += tmp;
        }
        if(sum > 0)
          printf(",%d", sum);
        else
          printf(",");
      }
    break;
    case 3:
      {
        double max = 0.0;
        int tmp = 0;
        for(int j = 0; j < data.size(); j++)
        {
          if(sscanf(data[j][i], "%d", &tmp) != 1)
          {
            max = 0.0;
            j = data.size();
          }
          double dtmp = double(tmp);
          if(strstr(data[j][i], "ms"))
            dtmp *= 1000.0;
          else if(!strstr(data[j][i], "us"))
            dtmp *= 1000000.0;
          if(dtmp > max)
            max = dtmp;
        }
        if(max > 99999999.0)
          printf(",%ds", int(max / 1000000.0));
        else if(max > 99999.0)
          printf(",%dms", int(max / 1000.0));
        else
          printf(",%dus", int(max));
      }
    break;
    }
  }
  printf("\n");
  return 0;
}

STR_VEC split(CPCCHAR delim, CPCCHAR buf)
{
  STR_VEC arr;
  char *tmp = strdup(buf);
  while(1)
  {
    arr.push_back(tmp);
    tmp = strstr(tmp, delim);
    if(!tmp)
      break;
    *tmp = '\0';
    tmp += strlen(delim);
  }
  return arr;
}

void read_in(CPCCHAR buf)
{
  STR_VEC arr = split(",", buf);
  if(strcmp(arr[0], "2") )
  {
    fprintf(stderr, "Can't process: %s\n", buf);
    free((void *)arr[0]);
    return;
  }

  data.push_back(arr);
}

