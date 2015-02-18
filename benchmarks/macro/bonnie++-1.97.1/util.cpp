#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#include "bonnie.h"

unsigned int size_from_str(CPCCHAR str, CPCCHAR conv)
{
  const unsigned int mult[3] = { 1<<10 , 1<<20, 1<<30 };
  unsigned int size = atoi(str);
  char c = tolower(str[strlen(str) - 1]);
  if(conv)
  {
    for(int i = 0; conv[i] != '\0' && i < 3; i++)
    {
      if(c == conv[i])
      {
        size *= mult[i];
        return size;
      }
    }
  }
  return size;
}
