#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
int main(int argc, char *argv[])
{
	
    char buffer[1024*1024]; 
    long count = atol(argv[1])/(1024*1024), i;
    int bytesleft = atol(argv[1])%(1024*1024);
    int fp, rand;

    rand= open(argv[2], O_WRONLY | O_SYNC | O_CREAT, 0777);
    if(rand < 0 ){
	    perror("Error in opening file");
	    exit(1);
    }
    fp = open("/dev/urandom", O_RDONLY, 0777);
    // printf("count:%ld", count);
    for(i = 0; i < count; i++)
    {
	    read(fp, buffer, 1024*1024);
	    write(rand, buffer, 1024*1024);
    }
    if(bytesleft > 0)
    {
	    read(fp, buffer, bytesleft);
	    write(rand, buffer, bytesleft);
    }
    fsync(rand);
    close(rand);
    close(fp);
}
