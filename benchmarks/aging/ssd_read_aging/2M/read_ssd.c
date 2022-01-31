#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <time.h>

#include "config.h"

int main()
{

    int i=0;
    char buf[SEG_SIZE]={0};
    struct timespec start, end;
    double elapsed, mb;
    int ret;

    int fd=open(dev_name, O_RDONLY, 0);
    if (fd==-1) {
        printf("Can not open the file\n");
        return -1;
    }


    clock_gettime(CLOCK_MONOTONIC, &start);
    for (i=0;i<num;i++) { 
	ret=pread(fd, buf, SEG_SIZE, address[i]*SEG_SIZE);
	if(ret!=SEG_SIZE) {
	   printf("ret: %d\n", ret);
           perror("error");
           return -1;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &end);

    close(fd);

    elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1000000000.0;
    mb = (SEG_SIZE*1000)/1.0E6;
    printf("size.MB, time.s, throughput.MBps\n");
    printf("%lf, %lf, %lf\n", mb, elapsed, mb / elapsed);
    return 0;
}

