#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <stdlib.h>
#include <getopt.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <math.h>
#include <sys/stat.h>
#include <openssl/rand.h>
#include <errno.h>
#include <string.h>

struct stat st = {0};

void swap(char **str1, char **str2) {
	char *tmp = *str1;
	*str1 = *str2;
	*str2 = tmp;
}

int createDir(char* name) {
	int r;
	if (stat(name, &st) == -1) {
		r = mkdir(name, 0777 );
	}
	if (r != 0) {
		printf("Can't create diri: %s", strerror(errno));
		exit(-1);
	}
	r = opendir(name, O_WRONLY | O_DIRECTORY);
	if (r < 0) {
		printf("error create dir r <0: %s\n", strerror(errno));
		exit(-1);
	}
	return r;
}

int main(int argc, char* argv[]) {

	char *dir = "/mnt/benchmark/";
	char *srcfname = "/mnt/benchmark/a";
	char *dstfname = "/mnt/benchmark/b";
	// char *dir = "/home/ftfs/ft-index/benchmarks/micro/circlingtradeoff/dir";
	// char *srcfname = "/home/ftfs/ft-index/benchmarks/micro/circlingtradeoff/dir/a";
	// char *dstfname = "/home/ftfs/ft-index/benchmarks/micro/circlingtradeoff/dir/b";
	struct timespec start, end;
	// struct timespec startrename, endrename;
	// struct timespec startfsync, endfsync;
	double elapsed; 
	// double elapsedrename; 
	// double elapsedfsync;
	int filefd, dirfd;
	int i, j;
	int status, status1;
	char *data;
	int errno;

	char *filesystem = argv[1];
	double fsize = pow(2.0, atof(argv[2]));
	char *filename = argv[3];
	
	FILE *result = fopen(filename, "ab");	

	if (result == NULL) {
		printf("Can't open result file");
		exit(0);
	}	

	data = (char *)malloc(fsize);
	RAND_pseudo_bytes((unsigned char *)data, fsize);

	if((filefd = open(srcfname, O_CREAT | O_WRONLY, 0644)) < 0) {
		fprintf(stderr, "open file failed\n");
		return -1;
	}
	
	status = pwrite(filefd, data, fsize, 0);
	if (status != fsize) {
		printf("pwrite failed. Only write: %d", status);
		exit(-1);
	}	

	status1 = fsync(filefd);
	if (status1 != 0)
		printf("Failed to sync file: %s\n", strerror(errno));	
	
	dirfd = open(dir, O_RDONLY); //createDir(dir);
	if (dirfd < 0) {
		printf("Can't open dir: %s", strerror(errno));
		exit(-1);
	}

	for (j=0; j<100; j++) {
		// printf("Src: %s Dst: %s\n", srcfname, dstfname);	
	
		clock_gettime(CLOCK_MONOTONIC, &start);
		// clock_gettime(CLOCK_MONOTONIC, &startrename);
		status = rename(srcfname, dstfname);
		// clock_gettime(CLOCK_MONOTONIC, &endrename);
		
		// clock_gettime(CLOCK_MONOTONIC, &startfsync);
		status1 = fsync(dirfd);
		// clock_gettime(CLOCK_MONOTONIC, &endfsync);
		clock_gettime(CLOCK_MONOTONIC, &end);
		
		if (status != 0) {
			printf("Failed to rename: %s\n", strerror(errno));
			exit(-1);
		}	
		if (status1 != 0) {
			printf("Failed to sync dir: %s\n", strerror(errno));
			exit(-1);
		}	
		
		//printf("Rename status: %d, Fsync status: %d\n", status, status1);
		elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1000000000.0;
		// elapsedrename = (endrename.tv_sec - startrename.tv_sec) + (endrename.tv_nsec - startrename.tv_nsec) / 1000000000.0;
		// elapsedfsync = (endfsync.tv_sec - startfsync.tv_sec) + (endfsync.tv_nsec - startfsync.tv_nsec) / 1000000000.0;
		fprintf(result, "%s, %lf, %lf\n", filesystem, fsize, elapsed);
		printf("%s, %lf, %lf\n", filesystem, fsize, elapsed);
		// fprintf(result, "%s, %d, %lf, %lf, %lf\n", filesystem, circle_size, fsize, elapsedrename, elapsedfsync);
		// printf("%s, %d, %lf, %lf, %lf\n", filesystem, circle_size, fsize, elapsedrename, elapsedfsync);
	
		swap(&srcfname, &dstfname);
	}

	free(data);
    	close(filefd);
   	close(dirfd);
	
	return 0;
}

