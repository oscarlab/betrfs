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
/*
 	r = opendir(name, O_WRONLY | O_DIRECTORY);
	if (r < 0) {
		printf("error create dir r <0: %s\n", strerror(errno));
		exit(-1);
	}
*/
	return r;
}

int main(int argc, char* argv[]) {

	char *srcdir = "/mnt/benchmark/b";
	char *dstdir = "/mnt/benchmark/d";
	char *basedir = "/mnt/benchmark/";
	char srcfname[50];
	char dstfname[50];
	struct timespec start, end;
	double elapsed; 
	int filefd, srcfilefd, dstfilefd, srcdirfd, dstdirfd;
	int i, j, k;
	int status, status1;
	char *data;
	int errno;

	int filecount = 1000;
	int sfilecount = 100;

	char *filesystem = argv[1];
	double fsize = pow(2.0, atof(argv[2]));
	double sfsize = 4194304.0;
	char *filename = argv[3];
	
	FILE *result = fopen(filename, "ab");	

	if (result == NULL) {
		printf("Can't open result file");
		exit(0);
	}	

	// create 100 4mb files in /mnt/benchmark: a0-a99
	for(k=0; k<sfilecount; k++) {
		data = (char *)malloc(sfsize);
		RAND_pseudo_bytes((unsigned char *)data, sfsize);
		
		sprintf(srcfname, "/mnt/benchmark/a%d", k);
		printf("creating %s\n", srcfname);
		if((filefd = open(srcfname, O_CREAT | O_WRONLY, 0644)) < 0) {
			fprintf(stderr, "open file failed\n");
			return -1;
		}
		
		status = pwrite(filefd, data, sfsize, 0);
		if (status != sfsize) {
			printf("pwrite failed. Only write: %d", status);
			exit(-1);
		}	

		status1 = fsync(filefd);
		if (status1 != 0)
			printf("Failed to sync file: %s\n", strerror(errno));	
		close(filefd);
		free(data);
	}
	
	// create /mnt/benchmark/b and put 1000 files of the given size in it 0-999
	createDir(srcdir);

	for(k=0; k<filecount; k++) {
		if (k % 10 == 0) {
			sleep(1);
		}
		data = (char *)malloc(fsize);
		RAND_pseudo_bytes((unsigned char *)data, fsize);
		
		sprintf(srcfname, "/mnt/benchmark/b/%d", k);
		printf("creating %s\n", srcfname);
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
		close(filefd);
		free(data);
	}

	// create 100 4mb files in /mnt/benchmark: c0-c99
	for(k=0; k<sfilecount; k++) {
		data = (char *)malloc(sfsize);
		RAND_pseudo_bytes((unsigned char *)data, sfsize);
		
		sprintf(srcfname, "/mnt/benchmark/c%d", k);
		printf("creating %s\n", srcfname);
		if((filefd = open(srcfname, O_CREAT | O_WRONLY, 0644)) < 0) {
			fprintf(stderr, "open file failed\n");
			return -1;
		}
		
		status = pwrite(filefd, data, sfsize, 0);
		if (status != sfsize) {
			printf("pwrite failed. Only write: %d", status);
			exit(-1);
		}	

		status1 = fsync(filefd);
		if (status1 != 0)
			printf("Failed to sync file: %s\n", strerror(errno));	
		close(filefd);
		free(data);
	}

	// create /mnt/benchmark/d and put 500 files of the given size in it 0,2,...,1000
	createDir(dstdir);

	for(k=0; k<filecount/2; k++) {
		if (k % 10 == 0) {
			sleep(1);
		}
		data = (char *)malloc(fsize);
		RAND_pseudo_bytes((unsigned char *)data, fsize);
		
		sprintf(srcfname, "/mnt/benchmark/d/%d", 2*k);
		printf("creating %s\n", srcfname);
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
		close(filefd);
		free(data);
	}

	// create 100 4mb files in /mnt/benchmark: e0-e99
	for(k=0; k<sfilecount; k++) {
		data = (char *)malloc(sfsize);
		RAND_pseudo_bytes((unsigned char *)data, sfsize);
		
		sprintf(srcfname, "/mnt/benchmark/e%d", k);
		printf("creating %s\n", srcfname);
		if((filefd = open(srcfname, O_CREAT | O_WRONLY, 0644)) < 0) {
			fprintf(stderr, "open file failed\n");
			return -1;
		}
		
		status = pwrite(filefd, data, sfsize, 0);
		if (status != sfsize) {
			printf("pwrite failed. Only write: %d", status);
			exit(-1);
		}	

		status1 = fsync(filefd);
		if (status1 != 0)
			printf("Failed to sync file: %s\n", strerror(errno));	
		close(filefd);
		free(data);
	}

	// fsync the file system before running the experiment
	srcdirfd = open(srcdir, O_RDONLY); //createDir(dir);
	if (srcdirfd < 0) {
		printf("Can't open dir: %s", strerror(errno));
		exit(-1);
	}

	dstdirfd = open(dstdir, O_RDONLY); //createDir(dir);
	if (dstdirfd < 0) {
		printf("Can't open dir: %s", strerror(errno));
		exit(-1);
	}

	status1 = fsync(srcdirfd);
	if (status1 != 0) {
		printf("Failed to sync dir: %s\n", strerror(errno));
		exit(-1);
	}	
	status1 = fsync(dstdirfd);
	if (status1 != 0) {
		printf("Failed to sync dir: %s\n", strerror(errno));
		exit(-1);
	}	

	//system("/home/betrfs/ft-index/benchmarks/remount-ftfs.sh");
	
	sleep(10);

	clock_gettime(CLOCK_MONOTONIC, &start);

	for (j=0; j<filecount/2; j++) {
		// rename all the odd files to interleave them in
		// the source and destination
		sprintf(srcfname, "/mnt/benchmark/b/%d", 2*j+1);
		sprintf(dstfname, "/mnt/benchmark/d/%d", 2*j+1);
	
		status = rename(srcfname, dstfname);
		if (status != 0) {
			printf("Failed to rename: %s\n", strerror(errno));
			exit(-1);
		}	
		// fync (or not)
		status1 = fsync(srcdirfd);
		if (status1 != 0) {
			printf("Failed to sync dir: %s\n", strerror(errno));
			exit(-1);
		}	
		status1 = fsync(dstdirfd);
		if (status1 != 0) {
			printf("Failed to sync dir: %s\n", strerror(errno));
			exit(-1);
		}	
	}

	// fsync at the end
	status1 = fsync(srcdirfd);
	if (status1 != 0) {
		printf("Failed to sync dir: %s\n", strerror(errno));
		exit(-1);
	}	
	status1 = fsync(dstdirfd);
	if (status1 != 0) {
		printf("Failed to sync dir: %s\n", strerror(errno));
		exit(-1);
	}	

	clock_gettime(CLOCK_MONOTONIC, &end);
		
	elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1000000000.0;
	fprintf(result, "%s, %lf, %lf\n", filesystem, fsize, 500.0/elapsed);
	printf("%s, %lf, %lf\n", filesystem, fsize, 500.0/elapsed);

   	close(srcdirfd);
   	close(dstdirfd);
	
	return 0;
}

