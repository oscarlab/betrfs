#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

int main(int argc, char **argv)
{
	char *file1;
	char *file2;
	int count = 0;
	int ret = 0;

	if (argc == 1) {
		file1 = "/mnt/benchmarks/file1";
		file2 = "/mnt/benchmarks/file2";
		count = 500000;
	} else if(argc < 4){
		printf("use: rename-test file1 file2 count\n");
		return 0;
	} else {
		file1 = argv[1];
		file2 = argv[2];
		count = atoi(argv[3]);
	}

	printf("file1: %s, file2: %s, count: %d\n", file1, file2, count);

	while(count >= 0){

		if(count % 2){
			ret = rename(file1, file2);
			if (ret) {
				printf("rename error: %d\n", errno);
				return ret;
			}
		} else {
			ret = rename(file2, file1);
			if (ret) {
				printf("rename error: %d\n", errno);
				return ret;
			}
		}

		count--;
	}

	return 0;
}
