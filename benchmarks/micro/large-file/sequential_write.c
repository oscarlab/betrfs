#include <stdio.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <stdlib.h>
#include <getopt.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define PAGE_SIZE 4096
#define BLOCK_SIZE 4096
#define RAND_BLOCKS 5
#define DEFAULT_SEED 42

static char * fname = NULL;
static long long fsize = 1073741824;
static char * prog_name;
static unsigned int seed = DEFAULT_SEED;
static unsigned int block_size = BLOCK_SIZE;
static unsigned int rand_blocks = RAND_BLOCKS;

static void process_options(int, char *[]);

int main(int argc, char* argv[]) {
	struct timespec start, end, gb_start, gb_end;
	double elapsed, mb;
	long long pos;
	int fd;
	int ret;
	int i, j;
	unsigned long nr_gb;
	char **array;

	prog_name = argv[0];
	process_options(argc, argv);
	if ( 0 == fsize || 0 == fname) {
		fprintf(stderr, "error option: you have to set both"
			" fname: %s and fsize: %lld\n", fname, fsize);
		return -1;
	}
	printf("\n\n\nfile_size:   %lld,\n", fsize);
	printf("block_size:  %d,\n", block_size);
	printf("rand_blocks: %d,\nWRITE SEQ,\n", rand_blocks);

	if (fsize <= 4) {
		fprintf(stderr, "fsize too small\n");
		return -1;
	}

	srand(seed);

	array = malloc(rand_blocks * sizeof(*array));
	if (!array)
		return -ENOMEM;

	/* we want some page aligned buffers with incompressible data */
	for (i = 0; i < rand_blocks; i++) {
		ret = posix_memalign((void**)&array[i], PAGE_SIZE, block_size);
		if (ret) {
			fprintf(stderr, "memalign failed: %d\n", ret);
			return ret;
		}
		for (j = 0; j < block_size; j++)
			array[i][j] = (char) rand();
	}

	fd = open(fname, O_RDWR | O_CREAT, 0644);
	if (fd < 0) {
		fprintf(stderr, "open file failed: %d\n", fd);
		return fd;
	}

	clock_gettime(CLOCK_MONOTONIC, &start);
	clock_gettime(CLOCK_MONOTONIC, &gb_start);

	i = 0;
	pos = 0;
	nr_gb = 1;
	for(pos = 0; pos < fsize; pos += block_size) {
		ret = pwrite(fd, array[i], block_size, pos);
		if (ret != block_size)
			printf("short write (ret: %d)\n", ret);
		i = (i+1) % rand_blocks;

		{
			unsigned long gb = nr_gb << 30;

			if (gb >= pos && (pos + block_size) >= gb) {
				clock_gettime(CLOCK_MONOTONIC, &gb_end);
				elapsed = (gb_end.tv_sec - gb_start.tv_sec) +
					(gb_end.tv_nsec - gb_start.tv_nsec) / 1000000000.0;
				printf("%lu, ", nr_gb);
				printf("%lf, %lf, %lf,\n", 1024.0, elapsed, 1024.0 / elapsed);
				gb_start = gb_end;
				nr_gb++;
			}
		}
	}


	{
		clock_gettime(CLOCK_MONOTONIC, &end);
		elapsed = (end.tv_sec - start.tv_sec) +
			(end.tv_nsec - start.tv_nsec) / 1000000000.0;
		printf("\n\nbefore fsync: write, seq, %lf, %lf, %lf, \n", fsize/1.0E6, elapsed, (fsize/1.0E6) / elapsed);
	}

	fsync(fd);
	clock_gettime(CLOCK_MONOTONIC, &end);
	close(fd);

	elapsed = (end.tv_sec - start.tv_sec) +
		(end.tv_nsec - start.tv_nsec) / 1000000000.0;
	mb = fsize/1.0E6;
	printf("op, seq.or.rand, size.MB, time.s, throughput.MBps, \n");
	printf("write.seq, %lf, %lf, %lf\n", mb, elapsed, mb / elapsed);
	return 0;
}

void print_usage (FILE * stream){
	fprintf(stream, "%s -s FILESIZE -f FNAME [-r RANDOMWRITES]\n", prog_name);
	fprintf(stream, "\t -r  --random_seed   seed for srand()\n");
	fprintf(stream, "\t -o  --filename      the output file name\n");
	fprintf(stream, "\t -s  --size          output file size (max pos)\n");

}

void process_options(int argc, char * argv[]){
	int opt;

	struct option long_opts[] = {
		{"output_file",   1,  0,  'o'},
		{"file_size",     1,  0,  's'},
		{"block_size",    1,  0,  'b'},
		{"num_blocks",    1,  0,  'n'},
		{"random_seed",   1,  0,  'r'},
	};


	while((opt = getopt_long(argc,argv, "r:o:s:b:n:", long_opts, NULL))!=EOF) {
		switch (opt) {
		case 'r':
			seed = atoi(optarg);
			break;
		case 'o':
			fname = optarg;
			break;
		case 's':
			fsize = atoll(optarg);
			break;
		case 'b':
			block_size = atoi(optarg);
			break;
		case 'n':
			rand_blocks = atoi(optarg);
			break;
		default:
			print_usage(stderr);
			return;
		}
	}
}

