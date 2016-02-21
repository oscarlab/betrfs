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
//#define BLOCK_SIZE 100
#define BLOCK_SIZE 4095
#define DEFAULT_SEED 42

static long randomwrites = 10000;
static char * fname = NULL;
static long long fsize = 0;
static unsigned int block_size = BLOCK_SIZE;
static unsigned int seed = DEFAULT_SEED;
static unsigned int numbuffers = 50;
static char * prog_name;

static void process_options(int, char *[]);
int main(int argc, char* argv[]) {
    struct timespec start, end;
    double elapsed, mb;
    int pos;
    int fd;
    int i, ret, k, pwriteret;
    long long *array;	
    long long max = 0;
    char **buffer;

    printf(" write_size = %d\n", block_size);
    prog_name = argv[0];
    process_options(argc, argv);
    if( 0 == fsize || 0 == fname) {
        fprintf(stderr, "error option: you have to set both fname: %s and fsize: %lld\n", fname, fsize);
        return -1;
    }

    if (fsize <= 4) {
        fprintf(stderr, "fsize too small\n");
        return -1;
    }

    array = malloc(sizeof(long long) * randomwrites);
    if (random == NULL) {
        fprintf(stderr, "Allocate memory failed\n");
        return -1;
    }
	
    srand(seed);

    for (i = 0; i < randomwrites; i++) {
	    array[i] = random();
	    array[i] = (array[i] << 32) | rand();
	    array[i] = array[i] % (fsize - 4096);
	    array[i] -= array[i]%4096;
    }
	
    buffer = malloc(numbuffers * sizeof(*buffer));
    if(!buffer)
	    return -ENOMEM;

    for (k=0; k<numbuffers; k++) {
	    ret = posix_memalign((void**)&buffer[k], PAGE_SIZE, block_size);
	    if (ret) {
		    fprintf(stderr, "memalign failed: %d\n", ret);
		    return ret;
	    }

	    for (i = 0; i < block_size; i++) {
		    buffer[k][i] = (char) rand();
	    }
    }

    if((fd = open(fname, O_RDWR, 0644)) < 0) {
        fprintf(stderr, "open file failed\n");
        return -1;
    }

    clock_gettime(CLOCK_MONOTONIC, &start);

    /* THIS IS THE ACTUAL TEST */
    srand(12321);
    for(i = 0; i < randomwrites; i ++) {
        k = rand() % numbuffers;
	pwrite(fd, buffer[k], block_size, array[i]);
    }
    fsync(fd);

    clock_gettime(CLOCK_MONOTONIC, &end);

    close(fd);

    elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1000000000.0;
    mb = (block_size*randomwrites)/1.0E6;
    printf("op, seq.or.rand, size.MB, time.s, throughput.MBps\n");
    printf("write, random, %lf, %lf, %lf\n", mb, elapsed, mb / elapsed);
    return 0;
}
void print_usage (FILE * stream){
           fprintf(stream, "%s -s FILESIZE -f FNAME [-r RANDOMWRITES]\n", prog_name);
           fprintf(stream, "\t -r  --randomwrites  how many randomwrites\n");
           fprintf(stream, "\t -f  --filename      the file name to be written\n");
           fprintf(stream, "\t -s  --size          the file size (max pos)\n");
           fprintf(stream, "\t -b  --blocksize     the size of each write\n");

}

void process_options(int argc, char * argv[]){
            int opt;
            struct option long_opts[] = {
                {"size",            1,  0,  's'},
                {"filename",        1,  0,  'f'},
                {"randomwrites",    1,  0,  'r'},
				{"block_size",      1,  0,  'b'},
                {0,0,0,0}
            };
            while((opt = getopt_long(argc,argv, "s:f:r:", long_opts, NULL))!=EOF) 
            {   switch(opt)
                {
                    case 'r':
                        randomwrites = atol(optarg); 
                        break;
                    case 'f':
                        fname = optarg; 
                        break;
                    case 's':
                        fsize = atoll(optarg); 
                        break;               
                	case 'b':
            			block_size = atoi(optarg);
            			break;
				    default:                               
                        print_usage(stderr); 
                        return;     
                }       
                         
            }

}

