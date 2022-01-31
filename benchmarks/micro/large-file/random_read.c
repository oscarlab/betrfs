#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>

static long randomwrites = (256 * 1024);
//static long randomwrites = (8 * 1);
static char * fname = NULL;
static long long fsize = 0;
static char * prog_name;
static void process_options(int, char *[]);
static long iosize = 4096;

static char *rand_string(char *str, size_t size)
{
    const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJK1234567890";
    int n;
    if (size) {
        --size;
        for (n = 0; n < size; n++) {
            int key = rand() % (int) (sizeof charset - 1);
            str[n] = charset[key];
        }
        str[size] = '\0';
    }
    return str;
}

int main(int argc, char* argv[]) {
    struct timespec start, end;
    double elapsed, mb;
    int pos;
    int fd;
    int i;
    long long *array;
    char *buffer;

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

    printf("iosize=%ld\n", iosize);

    buffer = (char *) malloc(iosize);
    assert(buffer != NULL);

    buffer = rand_string(buffer, iosize);

    array = malloc(sizeof(long long) * randomwrites);
    if (random == NULL) {
        fprintf(stderr, "Allocate memory failed\n");
        return -1;
    }

    srand(42);

    for (i = 0; i < randomwrites; i++) {
	array[i] = (random()<<32|random()) % (fsize - 4);
    }
 
    if((fd = open(fname, O_RDWR,0644)) < 0) {
        fprintf(stderr, "open file failed\n");
        return -1;
    }

    clock_gettime(CLOCK_MONOTONIC, &start);

    /* THIS IS THE ACTUAL TEST */
    for(i = 0; i < randomwrites; i ++) {
        pread(fd, buffer, iosize, array[i]);
    }
    fsync(fd);

    clock_gettime(CLOCK_MONOTONIC, &end);

    close(fd);
    free(buffer);

    elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1000000000.0;
    mb = (iosize*randomwrites)/1.0E6;
    printf("op, seq.or.rand, size.MB, time.s, throughput.MBps\n");
    printf("read.random.%ldB, %lf, %lf, %lf\n", iosize, mb, elapsed, mb / elapsed);
    return 0;
}
void print_usage (FILE * stream){
           fprintf(stream, "%s -s FILESIZE -f FNAME [-r RANDOMWRITES]\n", prog_name);
           fprintf(stream, "\t -r  --randomwrites  how many randomwrites\n");
           fprintf(stream, "\t -f  --filename      the file name to be written\n");
           fprintf(stream, "\t -s  --size          the file size (max pos)\n");
           fprintf(stream, "\t -i  --iosize        the io size\n");
}

void process_options(int argc, char * argv[]){
            int opt;
            struct option long_opts[] = {
                {"size",         1,  0,  's'},
                {"filename",      1,  0,  'f'},
                {"randomwrites",    1,  0,  'r'},
                {"iosize",           1,  0,  'i'}, 
                {0,0,0,0}
            };
            while((opt = getopt_long(argc,argv, "s:f:r:i:", long_opts, NULL))!=EOF) 
            {   switch(opt)
                {
                    case 'r':
                        randomwrites = atol(optarg); 
                        break;
                    case 'i':
                        iosize = atol(optarg); 
                        break;
                    case 'f':
                        fname = optarg; 
                        break;
                    case 's':
                        fsize = atoll(optarg); 
                        break;               
                    default:                               
                        print_usage(stderr); 
                        return;     
                }       
                         
            }

}
