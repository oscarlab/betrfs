#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


static long randomwrites = (256 * 1024);
static char * fname = NULL;
static long long fsize = 0;
static char * prog_name;

static void process_options(int, char *[]);
int main(int argc, char* argv[]) {
    struct timespec start, end;
    double elapsed, mb;
    int pos;
    int fd;
    int i;
    long long *array;
    
    char buffer[4] = {'\20','\10','\5','W'};

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
        pwrite(fd, buffer, 4, array[i]);
    }
    fsync(fd);

    clock_gettime(CLOCK_MONOTONIC, &end);

    close(fd);

    elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1000000000.0;
    mb = (4*randomwrites)/1.0E6;
    printf("op, seq.or.rand, size.MB, time.s, throughput.MBps\n");
    printf("write.random.4B, %lf, %lf, %lf\n", mb, elapsed, mb / elapsed);
    return 0;
}
void print_usage (FILE * stream){
           fprintf(stream, "%s -s FILESIZE -f FNAME [-r RANDOMWRITES]\n", prog_name);
           fprintf(stream, "\t -r  --randomwrites  how many randomwrites\n");
           fprintf(stream, "\t -f  --filename      the file name to be written\n");
           fprintf(stream, "\t -s  --size          the file size (max pos)\n");

}

void process_options(int argc, char * argv[]){
            int opt;
            struct option long_opts[] = {
                {"size",         1,  0,  's'},
                {"filename",      1,  0,  'f'},
                {"randomwrites",    1,  0,  'r'},
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
                    default:                               
                        print_usage(stderr); 
                        return;     
                }       
                         
            }

}
