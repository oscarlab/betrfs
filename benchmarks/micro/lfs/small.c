#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#ifdef TX
//#include "../tx.h"
#else
#define xbegin(a, b) 
#define xend() 
#endif

#ifdef SYNC
#define my_sync() sync()
#else
#define my_sync() 
#endif

static char buf[40960];
static char name[32];
static char *prog_name;

extern int errno;

#define NDIR 100


static char dir[32];
struct timeval s;

void 
start()
{
    gettimeofday(&s, NULL);
    xbegin(TX_DEFAULTS, NULL);
}


double 
stop()
{
  time_t ds;
  time_t du;
  struct timeval f;
  xend();
  my_sync();
  gettimeofday(&f, NULL);
  ds = f.tv_sec - s.tv_sec;
  du = f.tv_usec - s.tv_usec;
  return (ds) + ((double)du/1000000.0);
}


void 
creat_dir()
{
    int i;

    umask(0);
    for (i = 0; i < NDIR; i++) {
        sprintf(dir, "d%d", i);
        mkdir(dir, 0777);
    }
}

void 
clean_dir(int n_flush_files)
{
    int i;
    char filename[12];

    umask(0);
    for (i = 0; i < NDIR; i++) {
        sprintf(dir, "d%d", i);
        rmdir(dir);
    }
    for (i=0; i < n_flush_files; i++) {
      sprintf(filename, "FLUSH_TMP%d", i);
      unlink (filename);
    }
}



void
creat_test(int n, int size)
{
    int i;
    int r;
    int fd;
    int j;
    double elapsed;

    start();
    for (i = 0, j = 0; i < n; i ++) {

      j = i % NDIR;
      sprintf(name, "d%d/g%d", j, i);

      if((fd = open(name, O_RDWR | O_CREAT | O_TRUNC, S_IRWXU)) < 0) {
	printf("%s: create %d(%s) failed %d %d\n", prog_name, i, name,
	       fd, errno);
	exit(1);
      }
      
      if ((r = write(fd, buf, size)) < 0) {
	printf("%s: write failed %d %d\n", prog_name, r, errno);
	exit(1);
      }
      
#ifndef TX      
      /* DEP 4/1/09: If we have durable tx, we don't need to waste
       *  time calling fsync() 
       */
      if ((r = fsync (fd) < 0)) {
	printf ("%s: fsync failed: %s\n",prog_name, strerror (errno));
      }
#endif

      if ((r = close(fd)) < 0) {
	printf("%s: close failed %d %d\n", prog_name, r, errno);
      }
      
    }
    elapsed = stop();
    printf("creat: %d %d-byte files in  %f sec = %f KB/sec\n",  n, size, elapsed, (n*size)/(1000*elapsed));
}

#define FLUSH_BLOCK_SIZE 16384
#define NUM_FLUSH_BLOCKS 131072
int create_flush_file (int num_files) 
{
  int fd, r, i;
  long n;
  char buf[FLUSH_BLOCK_SIZE];
  char filename[12];

  if(num_files == 0){
    sync();
    fd = open("/proc/sys/vm/drop_caches", O_WRONLY);
    if(!fd){
      printf("Failed to open drop_caches: %d\n", errno);
      exit(1);
    }
    r = write(fd, "3", 1);
    if(r != 1){
      printf("Failed to write drop_caches: %d\n", errno);
      exit(1);
    }
  }

  for(i=0; i<num_files; i++) {
    sprintf(filename, "FLUSH_TMP%d", i);
    if((fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, S_IRWXU)) < 0) {
      printf("%s: failed to create temporary file used to flush the cache. "
	     "Make sure you have 1G free in this file system", 
	     prog_name);
      exit(1);
    }
    
    memset (buf, 0xc5, FLUSH_BLOCK_SIZE/2);
    for (n = 0; n < NUM_FLUSH_BLOCKS; n++) {
      if ((r = write(fd, buf, FLUSH_BLOCK_SIZE)) < 0) {
	printf("%s: failed to create temporary file used to flush the cache. "
	       "Make sure you have 1G free in this file system (%s)", 
	       prog_name,
	       strerror (errno));
	unlink (filename);
	exit(1);
      }
    }
    
    close (fd);
  }
}


int flush_cache(int num_files)
{
  int fd, r, i;
  long n;
  char buf[FLUSH_BLOCK_SIZE];
  char filename[12];

  if(num_files == 0){
    sync();
    fd = open("/proc/sys/vm/drop_caches", O_WRONLY);
    if(!fd){
      printf("Failed to open drop_caches: %d\n", errno);
      exit(1);
    }
    r = write(fd, "3", 1);
    if(r != 1){
      printf("Failed to write drop_caches: %d\n", errno);
      exit(1);
    }
    close(fd);
  }

  for(i=0; i<num_files; i++) {
    sprintf(filename, "FLUSH_TMP%d", i);
    if((fd = open(filename, O_RDONLY, S_IRWXU)) < 0) {
      printf("%s: failed to open the temporary file I use to flush the cache\n",
	     prog_name);
      exit(1);
    }
    
    for (n = 0; n < NUM_FLUSH_BLOCKS; n++) {
      if ((r = read(fd, buf, FLUSH_BLOCK_SIZE)) < 0) {
	printf("%s: failed to read temporary file used to flush the cache.\n",
	     prog_name);
	exit(1);
      }
    }
    close(fd);
  }
}

int read_test(int n, int size)
{
    int i;
    int r;
    int fd;
    int j;
    double elapsed;

    start();
    for (i = 0, j = 0; i < n; i ++) {
      
      j = i % NDIR;
      sprintf(name, "d%d/g%d", j, i);
      
      if((fd = open(name, O_RDONLY)) < 0) {
	printf("%s: open %d failed %d %d\n", prog_name, i, fd, errno);
	exit(1);
      }
      
      if ((r = read(fd, buf, size)) < 0) {
	printf("%s: read failed %d %d\n", prog_name, r, errno);
	exit(1);
      }
      
      if ((r = close(fd)) < 0) {
	printf("%s: close failed %d %d\n", prog_name, r, errno );
      }
      
    }
    elapsed = stop();
    
    printf("read: %d %d-byte files in %f sec = %f KB/sec\n",  
	   n, size, elapsed, (n*size)/(1000*elapsed));
}

int delete_test(int n)
{	
    int i;
    int r;
    int fd;
    int j;
    double elapsed;

    start();
    for (i = 0, j = 0; i < n; i ++) {
      
      j = i % NDIR;
      sprintf(name, "d%d/g%d", j, i);
      
      if ((r = unlink(name)) < 0) {
	printf("%s: unlink failed %d\n", prog_name, r);
	exit(1);
      }
      
    }

    elapsed = stop();
    printf("delete: %d files in %f sec = %f files/sec\n",  n, elapsed,  n/elapsed);
}


int main(int argc, char *argv[])
{
    int n;
    int size;
    int n_flush_files;

    prog_name = argv[0];

    if (argc != 4) {
	printf("%s: <num files> <file size (in bytes)> <RAM in GB>\n", prog_name);
	exit(1);
    }

    n = atoi (argv[1]);
    size = atoi (argv[2]);
    n_flush_files = atoi(argv[3]) / 2;

    printf ("creating a few temporary files used to flush the cache: ");
    fflush (stdout);
    creat_dir ();
    create_flush_file (n_flush_files);
    printf ("done.\n");

    printf("small file benchmark: %d %d-byte files\n", n, size);

    creat_test (n, size);
    flush_cache (n_flush_files);
    read_test (n, size);
    flush_cache (n_flush_files);
    delete_test (n);

    clean_dir (n_flush_files);
    unlink("t");
}

