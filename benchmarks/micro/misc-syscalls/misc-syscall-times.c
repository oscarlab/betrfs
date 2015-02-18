/*
 * DEP 2/29/08: This measures the time required for various methods of
 * making a system call on a system
 */

#include <stdio.h>
#include <elf.h>
#include <asm/unistd.h>
#include <stdint.h>
#include <math.h>
#include <stdlib.h>
#include "syscall.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>

#define TMP_FILE "/mnt/benchmark/tmpfile"
#define TMP_DIR "/mnt/benchmark/tmpdir"

static void usage(void)
{
    printf(
    "usage:\n"
    "    -a, access.\n"
    "    -s, stat.\n"
    "    -o, open.\n"
    "    -u, unlink.\n"
    "    -m, mkdir.\n"
    "    -r, read.\n"
    "    -w, write.\n"
    "    -c, chmod.\n"

    );
}

static void create_tempf(void)
{
	int fd = open(TMP_FILE, O_CREAT|O_RDWR, S_IRWXU|S_IRWXG|S_IRWXO);
	if (fd < 0) {
		printf("Could not create temp file %s (%d). aborting\n",
		       TMP_FILE, errno);
		exit(fd);
	}
}

static void delete_tempf(void)
{
	int ret = unlink(TMP_FILE);
	if (ret < 0) {
		printf("Could not delete temp file %s (%d). aborting\n",
		       TMP_FILE, errno);
		exit(ret);
	}
}

static void create_tempd(void)
{
	int ret = mkdir(TMP_DIR, S_IRWXU | S_IRWXG | S_IRWXO);
	if (ret < 0) {
		printf("Could not create temp dir %s (%d). aborting\n",
		       TMP_DIR, errno);
		exit(ret);
	}
}

static void delete_tempd(void)
{
	int ret = rmdir(TMP_DIR);
	if (ret < 0) {
		printf("Could not delete temp dir %s (%d). aborting\n",
		       TMP_DIR, errno);
		exit(ret);
	}
}


int main(int argc, char* argv[], char* envp[]){
  uint64_t start = 0, end = 0;
  uint64_t total = 0, count = 0;
  uint64_t samples[ITERATIONS];
  uint64_t avg = 0;
  double stdev = 0.0;
  char path[256];
  char buf[256];
  int fd;
  struct stat stat_buf;
  int rv, rv2;



  if ((argc > 1) && (argv[1][0] == '-')) {
	  switch (argv[1][1]) {
	  case 'a': // access
		  create_tempf();
		  exp(1, access(TMP_FILE, R_OK), 1, "access          ");
		  delete_tempf();
		  break;
	  case 's': // stat
		  create_tempf();
		  exp(1, stat(TMP_FILE, &stat_buf), 1, "stat            ");
		  delete_tempf();
		  break;

	  case 'o': // open
		  create_tempf();
		  exp(1, rv = open(TMP_FILE, 0), close(rv), "open            ");
		  delete_tempf();
		  break;

	  case 'u': // unlink
		  create_tempd();
		  exp({ sprintf(path, TMP_DIR "/%" PRIu64, count); rv = open(path, O_CREAT); close(rv);}, unlink(path), 1, "unlink          ");
		  delete_tempd();
		  break;

	  case 'm': // mkdir
		  create_tempd();
		  exp(sprintf(path, TMP_DIR "/%" PRIu64, count), mkdir(path, 0600), rmdir(path), "mkdir           ");
		  delete_tempd();
		  break;

	  case 'r': // read
		  create_tempd();
		  fd = open(TMP_DIR"/foo", O_CREAT|O_RDWR, S_IRWXU|S_IRWXG|S_IRWXO); write(fd, "A", 1);
		  exp({  lseek(fd, 0, SEEK_SET);}, read(fd, buf, 1), 1, "read            ");
		  close(fd); unlink(TMP_DIR"/foo");
		  delete_tempd();
		  break;

	  case 'w': // write
		  create_tempd();
		  fd = open(TMP_DIR"/foo", O_CREAT|O_RDWR, S_IRWXU|S_IRWXG|S_IRWXO); 
		  exp( 1, write(fd, "A", 1), 1, "write           ");
		  close(fd); unlink(TMP_DIR"/foo");
		  delete_tempd();
		  break;

	  case 'c': //chmod
		  create_tempd();
		  exp({ sprintf(path, TMP_DIR "/%" PRIu64, count); rv = open(path, O_CREAT);}, chmod(path, 0600), { close(rv); unlink(path);}, "chmod           ");
		  delete_tempd();
		  break;
	  default:
		  printf("Wrong Argument (or format): %s\n", argv[1]);
		  usage();
	  }
  } else {
	  usage();
  }

  //exp(sprintf(path, "./tmp/%" PRIu64, count), symlink("/etc/passwd", path), unlink(path), "symlink         ");
  //exp(sprintf(path, "./tmp/%" PRIu64, count), link("/etc/passwd", path), unlink(path), "link         ");
  //exp({ sprintf(path, "./tmp/%" PRIu64, count); rv = open(path, O_CREAT);}, chmod(path, 0600), { close(rv); unlink(path);}, "chmod           ");
  // Doesn't work not as non-root.  screw it
  //exp({ sprintf(path, "./tmp/%" PRIu64, count); rv = open(path, O_CREAT);}, chown(path, 65534, 65534), { close(rv); unlink(path);}, "chown           ");
  //exp(sprintf(path, "./tmp/%" PRIu64, count), mknod(path, 0600, makedev(8, 1)), unlink(path), "mknod           ");
  //exp({sprintf(path, "./tmp/%" PRIu64, count); mkdir(path, 0600);}, rmdir(path), 1, "rmdir           ");
  //exp({ sprintf(path, "./tmp/%" PRIu64, count); rv =open(path, O_CREAT);}, rename(path, "./tmp/boo"), {close(rv); unlink("./tmp/boo");}, "rename          ");
  return 0;
}

/* linux-c-mode */
