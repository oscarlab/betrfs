#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <getopt.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "tokufs.h"

#define TOKUFS_MOUNT "compress-bench.mount"

static int verbose;
static int help;
static int use_posix;

static char * input_file;
static char * output_file = "compress-bench.out";

#define MIN(a, b) ((a) < (b) ? (a) : (b))

#define echo(...)                                   \
    do {                                            \
        printf(__VA_ARGS__);                        \
        fflush(stdout);                             \
    } while(0)

static struct option long_options[] =
{
    {"verbose", no_argument, &verbose, 1},
    {"help", no_argument, &help, 1},
    {"use-ufs", no_argument, &use_posix, 1},
    {"input-file", required_argument, NULL, 'f'},
    {"output-file", required_argument, NULL, 'o'},
};
static char * opt_string = "vhuf:o:";

static void usage(void)
{
    printf(
    "usage:\n"
    "    -v, --verbose\n"
    "        enable verbose output.\n"
    "    -h, --help\n"
    "        print this help and quit.\n"
    "    -u, --use-ufs\n"
    "        perform benchmark on the underlying file system\n"
    "    -f, --input-file\n"
    "        input file name.\n"
    "    -o, --input-file\n"
    "        output file name.\n"
    );
}

static int parse_args(int argc, char * argv[])
{
    int i, c;

    while ((c = getopt_long(argc, argv, 
                    opt_string , long_options, &i)) != -1) {
        switch (c) {
        case 0:
            break;
        case 'f':
            if (strlen(optarg) == 0) {
                fprintf(stderr, "invalid file name\n");
                return 1;
            }
            input_file = strdup(optarg);
            break;
        case 'h':
            help = 1;
            break;
        case 'o':
            if (strlen(optarg) == 0) {
                fprintf(stderr, "invalid file name\n");
                return 1;
            }
            output_file = strdup(optarg);
            break;
        case 'u':
            use_posix = 1;
            break;
        case 'v':
            verbose = 1;
            break;
        case '?':
        default:
            return 1;
        }
    }

    return 0;
}

/**
 * Get the current time in microseconds
 */
static long current_time_usec(void)
{
    struct timeval t;
    gettimeofday(&t, NULL);
    return t.tv_usec + t.tv_sec * 1000000;
}

/**
 * Make it so that switching between tokufs and ufs
 * is transparent to the benchmarking code.
 *
 * We then define each benchmark function in terms of  
 * benchmark file struct
 */
struct benchmark_file
{
    int (*open)(const char * path, int flags, ...);
    int (*close)(int fd);
    ssize_t (*pwrite)(int fd, const void * buf,
            size_t count, off_t offset);
    ssize_t (*pread)(int fd, void * buf, 
            size_t count, off_t offset);
    int (*fsync)(int fd);
};

static int posix_sync_and_close(int fd)
{
    int ret;
    ret = fsync(fd);
    assert(ret == 0);
    ret = close(fd);
    assert(ret == 0);
    return ret;
}

static struct benchmark_file posix_file =
{
    .open = open,
    .close = posix_sync_and_close,
    .pwrite = pwrite,
    .pread = pread
};

struct input_file_info {
    char * buf;
    size_t size;
};

static void read_input_file_into_memory(
        struct input_file_info * info)
{
    int ret, fd;
    struct stat st;

    fd = open(input_file, O_RDONLY);
    if (fd < 0 && errno == ENOENT) {
        printf("can't find input file %s\n", input_file);
        exit(1);
    }
    assert(fd >= 0);
    ret = fstat(fd, &st);
    assert(ret == 0);
    info->size = st.st_size;
    assert(info->size > 0);
    info->buf = malloc(info->size);
    if (info->buf == NULL) {
        printf("couldn't allocate %ld bytes to bring "
                " the file into memory\n", info->size);
        exit(1);
    }
    ret = pread(fd, info->buf, info->size, 0);
    assert(ret == (int) info->size);
    ret = close(fd);
    assert(ret == 0);
}

static long run_benchmark(struct benchmark_file * file)
{
    const int pagesize = getpagesize();
    int ret, fd;
    long start, write_time;
    struct input_file_info file_info;
    size_t bytes_written = 0;

    printf("reading file into memory...\n");
    read_input_file_into_memory(&file_info);

    printf("writing output file...\n");
    start = current_time_usec();
    int flags = O_CREAT | O_TRUNC | O_WRONLY;
    fd = file->open(output_file, flags, 0644);
    assert(fd >= 0);

    while (bytes_written < file_info.size) {
        const int iosize = 8 * pagesize;
        const int bytes_left = file_info.size - bytes_written;
        int write_size = MIN(bytes_left, iosize);
        ssize_t n = file->pwrite(fd, file_info.buf + bytes_written, 
                write_size, bytes_written);
        if (n != write_size) {
            printf("wrote %ld bytes, wanted %d, errno %d\n", n, write_size, errno);
        }
        assert(n == write_size);
        bytes_written += n;
    }

    ret = file->close(fd);
    assert(ret == 0);
    write_time = current_time_usec() - start;
    printf("done.\n");
    printf("time %ld usec, %lf MBs/sec\n", 
            write_time, file_info.size / (write_time * 1.0));

    free(file_info.buf);
    return write_time;
}

int main(int argc, char * argv[])
{
    int ret;
    struct benchmark_file * file;

    ret = parse_args(argc, argv);
    if (ret != 0 || help) {
        usage();
        exit(ret);
    }

    ret = toku_fs_set_cachesize(64L * 1024 * 1024);
    assert(ret == 0);

    echo("compression benchmark\n");
    echo(" * input file: %s\n", input_file);
    echo(" * output file: %s\n", output_file);
    echo(" * file op interface: %s\n", use_posix ? "posix" : "tokufs");
    echo(" * pagesize: %lu\n",
            use_posix ? (size_t) getpagesize() : toku_fs_get_blocksize());
    echo(" * verbose? %s\n", verbose ? "yes" : "no");

    file = &posix_file;

    run_benchmark(file);

    return 0;
}
