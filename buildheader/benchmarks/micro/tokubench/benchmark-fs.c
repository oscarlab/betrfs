#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <getopt.h>
#include <sys/time.h>
#include <time.h>

#include "tokufs.h"

#define TOKUFS_MOUNT "dumpfile.mount"

static int verbose;

static int help;
static int use_ufs;
static int do_drop_caches;

static int do_serial_read;
static int do_serial_write;
static int do_random_read;
static int do_random_write;

static size_t num_records = 256 * 1024;
static size_t record_size = 512;
static size_t compressibility = 1;
static size_t cachesize_mb = 128;

#define RANDOM_TABLE_SIZE   (32*1024*1024)
static char * random_table;

static char * output_file = "dumpfile";

#define RECORD_MAGIC_BYTE   123
#define FILE_SIZE (num_records * record_size)

static size_t * bytes_done_so_far;
static long * operation_start_time;

#define echo(...)                                   \
    do {                                            \
        printf(__VA_ARGS__);                        \
        fflush(stdout);                             \
    } while(0)

#define verbose_echo(...)                           \
    do {                                            \
        if (verbose) {                              \
            echo("-- ");                            \
            echo(__VA_ARGS__);                      \
        }                                           \
    } while (0)

static struct option long_options[] =
{
    {"verbose", no_argument, &verbose, 1},
    {"help", no_argument, &help, 1},
    {"drop-caches", no_argument, &do_drop_caches, 1},
    {"file-name", required_argument, NULL, 'f'},
    {"num-records", required_argument, NULL, 'n'},
    {"record-size", required_argument, NULL, 'x'},
    {"compressibility", required_argument, NULL, 'c'},
    {"output-file", required_argument, NULL, 'o'},
    {"cache-size-mb", required_argument, NULL, 'm'},
    {"serial-read", no_argument, &do_serial_read, 1},
    {"serial-write", no_argument, &do_serial_write, 1},
    {"random-read", no_argument, &do_random_read, 1},
    {"random-write", no_argument, &do_random_write, 1}
};
static char * opt_string = "vhudf:n:x:o:t:m:";

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
    "    -d, --drop-caches\n"
    "        try to drop caches between runs. usually requires root.\n"
    "    -f, --file-name\n"
    "        target file name.\n"
    "    -n, --num-records\n"
    "        number of records to read/write.\n"
    "    -x, --record-size\n"
    "        size of each record.\n"
    "    -c, --compressibility\n"
    "        populate records with data that a compression algorithm\n"
    "        would likely reduce by a given factor\n"
    "    -m, --cache-size-mb\n"
    "        set the cache size in mb for non ufs runs\n"
    "    --serial-read\n"
    "        perform the serial read benchmark. target file required\n"
    "        if no write benchmark is specified\n"
    "    --serial-write\n"
    "        perform the serial write benchmark\n"
    "    --random-read\n"
    "        pefform the random read benchmark. target file required\n"
    "        if no write benchmark is specified\n"
    "    --random-write\n"
    "        perform the random write benchmark\n"
    "Note: If none of serial/random read/write are specified,\n"
    "      all are assumed.\n"
    );
}

static int parse_args(int argc, char * argv[])
{
    int i, c;
    long n;

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
            output_file = strdup(optarg);
            break;
        case 'h':
            help = 1;
            break;
        case 'n':
            n = atol(optarg);
            if (n <= 0) {
                fprintf(stderr, "number of records must be > 0\n");
                return 1;
            }
            num_records = n;
            break;
        case 'c':
            n = atol(optarg);
            if (n < 1) {
                fprintf(stderr, "compressibility must be >= 1\n");
                fprintf(stderr, "ie: a compressibility of 4 means the"
                        "data is roughly 1/4 the size when compressed\n");
                return 1;
            }
            compressibility = n;
            break;
        case 'm':
            cachesize_mb = atol(optarg);
            break;
        case 'v':
            verbose = 1;
            break;
        case 'd':
            do_drop_caches = 1;
            break;
        case 'x':
            n = atol(optarg);
            if (n <= 0) {
                fprintf(stderr, "record size must be > 0\n");
                return 1;
            }
            record_size = n;
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
static long current_time_us(void)
{
    struct timeval t;
    gettimeofday(&t, NULL);
    return t.tv_usec + t.tv_sec * 1000000;
}

/**
 * Get a string representing the current date.
 * The returned string needs to be freed by the caller.
 */
static char * date_string(void)
{
    time_t now;
    char * date;
    char * newline;

    now = time(NULL);
    date = ctime(&now);
    date = strdup(date);
    
    /* at least one ctime implementation is putting on its
     * own newline char, for some reason. */
    newline = strchr(date, '\n');
    if (newline != NULL) {
        *newline = '\0';
    }

    return date;
}

/**
 * Generate a random int
 */
static int random_int(void)
{
    int r;

    r = rand();

    return r;
}

/**
 * Generate a random long
 */
static long random_long(void)
{
    long r1, r2;

    r1 = rand();
    r2 = rand();

    return r1 | (r2 << 32);
}

/**
 * Generate a random permutation of uint64s
 */
static uint64_t * random_perm(uint64_t n)
{
    uint64_t i, j, tmp;
    uint64_t * perm;

    perm = malloc(sizeof(uint64_t) * n);
    for (i = 0; i < n; i++) {
        perm[i] = i;
    }
    for (i = n - 1; i > 0; i--) {
        j = random_long() % (i + 1);
        tmp = perm[i];
        perm[i] = perm[j];
        perm[j] = tmp;
    }

    return perm;
}

/**
 * Aggregate a bunch of strings into one, separated by spaces.
 */
static char * str_aggregate(int count, char ** strings)
{
    int i, length;
    char * buf;

    length = 0;
    for (i = 0; i < count; i++) {
        length += (strlen(strings[i]) + 1);
    }
    length++;

    buf = malloc(sizeof(char) * (length + 1));
    memset(buf, 0, length + 1);
    for (i = 0; i < count; i++) {
        strcat(buf, strings[i]);
        strcat(buf, " ");
    }

    return buf;
}

/**
 * Asks the virtual memory system to drop recently
 * accessed memory in the page file, I believe.
 */
static int drop_caches(void)
{
    int ret;

    ret = system("echo 3 > /proc/sys/vm/drop_caches");

    return ret;
}

/**
 * TokuDB compresses redundant bytes, so we need a way
 * to extract some reasonable random bytes quickly
 * during each benchmark iteration. Here, we generate
 * a big old table of random stuff to later grab from for
 * this purpose.
 */
static void init_random_table(void)
{
    size_t i;
    int * rand_ints;

    assert(RANDOM_TABLE_SIZE % sizeof(int) == 0);

    random_table = malloc(RANDOM_TABLE_SIZE);
    rand_ints = (int *) random_table;

    /* Generate randomness 4 bytes at a time. */
    for (i = 0; i < RANDOM_TABLE_SIZE / sizeof(int); i++) {
        if (i % compressibility == 0) {
            rand_ints[i] = 0; 
        } else {
            rand_ints[i] = random_int();
        }
    }
}

/**
 * Populate some subset of the record with stuff
 * from a random spot in the random table. Then,
 * fill the rest with the magic byte. The idea is that
 * this particular record will have its magic bytes
 * compressed to almost nothing, and the random bytes
 * will be uncompressed, giving us a record which
 * is roughly compressible by a tweakable factor.
 */
static char * get_write_buf()
{
    int r = random_int() % (RANDOM_TABLE_SIZE - record_size);
    return &random_table[r];
#if 0
    size_t offset, threshold;

    threshold = record_size / compressibility;
    offset = random_long() % (RANDOM_TABLE_SIZE - threshold);

    memcpy(record, random_table + offset, threshold);
#ifdef DUMPRECORD
    printf("record [");
    for (unsigned i = 0; i < threshold; i++) {
        printf("%x ", *((unsigned char *) record + i));
    }
    getchar();
#endif
    memset(record + threshold, 
            RECORD_MAGIC_BYTE, record_size - threshold);
#endif
}

/* TODO: Determine a good way to verify a record,
 * in light of the fact that it may be partly random. */
static int verify_record(void * record)
{
    (void) record;
#if 0
    size_t i;
    unsigned char expected;
    unsigned char * cbuf = record;

    for (i = 0; i < record_size; i++) {
        expected = i % 100;
        if (cbuf[i] != expected) {
            fprintf(stderr, "%s: byte %lu got %d wanted %d\n",
                    __FUNCTION__, i, cbuf[i], expected);
        }
        //assert(cbuf[i] == i % 100);
    }
#endif
    return 0;
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
    char * path;
    int (*open)(const char * path, int flags, ...);
    int (*close)(int fd);
    ssize_t (*pwrite)(int fd, const void * buf,
            size_t count, off_t offset);
    ssize_t (*pread)(int fd, void * buf, 
            size_t count, off_t offset);
};

struct benchmark_times
{
    long open_time;
    long io_time;
    long close_time;
};

static long benchmark_serial_write(struct benchmark_file * file,
        struct benchmark_times * times)
{
    int fd;
    long op_start, start, now;
    ssize_t n;
    size_t i, bytes_written;
    char * record;

    if (do_drop_caches) {
        assert(drop_caches() == 0);
    }

    verbose_echo("%s: opening %s\n", __FUNCTION__, file->path);
    start = current_time_us();

    fd = file->open(file->path, O_WRONLY | O_CREAT, 0644);
    assert(fd >= 0);

    now = current_time_us();
    times->open_time = now - start;
    verbose_echo("%s: done\n", __FUNCTION__);

    op_start = current_time_us();

    operation_start_time = &op_start;
    bytes_done_so_far = &bytes_written;

    for (i = 0, bytes_written = 0; i < num_records; i++) {
        record = get_write_buf();
        n = file->pwrite(fd, record,
                record_size, i * record_size);
        assert(n == (ssize_t) record_size);
        if (i > 0 && i % (num_records/20) == 0) {
            now = current_time_us() - op_start;
            verbose_echo("wrote %ld bytes so far, throughput %.3lf\n",
                    bytes_written, (bytes_written * 1.0 / now));
        }
        bytes_written += n;
    }

    assert(bytes_written == FILE_SIZE);
    now = current_time_us();
    times->io_time = now - op_start;

    /* Clear the global bytes_done and operation time pointers */
    bytes_done_so_far = NULL;
    operation_start_time = NULL;

    verbose_echo("%s: closing %s\n", __FUNCTION__, file->path);
    op_start = current_time_us();

    file->close(fd);
    now = current_time_us();
    times->close_time = now - op_start;
    verbose_echo("%s: done\n", __FUNCTION__);

    return now - start;
}

static long benchmark_serial_read(struct benchmark_file * file,
        struct benchmark_times * times)
{
    int fd;
    long op_start, start, now;
    ssize_t n;
    size_t i, bytes_read;
    unsigned char * record = malloc(record_size);

    if (do_drop_caches) {
        assert(drop_caches() == 0);
    }

    verbose_echo("%s: opening %s\n", __FUNCTION__, file->path);
    start = current_time_us();

    fd = file->open(file->path, O_RDONLY);
    assert(fd >= 0);

    now = current_time_us();
    times->open_time = now - start;
    verbose_echo("%s: done\n", __FUNCTION__);

    op_start = current_time_us();

    operation_start_time = &op_start;
    bytes_done_so_far = &bytes_read;

    for (i = 0, bytes_read = 0; i < num_records; i++) {
        n = file->pread(fd, record,
                record_size, i * record_size);
        if (n != (ssize_t) record_size) {
            printf("i = %lu, bytes_read %lu, n = %ld, record_size %ld\n",
                    i, bytes_read, n, record_size);
        }
        assert(n == (ssize_t) record_size);
        assert(verify_record(record) == 0);
        if (i > 0 && i % (num_records/20) == 0) {
            now = current_time_us() - op_start;
            verbose_echo("read %ld bytes so far, throughput %.3lf\n",
                    bytes_read, (bytes_read * 1.0 / now));
        }
        bytes_read += n;
    }

    free(record);

    assert(bytes_read == FILE_SIZE);
    now = current_time_us();
    times->io_time = now - op_start;

    /* Clear the global bytes_done and operation time pointers */
    bytes_done_so_far = NULL;
    operation_start_time = NULL;

    verbose_echo("%s: closing %s\n", __FUNCTION__, file->path);
    op_start = current_time_us();

    file->close(fd);
    now = current_time_us();
    times->close_time = now - op_start;
    verbose_echo("%s: done\n", __FUNCTION__);

    return now - start;
}

static long benchmark_random_write(struct benchmark_file * file,
        struct benchmark_times * times)
{
    int fd;
    long op_start, start, now;
    ssize_t n;
    size_t i, bytes_written, * offsets;
    char * record;

    offsets = random_perm(num_records);

    if (do_drop_caches) {
        assert(drop_caches() == 0);
    }

    verbose_echo("%s: opening %s\n", __FUNCTION__, file->path);
    start = current_time_us();

    fd = file->open(file->path, O_WRONLY | O_CREAT, 0644);
    assert(fd >= 0);

    now = current_time_us();
    times->open_time = now - start;
    verbose_echo("%s: done\n", __FUNCTION__);
    
    op_start = current_time_us();

    operation_start_time = &op_start;
    bytes_done_so_far = &bytes_written;

    for (i = 0, bytes_written = 0; i < num_records; i++) {
        record = get_write_buf();
        n = file->pwrite(fd, record,
                record_size, offsets[i] * record_size);
        assert(n == (ssize_t) record_size);
        if (i > 0 && i % (num_records/20) == 0) {
            now = current_time_us() - op_start;
            verbose_echo("wrote %ld bytes so far, throughput %.3lf\n",
                    bytes_written, (bytes_written * 1.0 / now));
        }
        bytes_written += n;
    }

    assert(bytes_written == FILE_SIZE);
    now = current_time_us();
    times->io_time = now - op_start;

    /* Clear the global bytes_done and operation time pointers */
    bytes_done_so_far = NULL;
    operation_start_time = NULL;

    free(offsets);

    verbose_echo("%s: closing %s\n", __FUNCTION__, file->path);
    op_start = current_time_us();

    file->close(fd);
    now = current_time_us();
    times->close_time = now - op_start;
    verbose_echo("%s: done\n", __FUNCTION__);

    return now - start;
}

static long benchmark_random_read(struct benchmark_file * file,
        struct benchmark_times * times)
{
    int fd;
    long op_start, start, now;
    ssize_t n;
    size_t i, bytes_read, * offsets;
    unsigned char * record = malloc(record_size);

    offsets = random_perm(num_records);

    if (do_drop_caches) {
        assert(drop_caches() == 0);
    }

    verbose_echo("%s: opening %s\n", __FUNCTION__, file->path);
    start = current_time_us();

    fd = file->open(file->path, O_RDONLY);
    assert(fd >= 0);
    now = current_time_us();
    times->open_time = now - start;
    verbose_echo("%s: done\n", __FUNCTION__);
    
    op_start = current_time_us();

    operation_start_time = &op_start;
    bytes_done_so_far = &bytes_read;

    for (i = 0, bytes_read = 0; i < num_records; i++) {
        n = file->pread(fd, record,
                record_size, offsets[i] * record_size);
        assert(n == (ssize_t) record_size);
        assert(verify_record(record) == 0);
        if (i > 0 && i % (num_records/20) == 0) {
            now = current_time_us() - op_start;
            verbose_echo("read %ld bytes so far, throughput %.3lf\n",
                    bytes_read, (bytes_read * 1.0 / now));
        }
        bytes_read += n;
    }

    free(record);

    assert(bytes_read == FILE_SIZE);
    now = current_time_us();
    times->io_time = now - op_start;

    /* Clear the global bytes_done and operation time pointers */
    bytes_done_so_far = NULL;
    operation_start_time = NULL;

    free(offsets);

    verbose_echo("%s: closing %s\n", __FUNCTION__, file->path);
    op_start = current_time_us();

    file->close(fd);
    now = current_time_us();
    times->close_time = now - op_start;
    verbose_echo("%s: done\n", __FUNCTION__);

    return now - start;
}

static struct benchmark_file ufs_file =
{
    .open = open,
    .close = close,
    .pwrite = pwrite,
    .pread = pread
};

static void run_benchmarks(struct benchmark_file * file)
{
    char * date;
    long sw_total_time, sr_total_time, rw_total_time, rr_total_time;
    struct benchmark_times sw_times, sr_times, rw_times, rr_times;

    init_random_table();

    sw_total_time = sr_total_time = rw_total_time = rr_total_time = 0;
    memset(&sw_times, 0, sizeof(struct benchmark_times));
    memset(&sr_times, 0, sizeof(struct benchmark_times));
    memset(&rw_times, 0, sizeof(struct benchmark_times));
    memset(&rr_times, 0, sizeof(struct benchmark_times));

    date = date_string();
    echo("Benchmarks starting: %s\n", date);
    free(date);

    if (do_serial_write) {
        sw_total_time = benchmark_serial_write(file, &sw_times);
    }

    if (do_random_write) {
        rw_total_time = benchmark_random_write(file, &rw_times);
    }

    if (do_serial_read) {
        sr_total_time = benchmark_serial_read(file, &sr_times);
    }

    if (do_random_read) {
        rr_total_time = benchmark_random_read(file, &rr_times);
    }

    date = date_string();
    echo("Benchmarks completed: %s\n", date);
    free(date);

    if (do_serial_write) {
        echo("Serial write times:\n");
        echo(" * open:           %ld\n", sw_times.open_time);
        echo(" * io:             %ld\n", sw_times.io_time);
        echo(" * close:          %ld\n", sw_times.close_time);
        echo(" * total:          %ld\n", sw_total_time);
        echo(" * io throughput:  %lf MB/s\n", 
                FILE_SIZE / (sw_times.io_time * 1.0));
        echo(" * effective:      %lf MB/s\n",
                FILE_SIZE / (sw_total_time * 1.0));
    }

    if (do_random_write) {
        echo("Random write times:\n");
        echo(" * open:           %ld\n", rw_times.open_time);
        echo(" * io:             %ld\n", rw_times.io_time);
        echo(" * close:          %ld\n", rw_times.close_time);
        echo(" * total:          %ld\n", rw_total_time);
        echo(" * io throughput:  %lf MB/s\n", 
                FILE_SIZE / (rw_times.io_time * 1.0));
        echo(" * effective:      %lf MB/s\n",
                FILE_SIZE / (rw_total_time * 1.0));
    }

    if (do_serial_read) {
        echo("Serial read times:\n");
        echo(" * open:           %ld\n", sr_times.open_time);
        echo(" * io:             %ld\n", sr_times.io_time);
        echo(" * close:          %ld\n", sr_times.close_time);
        echo(" * total:          %ld\n", sr_total_time);
        echo(" * io throughput:  %lf MB/s\n", 
                FILE_SIZE / (sr_times.io_time * 1.0));
        echo(" * effective:      %lf MB/s\n",
                FILE_SIZE / (sr_total_time * 1.0));
    }

    if (do_random_read) {
        echo("Random read times:\n");
        echo(" * open:           %ld\n", rr_times.open_time);
        echo(" * io:             %ld\n", rr_times.io_time);
        echo(" * close:          %ld\n", rr_times.close_time);
        echo(" * total:          %ld\n", rr_total_time);
        echo(" * io throughput:  %lf MB/s\n", 
                FILE_SIZE / (rr_times.io_time * 1.0));
        echo(" * effective:      %lf MB/s\n",
                FILE_SIZE / (rr_total_time * 1.0));
    }
}

static void handle_sigusr1(int sig)
{
    size_t bytes_done;
    long op_start;
    long now = current_time_us();

    assert(sig == SIGUSR1);

    if (bytes_done_so_far != NULL && operation_start_time != NULL) {
        bytes_done = *bytes_done_so_far;
        op_start = *operation_start_time;
        printf("bytes done %lu, throughput %.3lf\n",
                bytes_done, (bytes_done / (now - op_start * 1.0)));
        
    } else {
        printf("Nothing happening.\n");
    }
}

static void setup_signal_handlers(void)
{
    int ret;
    struct sigaction sigact;
    sigset_t signals;

    memset(&sigact, 0, sizeof(sigact));
    sigemptyset(&signals);
    sigaddset(&signals, SIGUSR1);

    sigact.sa_handler = handle_sigusr1;
    sigact.sa_mask = signals;

    ret = sigaction(SIGUSR1, &sigact, NULL);
    assert(ret == 0);
}

int main(int argc, char * argv[])
{
    int ret;
    int preference;
    char * invocation_str, host_name[256];
    struct benchmark_file * file;

    ret = parse_args(argc, argv);
    if (ret != 0 || help) {
        usage();
        exit(ret);
    }

    setup_signal_handlers();

    srand(current_time_us());
    invocation_str = str_aggregate(argc, argv);
    ret = gethostname(host_name, 256);
    assert(ret == 0);

    /* If no preference is given, do serial write and read. */
    preference = do_serial_read || do_serial_write
                     || do_random_read || do_random_write;

    if (!preference) {
        do_serial_read = 1;
        do_serial_write = 1;
    }

    ret = toku_fs_set_cachesize(cachesize_mb * 1024 * 1024);

    if (ret != 0) {
        printf("Couldn't set the cache size to %lu\n",
                cachesize_mb * 1024 * 1024);
        exit(-1);
    }

    echo("File system benchmark\n");
    echo("invoked via: %s\n", invocation_str);
    echo(" * pid: %d\n", getpid());
    echo(" * Hostname: %s\n", host_name);
    echo(" * Number of records: %lu\n", 
            num_records < 20 ? (num_records = 20) : num_records);
    echo(" * Record size: %lu\n", record_size);
    echo(" * File size: %lu\n", FILE_SIZE);
    echo(" * Compressibility: %lu\n", compressibility);
    echo(" * Target file name: %s\n", output_file);
    echo(" * Target file system: %s\n", use_ufs ? "ufs" : "TokuFS");
    echo(" * Page size: %lu\n",
            use_ufs ? (size_t) getpagesize() : toku_fs_get_blocksize());
    if (!use_ufs) {
    echo(" * Cache size: %lu MB\n", cachesize_mb);
    echo(" * Underlying store: TokuFS\n");
    }
    echo(" * Verbose? %s\n", verbose ? "yes" : "no");
    echo(" * Benchmarking serial write? %s\n",
            do_serial_write ? "yes" : "no");
    echo(" * Benchmarking random write? %s\n",
            do_random_write ? "yes" : "no");
    echo(" * Benchmarking serial read? %s\n",
            do_serial_read ? "yes" : "no");
    echo(" * Benchmarking random read? %s\n", 
            do_random_read ? "yes" : "no");

    free(invocation_str);

    file = &ufs_file;
    file->path = output_file;
 
    run_benchmarks(file);

    long start = current_time_us();
    //unmount here
    long end = current_time_us();
    echo("Unmount time: %ld\n", end - start);

    free(random_table);

    return 0;
}
