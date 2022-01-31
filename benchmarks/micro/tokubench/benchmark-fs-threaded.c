/**
 * TokuFS
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <pthread.h>
#include <getopt.h>
#include <dirent.h>
#include <errno.h>
#include <syscall.h>

#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <tokufs.h>
//#include <toku/debug.h>
//#include <toku/str.h>
//#include <toku/random.h>

#include "threadpool.h"

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

#define progress_printf(...) do { printf(__VA_ARGS__); fflush(stdout); } while(0)

int toku_debug;

static int verbose;
static int report_progress;
static int help;

static char * benchmark_root_dir = "benchmark-bucket";
static size_t iosize = 512;
static size_t cachesize = 512L * 1024 * 1024;

static int num_files = 1;
static int num_threads = 1;
static int num_operations = 1;
static int directory_max_children = 100;
static int file_read_flags = O_RDONLY;
static int file_write_flags = O_CREAT | O_WRONLY;

// do serial pwrites  by default
static int do_pwrite = 1;
static int do_serial = 1;
static int do_scan = 0;

static struct option long_options[] =
{
    {"verbose", no_argument, &verbose, 1},
    {"progress", no_argument, &report_progress, 1},
    {"help", no_argument, &help, 1},
    {"dir", required_argument, NULL, 'f'},
    {"cachesize", required_argument, NULL, 'c'},
    {"files", required_argument, NULL, 'n'},
    {"children", required_argument, NULL, 'd'},
    {"threads", required_argument, NULL, 'm'},
    {"iosize", required_argument, NULL, 'b'},
    {"operations", required_argument, NULL, 'x'},
    {"scan", no_argument, &do_scan, 1},
    {"serial", no_argument, &do_serial, 1},
    {"random", no_argument, &do_serial, 0},
    {"pwrite", no_argument, &do_pwrite, 1},
    {"pread", no_argument, &do_pwrite, 0},
};
static char * opt_string = "vhc:f:s:n:d:m:b:x:";

static void usage(void)
{
    printf(
    "usage:\n"
    "    -v, --verbose\n"
    "        enable verbose output.\n"
    "    -p, --progress\n"
    "        enable progress reporting, showing throughput at 20 intervals.\n"
    "    -h, --help\n"
    "        print this help and quit.\n"
    "    -f, --dir\n"
    "        directory where the benchmark should take place.\n"
    "    -c, --cachesize\n"
    "        set the cache size (MB) for tokufs runs. default 512\n"
    "    -n, --files\n"
    "        number of files to operate on. default 1.\n"
    "    -d, --children\n"
    "        maximum number of children in a single directory. default 100.\n"
    "    -m, --threads\n"
    "        number of concurrent threads. default 1.\n"
    "    -b, --iosize\n"
    "        size of each thread's read or write thread_operation. default 512.\n"
    "    -x, --operations\n"
    "        number of operations per thread. default 1.\n"
    "    -s, --scan\n"
    "        just perform a metadata scan on an existing benchmark-bucket\n"
    "    --serial\n"
    "        perform sequential IO on each file. default.\n"
    "    --random\n"
    "        perform random IO on each file\n"
    "    --pwrite\n"
    "        perform pwrites on each file\n"
    "    --pread\n"
    "        perform preads on each file\n"
    );
}

static int parse_args(int argc, char * argv[])
{
    int i, c;
    long n;

    while ((c = getopt_long(argc, argv, 
                    opt_string, long_options, &i)) != -1) {
        switch (c) {
        case 'v':
            verbose = 1;
            break;
        case 'p':
            report_progress = 1;
            break;
        case 'h':
            help = 1;
            break;
        case 'f':
            if (strlen(optarg) == 0) {
                printf("invalid root directory\n");
                return 1;
            }
            benchmark_root_dir = optarg;
            break;
        case 'c':
            n = atol(optarg);
            if (n <= 32) {
                printf("cachesize shouldn't be less than 32mb\n");
                return 1;
            }
            cachesize = n * 1024 * 1024L;
            break;
        case 'n':
            n = atol(optarg);
            if (n <= 0) {
                printf("need to operate on at least one file\n");
                return 1;
            }
            num_files = n;
            break;
        case 'd':
            n = atol(optarg);
            if (n <= 0) {
                printf("need at least one file per directory\n");
                return 1;
            }
            directory_max_children = n;
            break;
        case 'm':
            n = atol(optarg);
            if (n <= 0) {
                printf("need to use at least one thread\n");
                return 1;
            }
            num_threads = n;
            break;
        case 'b':
            n = atol(optarg);
            if (n <= 0) {
                printf("io size needs to be > 0\n");
                return 1;
            }
            iosize = n;
            break;
        case 'x':
            n = atol(optarg);
            if (n < 0) {
                printf("number of operations needs to be >= 0.\n"
                       "0 means only open and close files\n");
                return 1;
            }
            num_operations = n;
            break;
        case 0:
            break;
        case '?':
            printf("?\n");
        default:
            return 1;
        }
    }

    return 0;
}

/**
 * file operation interface used by benchmark threads. should
 * make it easy to switch between the posix and tokufs interface.
 */
struct benchmark_file_ops
{
    int (*open)(const char * path, int flags, ...);
    int (*close)(int fd);
    ssize_t (*pwrite)(int fd, const void * buf, size_t count, off_t offset);
    ssize_t (*pread)(int fd, void * buf, size_t count, off_t offset);
    int (*unlink)(const char * path);
    int (*mkdir)(const char * path, mode_t mode);
    int (*rmdir)(const char * path);
};

static struct benchmark_file_ops posix_file =
{
    .open = open,
    .close = close,
    .pwrite = pwrite,
    .pread = pread,
    .unlink = unlink,
    .mkdir = mkdir,
    .rmdir = rmdir
};

/**
 * Generate a random long
 */
static long toku_random_long(void)
{
    long r1, r2;

    r1 = rand();
    r2 = rand();

    return r1 | (r2 << 32);
}

static char * generate_random_name(const char * parent)
{
    char * filename = malloc(strlen(parent) + 50);
    uint64_t k = toku_random_long();
    sprintf(filename, "%s/benchfile.%lu", parent, k);
    return filename;
}

static char * generate_subdir_name(const char * parent, int i)
{
    char * subdir = malloc(strlen(parent) + 50);
    sprintf(subdir, "%s/benchdir.%d", parent, i);
    return subdir;
}

struct leaf_directories {
    char ** array;
    int num_entries;
    int capacity;
};

static void shuffle_leaf_directories(struct leaf_directories * leaves)
{
    for (int i = leaves->num_entries - 1; i > 0; i--) {
        // the knuth shuffle. swap i with some random index in [0, i]
        long r = toku_random_long() % (i + 1);
        char * tmp = leaves->array[i];
        leaves->array[i] = leaves->array[r];
        leaves->array[r] = tmp;
    }
}

static void add_leaf_directory(struct leaf_directories * leaves, 
        const char * leaf)
{
    // if we're full, double the capacity and realloc
    // this is just easy to do. hopefully it doesn't result
    // in resident set explosion when there are many leaves.
    if (leaves->num_entries == leaves->capacity) {
        leaves->capacity *= 2;
        size_t new_size = sizeof(char *) * leaves->capacity;
        leaves->array = realloc(leaves->array, new_size);
        assert(leaves->array != NULL);
    }
    assert(leaves->num_entries < leaves->capacity);
    leaves->array[leaves->num_entries++] = strdup(leaf);
}

static void free_leaf_directories(struct leaf_directories * leaves)
{
    for (int i = 0; i < leaves->num_entries; i++) {
        free(leaves->array[i]);
    }
    free(leaves->array);
}

static void create_directory_tree_structure(struct benchmark_file_ops * file_ops, 
        const char * parent, int n, struct leaf_directories * leaves)
{
    // if this parent directory needs to be created with less than
    // the max number of children for a directory, then it is going
    // to be a leaf in the structure. add it to the array of leaves
    // and be done.
    if (n <= directory_max_children) {
        add_leaf_directory(leaves, parent);
        return;
    }

    // create directory_max_children in this directory and 
    // divide the files amongst them.
    const int files_per_child = n / directory_max_children;
    const int remaining_files = n % directory_max_children;
    for (int i = 0; i < directory_max_children && n > 0; i++) {
        int files_this_child;
        char * subdir = generate_subdir_name(parent, i);
        if (n < directory_max_children) {
            // if there are less files to create than the max 
            // for one child, give it all to the child
            files_this_child = n;
        } else if (files_per_child < directory_max_children) {
            // there are enough files to fill at least one child,
            // but not all of them, so max out this child.
            files_this_child = directory_max_children;
        } else {
            // there are enough files to fill all of the children.
            // give them each a fair share and dish out the remainder
            // on a first come first serve basis.
            files_this_child = files_per_child;
            files_this_child += i < remaining_files ? 1 : 0;
        }

        assert(n >= files_this_child);
        int ret = file_ops->mkdir(subdir, 0777);
        assert(ret == 0);
        // recursively create a directory tree rooted at
        // the current subdir with the given number of children
        create_directory_tree_structure(file_ops, subdir, 
                files_this_child, leaves);
        free(subdir);
        n -= files_this_child;
    }
    // maintain the invariant that all files were given out.
    // the loop breaks if n == 0 or if all children have
    // been created. 
    assert(n == 0);
}

/**
 * Get the current time in microseconds
 */
static long toku_current_time_usec(void)
{
    struct timeval t;
    gettimeofday(&t, NULL);
    return t.tv_usec + t.tv_sec * 1000000;
}


struct benchmark_directory_scan_info {
    uint64_t num_files;
    size_t total_size;
};

// call me once to initialize. call me again to track progress.
static void maybe_report_directory_scan_progress(int files_so_far, int * stop)
{
    // only check every 2000 files. that is as long as 10 second
    // on a slow file system.
    const int check_interval = 2000;
    const long time_interval = 60 * 1000 * 1000L;
    const long timeout = 60 * 60 * 1000 * 1000L;
    static long start, last_progress_report;
    if (last_progress_report == 0) {
        last_progress_report = toku_current_time_usec();
        start = last_progress_report;
        return;
    }
    if (files_so_far > 0 && files_so_far % check_interval == 0) {
        long now = toku_current_time_usec();
        if (now - last_progress_report > time_interval) {
            progress_printf("* progress: scanned %d files, %ld usec elapsed, (cumulative throughput: %lf files/second)\n",
                    files_so_far, now - start, 
                    files_so_far / ((now - start) / 1000000.0));
            last_progress_report = now;
        }
        // if it's been more than the timeout, let the caller
        // know it should stop - it's been running long enough.
        if (now - start > timeout) {
            *stop = 1;
        } else {
            *stop = 0;
        }
    }
}

static void benchmark_directory_scan_helper(char * parent,
        struct benchmark_directory_scan_info * info)
{
    int ret;
    DIR * dir;
    struct dirent * entry;
    struct stat st;
    
    // opendir can come back with ENFILES if our recursion
    // goes too deep, but it shouldn't.
    dir = opendir(parent);
    assert(dir != NULL);
    ret = chdir(parent);
    assert(ret == 0);

    // call it once to initialize the timers.
    maybe_report_directory_scan_progress(0, NULL);
    int stop = 0;
    while ((entry = readdir(dir)) != NULL && !stop) {
        // skip self and parent
        if (strcmp(entry->d_name, ".") == 0 || 
                strcmp(entry->d_name, "..") == 0) {
            continue;
        }

        int child_fd = open(entry->d_name, O_RDONLY);
        assert(child_fd >= 0);
        ret = fstat(child_fd, &st);
        assert(ret == 0);
        close(child_fd);
        if (S_ISDIR(st.st_mode)) {
            benchmark_directory_scan_helper(entry->d_name, info);
        } else {
            info->total_size += st.st_size;
        }
        info->num_files++;
        // this will report our progress on a reasonable interval,
        // and tell us whether or not we should stop because
        // we've taken too long.
        maybe_report_directory_scan_progress(info->num_files, &stop);
    }

    ret = chdir("..");
    assert(ret == 0);
    ret = closedir(dir);
    assert(ret == 0);
}

static void benchmark_directory_scan(struct benchmark_directory_scan_info *info)
{
    // here we smash the file_ops abstraction, because it doesn't
    // provide a consistent way of reading directories. that's
    // because tokufs has its own directory api.
    memset(info, 0, sizeof(*info));
    benchmark_directory_scan_helper(benchmark_root_dir, info);
}

enum thread_operation {
    RANDOM_PREAD,
    SERIAL_PREAD,
    RANDOM_PWRITE,
    SERIAL_PWRITE,
};

static int thread_operation_is_writing(enum thread_operation op)
{
    return op == SERIAL_PWRITE || op == RANDOM_PWRITE;
}

static int should_report_file_io_progress(int operation_num)
{
    const int interval = 20;
    if (!report_progress || operation_num < interval) {
        // don't if progress reporting is off or thread_operation is too low
        return 0;
    } else if (operation_num == num_operations) {
        // do it for the last thread_operation, for accurate terminal throughput
        return 1;
    } else {
        // otherwise do it on some evenly spaced interval.
        return operation_num % (num_operations / interval) == 0;
    }
}


static int toku_gettid(void)
{
    // too much hassle getting syscall to prototype correctly
    return syscall(SYS_gettid);
}

// HACK should be replaced by a struct benchmark_info that
// is contained in benchmark_thread_info
static long benchmark_start_time;
static void maybe_report_file_io_progress(enum thread_operation op,
					  int operation_num)
{
    if (should_report_file_io_progress(operation_num)) {
        // adding 1 is a shameless hack to avoid start == now
        long elapsed_time = toku_current_time_usec()+1 - benchmark_start_time;
        size_t num_bytes = operation_num * iosize;
        progress_printf("* progress: thread [%d] %s %lu bytes, %ld usec elapsed, (cumulative throughput %lf bytes/usec)\n",
			toku_gettid(),
			thread_operation_is_writing(op) ? "wrote" : "read",
			num_bytes, elapsed_time, num_bytes*1.0f / elapsed_time);
    }
}

static void maybe_report_file_create_progress(int i)
{
    const int interval = 10000;
    static int files_created;
    static long start, last_progress_report;
    if (last_progress_report == 0) {
        last_progress_report = toku_current_time_usec();
        start = last_progress_report;
        return;
    }
    // when some thread has reached a multiple of interval
    // files created, record it and report overall progress.
    if (i > 0 && i % interval == 0) {
        long elapsed_time = toku_current_time_usec() - start;
        (void) __sync_fetch_and_add(&files_created, interval);
        progress_printf("* progress: created %d files, %ld usec elapsed, (cumulative throughput %lf files/sec)\n",
			files_created, elapsed_time,
			files_created / (elapsed_time / 1000000.0));
    }
}

struct benchmark_thread_info {
    enum thread_operation op;
    int num_files;
    char ** directories;
    int num_directories;
    const struct benchmark_file_ops * file_ops;
};

static struct benchmark_thread_info * benchmark_thread_info_create(
        enum thread_operation op, int nfiles, 
        char ** directories, int num_directories, 
        const struct benchmark_file_ops * file_ops)
{
    struct benchmark_thread_info * info;
    info = malloc(sizeof(struct benchmark_thread_info));
    info->op = op;
    info->file_ops = file_ops;
    info->directories = directories;
    info->num_directories = num_directories;
    info->num_files = nfiles;
    return info;
}

static char * get_write_buf(void)
{
    static char * random_buf;
    static const size_t random_buf_size = 16L * 1024 * 1024;
    assert(iosize < random_buf_size);
    // initialize the random buffer if it is not already
    if (random_buf == NULL) {
        assert(random_buf_size % sizeof(uint64_t) == 0);
        random_buf = malloc(random_buf_size);
        assert(random_buf != NULL);
        for (unsigned i = 0; i < random_buf_size / sizeof(uint64_t); i++) {
            uint64_t * b = (uint64_t *) random_buf;
            b[i] = toku_random_long();
        }
    }
    // then give out a random index into the buffer
    // from 0 to the size of the random buffer, minus the iosize.
    uint64_t r = toku_random_long() % (random_buf_size - iosize);
    return &random_buf[r];
}

static void benchmark_thread(void * arg)
{
    int fd, ret;
    struct benchmark_thread_info * info = arg;

    for (int i = 0; i < info->num_files; i++) {
        // create the necessary number of files evently over
        // the number of directories by choosing the i'th directory
        // in a cicular loop over the directories array
        const char * parent = info->directories[i % info->num_directories];
        char * filename = generate_random_name(parent);

        fd = info->file_ops->open(filename, 
                thread_operation_is_writing(info->op) ?
                file_write_flags : file_read_flags, 0644);
        if (fd < 0) {
            int e = errno;
            printf("couldn't create file %s, ret %d, errno %d\n",
                    parent, fd, e);
            exit(fd);
        }
        assert(fd >= 0);
        free(filename);

        // do the file IO, if necessary
        for (int j = 0; j < num_operations; j++) {
            int k = j;
            ssize_t n, expected = iosize;
            char * buf = get_write_buf();
            switch (info->op) {
                case RANDOM_PREAD:
                    k = toku_random_long() % num_operations + j;
                case SERIAL_PREAD:
                    n = info->file_ops->pread(fd, buf, iosize, k * iosize);
                    break;
                case RANDOM_PWRITE:
                    k = toku_random_long() % num_operations + j;
                case SERIAL_PWRITE:
                    n = info->file_ops->pwrite(fd, buf, iosize, k * iosize);
                    break;
                default:
                    assert(0);
            }
            assert(n == expected);

            maybe_report_file_io_progress(info->op, j);
        }
        ret = info->file_ops->close(fd);
        assert(ret == 0);

        maybe_report_file_create_progress(i);
    }

    free(info);
}

static void do_meta_scan(void)
{
    long start, end;
    printf("Scanning the entire directory tree, counting children, total size\n");

    start = toku_current_time_usec();
    struct benchmark_directory_scan_info scan_info;
    benchmark_directory_scan(&scan_info);
    end = toku_current_time_usec();

    assert(scan_info.num_files > 0);
    printf("scanned %ld files in %ld usec, %lf files/second\n",
            scan_info.num_files, end - start, 
            scan_info.num_files / ((end - start) / 1000000.0));
    printf("total size of all files not including metadata %lu\n",
            scan_info.total_size);
}

static void run_benchmarks(struct benchmark_file_ops * file_ops)
{
    int ret;
    struct threadpool tp;
    enum thread_operation op;
    long start;

    // XXX still ugly, but better
    if (do_scan) {
        do_meta_scan();
        return;
    }

    ret = toku_threadpool_init(&tp, num_threads);

    assert(ret == 0);
    if (do_pwrite) {
        op = do_serial ? SERIAL_PWRITE : RANDOM_PWRITE;
    } else {
        op = do_serial ? RANDOM_PREAD : RANDOM_PREAD;
    }

    const char * parent = benchmark_root_dir;

    // first create the directory structure and record all 
    // the leaf directories the benchmark will put its
    // files into.
    struct leaf_directories leaves = {
        .array = malloc(sizeof(char *) * 8),
        .capacity = 8,
        .num_entries = 0
    };
   
    // create the top level directory, then create the structure
    // underneath it
    ret = file_ops->mkdir(parent, 0777);
    if (ret != 0) {
        int e = errno;
        printf("couldn't make directory %s, ret %d, errno %d\n",
                parent, ret, e);
        exit(ret);
    }
    assert(ret == 0);
    create_directory_tree_structure(file_ops, parent, 
            num_files, &leaves);

    printf("created directory structure with %d leaves\n", 
            leaves.num_entries);

    // shuffle the leaf directories so when we iterate over
    // them, it is random.
    shuffle_leaf_directories(&leaves);

    // to help maintain invariants
    int total_files = 0;
    int total_directories = 0;

    // call this once to initialize interal timers and counters
    maybe_report_file_create_progress(0);
    start = toku_current_time_usec();

    struct benchmark_thread_info * info;
    char ** directories = NULL;
    for (int i = 0; i < num_threads; i++) {
        // each thread will create its share of files in some subset
        // of the the leaf directories. determine which directories 
        // this thread will operate over.
        //
        // the outcome of this condition is the same at every
        // iteration of the loop. the code probably wouldn't work otherwise
        int num_directories;
        if (num_threads > leaves.num_entries) {
            // there are more threads than directories, so we will give
            // each thread one directory. the directory given is the
            // i'th directory in a cicular loop over the leaves
            num_directories = 1;
            directories = &leaves.array[i % leaves.num_entries];
            // record this only if we haven't used the directory yet
            total_directories += i < leaves.num_entries ? 1 : 0;
        } else {
            // there are at least as many directories as threads, so we
            // can give each thread its divided share.
            // the remainder is given out first come first serve
            num_directories = leaves.num_entries / num_threads;
            num_directories += i < leaves.num_entries % num_threads ? 1 : 0;
            directories = leaves.array + total_directories;
            total_directories += num_directories;
        }
        // determine how many files this thread will operate on
        // the remainder is given out first come first serve
        int nfiles = num_files / num_threads;
        assert(nfiles > 0); // already an invariant that num_threads <= num_files
        nfiles += i < num_files % num_threads ? 1 : 0;
        // create and dispatch a thread to do the work. to recap, this thread
        // creates nfiles evenly over num_directories whose paths are in the
        // array starting at directories. the op dictates what kind of
        // IO goes down and the file ops dictates which interface is used
        assert(num_directories <= num_files);
        info = benchmark_thread_info_create(op, 
                nfiles, directories, num_directories, file_ops);
        toku_threadpool_dispatch(&tp, benchmark_thread, info);
        total_files += nfiles;
    }

    // make sure that we used the correct number of leaf directories
    assert(total_files == num_files);
    assert(total_directories == leaves.num_entries);

    printf("waiting for work to finish...\n");
    ret = toku_threadpool_wait(&tp);
    assert(ret == 0);

    //sync();

    long elapsed_time = toku_current_time_usec() - start;
    printf("finished: time %ld usec\n", elapsed_time);
    // if there's more than one file, we probably care about
    // file creations per second
    if (num_files > 1) {
        double seconds = elapsed_time / 1000000.0;
        printf("%d files, %lf files/sec\n", 
                num_files,
                num_files / seconds);
    }
    // if there are data operations happening, we probably
    // care about byte throughput
    if (num_operations > 0) {
        long bytes = num_files * num_operations * iosize;
        printf("throughput: %lf MB/sec\n", bytes / (elapsed_time * 1.0));
    }

    if (num_files > 1 && num_operations > 0) {
        long bytes = num_files * num_operations * iosize;
        double seconds = elapsed_time / 1000000.0;
        printf("tokubench, %d, %lf, %lf\n", num_files, num_files / seconds,  bytes / (elapsed_time * 1.0));
    }

    ret = toku_threadpool_destroy(&tp);
    free_leaf_directories(&leaves);
    assert(ret == 0);
}

static void handle_sigusr1(int sig)
{
    assert(sig == SIGUSR1);

    printf("progress reporting unimplemented\n");
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

/**
 * Aggregate a bunch of strings into one, separated by spaces.
 */
static char * toku_strcombine(char ** strings, int count)
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
 * Get a string representing the current date.
 * The returned string needs to be freed by the caller.
 */
static char * toku_strdate(void)
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


int main(int argc, char * argv[])
{
    int ret;
    char * invocation_str, * date_str;
    char hostname[256];
    struct benchmark_file_ops * file_ops;

    ret = parse_args(argc, argv);
    if (ret != 0 || help) {
        usage();
        exit(ret);
    }
    if (num_threads > num_files) {
        printf("cannot have more threads (%d) than files (%d)\n",
                num_threads, num_files);
        exit(1);
    } else if (num_files == 1) {
        printf("reporting 20 intervals of throughput progress "
                "for a one file benchmark\n");
        report_progress = 1;
    }

    toku_debug = verbose != 0;
    setup_signal_handlers();
    invocation_str = toku_strcombine(argv, argc);
    ret = gethostname(hostname, 256);
    assert(ret == 0);

    ret = toku_fs_set_cachesize(cachesize);
    if (ret != 0) {
            printf("Couldn't set the cache size to %lu\n", cachesize);
            exit(1);
    }

    int pgsize = getpagesize();
    date_str = toku_strdate();
    printf("filesystem benchmark\n");
    printf("invoked via: %s\n", invocation_str);
    printf(" * dir: %s\n", benchmark_root_dir);
    printf(" * started: %s\n", date_str);
    printf(" * pid: %d\n", getpid());
    printf(" * hostname: %s\n", hostname);
    printf(" * num files: %d\n", num_files);
    printf(" * max children: %d\n", directory_max_children);
    printf(" * num threads: %d\n", num_threads);
    printf(" * num operations: %d\n", num_operations);
    printf(" * IO type: %s\n", do_pwrite ? "pwrite" : "pread");
    printf(" * IO pattern: %s\n", do_serial ? "serial" : "random");
    printf(" * size of each IO op: %lu\n", iosize);
    printf(" * size of each file: %lu\n", num_operations * iosize);
    printf(" * filesystem pagesize: %d\n", pgsize);
    printf(" * cachesize: %lu MB\n", cachesize / (1024*1024));
    printf(" * verbose? %s\n", verbose ? "yes" : "no");
    printf(" * report progress? %s\n", report_progress ? "yes" : "no");

    free(date_str);
    free(invocation_str);

    file_ops = &posix_file;

    run_benchmarks(file_ops);
    
    return 0;
}
