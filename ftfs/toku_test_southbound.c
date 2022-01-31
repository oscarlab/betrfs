/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:
//
#ifdef __KERNEL__
#include <linux/module.h>    // included for all kernel modules
#include <linux/kernel.h>    // included for KERN_INFO
#include <linux/init.h>      // included for __init and __exit macros
#include <linux/statfs.h>
#include <linux/err.h>
#include <linux/gfp.h>
#include <linux/string.h>
#include <linux/moduleparam.h>
#include <linux/slab.h>
#include <linux/proc_fs.h>
#include <linux/fs.h>
#include <linux/fs_struct.h>
#include <asm/uaccess.h>
#include <linux/kallsyms.h>
#include <linux/dcache.h>
#include <linux/dirent.h>
#include <linux/page-flags.h>
#include <linux/mm_types.h>
#include <linux/mm.h>
#include "ftfs.h"
#include "sb_stat.h"

extern int usleep(unsigned long);
#define MNTPNT ""

#else /* ! __KERNEL__ */
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <sys/statfs.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdarg.h>

#define BUG_ON(x)                                                       \
do {    if (!(x)) break;                                                \
        printf("### ASSERTION FAILED %s: %s: %d: %s\n",                 \
               __FILE__, __func__, __LINE__, #x); assert(false);        \
} while (0)

#define kmalloc(x, y) malloc((x))
#define kfree free
#define MNTPNT "/oscar/betrfs/user-kv/mnt"
#define open64 open
#define pread64 pread
#define pwrite64 pwrite
#define ftello64 ftello
#define KERN_ALERT
#define KERN_INFO
#define KERN_DEBUG
#define printk printf
#define off64_t int64_t

void ftfs_error (const char * function, const char * fmt, ...)
{
	va_list argptr;
	printf("ftfs error: %s: ", function);
	va_start(argptr, fmt);
	vfprintf(stderr, fmt, argptr);
	va_end(argptr);
}

void ftfs_log(const char * function, const char * fmt, ...)
{
	va_list argptr;
	printf("ftfs log: %s: ", function);
	va_start(argptr, fmt);
	vfprintf(stderr, fmt, argptr);
	va_end(argptr);
}
#endif /* __KERNEL __ */

#include "sb_malloc.h"
#include "sb_files.h"

static int ftfs_zero_out_file(const char *pathname)
{
	int header_size = 4096 * 5;
	char *buf = kmalloc(header_size, GFP_KERNEL);
	int fd;
	int ret;

	BUG_ON(buf == NULL);
	memset(buf, 0, header_size);

	fd = open64(pathname, O_RDWR, 0755);
	if (fd < 0) {
		ftfs_error(__func__, "open(%s): %d", pathname, fd);
		return fd;
	}

	ret = pwrite64(fd, buf, header_size, 0);
	/* Suppose pwrite64 succeed in one shot */
	BUG_ON(ret != header_size);
	kfree(buf);
	fsync(fd);
	close(fd);
	return 0;
}

int ftfs_fs_reset(const char *pathname, mode_t mode)
{
	int r;
	r = ftfs_zero_out_file(MNTPNT"/db/ftfs_data_2_1_19.tokudb");
	BUG_ON(r != 0);
	r = ftfs_zero_out_file(MNTPNT"/db/ftfs_meta_2_1_19.tokudb");
	BUG_ON(r != 0);
	return 0;
}

static int _setup_f_test(void){
	int fd, i;
	int r = ftfs_fs_reset("/db", 0777);
	BUG_ON(r != 0);

	fd = open64(MNTPNT"/db/ftfs_data_2_1_19.tokudb", O_RDWR|O_CREAT, 0755);
	if(fd < 0)
		return -1;
	for(i = 0; i < 10000; i++) {
		write(fd, "0123456789ABCDEFGHIJ0123456789" , 30);
	}
	close(fd);
	return 0;
}

int test_openclose(void)
{
	int fd, ret;

	int r = ftfs_fs_reset(MNTPNT"/db", 0777);
	BUG_ON(r != 0);

	fd = open64(MNTPNT"/db/ftfs_data_2_1_19.tokudb", O_CREAT|O_RDWR, 0755);
	if(fd < 0) {
		ftfs_error(__func__, "open(%s): %d\n", MNTPNT"/db/ftfs_data_2_1_19.tokudb", fd);
		return fd;
	}

	ret = close(fd);
	if (fd)
		ftfs_error(__func__, "close(%d): %d\n", fd, ret);

	return 0;
}

int test_preadwrite(void)
{
	int fd, len, ret, i;
	const char *test = MNTPNT"/db/ftfs_data_2_1_19.tokudb";
	char buf[64];
	int r;

	r = ftfs_fs_reset(MNTPNT"/db", 0777);
	BUG_ON(r != 0);

	fd = open64(test, O_CREAT | O_RDWR, 0755);
	if (fd < 0) {
		ftfs_error(__func__, "open(%s): %d", test, fd);
		return fd;
	}

	len = strlen(test);
	for (i = 0; i < 10; i++) {
		ret = pwrite64(fd, test, len, i);
		if (ret != len)
			return ret;
		ret = pread64(fd, buf, len, i);
		if (ret != len)
			return ret;
		if (memcmp(test, buf, len))
			return -EIO;
	}
	close(fd);
	return 0;
}

int test_readwrite(void)
{
	int fd, len, ret, i;
	const char *test = MNTPNT"/db/ftfs_data_2_1_19.tokudb";
	char buf[64];

	ret = ftfs_fs_reset(MNTPNT"/db", 0777);
	BUG_ON(ret != 0);

	fd = open64(test, O_CREAT | O_RDWR, 0755);
	if (fd < 0) {
		ftfs_error(__func__, "open(%s): %d", test, fd);
		return fd;
	}

	len = strlen(test);
	for (i = 0; i < 10; i++) {
		ret = pwrite64(fd, test, len, i*len);
		if (ret != len)
			return ret;
		ret = read(fd, buf, len);
		if (ret != len)
			return ret;
		if (memcmp(test, buf, len))
			return -EIO;
	}

	close(fd);
	return 0;
}

int test_pwrite(void)
{
	int fd, len, ret, i;
	const char *test = MNTPNT"/db/ftfs_data_2_1_19.tokudb";

	ret = ftfs_fs_reset(MNTPNT"/db", 0777);
	BUG_ON(ret != 0);

	fd = open64(test, O_CREAT | O_RDWR, 0755);
	if (fd < 0) {
		ftfs_error(__func__, "open(%s): %d", test, fd);
		return fd;
	}

	len = strlen(test);
	for (i = 0; i < 10; i++) {
		ret = pwrite64(fd, test, len, i);
		if (ret != len)
			return ret;
	}

	close(fd);
	return 0;
}

int test_write(void)
{
	int fd, len, ret, i;
	const char *test = MNTPNT"/db/ftfs_data_2_1_19.tokudb";

	ret = ftfs_fs_reset(MNTPNT"/db", 0777);
	BUG_ON(ret != 0);

	fd = open64(test, O_CREAT | O_RDWR, 0755);
	if (fd < 0) {
		ftfs_error(__func__, "open(%s): %d", test, fd);
		return fd;
	}

	len = strlen(test);
	for (i = 0; i < 10; i++) {
		ret = write(fd, test, len);
		if (ret != len)
			return ret;
	}

	close(fd);
	return 0;
}

extern int test_no_fsync(void);

int test_fsync(void)
{
	int ret = 0;
	int wrlen = 0;
	int to_write = 10*1000;
	int i;
	int fd;

	ret = ftfs_fs_reset(MNTPNT"/db", 0777);
	BUG_ON(ret != 0);

	fd = open64(MNTPNT"/db/ftfs_data_2_1_19.tokudb" , O_RDWR | O_CREAT,0755);
	if(fd < 0)
		goto open_fail;

	ftfs_log(__func__, "fsync is expensive, taking a while to write "
		 "through the large records...\n"
		 "You can check manually the disk writethrough of file "
		 "test_fsync by mounting the loop device");

	for(i = 0; i < to_write; i++) {
		wrlen = write(fd, "0123456789ABCDEFGHIJ0123456789" , 30);
		if(wrlen != 30)
			goto write_fail;
		ret = fsync(fd);
		if(ret)
			goto fsync_fail;
	}
	goto clean;

open_fail:
	ftfs_error(__func__, "open failed for: test_fsync");
	return fd;

write_fail:
	ftfs_error(__func__, "write failed for fd: %d", fd);
	goto clean;

fsync_fail:
	ftfs_error(__func__, "fsync failed for fd: %d", fd);
clean:
	close (fd);

	ret = ftfs_fs_reset(MNTPNT"/db", 0777);
	BUG_ON(ret != 0);

	return ret;
}
#ifdef __KERNEL__
//this is just to compare with test_sync.
int test_no_fsync(void)
{
	int ret = 0;
	int wrlen = 0;
	int to_write = 10*1000;
	int i;
	int fd;

	ret = ftfs_fs_reset(MNTPNT"/db", 0777);
	BUG_ON(ret != 0);

	fd = open64(MNTPNT"/db/ftfs_data_2_1_19.tokudb" , O_RDWR | O_CREAT,0755);
	if(fd < 0)
		goto open_fail;

	ftfs_log(__func__, "this is simply to do a comparision with fsync");
	for(i = 0; i < to_write; i++) {
		wrlen = write(fd, "0123456789ABCDEFGHIJ0123456789" , 30);
		if(wrlen != 30)
			goto write_fail;
	}
	goto clean;

open_fail:
	ftfs_error(__func__, "open failed for: test_fsync");
	return fd;

write_fail:
	ftfs_error(__func__, "write failed for fd: %d", fd);
	goto clean;

clean:
	close (fd);

	ret = ftfs_fs_reset(MNTPNT"/db", 0777);
	BUG_ON(ret != 0);

	return ret;
}
#endif

static void print_stat(struct stat *statbuf)
{
        printk(KERN_INFO "dev: %lu\n", statbuf->st_dev);
        printk(KERN_INFO "ino: %lu\n", statbuf->st_ino);
        printk(KERN_INFO "nlink: %lu\n", statbuf->st_nlink);
        printk(KERN_INFO "mode: %u\n", statbuf->st_mode);
        printk(KERN_INFO "uid: %u\n", statbuf->st_uid);
        printk(KERN_INFO "gid: %u\n", statbuf->st_gid);
        printk(KERN_INFO "rdev: %lu\n", statbuf->st_rdev);
        printk(KERN_INFO "size: %ld\n", statbuf->st_size);
        printk(KERN_INFO "blksize: %ld\n", statbuf->st_blksize);
        printk(KERN_INFO "blocks: %ld\n", statbuf->st_blocks);
        printk(KERN_INFO "atime: %ld\n", statbuf->st_atime);
        printk(KERN_INFO "ctime: %ld\n", statbuf->st_ctime);
        printk(KERN_INFO "mtime: %ld\n", statbuf->st_mtime);
}

int test_stat_ftfs(void)
{
        int ret = -1;
        int fd;
        struct stat buf1;
        struct stat buf2;

	ret = ftfs_fs_reset(MNTPNT"/db", 0777);
	BUG_ON(ret != 0);

        fd = open64(MNTPNT"/db/ftfs_data_2_1_19.tokudb", O_CREAT|O_RDWR, 0755);
        if (fd < 0)
                goto exit;
        ret = stat(MNTPNT"/", &buf1);
        if (ret < 0) {
                printk(KERN_ALERT "stat failed (test_stat)\n");
                goto exit;
        }

	print_stat(&buf1);

        printk(KERN_ALERT "Result of stat\n");
        printk(KERN_ALERT "block size: %ld\n", buf1.st_blksize);
        printk(KERN_ALERT "size: %ld\n", buf1.st_size);
        printk(KERN_ALERT "ino: %lu\n", buf1.st_ino);
        printk(KERN_ALERT "uid: %u\n", buf1.st_uid);

        ret = fstat(fd, &buf2);
        if (ret < 0) {
                printk(KERN_ALERT "fstat failed (test_stat)\n");
                goto exit;
        }

        printk(KERN_ALERT "Result of fstat\n");
        printk(KERN_ALERT "block size: %ld\n", buf2.st_blksize);
        printk(KERN_ALERT "size: %ld\n", buf2.st_size);
        printk(KERN_ALERT "ino: %lu\n", buf2.st_ino);
        printk(KERN_ALERT "uid: %u\n", buf2.st_uid);
	close(fd);
exit:
        if (fd > 0)
                close(fd);
        return ret;
}

static int test_fcopy_internal(bool use_dio) {
	int r;
	struct stat stat_buf;
	ssize_t src_size;

	r = _setup_f_test();
	BUG_ON(r != 0);

	r = stat(MNTPNT"/db/ftfs_meta_2_1_19.tokudb" , &stat_buf);
	if (r < 0) {
		printk(KERN_ALERT "stat failed (test_fcopy)\n");
		return -EINVAL;
	}

	src_size = stat_buf.st_size;
#ifdef __KERNEL__
	/* YZJ: copy small file to larger file */
	if (use_dio)
		r = fcopy_dio(MNTPNT"/db/ftfs_meta_2_1_19.tokudb", MNTPNT"/db/ftfs_data_2_1_19.tokudb", src_size);
	else
		r = fcopy(MNTPNT"/db/ftfs_meta_2_1_19.tokudb", MNTPNT"/db/ftfs_data_2_1_19.tokudb", src_size);
#else
	r = fcopy(MNTPNT"/db/ftfs_meta_2_1_19.tokudb", MNTPNT"/db/ftfs_data_2_1_19.tokudb", src_size);
#endif
	if(r != 0) {
		ftfs_log(__func__, "test fcopy failed");
	}
	return r;
}

int test_fcopy(void) {
	return test_fcopy_internal(false);
}

#ifdef __KERNEL__
#define TEST_DIO_BUFFER_SIZE (4096)
int test_fcopy_dio(void) {
	return test_fcopy_internal(true);
}

int test_sfs_dio_read_write(void)
{
	const char *test = "/db/ftfs_data_2_1_19.tokudb";
	char *buf;
	int r, fd;

	buf = (char *)__get_free_pages(GFP_KERNEL, 0);
	BUG_ON(buf == NULL);

	r = ftfs_fs_reset("/db", 0777);
	BUG_ON(r != 0);

	fd = open64(test, O_CREAT | O_RDWR, 0755);
	if (fd < 0) {
		ftfs_error(__func__, "open(%s): %d", test, fd);
		return fd;
	}
	r = sb_sfs_truncate_page_cache(fd);
	BUG_ON(r < 0);

	memset(buf, 0, TEST_DIO_BUFFER_SIZE);
	memcpy(buf, "0123456789ABCDEFGHIJ0123456789", 30);
	sb_sfs_dio_read_write(fd, buf, TEST_DIO_BUFFER_SIZE, 0, 1, NULL);

	fsync(fd); /* Need fsync to issue a flush command */
	memset(buf, 0, TEST_DIO_BUFFER_SIZE);
	sb_sfs_dio_read_write(fd, buf, TEST_DIO_BUFFER_SIZE, 0, 0, NULL);
	BUG_ON(0 != memcmp(buf, "0123456789ABCDEFGHIJ0123456789", 30));
	close(fd);
	free_pages((unsigned long)buf, 0);
	return 0;
}
#else
int test_fcopy_dio(void) {
	printf("Not supported for user-kv\n");
	return 0;
}

int test_sfs_dio_read_write(void) {
	printf("Not supported for user-kv\n");
	return 0;
}
#endif
int test_statfs(void)
{
        int ret = -1;
        int fd;
        struct statfs buf1;
        struct statfs buf2;

        ret = statfs(MNTPNT"/", &buf1);
        if (ret < 0) {
                printk(KERN_ALERT "statfs failed \n");
                goto exit;
        }

        printk(KERN_ALERT "Result of fstatfs:\n");
        printk(KERN_ALERT "blocks: %ld\n", buf1.f_blocks);
        printk(KERN_ALERT "bfree: %ld\n", buf1.f_bfree);
        printk(KERN_ALERT "files: %ld\n", buf1.f_files);
        printk(KERN_ALERT "uid: %ld\n", buf1.f_ffree);

        fd = open64("/", O_RDONLY, 0755);
        if (fd < 0)
                goto exit;

        ret = fstatfs(fd, &buf2);
        if (ret < 0) {
                printk(KERN_ALERT "statfs failed \n");
                goto exit;
        }

        printk(KERN_ALERT "Result of fstatfs:\n");
        printk(KERN_ALERT "blocks: %ld\n", buf2.f_blocks);
        printk(KERN_ALERT "bfree: %ld\n", buf2.f_bfree);
        printk(KERN_ALERT "files: %ld\n", buf2.f_files);
        printk(KERN_ALERT "uid: %ld\n", buf2.f_ffree);

exit:
        if(fd > 0)
                close(fd);
        return ret;
}

int test_ftfs_realloc(void)
{
#ifdef __KERNEL__
	unsigned long i = FTFS_KMALLOC_MAX_SIZE << 2;
#else
	unsigned long i = sysconf(_SC_PAGESIZE) << 2;
#endif
	char *ptr = NULL;
	int x = 128;
	char tmp[] = "0123456789qwertyui";
	int old_size = x;

	printk(KERN_ALERT "Max allocation size: %lu\n", i);
	ptr = sb_malloc(x);
	if (!ptr) {
		printk(KERN_ALERT "No memory\n");
		return -ENOMEM;
	}

	memcpy(ptr, tmp, sizeof(tmp));

	for (x = 256; x <= i; x = x + x) {
		ptr = (char*) sb_realloc(ptr, old_size, x);
		old_size = x;
		if (!ptr) {
			printk(KERN_ALERT " failed size:%d\n", x);
			return -ENOMEM;
		}

		usleep(100);

		printk(KERN_ALERT "size:%d %s\n", x, ptr);
	}

	if (ptr)
		sb_free(ptr);
	return 0;
}

#ifdef __KERNEL__
void print_page_stats(const char *msg, struct page *page)
{
	printk(KERN_DEBUG "page_stats (%s):\n", msg);
	printk(KERN_DEBUG "\tslab_cache: %p\n", page->slab_cache);
	printk(KERN_DEBUG "\tPG_slab: %d\n\n", PageSlab(page));
}

void print_pointer_stats(const char *msg, void *p)
{
	printk(KERN_DEBUG "pointer stats (%s):\n", msg);
	if (is_vmalloc_addr(p))
		printk(KERN_DEBUG "\t%p is from vmalloc()\n", p);
	else {
		struct page *page = virt_to_head_page(p);
		printk(KERN_DEBUG "\t%p is from kmalloc()\n", p);
		print_page_stats(msg, page);
	}
}
#endif
