/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:
//

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
#include "ftfs_files.h"
#include "ftfs_dir.h"
#include "ftfs_stat.h"
#include "ftfs_error.h"
#include "ftfs_malloc.h"

extern int usleep(unsigned long);

int test_trunc(void)
{
	int fd, len, err;
	const char *test = "content\n";
	const char *fname = "test_trunc";
	fd = open64(fname, O_CREAT | O_RDWR, 0755);
	if (fd < 0) {
		ftfs_error(__func__, "open(%s): %d", fname, fd);
		return fd;
	}

	len = strlen(test);
	err = pwrite64(fd, test, len, 0);
	if (err != len) {
		ftfs_error(__func__, "pwrite wrote: %d expected: %d",
			   err, len);
		close(fd);
		return err;
	}

	err = close(fd);
	if (err) {
		ftfs_error(__func__, "close(%d): %d", fd, err);
		return err;
	}

	err = truncate64(fname, 10);
	if (err) {
		ftfs_error(__func__, "truncate(%s, 10): %d", fname, err);
		return err;
	}

	err = truncate64(fname, 0);
	if (err) {
		ftfs_error(__func__, "truncate(%s, 0): %d", fname, err);
		return err;
	}

	return err;
}

int test_ftrunc(void)
{
	int fd, err;
	const char *fname = "test_ftrunc";
	fd = open64(fname, O_CREAT | O_RDWR, 0755);
	if (fd < 0) {
		ftfs_error(__func__, "open(%s): %d", fname, fd);
		return fd;
	}

	err = truncate64(fname, 10);
	if (err) {
		ftfs_error(__func__, "ftruncate(%s, 10): %d", fname, err);
		return err;
	}

	err = truncate64(fname, 0);
	if (err) {
		ftfs_error(__func__, "ftruncate(%s, 0): %d", fname, err);
		return err;
	}

	err = truncate64(fname, 5);
	if (err) {
		ftfs_error(__func__, "ftruncate(%s, 0): %d", fname, err);
		return err;
	}

	err = close(fd);
	if (err) {
		ftfs_error(__func__, "close(%d): %d", fd, err);
		return err;
	}


	return err;
}


int test_openclose(void)
{
	int fd, ret;

	fd = open64("//.././test-open", O_CREAT|O_RDWR, 0755);
	if(fd < 0) {
		ftfs_error(__func__, "open(%s): %d\n", "test-open", fd);
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
	const char *test = "test_preadwrite";
	char buf[16];

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

int test_directio_vmalloc(void)
{
	int fd, ret, i;

	char *buf = (char *)vmalloc(4096);
	char *test = (char *)vmalloc(4096);
	int times = 4096 / 512;

	ftfs_log(__func__,"buf = %p, test = %p\n", buf, test);

	fd = open64("dio", O_CREAT | O_RDWR | O_DIRECT, 0755);
	if (fd < 0) {
		ftfs_error(__func__, "open(%s): %d", test, fd);
		return fd;
	}

	for (i = 0; i < 4096; i++)
		buf[i] = i % 255;

	for (i = 0; i < times; i++) {
		ret = pwrite64(fd, buf + i * 512, 512, i * 512);
		if (ret != 512)
			return ret;
		ret = pread64(fd, test + i * 512, 512 , i * 512);
		if (ret != 512)
			return ret;
	}

	if (memcmp(test, buf, 4096)) {
		close(fd);
		return -EIO;
	}

	vfree(buf);
	vfree(test);

	close(fd);
	return 0;
}

#define BUF_ALIGN(buf, ptr_type) (ptr_type)(((long int)buf) & ~(PAGE_SIZE-1))

int test_directio_kmalloc(void)
{
	int fd, ret, i;

	char *a = kmalloc(4096*2, GFP_KERNEL);
	char *b = kmalloc(4096*2, GFP_KERNEL);

	char *buf = BUF_ALIGN(a, char *);
	char *test = BUF_ALIGN(b, char *);

	int times = 4096 / 512;

	ftfs_log(__func__,"buf = %p, test = %p\n", buf, test);

	fd = open64("dio", O_CREAT | O_RDWR | O_DIRECT, 0755);
	if (fd < 0) {
		ftfs_error(__func__, "open(%s): %d", test, fd);
		return fd;
	}

	for (i = 0; i < 4096; i++)
		buf[i] = i % 255;

	for (i = 0; i < times; i++) {
		ret = pwrite64(fd, buf + i * 512, 512, i * 512);
		if (ret != 512)
			return ret;
		ret = pread64(fd, test + i * 512, 512 , i * 512);
		if (ret != 512)
			return ret;
	}

	if (memcmp(test, buf, 4096)) {
		close(fd);
		return -EIO;
	}

	kfree(a);
	kfree(b);

	close(fd);
	return 0;
}


int test_directio(void)
{
	int ret;
	//ret = test_directio_kmalloc();
	//if (ret) {
	//	ftfs_error(__func__, "directio_kmalloc: %d", ret);
	//	return ret;
	//}
	ret = test_directio_vmalloc();
	if (ret) {
		ftfs_error(__func__, "directio_vmalloc: %d", ret);
		return ret;
	}

	return 0;
}

int test_readwrite(void)
{
	int fd, len, ret, i;
	const char *test = "test_readwrite";
	char buf[16];

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
	const char *test = "test_pwrite";

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
	const char *test = "test_write";

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

extern int feof(FILE *f);
int test_fgetc(void)
{
	FILE *fp;
	int fd, i, ret;
	int ch;
	const char * test = "0123456789ABCDEFGHIJ0123456789";
	fd = open64("test-fopen", O_RDWR|O_CREAT, 0755);
	for(i = 0; i < 10000; i++) {
		write(fd, "0123456789ABCDEFGHIJ0123456789" , 30);
	}
	close(fd);

	fp = fopen64("test-fopen", "r");
	if(!fp) {
		return -EBADF;
	}

	ch = i = ret = 0;
	ch = fgetc(fp);
	if (ch == EOF)
		goto out;
	do {
		if ((char)ch != test[i % 30]) {
			ftfs_error(__func__, "Expected %c, got %c. i=%i",
				   (char)ch, test[i % 30], i);
			ret = -1;
			goto out;
		}
		i++;
		ch = fgetc(fp);
	} while (ch != EOF);

	if (feof(fp))
		printk(KERN_DEBUG "End of file reached.\n");
	else
		ftfs_error(__func__, "Something went wrong. feof() expected");
out:
	fclose(fp);
	unlink("test-fopen");
	return ret;

}


static int _setup_f_test(void){
    int fd, i;
    fd = open64("test-fopen", O_RDWR|O_CREAT, 0755);
    if(fd < 0)
	return -1;
    for(i = 0; i < 10000; i++) {
      write(fd, "0123456789ABCDEFGHIJ0123456789" , 30);
    }
    close(fd);
    return 0;
}

static int _teardown_f_test(void){
    return unlink("test-fopen");
}
//a big test for fopen/fclose/ftell/fseek/fread/fwrite/ftello64
int test_f_all(void) {
    FILE * fp;
    int ret = 0;
    long res1 = 0;
    off64_t res2 = 0;
    char * buf;
    _setup_f_test();

    fp = fopen64("test-fopen", "r+b");
    if(!fp) goto open_fail;

    ret = fseek(fp,900, SEEK_SET);
    if(ret) goto seek_fail;

    res1 = ftell(fp);
    if(res1 != 900) {
        ftfs_error(__func__, "!!ftell is not reporting the correct value:%ld, expected 900", res1);
        ret = -1;
        goto cleanup;
    }
    // now we are in the middle of the file, let's play fread and fwrite

    ftfs_log(__func__, "before reading the offset = %d", res1);
    buf = (char *) kzalloc(sizeof(char)*61, GFP_KERNEL);
    ret = fread(buf, 1, 60, fp);
    if(ret != 60) {
	ftfs_error(__func__, "!!1st fread failed to read:%d", ret);
        ret = -1;
        goto cleanup;
    }
    ftfs_log(__func__, "1st fread succeeded:%s", buf);
    fseek(fp, -60, SEEK_CUR);
    ret = ftello64(fp);
    ftfs_log(__func__, "after reading 60 bytes and move backward 60 bytes the offset = %d", ret);

    fwrite("9876543210JIHGFEDCBA9876543210",1,30,fp);
    fwrite("9876543210JIHGFEDCBA9876543210",1,30,fp); // buffered write should be practiced...

    // WARN: use fseek to force flush write first or you may not get the right position.
    fseek(fp,0,SEEK_CUR);
    ret = ftello64(fp);
    ftfs_log(__func__, "after writing 60 bytes the offset = %d", ret);

    fseek(fp, -60, SEEK_CUR);
    ret = ftello64(fp);
    ftfs_log(__func__, "again move backward 60 bytes the offset = %d", ret);

    ret = fread(buf, 1, 60, fp);
    if(ret != 60) {
	ftfs_error(__func__, "!!2nd fread failed to read:%d", ret);
        ret = -1;
        goto cleanup;
    }
    ftfs_log(__func__, "2nd fread succeeded:%s", buf);
     ret = ftello64(fp);
    ftfs_log(__func__, "after 2nd reading of 60bytes the offset = %d", ret);


    ret = fseek(fp, 0, SEEK_END);
    if(ret) goto seek_fail;

    res2 = ftello64(fp);
    if(res2 != 300000) {
        ftfs_error(__func__, "!!ftello64 is not reporting the correct value:%ld, expected file size", res2);
        ret = -1;
        goto cleanup;
    }

    ret = fseek(fp, -100000, SEEK_CUR);
    if(ret) goto seek_fail;

    res2 = ftello64(fp);
    if(res2 != 200000) {
        ftfs_error(__func__, "!!ftello64 is not reporting the correct value:%ld, expected 200000", res2);
        ret = -1;
    }
    goto cleanup;

open_fail:
    ftfs_error(__func__, "fseek failed for open-fopen");
    goto cleanup;
seek_fail:
    ftfs_error(__func__, "fseek failed for test-fopen");
cleanup:
    kfree(buf);
    fclose(fp);
    _teardown_f_test();
    return ret;

}
//testing recursive deletion....
extern int recursive_delete(char * path);
int test_recursive_deletion(void) {
    int ret = 0;
    int fd;
    ret = mkdir("test_recursive", 0755);
    if(ret) {
	ftfs_error(__func__, "failed to mkdir test_recursive");
	return -1;
    }
    ret = mkdir("test_recursive/1",0755);
    if(ret) {
	ftfs_error(__func__, "failed to mkdir test_recursive/1");
	return -1;
    }
    ret = mkdir("test_recursive/1/2",0755);
    if(ret) {
	ftfs_error(__func__, "failed to mkdir test_recursive/1/2");
	return -1;
    }
    ret = mkdir("test_recursive/1/2/3",0755);
    if(ret) {
	ftfs_error(__func__, "failed to mkdir test_recursive/1/2/3");
	return -1;
    }

    ret = mkdir("test_recursive/1/2/4",0755);
    if(ret) {
	ftfs_error(__func__, "failed to mkdir test_recursive/1/2/3");
	return -1;
    }
    fd = open64( "test_recursive/1/2/test_file" , O_RDWR | O_CREAT,0755);
    if(fd < 0) {
        ftfs_error(__func__, "can not create test file ...");
    }
    ret = recursive_delete("test_recursive");
    if(ret) {
	ftfs_error(__func__, "failed to recursively delete test_recursive");
	return -1;
    }
    return 0;
}

//DIR stream based
int test_openclose_dir(void) {
	int ret = 0;
	DIR * dirp;
	struct dirent64 * de;
	int fd;

	/* positive test */
	ret = mkdir("test_readdir", 0755);
	ret = mkdir("test_readdir/1",0755);
	ret = mkdir("test_readdir/2",0755);
	ret = mkdir("test_readdir/3",0755);

	dirp = opendir("test_readdir");
	if(!dirp) {
		ftfs_error(__func__, "opendir failed");
		return -1;
	}

	fd = dirfd(dirp);
	if(fd < 0) {
		ftfs_error(__func__, "dirfd failed: %d", fd);
		return -1;
	}

	while((de = readdir64(dirp)) != NULL) {
		ftfs_log(__func__,"readdir output: %s", de->d_name);
	}
	closedir(dirp);

	rmdir("test_readdir/1");
	rmdir("test_readdir/2");
	rmdir("test_readdir/3");
	rmdir("test_readdir");

	/* negative test */
	fd = open64( "test_file" , O_RDWR | O_CREAT, 0755);
	if(fd < 0) {
		ftfs_error(__func__, "can not open test file ...");
		return -1;
	}
	close(fd);

	dirp = opendir("test_file");
	if(dirp) {
		ftfs_error(__func__, "ERROR! opendir should not open "
			   "regular file \"test_file\"");
		ret = -1;
	}
	ftfs_log(__func__, "opendir correctly failed on \"test_file\", "
		 "which is not a dir.");

	unlink("test_file");

	closedir(dirp);

	return 0;
}

extern int test_no_fsync(void);

int test_fsync(void)
{
	int ret = 0;
	int wrlen = 0;
	int to_write = 10*1000;
	int i;
	int fd = open64( "test_fsync" , O_RDWR | O_CREAT,0755);
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
	unlink("test_fsync");
	return ret;
}

//this is just to compare with test_sync.
int test_no_fsync(void)
{
	int ret = 0;
	int wrlen = 0;
	int to_write = 10*1000;
	int i;
	int fd = open64( "test_no_fsync" , O_RDWR | O_CREAT,0755);
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
	unlink("test_no_fsync");
	return ret;
}

int test_readlink(void) {
	int ret = 0;
	char * buf;
	int fd = open64("test_readlink", O_RDWR|O_CREAT, 0755);
	if(fd < 0) {
		ftfs_error(__func__, "open failed for test_readlink");
	}
	close(fd);
	symlink("test_readlink", "symlink_2_test_readlink");
	buf = (char *)kzalloc(sizeof(char)*(PATH_MAX), GFP_KERNEL);
	if(!buf) {
		ftfs_error(__func__, "kmalloc failed!");
		return -1;
	}

	readlink("symlink_2_test_readlink", buf, PATH_MAX);
	ftfs_log(__func__, "readlink test output: %s", buf);

	if(strcmp(buf,"test_readlink")) ret = -1;
	kfree(buf);
	unlink("symlink_2_test_readlink");
	unlink("test_readlink");
	return ret;
}

int test_mkdir(void)
{
	int err;
	err = mkdir("/test_mkdir/", 0755);
	if (err)
		ftfs_error(__func__, "LINE=%d mkdir failed for err: %d", __LINE__, err);
	err = mkdir("/test_mkdir/1", 0755);
	if (err)
		ftfs_error(__func__, "LINE=%d mkdir failed for err: %d", __LINE__, err);
	err = mkdir("/test_mkdir/1/2", 0755);
	if (err)
		ftfs_error(__func__, "LINE=%d mkdir failed for err: %d", __LINE__, err);
	err = mkdir("/test_mkdir/1/2/3", 0755);
	if (err)
		ftfs_error(__func__, "LINE=%d mkdir failed for err: %d", __LINE__, err);
	err = mkdir("/test_mkdir/1/2/3/4", 0755);
	if (err)
		ftfs_error(__func__, "LINE=%d mkdir failed for err: %d", __LINE__, err);

	printk(KERN_ALERT "mkdir test finished: err=%d\n", err);

	return err;
}

int test_rmdir(void)
{
	int err;
	err = rmdir("/test_mkdir/1/2/3/4");
	if (err)
		ftfs_error(__func__, "LINE=%d rmdir failed for err: %d", __LINE__, err);
	err = rmdir("/test_mkdir/1/2/3");
	if (err)
		ftfs_error(__func__, "LINE=%d rmdir failed for err: %d", __LINE__, err);
	err = rmdir("/test_mkdir/1/2");
	if (err)
		ftfs_error(__func__, "LINE=%d rmdir failed for err: %d", __LINE__, err);
	err = rmdir("/test_mkdir/1");
	if (err)
		ftfs_error(__func__, "LINE=%d rmdir failed for err: %d", __LINE__, err);
	err = rmdir("/test_mkdir");
	if (err)
		ftfs_error(__func__, "LINE=%d rmdir failed for err: %d", __LINE__, err);

	printk(KERN_ALERT "rmdir test finished: err=%d\n", err);
	return err;
}

int test_mkrmdir(void)
{
	int i=0, err = 0;
	err = recursive_delete("/test_mkdir");
	if (err != 0 && err != -ENOENT) {
		ftfs_error(__func__, "Make sure recursive deletion work: err=%d\n", err);
		return err;
	}

	for (i=0; i<10; i++) {
		err = test_mkdir();
		if (err) break;
		usleep(1000);
		err = test_rmdir();
		if (err) break;
 	}
	printk(KERN_ALERT "mkrmdir test finished: err=%d\n", err);
	return err;
}

int test_unlink(void)
{
	int fd = 0, err = 0;

	unlink("/not-exist");
	fd = open64("/test-unlink", O_CREAT|O_RDWR, 0755);
	if (fd < 0)
		ftfs_error(__func__, "open(%s)", "/test-unlink");
	else
		err = unlink("/test-unlink");

	if (err)
		ftfs_error(__func__, "unlink failed for err: %d", err);

	close(fd);
	return err;
}

static int delete_files(void)
{
	int ret, num = 1;

	ret = unlink("/test/1");
	if (ret)
		goto err;

	num = 2;
	ret = unlink("/test/2");
	if (ret)
		goto err;

	num = 3;
	ret = unlink("/test/3");
	if (ret)
		goto err;

	ret = rmdir("/test/");
	if (ret) {
		ftfs_error(__func__, "delete dir /test failed");
		return -1;
	}

	return 0;

err:
	ftfs_error(__func__, "delete file /test/%d failed", num);
	return -1;
}

static int create_files(void)
{
	int ret, num = 1;

	ret = mkdir("/test/", 0755);
	if (ret) {
		ftfs_error(__func__, "mkdir failed for err: %d", ret);
		return -1;
	}

	ret = open64("/test/1", O_CREAT|O_RDWR, 0755);
	if (ret < 0)
		goto err;
	close(ret);

	num = 2;
	ret = open64("/test/2", O_CREAT|O_RDWR, 0755);
	if (ret < 0)
		goto err;
	close(ret);

	num = 3;
	ret = open64("/test/3", O_CREAT|O_RDWR, 0755);
	if (ret < 0)
		goto err;
	close(ret);

	return 0;

err:
	ftfs_error(__func__, "open file /test/%d failed", num);
	return -1;
}

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

        fd = open64("/test_stat", O_CREAT|O_RDWR, 0755);
        if (fd < 0)
                goto exit;
        ret = stat("/", &buf1);
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

int test_statfs(void)
{
        int ret = -1;
        int fd;
        struct statfs buf1;
        struct statfs buf2;

        ret = statfs("/", &buf1);
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


int test_getdents64(void)
{

	int fd, ret, cnt, status = 0;
	struct linux_dirent64 dent, *p;

	ret = create_files();
	if (ret < 0)
		return ret;

	fd = open64("/test/", O_RDONLY, 0777);
	if (fd < 0)
		return fd;

	while (1) {
		status = getdents64(fd, &dent, sizeof(struct linux_dirent64));
		if (status < 0) {
			ftfs_error(__func__, "getdents64 failed!");
			break;
		} else if (status == 0)
			break;

		for (cnt=0, p=&dent; cnt<status; ) {
			printk(KERN_INFO "inode number: %llu\n", p->d_ino);
			printk(KERN_INFO "offset: %lld\n", p->d_off);
			printk(KERN_INFO "len: %u\n", p->d_reclen);
			printk(KERN_INFO "file name: %s\n", p->d_name);
			cnt += p->d_reclen;
			p = (struct linux_dirent64 *) ((char *)p + p->d_reclen);
		}
	}

	close(fd);
	delete_files();
	return status;
}

int test_remove(void)
{
	int ret;
	ret = create_files();
	if (ret < 0) {
		set_errno(ret);
		perror("test_remove");
	}

	ret = remove("/test/1");
	if (ret < 0) {
		set_errno(ret);
		perror("test_remove");
	}
	ret = remove("/test/2");
	if (ret < 0) {
		set_errno(ret);
		perror("test_remove");
	}
	ret = remove("/test/3");
	if (ret < 0) {
		set_errno(ret);
		perror("test_remove");
	}

	ret = remove("/test/");
	if (ret < 0) {
		set_errno(ret);
		perror("test_remove /test/");
	}

	return ret;

}

int test_ftfs_realloc(void)
{
	int i = FTFS_KMALLOC_MAX_SIZE << 2;
	char *ptr = NULL;
	int x = 128;
	char tmp[] = "0123456789qwertyui";


	printk(KERN_ALERT "Max: %lu\n", FTFS_KMALLOC_MAX_SIZE);
	ptr = ftfs_malloc(x);
	if (!ptr) {
		printk(KERN_ALERT "No memory\n");
		return -ENOMEM;
	}

	memcpy(ptr, tmp, sizeof(tmp));

	for (x = 256; x <= i; x = x + x) {
		ptr = (char*) ftfs_realloc(ptr, x);
		if (!ptr) {
			printk(KERN_ALERT " failed size:%d\n", x);
			return -ENOMEM;
		}

		usleep(100);

		printk(KERN_ALERT "size:%d %s\n", x, ptr);
	}

	if (ptr)
		ftfs_free(ptr);
	return 0;
}


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

int test_slab(void)
{
	void *p;

	ftfs_log(__func__, "KMALLOC_MAX_SIZE: %d", KMALLOC_MAX_SIZE);
	ftfs_log(__func__, "MAX_ORDER: %d", MAX_ORDER);
	ftfs_log(__func__, "PAGE_SHIFT: %d", PAGE_SHIFT);

	p = vmalloc(2* KMALLOC_MAX_SIZE);
	if (!p) {
		ftfs_error(__func__, "vmalloc :(");
		return -ENOMEM;
	}
	print_pointer_stats("vmalloc", p);
	vfree(p);

	p = kmalloc(FTFS_KMALLOC_MAX_SIZE, GFP_KERNEL);
	if (!p) {
		ftfs_error(__func__, "kmalloc :(");
		return -ENOMEM;
	}
	print_pointer_stats("kmalloc", p);
	kfree(p);

	p = kmalloc(0, GFP_KERNEL);
	ftfs_error(__func__, "kmalloc(0): %p\n", p);
	if (p)
		kfree(p);

	return 0;
}


int test_posix_memalign(void) {
	void *p;
	int r;
	ftfs_log(__func__,"aligned to 512\n");
	r = posix_memalign(&p, 512, 1024);
	if(r!=0) {
		p = NULL;
		return -1;
	}
	ftfs_log(__func__, "malloced address aligned to 512:%p", p);
	vfree(p);
	return 0;
}

int test_fcopy(void) {
	int r;
	_setup_f_test();
	r = fcopy("test-fopen","test-fopen-copy");
	_teardown_f_test();
	if(r != 0) {
		 ftfs_log(__func__, "test fcopy failed");
	}
	unlink("test-fopen-copy");
	return r;
}
