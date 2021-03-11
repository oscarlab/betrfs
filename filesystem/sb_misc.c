/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

#include <linux/string.h>
#include <linux/cpumask.h>
#include <linux/cpufreq.h>
#include <linux/smp.h>
#include <linux/slab.h>
#include <linux/percpu.h>
#include <linux/syscalls.h>
#include <asm/page_types.h>
#include <linux/profile.h>
#include <linux/mm.h>
#include <linux/file.h>
#include <linux/mman.h>
#include <asm/page_types.h>
#include <linux/notifier.h>
#include <linux/ctype.h>
#include <linux/resource.h>
#include "ftfs.h"
#include "ftfs_files.h"
#include "ftfs_southbound.h"
#include "ftfs_error.h"
#include "ftfs_dir.h"
#include "toku_misc.h"

typedef long (* sys_newlstat_t)(const char *, struct stat *);
typedef long (* sys_setrlimit_t) (unsigned int, struct rlimit *);
typedef long (* sys_getrlimit_t) (unsigned int, struct rlimit *);
DECLARE_SYMBOL_FTFS(sys_newlstat);
DECLARE_SYMBOL_FTFS(sys_setrlimit);
DECLARE_SYMBOL_FTFS(sys_getrlimit);

int resolve_toku_misc_symbols(void)
{
	LOOKUP_SYMBOL_FTFS(sys_newlstat);
	LOOKUP_SYMBOL_FTFS(sys_setrlimit);
	LOOKUP_SYMBOL_FTFS(sys_getrlimit);
	return 0;
}

#ifdef SOUTHBOUND_TESTING /* only used during toku unit testing */
extern char* ftfs_test_filename;
const char *toku_test_filename(const char *default_filename)
{
  int i;

  if (ftfs_test_filename)
    return ftfs_test_filename;

  i = strlen(default_filename);
  while (i > 0 && default_filename[i-1] != '/')
    i--;
  return default_filename+i;
}
#endif

int toku_os_get_number_processors(void)
{
  return num_present_cpus();
}

int toku_os_get_number_active_processors(void)
{
  int n = num_online_cpus();
  return n < toku_ncpus ? n : toku_ncpus;
}

//On x86_64:There is one single variable cpu_khz that gets written by all the CPUs. So,
//the frequency set by last CPU will be seen on /proc/cpuinfo of all the CPUs in the system.

extern uint64_t toku_cached_hz;
int toku_os_get_processor_frequency(uint64_t* hzret){
    int r;
    uint64_t khz;
    if (toku_cached_hz) {
        *hzret = toku_cached_hz;
        r = 0;
    } else {
        khz = cpufreq_quick_get(smp_processor_id());
        if(!khz)
          khz = cpu_khz;
        *hzret = khz * 1000U;
        toku_cached_hz = *hzret;
        r = 0;
    }
    return r;
}

extern bool toku_huge_pages_ok;
bool toku_huge_pages_ok = false;
//passed as module parameter, getenv is no longer needed.
extern bool check_huge_pages_in_practice(void);
bool complain_and_return_true_if_huge_pages_are_enabled(void){
    if(toku_huge_pages_ok) {
        return false;
    } else {
        #ifdef CONFIG_TRANSPARENT_HUGEPAGE_ALWAYS
        return true;
        #else
        return check_huge_pages_in_practice();
        #endif
    }
}
/*the following three mem funcs are for check_huge_pages_in_practice()*/
void * mmap(void *addr, size_t len, int prot, int flags,int fd, off_t offset){
    struct file * file  = NULL;
    unsigned long retval = - EBADF;
    if (offset_in_page(offset) != 0)
        return (void *) -EINVAL;

    flags &= ~(MAP_EXECUTABLE | MAP_DENYWRITE);

    retval = vm_mmap(file, (unsigned long)addr, len, prot, flags, offset);

    if (file)
            fput(file);
    return (void *) retval;

}

/* warning: mincore kernel version is unfinished yet, needs kallsyms_look_up*/
int mincore(void *addr, size_t len, unsigned char *vec){
     /*  long retval;
     unsigned long pages;
     unsigned char *tmp;
     pages = len >> PAGE_SHIFT;
     pages += (len & ~PAGE_MASK) != 0;
     tmp = (void *) __get_free_page(GFP_KERNEL);
     if (!tmp)
        return -EAGAIN;
     retval = 0;
     while (pages) {
         down_read(&current->mm->mmap_sem);
         retval = do_mincore(addr, min(pages, PAGE_SIZE), tmp);
         up_read(&current->mm->mmap_sem);
          if (retval <= 0)
            break;
            if (memcpy(vec, tmp, retval)) {
                retval = -EFAULT;
                break;
             }
                pages -= retval;
                vec += retval;
                addr += retval << PAGE_SHIFT;
                retval = 0;
     }
     free_page((unsigned long) tmp);
     return retval;*/
     return -1;
 }


static BLOCKING_NOTIFIER_HEAD(munmap_notifier);
int munmap(void *addr, size_t len){
    int ret;
    blocking_notifier_call_chain(&munmap_notifier, 0, addr);
    ret = vm_munmap((unsigned long)addr, len);
    return ret;
}

extern int toku_cached_pagesize;

int toku_os_get_pagesize(void) {
    int pagesize = toku_cached_pagesize;
    if(pagesize == 0) {
        pagesize = PAGE_SIZE ;
        if(pagesize) {
            toku_cached_pagesize = pagesize;
        }
    }
    return pagesize;
}

uint64_t toku_os_get_phys_memory_size(void) {
	ftfs_log(__func__, "totalram_pages = %8lu", totalram_pages);
	return totalram_pages * PAGE_SIZE;
}

//typical undergrad recursive algo. just watch "." and ".."
//i assume there is no loops by links.


#define BUF_SIZE 1024
static int ftfs_recursive_delete(char * pathname)
{
    int fd, num, cnt;
    void * buf;
    struct dirent64 * dirp;
    char *full_name = kzalloc(PATH_MAX, GFP_KERNEL);
    SOUTHBOUND_VARS;

    if (!full_name)
	  return -ENOMEM;

    ftfs_log(__func__, "pathname = %s",pathname);
    fd = open(pathname, O_RDONLY, 0);
    if (fd < 0) {
	    ftfs_log(__func__, "open failed for recursive deletion %d",fd);
	    kfree(full_name);
	    return fd;
    }
    buf = kzalloc(BUF_SIZE, GFP_KERNEL);
    if (!buf) {
	    kfree(full_name);
	    close(fd);
	    return -ENOMEM;
    }
    dirp = buf;
    while (1) {
        num = getdents64(fd, (struct linux_dirent64 *)dirp, BUF_SIZE);
        if (num < 0) {
		kfree(buf);
		kfree(full_name);
		close(fd);
		ftfs_log(__func__, "getdents64 failed for recursive deletion");
		return -ENOTDIR;
	} if (num == 0) {
		goto out;
	}
	cnt = num;
	while (cnt > 0) {
		struct stat st;
		int ret;
		char *name = dirp->d_name;
		if (strcmp(name,".") == 0)
			goto dot;
                if (strcmp(name, "..") == 0)
			goto dot;

		strcpy(full_name, pathname);
		strcat(full_name, "/");
		strcat(full_name, dirp->d_name);
		SOUTHBOUND_ATTACH;
		ret = ftfs_sys_newlstat(full_name, &st);
		SOUTHBOUND_RESTORE;
		WARN_ON_ONCE(ret);
		if (ret < 0) {
			ftfs_log(__func__, "newlstat failed for recursive deletion %d",ret);
			kfree(buf);
			kfree(full_name);
			close(fd);
			return ret;
		}

		if(S_ISDIR(st.st_mode)) {
			ftfs_recursive_delete(full_name);
		}
		else {
			#ifdef __TOKU_MISC_DEBUG
			ftfs_log(__func__, "going to delete file%s", full_name);
			#endif
			unlink(full_name);
		}
dot:

		cnt -= dirp->d_reclen;
		dirp = (void *) dirp + dirp->d_reclen;
	}

	dirp = buf;
	memset(dirp,0, BUF_SIZE);

    }

out:
    close(fd);
    kfree(buf);
    kfree(full_name);
   #ifdef __TOKU_MISC_DEBUG
    ftfs_log(__func__, "going to delete dir:%s", pathname);
   #endif
    rmdir(pathname);
    return 0;
}

/* YZJ: make the error code available */
int recursive_delete(char * pathname)
{
	int r = ftfs_recursive_delete(pathname);
	if(r==-ENOTDIR) {
		r = unlink(pathname);
	}
	ftfs_set_errno(r);
	return r;
}

int setrlimit64(int resource, struct rlimit *rlim)
{
	mm_segment_t saved;
	int ret;

	saved = get_fs();
	set_fs(get_ds());
	ret = ftfs_sys_setrlimit(resource, rlim);
	set_fs(saved);

	return ret;

}

int getrlimit64(int resource, struct rlimit *rlim)
{
	mm_segment_t saved;
	int ret;

	saved = get_fs();
	set_fs(get_ds());
	ret = ftfs_sys_getrlimit(resource, rlim);
	set_fs(saved);

	return ret;
}

/* isalnum defined as a macro in linux/ctype.h. use () to prevent expansion */
int (isalnum)(int c)
{
	return isalnum(c);
}
