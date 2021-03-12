/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:
#ifndef _FTFS_FILES_H
#define _FTFS_FILES_H

#include <linux/fs.h> // for filp_close()
#include <linux/file.h> // for struct fd
#include "ftfs_southbound.h" // for ftfs_files in ftfs_filp_close()
#define DEFAULT_PERMS 0755

/* we need to handle our own SB file descriptor table */

extern struct file *ftfs_fget_light(unsigned int fd, int *fput_needed);
static inline struct fd ftfs_fdget(unsigned int fd)
{
	int b;
	struct file *f = ftfs_fget_light(fd, &b);
	return (struct fd){f,b};
}

/* just reuse fdput since it does not operate on file descriptors */
#define ftfs_fdput(fd) fdput(fd)

void ftfs_put_unused_fd(unsigned int fd);
int ftfs_get_unused_fd_flags(unsigned flags);
void ftfs_fd_install(unsigned int fd, struct file *file);
static inline int ftfs_filp_close(struct file *file)
{
	return filp_close(file, ftfs_files);
}

int resolve_ftfs_files_symbols(void);

int open(const char *pathname, int flags, umode_t mode);
int open64(const char *filename, int flags, umode_t mode);

int close(int fd);

int fsync(int fd);

int remove(const char *path);

int truncate64(const char *path, loff_t length);
int ftruncate64(int fd, loff_t length);

ssize_t pread64(int fd, void *buf, size_t count, loff_t offset);
ssize_t read(int fd, void *buf, size_t count);

ssize_t write(int fd, const void *buf, size_t count);
ssize_t pwrite64(int fd, const void *buf, size_t count, loff_t offset);

int fsync(int fd);
int symlink(const char*, const char *);
long sync(void);
ssize_t readlink(const char *, char *, size_t);
int unlink(const char *pathname);
unsigned long dup2(int oldfd, unsigned int newfd);
int fcopy(const char *, const char *);
/* stdio_impl.h
struct _IO_FILE {
	unsigned flags;
	unsigned char *rpos, *rend;
	int (*close)(FILE *);
	unsigned char *wend, *wpos;
	unsigned char *mustbezero_1;
	unsigned char *wbase;
	size_t (*read)(FILE *, unsigned char *, size_t);
	size_t (*write)(FILE *, const unsigned char *, size_t);
	off_t (*seek)(FILE *, off_t, int);
	unsigned char *buf;
	size_t buf_size;
	FILE *prev, *next;
	int fd;
	int pipe_pid;
	long lockcount;
	short dummy3;
	signed char mode;
	signed char lbf;
	int lock;
	int waiters;
	void *cookie;
	off_t off;
	char *getln_buf;
	void *mustbezero_2;
	unsigned char *shend;
	off_t shlim, shcnt;
};
*/

#define F_PERM 1
#define F_NORD 4
#define F_NOWR 8
#define F_EOF 16
#define F_ERR 32
#define F_SVB 64
#define F_APP 128

#define _IOFBF 0
#define _IOLBF 1
#define _IONBF 2



/*
 * Mimic the user space FILE stream struct. Please modify it on
 * demand.
 * An audit of the ft-index code showed that only __fileno was used. A
 * lot of other fields in glibc FILE struct, such as read ahead and
 * buffer writes in user level, may not be necessary
 */
typedef struct FILE {
	unsigned flags;
	int fd; // file descriptor;
	unsigned char *rpos, *rend;
	unsigned char *wpos, *wend;
	unsigned char *wbase;
	ssize_t (*write)(struct FILE *stream, const unsigned char *buf,
			 size_t len);
	ssize_t (*read)(struct FILE *stream, unsigned char *buf, size_t len);
	//size_t (*seek)(FILE *stream, off_t);
	unsigned char *buf;
	size_t bufsize;
	struct mutex lock;
	signed char lbf;
} FILE;

extern FILE *const stderr;
extern FILE *const stdout;
extern int fprintf(FILE *, const char *, ...);
 
#ifndef EOF
#define EOF (-1)
#endif
#define FSTREAM_BUFSZ 1024
/* file position funcs */
int fseek(FILE *stream, long offset, int whence);
long ftell(FILE * stream);

loff_t lseek64(int fd, loff_t offset, int whence);

typedef long long off64_t;
typedef uint64_t ino64_t;
off64_t ftello64(FILE * stream);
off_t ftello(FILE * stream);
/* FILE stream based funcs */
FILE * fopen(const char * path, const char * mode);
int fclose(FILE * stream);
FILE * fopen64(const char *, const char *);
int fgetc(FILE *f);
size_t fread(void *dst, size_t size, size_t nmemb, FILE *f);
size_t fwrite(const void *src, size_t size, size_t nmemb, FILE *f);
ssize_t getline(char **s, size_t *n, FILE *f);

struct dirent {
	ino_t          d_ino;       /*  inode number */
	off_t          d_off;       /*  offset to the next dirent */
	unsigned short d_reclen;    /*  length of this record */
	unsigned char  d_type;      /*  type of file; not supported by all file system types: DT_UNKNOWN for linux*/
	char           d_name[256]; /*  filename */
};

struct dirent64 {
	ino64_t          d_ino;       /*  inode number */
	off64_t          d_off;       /*  offset to the next dirent */
	unsigned short d_reclen;    /*  length of this record */
	unsigned char  d_type;      /*  type of file; not supported by all file system types: DT_UNKNOWN for linux*/
	char           d_name[256]; /*  filename */
};

/*
 * Mimic the user space DIR stream struct. Please modify it on
 * demand.
 * An audiot of the ft-index code only showed that only fd was
 * accessed, and only does so through calls to dirfd(). A lot of other fields in glibc DIR, __libc_lock in user level, may not be
 * necessary
*/

#define BUF_SIZE 1024
typedef struct DIR {
	int fd;
	char buf[BUF_SIZE];
        int buf_pos;
	int buf_end;	
} DIR;
/* new kernel no long uses fillonedir :( there is no way but using a buffer and pos,since more than one dirs are going to be filled. */

/* DIR stream based funcs  */
DIR *opendir(const char *name);
int closedir(DIR *dirp);

struct dirent64 * readdir64(DIR *dirp);
int dirfd(DIR* dirp);
//int flock(int fd, int cmd);


#endif /* _FTFS_FILES_H */
