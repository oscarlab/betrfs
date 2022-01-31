#include <linux/fs.h>

ssize_t sfs_directIO_read_write(struct file *file, uint8_t *raw_block,
			int64_t size, int64_t offset, int rw,
			void (*fun)(void*));
