#ifndef _FTFS_ERROR_H
#define _FTFS_ERROR_H

void ftfs_set_errno(int ret);
int ftfs_get_errno(void);

void set_errno(int ret);
int get_errno(void);
void perror(const char *s);

/* set errno only if err is nonzero */
static inline int return_errno_nonzero(int err)
{
	if (err)  {
		ftfs_set_errno(err);
		return -err;
	}
	return 0;
}

/* set errno only if err is negative */
static inline int return_errno_pos(int err)
{
	if (err < 0)  {
		ftfs_set_errno(err);
		return -err;
	}
	return err;
}

/* unconditionally set errno, return userspace error condition (-1) */
static inline int return_set_errno(int err)
{
	ftfs_set_errno(err);
	return -err;
}

/* same as above. these should not overflow */
static inline long int return_errno_nonzero_l(long int err)
{
	if (err)  {
		ftfs_set_errno(err);
		return -err;
	}
	return 0;
}

static inline long int return_errno_pos_l(long int err)
{
	if (err < 0)  {
		ftfs_set_errno(err);
		return -err;
	}
	return err;
}

static inline long int return_set_errno_l(long int err)
{
	ftfs_set_errno(err);
	return -err;
}
#endif
