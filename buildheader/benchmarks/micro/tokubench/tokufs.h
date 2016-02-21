#ifndef _TOKUFS_H
#define  _TOKUFS_H

static inline int toku_fs_set_cachesize(size_t cachesize)
{
	return cachesize - cachesize;
}

static inline size_t toku_fs_get_blocksize(void)
{
	return 0;
}

static inline int toku_fs_mount(void)
{
	return 0;
}

static inline int toku_fs_unmount(void)
{
	return 0;
}

#endif //_TOKUFS_H
