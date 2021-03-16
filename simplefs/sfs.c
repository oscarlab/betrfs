#include <linux/init.h>
#include <linux/module.h>
#include <linux/fs.h>
#include <linux/namei.h>
#include <linux/buffer_head.h>
#include <linux/slab.h>
#include <linux/random.h>
#include <linux/version.h>
#include <linux/jbd2.h>
#include <linux/parser.h>
#include <linux/blkdev.h>
#include <linux/mpage.h>
#include <linux/aio.h>
#include <linux/pagevec.h>
#include <linux/statfs.h>
#include <asm/div64.h>

#include "super.h"
#include "sfs.h"
#include "alloc_table.h"

static struct kmem_cache *sfs_inode_cachep;

#define BLK_SIZE_4K	(4096)
#define BLK_SIZE_512B	(512)

static uint64_t sfs_sector_per_block(struct super_block *sb)
{
	BUG_ON(!sb);
	if (sb->s_blocksize == BLK_SIZE_4K) {
		return 1;
	} else if (sb->s_blocksize == BLK_SIZE_512B) {
		return 8;
	} else {
		printk("This block size is not supported yet\n");
		BUG();
		return 0;
	}
}

/*
 * YZJ: This hook is a part of readdir/getdents. In simplefs a directory
 * only has 4096 B data. It does not support large directory.
 * Read this block and copy dir entries to ctx starting from ctx->pos
 */
static int sfs_iterate(struct file *filp, struct dir_context *ctx)
{
	struct inode *inode;
	struct super_block *sb;
	struct buffer_head *bh;
	struct sfs_inode *sfs_inode;
	struct sfs_dir_entry *entry;
	struct sfs_super_block *sfs_sb;
	uint64_t sector_per_blk;
	uint64_t pos;
	int i;
	int ret = 0;

	pos = ctx->pos;
	inode = filp->f_dentry->d_inode;
	sb = inode->i_sb;
	sfs_sb = SFS_SB(sb);
	BUG_ON(!sfs_sb);
	sector_per_blk = sfs_sb->nr_sector_per_blk;

	/* Sanity Check: ctx->pos is multiply of size of sfs_dir_entry*/
	do_div(pos, sizeof(struct sfs_dir_entry));
	if (ctx->pos - sizeof(struct sfs_dir_entry) * pos != 0) {
		printk("%s:ctx->pos is invalid!\n", __func__);
		return -EINVAL;
	}

	sfs_inode = SFS_INODE(inode);
	BUG_ON(!sfs_inode);
	if (unlikely(!S_ISDIR(sfs_inode->mode))) {
		printk("fs object is not a directory\n");
		return -ENOTDIR;
	}
	/* pos is smaller than dir_children_count */
	if (pos > sfs_inode->dir_children_count) {
		printk("%s:ctx->pos is invalid!\n", __func__);
		return -EINVAL;
	}

	bh = sb_bread(sb, sfs_inode->start_block_number * sector_per_blk);
	BUG_ON(!bh);

	entry = (struct sfs_dir_entry *)bh->b_data + ctx->pos;
	for (i = pos; i < sfs_inode->dir_children_count; i++) {
		ret = ctx->actor(ctx, entry->filename,
				SFS_FILENAME_MAXLEN, ctx->pos,
				entry->inode_no, DT_UNKNOWN);
		/* ret=0, keep the iteration; otherwise return error */
		if (ret)
			break;
		ctx->pos += sizeof(struct sfs_dir_entry);
		pos += sizeof(struct sfs_dir_entry);
		entry++;
	}
	brelse(bh);
	return ret;
}


/* This function returns a sfs_inode with the given inode_no from the inode
 * table. If it exists
 */
struct sfs_inode *sfs_get_inode(struct super_block *sb, uint64_t inode_no,
				struct sfs_inode *inode_buffer)
{
	struct sfs_super_block *sfs_sb = SFS_SB(sb);
	struct sfs_inode *sfs_inode = NULL;
	struct sfs_inode *ret = NULL;
	struct buffer_head *bh;
	uint64_t sec_per_blk = sfs_sb->nr_sector_per_blk;
	int i;

	/*
	 * YZJ: We rely on 4KiB block size here, double check here
	 * if block size is changed
	 */
	bh = sb_bread(sb, SFS_INODETABLE_BLOCK_NUMBER * sec_per_blk);
	BUG_ON(!bh);
	sfs_inode = (struct sfs_inode *) bh->b_data;

	for (i = 0; i < sfs_sb->inodes_count; i++) {
		if (sfs_inode->inode_no != inode_no) {
			sfs_inode++;
			continue;
		}
		BUG_ON(inode_buffer == NULL);
		memcpy(inode_buffer, sfs_inode, sizeof(*inode_buffer));
		ret = inode_buffer;
		break;
	}
	brelse(bh);
	return ret;
}

bool sfs_is_inode_log(struct inode *inode) {
	struct sfs_inode *sfs_inode = SFS_INODE(inode);

	BUG_ON(!sfs_inode);
	if (sfs_inode->inode_no == FT_META_INODE_NUMBER)
		return true;
	return false;
}

void sfs_print_inode_iblock(struct inode *inode, sector_t iblock)
{
	struct sfs_inode *sfs_inode;

	sfs_inode = SFS_INODE(inode);
	BUG_ON(!sfs_inode);

	printk("sfs_get_block in: ino=%u, start_blk_no=%llu\n",
		sfs_inode->inode_no, sfs_inode->start_block_number);
	printk("sfs_get_block in: iblock=%llu, block_count=%llu\n",
		(uint64_t)iblock, sfs_inode->block_count);
}

int sfs_get_block(struct inode *inode, sector_t iblock,
		  struct buffer_head *bh_result, int create)
{
	struct sfs_inode *sfs_inode;
	struct super_block *sb = inode->i_sb;
	uint64_t sec_per_blk = sfs_sector_per_block(sb);
	sector_t nr_sec;

	sfs_inode = SFS_INODE(inode);
	if (iblock > sfs_inode->block_count * sec_per_blk) {
		printk("iblock=%lu\n", iblock);
		printk("iblock=%llu\n", sfs_inode->block_count);
		printk("sec_per_blk=%llu\n", sec_per_blk);
		printk("total=%llu\n", sfs_inode->block_count * sec_per_blk);
		BUG();
	}

	clear_buffer_new(bh_result);
	set_buffer_new(bh_result);
	nr_sec = sfs_inode->start_block_number * sec_per_blk + iblock;
	map_bh(bh_result, sb, nr_sec);
	bh_result->b_size = sb->s_blocksize; 

	return 0;
}

void sfs_dump_write_begin_page(struct page *page)
{
	char *buf = kmap(page);

	print_hex_dump(KERN_ALERT, "PAGE 0 of LOG:",
		       DUMP_PREFIX_ADDRESS,
		       16, 1, buf, 16, 1);
	kunmap(page);	
}

static int
sfs_write_begin(struct file *file, struct address_space *mapping,
		loff_t pos, unsigned len, unsigned flags,
		struct page **pagep, void **fsdata)
{
	int ret;
	
	ret = sfs_block_write_begin(mapping, pos, len,
				    flags, pagep,
				    sfs_get_block);
	if (ret < 0) {
		BUG();
	}
	return ret;
}

static int sfs_write_end(struct file *file, struct address_space *mapping,
			loff_t pos, unsigned len, unsigned copied,
			struct page *page, void *fsdata)
{
	int ret;

	ret = sfs_generic_write_end(file, mapping, pos,
				    len, copied, page,
				    fsdata);
	if (ret < len) {
		BUG();
	}
	return ret;
}

bool sfs_is_dictionary_file(struct file *filp) {
	struct inode *inode = filp->f_mapping->host;
	struct sfs_inode *sfs_inode;

	sfs_inode = SFS_INODE(inode);
	if (sfs_inode->inode_no == FT_DATA_INODE_NUMBER ||
		sfs_inode->inode_no == FT_META_INODE_NUMBER) {
		return true;
	}
	return false;
}

ssize_t sfs_sync_read(struct file *filp, char __user *buf,
			size_t len, loff_t *ppos)
{
	return do_sync_read(filp, buf, len, ppos);
}

ssize_t sfs_sync_write(struct file *filp, const char __user *buf,
			size_t len, loff_t *ppos)
{
	ssize_t ret;
	ret = do_sync_write(filp, buf, len, ppos);
	return ret;
}


static ssize_t
sfs_file_read(struct kiocb *iocb, const struct iovec *iov,
		unsigned long nr_segs, loff_t pos)
{
	return generic_file_aio_read(iocb, iov, nr_segs, pos);
}

static ssize_t
sfs_file_write(struct kiocb *iocb, const struct iovec *iov,
		unsigned long nr_segs, loff_t pos)
{
	return sfs_in_kernel_write(iocb, iov, nr_segs, pos);
}

static int
sfs_fsync(struct file *file, loff_t start, loff_t end, int datasync)
{
	int ret;
	struct dentry *dentry = file->f_path.dentry;
	struct inode *inode = dentry->d_inode;

	ret = generic_file_fsync(file, start, end, datasync);
#if LINUX_VERSION_CODE <  KERNEL_VERSION(3,16,0)
	if (ret == 0) {
		ret = blkdev_issue_flush(inode->i_sb->s_bdev, GFP_KERNEL, NULL);
	} else {
		printk("generic_file_fsync encounter error\n");
		BUG();
	}
#endif
	return ret;
}

static int sfs_setattr(struct dentry *dentry, struct iattr *iattr)
{
	int ret;
	struct inode *inode = dentry->d_inode;
	loff_t size;

	ret = inode_change_ok(inode, iattr);
	if (ret)
		return ret;

	size = i_size_read(inode);

	if ((iattr->ia_valid & ATTR_SIZE) && iattr->ia_size < size) {
		printk("I am immutable, don't change my size\n");
		BUG();
	}
	return ret;
}

/* --------------- address_space_operations --------------- */

static int sfs_readpage(struct file *file, struct page *page)
{
	return mpage_readpage(page, sfs_get_block);
}

static int sfs_readpages(struct file *file, struct address_space *mapping,
			struct list_head *pages, unsigned nr_pages)
{
	return mpage_readpages(mapping, pages, nr_pages, sfs_get_block);
}

static int
sfs_writepage(struct page *page, struct writeback_control *wbc)
{
	int ret = block_write_full_page(page, sfs_get_block, wbc);
	return ret;
}

static int
sfs_writepages(struct address_space *mapping, struct writeback_control *wbc)
{
	int ret;

	ret = mpage_writepages(mapping, wbc, sfs_get_block);
	return ret;
}

static sector_t sfs_bmap(struct address_space *mapping, sector_t block)
{
	struct sfs_inode *sfs_inode;
	struct inode *inode = mapping->host;
	struct super_block *sb = inode->i_sb;
	struct sfs_super_block *sfs_sb = SFS_SB(sb);
	uint64_t sec_per_blk = sfs_sb->nr_sector_per_blk;

	sfs_inode = SFS_INODE(mapping->host);
	BUG_ON(block > sfs_inode->block_count * sec_per_blk);

	return sfs_inode->start_block_number * sec_per_blk + block;
}

const struct address_space_operations sfs_aops = {
	.readpage	= sfs_readpage,
	.readpages	= sfs_readpages,
	.writepage	= sfs_writepage,
	.writepages	= sfs_writepages,
	.write_begin	= sfs_write_begin,
	.write_end	= sfs_write_end,
	.bmap		= sfs_bmap,
};

struct dentry *sfs_lookup(struct inode *parent_inode,
			  struct dentry *child_dentry,
			  unsigned int flags);
static int sfs_create(struct inode *dir,
		      struct dentry *dentry,
		      umode_t mode,
		      bool excl);
static int sfs_mkdir(struct inode *dir,
		     struct dentry *dentry,
		     umode_t mode);

static int sfs_mkdir(struct inode *dir,
		     struct dentry *dentry,
		     umode_t mode)
{
	printk("do not create dir:%s\n", dentry->d_name.name);
	BUG();
	return -EINVAL;
}

static struct dentry *__sfs_lookup(struct inode *parent_inode,
				   struct dentry *child_dentry,
				   unsigned int flags);

static int sfs_create(struct inode *dir,
		      struct dentry *dentry,
		      umode_t mode, bool excl)
{
	struct dentry *ret;

	ret = __sfs_lookup(dir, dentry, 0);
	if (ret == NULL) {
		printk("No permission to create new files\n");
		return -EPERM;
	}
	return 0;
}

static loff_t sfs_file_llseek(struct file *file, loff_t offset, int whence)
{
	loff_t ret;
	ret = generic_file_llseek(file, offset, whence);
	return ret;
}

const struct file_operations sfs_file_ops = {
	.llseek = sfs_file_llseek,
	.fsync = sfs_fsync,
	.read = sfs_sync_read,
	.write = sfs_sync_write,
	.aio_read = sfs_file_read,
	.aio_write = sfs_file_write,
};

const struct file_operations sfs_dir_ops = {
	.owner = THIS_MODULE,
	.iterate = sfs_iterate,
	.fsync = sfs_fsync,
	.llseek = sfs_file_llseek,
};

static const struct inode_operations sfs_file_inode_ops = {
	.setattr = sfs_setattr
};

static struct inode_operations sfs_dir_inode_ops = {
	.create = sfs_create,
	.lookup = sfs_lookup,
	.mkdir = sfs_mkdir,
	.setattr = sfs_setattr,
};

static struct inode *sfs_iget(struct super_block *sb,
			      uint32_t ino,
			      struct sfs_inode *sfs_inode)
{
	struct inode *inode = NULL;

	inode = iget_locked(sb, ino);
	if (IS_ERR(inode)) 
		goto exit;
	if (!(inode->i_state & I_NEW))
		return inode;

	inode->i_ino = ino;
	inode->i_sb = sb;

	if (S_ISDIR(sfs_inode->mode)) {
		inode->i_size = sfs_inode->dir_children_count *
				sizeof(sfs_inode);
		inode->i_fop = &sfs_dir_ops;
		inode->i_op = &sfs_dir_inode_ops;
	} else if (S_ISREG(sfs_inode->mode)) {
		inode->i_size = sfs_inode->block_count * SFS_DEFAULT_BLOCK_SIZE;
		inode->i_fop = &sfs_file_ops;
		inode->i_op = &sfs_file_inode_ops;
		inode->i_data.a_ops = &sfs_aops;
	} else {
		printk(KERN_ERR "Unknown inode type\n");
	}

	inode->i_atime = inode->i_mtime = inode->i_ctime = CURRENT_TIME;
	inode->i_private = sfs_inode;
	unlock_new_inode(inode);
exit:
	return inode;
}

static void sfs_setup_dio_data(struct sfs_inode *sfs_inode)
{
	struct sfs_dio_data *dio;

	dio = (struct sfs_dio_data*) kmalloc(sizeof(*dio), GFP_KERNEL);
	BUG_ON(dio == NULL);
	INIT_LIST_HEAD(&dio->data);
	dio->nr_items = 0;
	mutex_init(&dio->lock);
	sfs_inode->dio = dio;
}

/*
 * YZJ: inode are readonly and there is no lock.
 * sfs_create only checks the existentce of a file
 */
static struct dentry *__sfs_lookup(struct inode *p_inode,
				   struct dentry *child_dentry,
				   unsigned int flags)
{
	struct sfs_inode *parent = SFS_INODE(p_inode);
	struct super_block *sb = p_inode->i_sb;
	struct buffer_head *bh;
	struct sfs_dir_entry *entry;
	struct sfs_inode *sfs_inode;
	struct inode *inode;
	struct dentry *ret = NULL;
	uint64_t sector_per_blk;
	int i;
	int nbytes = 0;
	bool found = false;

	sector_per_blk = sfs_sector_per_block(sb);
	bh = sb_bread(sb, parent->start_block_number * sector_per_blk);
	BUG_ON(!bh);

	entry = (struct sfs_dir_entry *) bh->b_data;
	for (i = 0; i < parent->dir_children_count; i++) {
		if (0 == strcmp(entry->filename, child_dentry->d_name.name)) {
			found = true;
			break;
		}
		entry++;
		nbytes += sizeof(*entry);
	}

	if (found) {
		sfs_inode = kmem_cache_alloc(sfs_inode_cachep, GFP_KERNEL);
		if (!sfs_inode) {
			printk("%s:Alloc sfs_inode failed\n", __func__);
			goto out;
		}
		if (!sfs_get_inode(sb, entry->inode_no, sfs_inode)) {
			kmem_cache_free(sfs_inode_cachep, sfs_inode);
			printk("%s:Get sfs_inode failed\n", __func__);
			goto out;
		}
		sfs_setup_dio_data(sfs_inode);
		inode = sfs_iget(sb, entry->inode_no, sfs_inode);
		BUG_ON(inode == NULL || IS_ERR(inode));
		inode_init_owner(inode, p_inode, sfs_inode->mode);
		ret = d_splice_alias(inode, child_dentry);
	}
out:
	brelse(bh);
	return ret;
}

struct dentry *sfs_lookup(struct inode *parent_inode,
			  struct dentry *child_dentry,
			  unsigned int flags)
{
	return __sfs_lookup(parent_inode, child_dentry, flags);
}

void sfs_destroy_inode(struct inode *inode)
{
	struct sfs_inode *sfs_inode = SFS_INODE(inode);
	if (sfs_inode->dio) {
		kfree(sfs_inode->dio);
	}
	kmem_cache_free(sfs_inode_cachep, sfs_inode);
}

static void sfs_put_super(struct super_block *sb)
{
	sync_filesystem(sb);
}

int sfs_write_inode(struct inode *inode, struct writeback_control *wbc)
{	
	return 0;
}

static int sfs_sync_fs(struct super_block *sb, int wait)
{
	return 0;
}

/* YZJ 9/11/2019: we really just fake some of the stats here
 * so that df will list ftfs after it is mounted. This feature
 * is needed for xfstests which check mountpoint is mounted
 * to correct file system
 */
static inline int sfs_super_statfs(struct dentry *d, struct kstatfs *buf)
{
	struct super_block *sb = d->d_sb;
	struct block_device *bdev = sb->s_bdev;

	buf->f_bsize = sb->s_blocksize;
	buf->f_bfree = i_size_read(bdev->bd_inode) >> SFS_DEFAULT_BLOCK_BITS;
	buf->f_bavail = i_size_read(bdev->bd_inode) >> SFS_DEFAULT_BLOCK_BITS;
	buf->f_blocks = i_size_read(bdev->bd_inode) >> SFS_DEFAULT_BLOCK_BITS;
	buf->f_ffree = 0xdeadbeef;
	buf->f_files = 0xdead0000;
	buf->f_namelen = SFS_NAME_LEN;
	return 0;
}

static const struct super_operations sfs_sops = {
	.destroy_inode = sfs_destroy_inode,
	.put_super = sfs_put_super,
	.write_inode = sfs_write_inode,
	.sync_fs = sfs_sync_fs,
	.statfs	= sfs_super_statfs,
};


int sfs_fill_super(struct super_block *sb, void *data, int silent)
{
	struct inode *root_inode;
	struct buffer_head *bh;
	struct sfs_super_block *sb_disk;
	int ret = -EPERM;
	uint64_t sector_per_blk;
	void *sfs_info_buf;
	struct sfs_inode *inode_buffer;


	sector_per_blk = sfs_sector_per_block(sb); 
	bh = sb_bread(sb, SFS_SUPERBLOCK_BLOCK_NUMBER * sector_per_blk);
	BUG_ON(!bh);
	
	sb_disk = (struct sfs_super_block *) bh->b_data;
	sb_disk->nr_sector_per_blk = sector_per_blk;

	if (unlikely(sb_disk->magic != SFS_MAGIC)) {
		printk("Magic number mismatch\n");
		ret = -EINVAL;
		goto out;
	}

	if (unlikely(sb_disk->sfs_block_size != SFS_DEFAULT_BLOCK_SIZE)) {
		printk("Non standard block size\n");
		ret = -EINVAL;
		goto out;
	}

	sfs_info_buf = kmemdup(bh->b_data, bh->b_size, GFP_KERNEL);
	if (!sfs_info_buf) {
		printk("Alloc sfs_super_block failed\n");
		ret = -ENOMEM;
		goto out;
	}

	sb->s_magic = SFS_MAGIC;
	sb->s_maxbytes = SFS_DEFAULT_MAX_BYTES;
	sb->s_op = &sfs_sops;
	sb->s_fs_info = sfs_info_buf;

	root_inode = new_inode(sb);
	root_inode->i_ino = SFS_ROOTDIR_INODE_NUMBER;
	inode_init_owner(root_inode, NULL, S_IFDIR);
	root_inode->i_sb = sb;
	root_inode->i_op = &sfs_dir_inode_ops;
	root_inode->i_fop = &sfs_dir_ops;
	root_inode->i_atime = CURRENT_TIME;
	root_inode->i_mtime = CURRENT_TIME;
	root_inode->i_ctime = CURRENT_TIME;

	/* Create sfs_inode and link it to vfs inode later */
	inode_buffer = kmem_cache_alloc(sfs_inode_cachep, GFP_KERNEL);
	if (!inode_buffer) {
		kfree(sfs_info_buf);
		printk("Alloc sfs_inode for root inode failed\n");
		ret = -ENOMEM;
		goto out;
	}

	/* Copy on disk sfs_inode to inode_buffer */
	if (!sfs_get_inode(sb, SFS_ROOTDIR_INODE_NUMBER, inode_buffer)) {
		kfree(sfs_info_buf);
		kmem_cache_free(sfs_inode_cachep, inode_buffer);
		printk("Get sfs_inode for root inode failed\n");
		ret = -EINVAL;
		goto out;
	}

	root_inode->i_private = inode_buffer;
	sb->s_root = d_make_root(root_inode);
	if (!sb->s_root) {
		kfree(sfs_info_buf);
		kmem_cache_free(sfs_inode_cachep, inode_buffer);
		ret = -ENOMEM;
		goto out;
	}
	ret = 0;
out:
	brelse(bh);
	return ret;
}

static struct dentry *sfs_mount(struct file_system_type *fs_type,
				int flags, const char *dev_name,
				void *data)
{
	struct dentry *ret;

	ret = mount_bdev(fs_type, flags, dev_name, data, sfs_fill_super);

	if (unlikely(IS_ERR(ret)))
		printk("ERROR mounting sfs\n");
	else
		printk("sfs is successfully mounted on [%s]\n", dev_name);
	return ret;	
}

static void sfs_kill_sb(struct super_block *sb)
{
	sync_filesystem(sb);
	if (sb->s_fs_info != NULL) {
		kfree(sb->s_fs_info);
	}
	kill_block_super(sb);
	printk("SFS superblock is destroyed. Unmount successful.\n");
	return;
}

struct file_system_type sfs_fs_type = {
	.owner = THIS_MODULE,
	.name = "sfs",
	.mount = sfs_mount,
	.kill_sb = sfs_kill_sb,
	.fs_flags = FS_REQUIRES_DEV,
};

static int sfs_init(void)
{
	int ret;

	/*
	 * FIXME: Do we really need kmem_cache_create
	 * since we only 13 inodes
	 */
	sfs_inode_cachep = kmem_cache_create("sfs_inode_cache",
				sizeof(struct sfs_inode),
				0,
				(SLAB_RECLAIM_ACCOUNT | SLAB_MEM_SPREAD),
				NULL);

	if (!sfs_inode_cachep) {
		return -ENOMEM;
	}

	ret = register_filesystem(&sfs_fs_type);
	if (likely(ret == 0)) 
		printk(KERN_INFO "Successfully registered sfs\n");
	else
		printk(KERN_INFO "Failed to register sfs, error %d\n", ret);

	return ret;
}

static void sfs_exit(void)
{
	int ret;

	ret = unregister_filesystem(&sfs_fs_type);
	kmem_cache_destroy(sfs_inode_cachep);

	if (likely(ret == 0)) 
		printk(KERN_INFO "Successfully unregistered sfs\n");
	else
		printk(KERN_INFO "Failed to unregister sfs, error %d\n", ret);
}

module_init(sfs_init);
module_exit(sfs_exit);
MODULE_LICENSE("GPL");
