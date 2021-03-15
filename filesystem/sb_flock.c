/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:
#include <linux/slab.h>
#include <asm/uaccess.h>

struct mutex lock_list;
DEFINE_MUTEX(lock_list);

struct toku_lockfile {
	char *fname;
	struct list_head list;
};

struct list_head toku_lockfile_list;
LIST_HEAD(toku_lockfile_list);

int ftfs_toku_lock_file(const char *fname, size_t len)
{
	int r = 0;
	char *kfname = NULL;
	struct list_head *ptr;
	struct toku_lockfile *entry;
	struct toku_lockfile *new = NULL;

	mutex_lock(&lock_list);
	if (fname == NULL){
		r = -EINVAL;
		goto out;
	}

	kfname = kmalloc(len+1, GFP_KERNEL);
	if (kfname == NULL){
		r = -ENOMEM;
		goto out;
	}

	memcpy(kfname, fname, len+1);

	list_for_each(ptr, &toku_lockfile_list) {
		entry = list_entry(ptr, struct toku_lockfile, list);
		if (strcmp(entry->fname, kfname) == 0) {
			r = -EEXIST;
			goto out;
		}
	}
	new = kmalloc(sizeof(struct toku_lockfile), GFP_KERNEL);
        if (new == NULL){
		r = -ENOMEM;
		goto out;
	}
	new->fname = kfname;
	list_add(&new->list, &toku_lockfile_list);
out:
	if (r != 0){
		if (kfname != NULL)
			kfree(kfname);
		if (new != NULL)
			kfree(new);
	}
	mutex_unlock(&lock_list);
	return r;
}

int ftfs_toku_unlock_file(const char *fname, size_t len)
{
        int r = 0;
        struct list_head *ptr;
        struct list_head *temp;
        struct toku_lockfile *entry;
	char *kfname = NULL;

	mutex_lock(&lock_list);
	if (fname == NULL){
                r = -EINVAL;
                goto out;
        }

	kfname = kmalloc(len+1, GFP_KERNEL);
        if (kfname == NULL){
                r = -ENOMEM;
                goto out;
        }

	memcpy(kfname, fname, len+1);

        list_for_each_safe(ptr, temp, &toku_lockfile_list) {
                entry = list_entry(ptr, struct toku_lockfile, list);
                if (strcmp(entry->fname, fname) == 0) {
                        list_del(&entry->list);
			kfree(entry->fname);
                        kfree(entry);
			goto out;
                }
        }
	r = -ENOENT;

out:
	mutex_unlock(&lock_list);
        return r;
}
