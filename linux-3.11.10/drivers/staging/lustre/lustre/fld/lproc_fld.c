/*
 * GPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License version 2 for more details (a copy is included
 * in the LICENSE file that accompanied this code).
 *
 * You should have received a copy of the GNU General Public License
 * version 2 along with this program; If not, see
 * http://www.sun.com/software/products/lustre/docs/GPLv2.pdf
 *
 * Please contact Sun Microsystems, Inc., 4150 Network Circle, Santa Clara,
 * CA 95054 USA or visit www.sun.com if you need additional information or
 * have any questions.
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2012, 2013, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/fld/lproc_fld.c
 *
 * FLD (FIDs Location Database)
 *
 * Author: Yury Umanets <umka@clusterfs.com>
 *	Di Wang <di.wang@whamcloud.com>
 */

#define DEBUG_SUBSYSTEM S_FLD

# include <linux/libcfs/libcfs.h>
# include <linux/module.h>

#include <obd.h>
#include <obd_class.h>
#include <dt_object.h>
#include <md_object.h>
#include <obd_support.h>
#include <lustre_req_layout.h>
#include <lustre_fld.h>
#include <lustre_fid.h>
#include "fld_internal.h"

#ifdef LPROCFS
static int
fld_proc_targets_seq_show(struct seq_file *m, void *unused)
{
	struct lu_client_fld *fld = (struct lu_client_fld *)m->private;
	struct lu_fld_target *target;
	ENTRY;

	LASSERT(fld != NULL);

	spin_lock(&fld->lcf_lock);
	list_for_each_entry(target,
				&fld->lcf_targets, ft_chain)
		seq_printf(m, "%s\n", fld_target_name(target));
	spin_unlock(&fld->lcf_lock);

	RETURN(0);
}

static int
fld_proc_hash_seq_show(struct seq_file *m, void *unused)
{
	struct lu_client_fld *fld = (struct lu_client_fld *)m->private;
	ENTRY;

	LASSERT(fld != NULL);

	spin_lock(&fld->lcf_lock);
	seq_printf(m, "%s\n", fld->lcf_hash->fh_name);
	spin_unlock(&fld->lcf_lock);

	RETURN(0);
}

static ssize_t
fld_proc_hash_seq_write(struct file *file, const char *buffer,
			size_t count, loff_t *off)
{
	struct lu_client_fld *fld = ((struct seq_file *)file->private_data)->private;
	struct lu_fld_hash *hash = NULL;
	int i;
	ENTRY;

	LASSERT(fld != NULL);

	for (i = 0; fld_hash[i].fh_name != NULL; i++) {
		if (count != strlen(fld_hash[i].fh_name))
			continue;

		if (!strncmp(fld_hash[i].fh_name, buffer, count)) {
			hash = &fld_hash[i];
			break;
		}
	}

	if (hash != NULL) {
		spin_lock(&fld->lcf_lock);
		fld->lcf_hash = hash;
		spin_unlock(&fld->lcf_lock);

		CDEBUG(D_INFO, "%s: Changed hash to \"%s\"\n",
		       fld->lcf_name, hash->fh_name);
	}

	RETURN(count);
}

static ssize_t
fld_proc_cache_flush_write(struct file *file, const char __user *buffer,
			       size_t count, loff_t *pos)
{
	struct lu_client_fld *fld = file->private_data;
	ENTRY;

	LASSERT(fld != NULL);

	fld_cache_flush(fld->lcf_cache);

	CDEBUG(D_INFO, "%s: Lookup cache is flushed\n", fld->lcf_name);

	RETURN(count);
}

static int fld_proc_cache_flush_open(struct inode *inode, struct file *file)
{
	file->private_data = PDE_DATA(inode);
	return 0;
}

static int fld_proc_cache_flush_release(struct inode *inode, struct file *file)
{
	file->private_data = NULL;
	return 0;
}

struct file_operations fld_proc_cache_flush_fops = {
	.owner		= THIS_MODULE,
	.open		= fld_proc_cache_flush_open,
	.write		= fld_proc_cache_flush_write,
	.release	= fld_proc_cache_flush_release,
};

struct fld_seq_param {
	struct lu_env		fsp_env;
	struct dt_it		*fsp_it;
	struct lu_server_fld	*fsp_fld;
	unsigned int		fsp_stop:1;
};

static void *fldb_seq_start(struct seq_file *p, loff_t *pos)
{
	struct fld_seq_param    *param = p->private;
	struct lu_server_fld    *fld;
	struct dt_object	*obj;
	const struct dt_it_ops  *iops;

	if (param == NULL || param->fsp_stop)
		return NULL;

	fld = param->fsp_fld;
	obj = fld->lsf_obj;
	LASSERT(obj != NULL);
	iops = &obj->do_index_ops->dio_it;

	iops->load(&param->fsp_env, param->fsp_it, *pos);

	*pos = be64_to_cpu(*(__u64 *)iops->key(&param->fsp_env, param->fsp_it));
	return param;
}

static void fldb_seq_stop(struct seq_file *p, void *v)
{
	struct fld_seq_param    *param = p->private;
	const struct dt_it_ops	*iops;
	struct lu_server_fld	*fld;
	struct dt_object	*obj;

	if (param == NULL)
		return;

	fld = param->fsp_fld;
	obj = fld->lsf_obj;
	LASSERT(obj != NULL);
	iops = &obj->do_index_ops->dio_it;

	iops->put(&param->fsp_env, param->fsp_it);
}

static void *fldb_seq_next(struct seq_file *p, void *v, loff_t *pos)
{
	struct fld_seq_param    *param = p->private;
	struct lu_server_fld	*fld;
	struct dt_object	*obj;
	const struct dt_it_ops	*iops;
	int			rc;

	if (param == NULL || param->fsp_stop)
		return NULL;

	fld = param->fsp_fld;
	obj = fld->lsf_obj;
	LASSERT(obj != NULL);
	iops = &obj->do_index_ops->dio_it;

	rc = iops->next(&param->fsp_env, param->fsp_it);
	if (rc > 0) {
		param->fsp_stop = 1;
		return NULL;
	}

	*pos = be64_to_cpu(*(__u64 *)iops->key(&param->fsp_env, param->fsp_it));
	return param;
}

static int fldb_seq_show(struct seq_file *p, void *v)
{
	struct fld_seq_param    *param = p->private;
	struct lu_server_fld	*fld;
	struct dt_object	*obj;
	const struct dt_it_ops	*iops;
	struct fld_thread_info	*info;
	struct lu_seq_range	*fld_rec;
	int			rc;

	if (param == NULL || param->fsp_stop)
		return 0;

	fld = param->fsp_fld;
	obj = fld->lsf_obj;
	LASSERT(obj != NULL);
	iops = &obj->do_index_ops->dio_it;

	info = lu_context_key_get(&param->fsp_env.le_ctx,
				  &fld_thread_key);
	fld_rec = &info->fti_rec;
	rc = iops->rec(&param->fsp_env, param->fsp_it,
		       (struct dt_rec *)fld_rec, 0);
	if (rc != 0) {
		CERROR("%s:read record error: rc %d\n",
		       fld->lsf_name, rc);
	} else if (fld_rec->lsr_start != 0) {
		range_be_to_cpu(fld_rec, fld_rec);
		rc = seq_printf(p, DRANGE"\n", PRANGE(fld_rec));
	}

	return rc;
}

struct seq_operations fldb_sops = {
	.start = fldb_seq_start,
	.stop = fldb_seq_stop,
	.next = fldb_seq_next,
	.show = fldb_seq_show,
};

static int fldb_seq_open(struct inode *inode, struct file *file)
{
	struct seq_file		*seq;
	struct lu_server_fld    *fld = (struct lu_server_fld *)PDE_DATA(inode);
	struct dt_object	*obj;
	const struct dt_it_ops  *iops;
	struct fld_seq_param    *param = NULL;
	int			env_init = 0;
	int			rc;

	rc = seq_open(file, &fldb_sops);
	if (rc)
		GOTO(out, rc);

	obj = fld->lsf_obj;
	if (obj == NULL) {
		seq = file->private_data;
		seq->private = NULL;
		return 0;
	}

	OBD_ALLOC_PTR(param);
	if (param == NULL)
		GOTO(out, rc = -ENOMEM);

	rc = lu_env_init(&param->fsp_env, LCT_MD_THREAD);
	if (rc != 0)
		GOTO(out, rc);

	env_init = 1;
	iops = &obj->do_index_ops->dio_it;
	param->fsp_it = iops->init(&param->fsp_env, obj, 0, NULL);
	if (IS_ERR(param->fsp_it))
		GOTO(out, rc = PTR_ERR(param->fsp_it));

	param->fsp_fld = fld;
	param->fsp_stop = 0;

	seq = file->private_data;
	seq->private = param;
out:
	if (rc != 0) {
		if (env_init == 1)
			lu_env_fini(&param->fsp_env);
		if (param != NULL)
			OBD_FREE_PTR(param);
	}
	return rc;
}

static int fldb_seq_release(struct inode *inode, struct file *file)
{
	struct seq_file		*seq = file->private_data;
	struct fld_seq_param	*param;
	struct lu_server_fld	*fld;
	struct dt_object	*obj;
	const struct dt_it_ops	*iops;

	param = seq->private;
	if (param == NULL) {
		lprocfs_seq_release(inode, file);
		return 0;
	}

	fld = param->fsp_fld;
	obj = fld->lsf_obj;
	LASSERT(obj != NULL);
	iops = &obj->do_index_ops->dio_it;

	LASSERT(iops != NULL);
	LASSERT(obj != NULL);
	LASSERT(param->fsp_it != NULL);
	iops->fini(&param->fsp_env, param->fsp_it);
	lu_env_fini(&param->fsp_env);
	OBD_FREE_PTR(param);
	lprocfs_seq_release(inode, file);

	return 0;
}

struct lprocfs_vars fld_server_proc_list[] = {
	{ NULL }};

LPROC_SEQ_FOPS_RO(fld_proc_targets);
LPROC_SEQ_FOPS(fld_proc_hash);

struct lprocfs_vars fld_client_proc_list[] = {
	{ "targets", &fld_proc_targets_fops },
	{ "hash", &fld_proc_hash_fops },
	{ "cache_flush", &fld_proc_cache_flush_fops },
	{ NULL }};

struct file_operations fld_proc_seq_fops = {
	.owner   = THIS_MODULE,
	.open    = fldb_seq_open,
	.read    = seq_read,
	.release = fldb_seq_release,
};

#endif
