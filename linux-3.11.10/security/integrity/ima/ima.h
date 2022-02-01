/*
 * Copyright (C) 2005,2006,2007,2008 IBM Corporation
 *
 * Authors:
 * Reiner Sailer <sailer@watson.ibm.com>
 * Mimi Zohar <zohar@us.ibm.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, version 2 of the
 * License.
 *
 * File: ima.h
 *	internal Integrity Measurement Architecture (IMA) definitions
 */

#ifndef __LINUX_IMA_H
#define __LINUX_IMA_H

#include <linux/types.h>
#include <linux/crypto.h>
#include <linux/security.h>
#include <linux/hash.h>
#include <linux/tpm.h>
#include <linux/audit.h>

#include "../integrity.h"

enum ima_show_type { IMA_SHOW_BINARY, IMA_SHOW_ASCII };
enum tpm_pcrs { TPM_PCR0 = 0, TPM_PCR8 = 8 };

/* digest size for IMA, fits SHA1 or MD5 */
#define IMA_DIGEST_SIZE		SHA1_DIGEST_SIZE
#define IMA_EVENT_NAME_LEN_MAX	255

#define IMA_HASH_BITS 9
#define IMA_MEASURE_HTABLE_SIZE (1 << IMA_HASH_BITS)

/* set during initialization */
extern int ima_initialized;
extern int ima_used_chip;
extern char *ima_hash;
extern int ima_appraise;

/* IMA inode template definition */
struct ima_template_data {
	u8 digest[IMA_DIGEST_SIZE];	/* sha1/md5 measurement hash */
	char file_name[IMA_EVENT_NAME_LEN_MAX + 1];	/* name + \0 */
};

struct ima_template_entry {
	u8 digest[IMA_DIGEST_SIZE];	/* sha1 or md5 measurement hash */
	const char *template_name;
	int template_len;
	struct ima_template_data template;
};

struct ima_queue_entry {
	struct hlist_node hnext;	/* place in hash collision list */
	struct list_head later;		/* place in ima_measurements list */
	struct ima_template_entry *entry;
};
extern struct list_head ima_measurements;	/* list of all measurements */

/* Internal IMA function definitions */
int ima_init(void);
void ima_cleanup(void);
int ima_fs_init(void);
void ima_fs_cleanup(void);
int ima_inode_alloc(struct inode *inode);
int ima_add_template_entry(struct ima_template_entry *entry, int violation,
			   const char *op, struct inode *inode);
int ima_calc_file_hash(struct file *file, char *digest);
int ima_calc_buffer_hash(const void *data, int len, char *digest);
int ima_calc_boot_aggregate(char *digest);
void ima_add_violation(struct inode *inode, const unsigned char *filename,
		       const char *op, const char *cause);
int ima_init_crypto(void);

/*
 * used to protect h_table and sha_table
 */
extern spinlock_t ima_queue_lock;

struct ima_h_table {
	atomic_long_t len;	/* number of stored measurements in the list */
	atomic_long_t violations;
	struct hlist_head queue[IMA_MEASURE_HTABLE_SIZE];
};
extern struct ima_h_table ima_htable;

static inline unsigned long ima_hash_key(u8 *digest)
{
	return hash_long(*digest, IMA_HASH_BITS);
}

/* LIM API function definitions */
int ima_get_action(struct inode *inode, int mask, int function);
int ima_must_measure(struct inode *inode, int mask, int function);
int ima_collect_measurement(struct integrity_iint_cache *iint,
			    struct file *file);
void ima_store_measurement(struct integrity_iint_cache *iint, struct file *file,
			   const unsigned char *filename);
void ima_audit_measurement(struct integrity_iint_cache *iint,
			   const unsigned char *filename);
int ima_store_template(struct ima_template_entry *entry, int violation,
		       struct inode *inode);
void ima_template_show(struct seq_file *m, void *e, enum ima_show_type show);
const char *ima_d_path(struct path *path, char **pathbuf);

/* rbtree tree calls to lookup, insert, delete
 * integrity data associated with an inode.
 */
struct integrity_iint_cache *integrity_iint_insert(struct inode *inode);
struct integrity_iint_cache *integrity_iint_find(struct inode *inode);

/* IMA policy related functions */
enum ima_hooks { FILE_CHECK = 1, MMAP_CHECK, BPRM_CHECK, MODULE_CHECK, POST_SETATTR };

int ima_match_policy(struct inode *inode, enum ima_hooks func, int mask,
		     int flags);
void ima_init_policy(void);
void ima_update_policy(void);
ssize_t ima_parse_add_rule(char *);
void ima_delete_rules(void);

/* Appraise integrity measurements */
#define IMA_APPRAISE_ENFORCE	0x01
#define IMA_APPRAISE_FIX	0x02
#define IMA_APPRAISE_MODULES	0x04

#ifdef CONFIG_IMA_APPRAISE
int ima_appraise_measurement(int func, struct integrity_iint_cache *iint,
			     struct file *file, const unsigned char *filename);
int ima_must_appraise(struct inode *inode, int mask, enum ima_hooks func);
void ima_update_xattr(struct integrity_iint_cache *iint, struct file *file);
enum integrity_status ima_get_cache_status(struct integrity_iint_cache *iint,
					   int func);

#else
static inline int ima_appraise_measurement(int func,
					   struct integrity_iint_cache *iint,
					   struct file *file,
					   const unsigned char *filename)
{
	return INTEGRITY_UNKNOWN;
}

static inline int ima_must_appraise(struct inode *inode, int mask,
				    enum ima_hooks func)
{
	return 0;
}

static inline void ima_update_xattr(struct integrity_iint_cache *iint,
				    struct file *file)
{
}

static inline enum integrity_status ima_get_cache_status(struct integrity_iint_cache
							 *iint, int func)
{
	return INTEGRITY_UNKNOWN;
}
#endif

/* LSM based policy rules require audit */
#ifdef CONFIG_IMA_LSM_RULES

#define security_filter_rule_init security_audit_rule_init
#define security_filter_rule_match security_audit_rule_match

#else

static inline int security_filter_rule_init(u32 field, u32 op, char *rulestr,
					    void **lsmrule)
{
	return -EINVAL;
}

static inline int security_filter_rule_match(u32 secid, u32 field, u32 op,
					     void *lsmrule,
					     struct audit_context *actx)
{
	return -EINVAL;
}
#endif /* CONFIG_IMA_LSM_RULES */
#endif
