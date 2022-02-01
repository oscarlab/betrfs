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
 * Copyright (c) 2001, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2012, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#define DEBUG_SUBSYSTEM S_MDC

# include <linux/module.h>
# include <linux/pagemap.h>
# include <linux/miscdevice.h>
# include <linux/init.h>
# include <linux/utsname.h>

#include <lustre_acl.h>
#include <obd_class.h>
#include <lustre_fid.h>
#include <lprocfs_status.h>
#include <lustre_param.h>
#include <lustre_log.h>

#include "mdc_internal.h"

#define REQUEST_MINOR 244

struct mdc_renew_capa_args {
	struct obd_capa	*ra_oc;
	renew_capa_cb_t	 ra_cb;
};

static int mdc_cleanup(struct obd_device *obd);

int mdc_unpack_capa(struct obd_export *exp, struct ptlrpc_request *req,
		    const struct req_msg_field *field, struct obd_capa **oc)
{
	struct lustre_capa *capa;
	struct obd_capa *c;
	ENTRY;

	/* swabbed already in mdc_enqueue */
	capa = req_capsule_server_get(&req->rq_pill, field);
	if (capa == NULL)
		RETURN(-EPROTO);

	c = alloc_capa(CAPA_SITE_CLIENT);
	if (IS_ERR(c)) {
		CDEBUG(D_INFO, "alloc capa failed!\n");
		RETURN(PTR_ERR(c));
	} else {
		c->c_capa = *capa;
		*oc = c;
		RETURN(0);
	}
}

static inline int mdc_queue_wait(struct ptlrpc_request *req)
{
	struct client_obd *cli = &req->rq_import->imp_obd->u.cli;
	int rc;

	/* mdc_enter_request() ensures that this client has no more
	 * than cl_max_rpcs_in_flight RPCs simultaneously inf light
	 * against an MDT. */
	rc = mdc_enter_request(cli);
	if (rc != 0)
		return rc;

	rc = ptlrpc_queue_wait(req);
	mdc_exit_request(cli);

	return rc;
}

/* Helper that implements most of mdc_getstatus and signal_completed_replay. */
/* XXX this should become mdc_get_info("key"), sending MDS_GET_INFO RPC */
static int send_getstatus(struct obd_import *imp, struct lu_fid *rootfid,
			  struct obd_capa **pc, int level, int msg_flags)
{
	struct ptlrpc_request *req;
	struct mdt_body       *body;
	int		    rc;
	ENTRY;

	req = ptlrpc_request_alloc_pack(imp, &RQF_MDS_GETSTATUS,
					LUSTRE_MDS_VERSION, MDS_GETSTATUS);
	if (req == NULL)
		RETURN(-ENOMEM);

	mdc_pack_body(req, NULL, NULL, 0, 0, -1, 0);
	lustre_msg_add_flags(req->rq_reqmsg, msg_flags);
	req->rq_send_state = level;

	ptlrpc_request_set_replen(req);

	rc = ptlrpc_queue_wait(req);
	if (rc)
		GOTO(out, rc);

	body = req_capsule_server_get(&req->rq_pill, &RMF_MDT_BODY);
	if (body == NULL)
		GOTO(out, rc = -EPROTO);

	if (body->valid & OBD_MD_FLMDSCAPA) {
		rc = mdc_unpack_capa(NULL, req, &RMF_CAPA1, pc);
		if (rc)
			GOTO(out, rc);
	}

	*rootfid = body->fid1;
	CDEBUG(D_NET,
	       "root fid="DFID", last_committed="LPU64"\n",
	       PFID(rootfid),
	       lustre_msg_get_last_committed(req->rq_repmsg));
	EXIT;
out:
	ptlrpc_req_finished(req);
	return rc;
}

/* This should be mdc_get_info("rootfid") */
int mdc_getstatus(struct obd_export *exp, struct lu_fid *rootfid,
		  struct obd_capa **pc)
{
	return send_getstatus(class_exp2cliimp(exp), rootfid, pc,
			      LUSTRE_IMP_FULL, 0);
}

/*
 * This function now is known to always saying that it will receive 4 buffers
 * from server. Even for cases when acl_size and md_size is zero, RPC header
 * will contain 4 fields and RPC itself will contain zero size fields. This is
 * because mdt_getattr*() _always_ returns 4 fields, but if acl is not needed
 * and thus zero, it shrinks it, making zero size. The same story about
 * md_size. And this is course of problem when client waits for smaller number
 * of fields. This issue will be fixed later when client gets aware of RPC
 * layouts.  --umka
 */
static int mdc_getattr_common(struct obd_export *exp,
			      struct ptlrpc_request *req)
{
	struct req_capsule *pill = &req->rq_pill;
	struct mdt_body    *body;
	void	       *eadata;
	int		 rc;
	ENTRY;

	/* Request message already built. */
	rc = ptlrpc_queue_wait(req);
	if (rc != 0)
		RETURN(rc);

	/* sanity check for the reply */
	body = req_capsule_server_get(pill, &RMF_MDT_BODY);
	if (body == NULL)
		RETURN(-EPROTO);

	CDEBUG(D_NET, "mode: %o\n", body->mode);

	if (body->eadatasize != 0) {
		mdc_update_max_ea_from_body(exp, body);

		eadata = req_capsule_server_sized_get(pill, &RMF_MDT_MD,
						      body->eadatasize);
		if (eadata == NULL)
			RETURN(-EPROTO);
	}

	if (body->valid & OBD_MD_FLRMTPERM) {
		struct mdt_remote_perm *perm;

		LASSERT(client_is_remote(exp));
		perm = req_capsule_server_swab_get(pill, &RMF_ACL,
						lustre_swab_mdt_remote_perm);
		if (perm == NULL)
			RETURN(-EPROTO);
	}

	if (body->valid & OBD_MD_FLMDSCAPA) {
		struct lustre_capa *capa;
		capa = req_capsule_server_get(pill, &RMF_CAPA1);
		if (capa == NULL)
			RETURN(-EPROTO);
	}

	RETURN(0);
}

int mdc_getattr(struct obd_export *exp, struct md_op_data *op_data,
		struct ptlrpc_request **request)
{
	struct ptlrpc_request *req;
	int		    rc;
	ENTRY;

	/* Single MDS without an LMV case */
	if (op_data->op_flags & MF_GET_MDT_IDX) {
		op_data->op_mds = 0;
		RETURN(0);
	}
	*request = NULL;
	req = ptlrpc_request_alloc(class_exp2cliimp(exp), &RQF_MDS_GETATTR);
	if (req == NULL)
		RETURN(-ENOMEM);

	mdc_set_capa_size(req, &RMF_CAPA1, op_data->op_capa1);

	rc = ptlrpc_request_pack(req, LUSTRE_MDS_VERSION, MDS_GETATTR);
	if (rc) {
		ptlrpc_request_free(req);
		RETURN(rc);
	}

	mdc_pack_body(req, &op_data->op_fid1, op_data->op_capa1,
		      op_data->op_valid, op_data->op_mode, -1, 0);

	req_capsule_set_size(&req->rq_pill, &RMF_MDT_MD, RCL_SERVER,
			     op_data->op_mode);
	if (op_data->op_valid & OBD_MD_FLRMTPERM) {
		LASSERT(client_is_remote(exp));
		req_capsule_set_size(&req->rq_pill, &RMF_ACL, RCL_SERVER,
				     sizeof(struct mdt_remote_perm));
	}
	ptlrpc_request_set_replen(req);

	rc = mdc_getattr_common(exp, req);
	if (rc)
		ptlrpc_req_finished(req);
	else
		*request = req;
	RETURN(rc);
}

int mdc_getattr_name(struct obd_export *exp, struct md_op_data *op_data,
		     struct ptlrpc_request **request)
{
	struct ptlrpc_request *req;
	int		    rc;
	ENTRY;

	*request = NULL;
	req = ptlrpc_request_alloc(class_exp2cliimp(exp),
				   &RQF_MDS_GETATTR_NAME);
	if (req == NULL)
		RETURN(-ENOMEM);

	mdc_set_capa_size(req, &RMF_CAPA1, op_data->op_capa1);
	req_capsule_set_size(&req->rq_pill, &RMF_NAME, RCL_CLIENT,
			     op_data->op_namelen + 1);

	rc = ptlrpc_request_pack(req, LUSTRE_MDS_VERSION, MDS_GETATTR_NAME);
	if (rc) {
		ptlrpc_request_free(req);
		RETURN(rc);
	}

	mdc_pack_body(req, &op_data->op_fid1, op_data->op_capa1,
		      op_data->op_valid, op_data->op_mode,
		      op_data->op_suppgids[0], 0);

	if (op_data->op_name) {
		char *name = req_capsule_client_get(&req->rq_pill, &RMF_NAME);
		LASSERT(strnlen(op_data->op_name, op_data->op_namelen) ==
				op_data->op_namelen);
		memcpy(name, op_data->op_name, op_data->op_namelen);
	}

	req_capsule_set_size(&req->rq_pill, &RMF_MDT_MD, RCL_SERVER,
			     op_data->op_mode);
	ptlrpc_request_set_replen(req);

	rc = mdc_getattr_common(exp, req);
	if (rc)
		ptlrpc_req_finished(req);
	else
		*request = req;
	RETURN(rc);
}

static int mdc_is_subdir(struct obd_export *exp,
			 const struct lu_fid *pfid,
			 const struct lu_fid *cfid,
			 struct ptlrpc_request **request)
{
	struct ptlrpc_request  *req;
	int		     rc;

	ENTRY;

	*request = NULL;
	req = ptlrpc_request_alloc_pack(class_exp2cliimp(exp),
					&RQF_MDS_IS_SUBDIR, LUSTRE_MDS_VERSION,
					MDS_IS_SUBDIR);
	if (req == NULL)
		RETURN(-ENOMEM);

	mdc_is_subdir_pack(req, pfid, cfid, 0);
	ptlrpc_request_set_replen(req);

	rc = ptlrpc_queue_wait(req);
	if (rc && rc != -EREMOTE)
		ptlrpc_req_finished(req);
	else
		*request = req;
	RETURN(rc);
}

static int mdc_xattr_common(struct obd_export *exp,const struct req_format *fmt,
			    const struct lu_fid *fid,
			    struct obd_capa *oc, int opcode, obd_valid valid,
			    const char *xattr_name, const char *input,
			    int input_size, int output_size, int flags,
			    __u32 suppgid, struct ptlrpc_request **request)
{
	struct ptlrpc_request *req;
	int   xattr_namelen = 0;
	char *tmp;
	int   rc;
	ENTRY;

	*request = NULL;
	req = ptlrpc_request_alloc(class_exp2cliimp(exp), fmt);
	if (req == NULL)
		RETURN(-ENOMEM);

	mdc_set_capa_size(req, &RMF_CAPA1, oc);
	if (xattr_name) {
		xattr_namelen = strlen(xattr_name) + 1;
		req_capsule_set_size(&req->rq_pill, &RMF_NAME, RCL_CLIENT,
				     xattr_namelen);
	}
	if (input_size) {
		LASSERT(input);
		req_capsule_set_size(&req->rq_pill, &RMF_EADATA, RCL_CLIENT,
				     input_size);
	}

	rc = ptlrpc_request_pack(req, LUSTRE_MDS_VERSION, opcode);
	if (rc) {
		ptlrpc_request_free(req);
		RETURN(rc);
	}

	if (opcode == MDS_REINT) {
		struct mdt_rec_setxattr *rec;

		CLASSERT(sizeof(struct mdt_rec_setxattr) ==
			 sizeof(struct mdt_rec_reint));
		rec = req_capsule_client_get(&req->rq_pill, &RMF_REC_REINT);
		rec->sx_opcode = REINT_SETXATTR;
		/* TODO:
		 *  cfs_curproc_fs{u,g}id() should replace
		 *  current->fs{u,g}id for portability.
		 */
		rec->sx_fsuid  = current_fsuid();
		rec->sx_fsgid  = current_fsgid();
		rec->sx_cap    = cfs_curproc_cap_pack();
		rec->sx_suppgid1 = suppgid;
		rec->sx_suppgid2 = -1;
		rec->sx_fid    = *fid;
		rec->sx_valid  = valid | OBD_MD_FLCTIME;
		rec->sx_time   = cfs_time_current_sec();
		rec->sx_size   = output_size;
		rec->sx_flags  = flags;

		mdc_pack_capa(req, &RMF_CAPA1, oc);
	} else {
		mdc_pack_body(req, fid, oc, valid, output_size, suppgid, flags);
	}

	if (xattr_name) {
		tmp = req_capsule_client_get(&req->rq_pill, &RMF_NAME);
		memcpy(tmp, xattr_name, xattr_namelen);
	}
	if (input_size) {
		tmp = req_capsule_client_get(&req->rq_pill, &RMF_EADATA);
		memcpy(tmp, input, input_size);
	}

	if (req_capsule_has_field(&req->rq_pill, &RMF_EADATA, RCL_SERVER))
		req_capsule_set_size(&req->rq_pill, &RMF_EADATA,
				     RCL_SERVER, output_size);
	ptlrpc_request_set_replen(req);

	/* make rpc */
	if (opcode == MDS_REINT)
		mdc_get_rpc_lock(exp->exp_obd->u.cli.cl_rpc_lock, NULL);

	rc = ptlrpc_queue_wait(req);

	if (opcode == MDS_REINT)
		mdc_put_rpc_lock(exp->exp_obd->u.cli.cl_rpc_lock, NULL);

	if (rc)
		ptlrpc_req_finished(req);
	else
		*request = req;
	RETURN(rc);
}

int mdc_setxattr(struct obd_export *exp, const struct lu_fid *fid,
		 struct obd_capa *oc, obd_valid valid, const char *xattr_name,
		 const char *input, int input_size, int output_size,
		 int flags, __u32 suppgid, struct ptlrpc_request **request)
{
	return mdc_xattr_common(exp, &RQF_MDS_REINT_SETXATTR,
				fid, oc, MDS_REINT, valid, xattr_name,
				input, input_size, output_size, flags,
				suppgid, request);
}

int mdc_getxattr(struct obd_export *exp, const struct lu_fid *fid,
		 struct obd_capa *oc, obd_valid valid, const char *xattr_name,
		 const char *input, int input_size, int output_size,
		 int flags, struct ptlrpc_request **request)
{
	return mdc_xattr_common(exp, &RQF_MDS_GETXATTR,
				fid, oc, MDS_GETXATTR, valid, xattr_name,
				input, input_size, output_size, flags,
				-1, request);
}

#ifdef CONFIG_FS_POSIX_ACL
static int mdc_unpack_acl(struct ptlrpc_request *req, struct lustre_md *md)
{
	struct req_capsule     *pill = &req->rq_pill;
	struct mdt_body	*body = md->body;
	struct posix_acl       *acl;
	void		   *buf;
	int		     rc;
	ENTRY;

	if (!body->aclsize)
		RETURN(0);

	buf = req_capsule_server_sized_get(pill, &RMF_ACL, body->aclsize);

	if (!buf)
		RETURN(-EPROTO);

	acl = posix_acl_from_xattr(&init_user_ns, buf, body->aclsize);
	if (IS_ERR(acl)) {
		rc = PTR_ERR(acl);
		CERROR("convert xattr to acl: %d\n", rc);
		RETURN(rc);
	}

	rc = posix_acl_valid(acl);
	if (rc) {
		CERROR("validate acl: %d\n", rc);
		posix_acl_release(acl);
		RETURN(rc);
	}

	md->posix_acl = acl;
	RETURN(0);
}
#else
#define mdc_unpack_acl(req, md) 0
#endif

int mdc_get_lustre_md(struct obd_export *exp, struct ptlrpc_request *req,
		      struct obd_export *dt_exp, struct obd_export *md_exp,
		      struct lustre_md *md)
{
	struct req_capsule *pill = &req->rq_pill;
	int rc;
	ENTRY;

	LASSERT(md);
	memset(md, 0, sizeof(*md));

	md->body = req_capsule_server_get(pill, &RMF_MDT_BODY);
	LASSERT(md->body != NULL);

	if (md->body->valid & OBD_MD_FLEASIZE) {
		int lmmsize;
		struct lov_mds_md *lmm;

		if (!S_ISREG(md->body->mode)) {
			CDEBUG(D_INFO, "OBD_MD_FLEASIZE set, should be a "
			       "regular file, but is not\n");
			GOTO(out, rc = -EPROTO);
		}

		if (md->body->eadatasize == 0) {
			CDEBUG(D_INFO, "OBD_MD_FLEASIZE set, "
			       "but eadatasize 0\n");
			GOTO(out, rc = -EPROTO);
		}
		lmmsize = md->body->eadatasize;
		lmm = req_capsule_server_sized_get(pill, &RMF_MDT_MD, lmmsize);
		if (!lmm)
			GOTO(out, rc = -EPROTO);

		rc = obd_unpackmd(dt_exp, &md->lsm, lmm, lmmsize);
		if (rc < 0)
			GOTO(out, rc);

		if (rc < sizeof(*md->lsm)) {
			CDEBUG(D_INFO, "lsm size too small: "
			       "rc < sizeof (*md->lsm) (%d < %d)\n",
			       rc, (int)sizeof(*md->lsm));
			GOTO(out, rc = -EPROTO);
		}

	} else if (md->body->valid & OBD_MD_FLDIREA) {
		int lmvsize;
		struct lov_mds_md *lmv;

		if(!S_ISDIR(md->body->mode)) {
			CDEBUG(D_INFO, "OBD_MD_FLDIREA set, should be a "
			       "directory, but is not\n");
			GOTO(out, rc = -EPROTO);
		}

		if (md->body->eadatasize == 0) {
			CDEBUG(D_INFO, "OBD_MD_FLDIREA is set, "
			       "but eadatasize 0\n");
			RETURN(-EPROTO);
		}
		if (md->body->valid & OBD_MD_MEA) {
			lmvsize = md->body->eadatasize;
			lmv = req_capsule_server_sized_get(pill, &RMF_MDT_MD,
							   lmvsize);
			if (!lmv)
				GOTO(out, rc = -EPROTO);

			rc = obd_unpackmd(md_exp, (void *)&md->mea, lmv,
					  lmvsize);
			if (rc < 0)
				GOTO(out, rc);

			if (rc < sizeof(*md->mea)) {
				CDEBUG(D_INFO, "size too small:  "
				       "rc < sizeof(*md->mea) (%d < %d)\n",
					rc, (int)sizeof(*md->mea));
				GOTO(out, rc = -EPROTO);
			}
		}
	}
	rc = 0;

	if (md->body->valid & OBD_MD_FLRMTPERM) {
		/* remote permission */
		LASSERT(client_is_remote(exp));
		md->remote_perm = req_capsule_server_swab_get(pill, &RMF_ACL,
						lustre_swab_mdt_remote_perm);
		if (!md->remote_perm)
			GOTO(out, rc = -EPROTO);
	}
	else if (md->body->valid & OBD_MD_FLACL) {
		/* for ACL, it's possible that FLACL is set but aclsize is zero.
		 * only when aclsize != 0 there's an actual segment for ACL
		 * in reply buffer.
		 */
		if (md->body->aclsize) {
			rc = mdc_unpack_acl(req, md);
			if (rc)
				GOTO(out, rc);
#ifdef CONFIG_FS_POSIX_ACL
		} else {
			md->posix_acl = NULL;
#endif
		}
	}
	if (md->body->valid & OBD_MD_FLMDSCAPA) {
		struct obd_capa *oc = NULL;

		rc = mdc_unpack_capa(NULL, req, &RMF_CAPA1, &oc);
		if (rc)
			GOTO(out, rc);
		md->mds_capa = oc;
	}

	if (md->body->valid & OBD_MD_FLOSSCAPA) {
		struct obd_capa *oc = NULL;

		rc = mdc_unpack_capa(NULL, req, &RMF_CAPA2, &oc);
		if (rc)
			GOTO(out, rc);
		md->oss_capa = oc;
	}

	EXIT;
out:
	if (rc) {
		if (md->oss_capa) {
			capa_put(md->oss_capa);
			md->oss_capa = NULL;
		}
		if (md->mds_capa) {
			capa_put(md->mds_capa);
			md->mds_capa = NULL;
		}
#ifdef CONFIG_FS_POSIX_ACL
		posix_acl_release(md->posix_acl);
#endif
		if (md->lsm)
			obd_free_memmd(dt_exp, &md->lsm);
	}
	return rc;
}

int mdc_free_lustre_md(struct obd_export *exp, struct lustre_md *md)
{
	ENTRY;
	RETURN(0);
}

/**
 * Handles both OPEN and SETATTR RPCs for OPEN-CLOSE and SETATTR-DONE_WRITING
 * RPC chains.
 */
void mdc_replay_open(struct ptlrpc_request *req)
{
	struct md_open_data *mod = req->rq_cb_data;
	struct ptlrpc_request *close_req;
	struct obd_client_handle *och;
	struct lustre_handle old;
	struct mdt_body *body;
	ENTRY;

	if (mod == NULL) {
		DEBUG_REQ(D_ERROR, req,
			  "Can't properly replay without open data.");
		EXIT;
		return;
	}

	body = req_capsule_server_get(&req->rq_pill, &RMF_MDT_BODY);
	LASSERT(body != NULL);

	och = mod->mod_och;
	if (och != NULL) {
		struct lustre_handle *file_fh;

		LASSERT(och->och_magic == OBD_CLIENT_HANDLE_MAGIC);

		file_fh = &och->och_fh;
		CDEBUG(D_HA, "updating handle from "LPX64" to "LPX64"\n",
		       file_fh->cookie, body->handle.cookie);
		old = *file_fh;
		*file_fh = body->handle;
	}
	close_req = mod->mod_close_req;
	if (close_req != NULL) {
		__u32 opc = lustre_msg_get_opc(close_req->rq_reqmsg);
		struct mdt_ioepoch *epoch;

		LASSERT(opc == MDS_CLOSE || opc == MDS_DONE_WRITING);
		epoch = req_capsule_client_get(&close_req->rq_pill,
					       &RMF_MDT_EPOCH);
		LASSERT(epoch);

		if (och != NULL)
			LASSERT(!memcmp(&old, &epoch->handle, sizeof(old)));
		DEBUG_REQ(D_HA, close_req, "updating close body with new fh");
		epoch->handle = body->handle;
	}
	EXIT;
}

void mdc_commit_open(struct ptlrpc_request *req)
{
	struct md_open_data *mod = req->rq_cb_data;
	if (mod == NULL)
		return;

	/**
	 * No need to touch md_open_data::mod_och, it holds a reference on
	 * \var mod and will zero references to each other, \var mod will be
	 * freed after that when md_open_data::mod_och will put the reference.
	 */

	/**
	 * Do not let open request to disappear as it still may be needed
	 * for close rpc to happen (it may happen on evict only, otherwise
	 * ptlrpc_request::rq_replay does not let mdc_commit_open() to be
	 * called), just mark this rpc as committed to distinguish these 2
	 * cases, see mdc_close() for details. The open request reference will
	 * be put along with freeing \var mod.
	 */
	ptlrpc_request_addref(req);
	spin_lock(&req->rq_lock);
	req->rq_committed = 1;
	spin_unlock(&req->rq_lock);
	req->rq_cb_data = NULL;
	obd_mod_put(mod);
}

int mdc_set_open_replay_data(struct obd_export *exp,
			     struct obd_client_handle *och,
			     struct ptlrpc_request *open_req)
{
	struct md_open_data   *mod;
	struct mdt_rec_create *rec;
	struct mdt_body       *body;
	struct obd_import     *imp = open_req->rq_import;
	ENTRY;

	if (!open_req->rq_replay)
		RETURN(0);

	rec = req_capsule_client_get(&open_req->rq_pill, &RMF_REC_REINT);
	body = req_capsule_server_get(&open_req->rq_pill, &RMF_MDT_BODY);
	LASSERT(rec != NULL);
	/* Incoming message in my byte order (it's been swabbed). */
	/* Outgoing messages always in my byte order. */
	LASSERT(body != NULL);

	/* Only if the import is replayable, we set replay_open data */
	if (och && imp->imp_replayable) {
		mod = obd_mod_alloc();
		if (mod == NULL) {
			DEBUG_REQ(D_ERROR, open_req,
				  "Can't allocate md_open_data");
			RETURN(0);
		}

		/**
		 * Take a reference on \var mod, to be freed on mdc_close().
		 * It protects \var mod from being freed on eviction (commit
		 * callback is called despite rq_replay flag).
		 * Another reference for \var och.
		 */
		obd_mod_get(mod);
		obd_mod_get(mod);

		spin_lock(&open_req->rq_lock);
		och->och_mod = mod;
		mod->mod_och = och;
		mod->mod_open_req = open_req;
		open_req->rq_cb_data = mod;
		open_req->rq_commit_cb = mdc_commit_open;
		spin_unlock(&open_req->rq_lock);
	}

	rec->cr_fid2 = body->fid1;
	rec->cr_ioepoch = body->ioepoch;
	rec->cr_old_handle.cookie = body->handle.cookie;
	open_req->rq_replay_cb = mdc_replay_open;
	if (!fid_is_sane(&body->fid1)) {
		DEBUG_REQ(D_ERROR, open_req, "Saving replay request with "
			  "insane fid");
		LBUG();
	}

	DEBUG_REQ(D_RPCTRACE, open_req, "Set up open replay data");
	RETURN(0);
}

int mdc_clear_open_replay_data(struct obd_export *exp,
			       struct obd_client_handle *och)
{
	struct md_open_data *mod = och->och_mod;
	ENTRY;

	/**
	 * It is possible to not have \var mod in a case of eviction between
	 * lookup and ll_file_open().
	 **/
	if (mod == NULL)
		RETURN(0);

	LASSERT(mod != LP_POISON);

	mod->mod_och = NULL;
	och->och_mod = NULL;
	obd_mod_put(mod);

	RETURN(0);
}

/* Prepares the request for the replay by the given reply */
static void mdc_close_handle_reply(struct ptlrpc_request *req,
				   struct md_op_data *op_data, int rc) {
	struct mdt_body  *repbody;
	struct mdt_ioepoch *epoch;

	if (req && rc == -EAGAIN) {
		repbody = req_capsule_server_get(&req->rq_pill, &RMF_MDT_BODY);
		epoch = req_capsule_client_get(&req->rq_pill, &RMF_MDT_EPOCH);

		epoch->flags |= MF_SOM_AU;
		if (repbody->valid & OBD_MD_FLGETATTRLOCK)
			op_data->op_flags |= MF_GETATTR_LOCK;
	}
}

int mdc_close(struct obd_export *exp, struct md_op_data *op_data,
	      struct md_open_data *mod, struct ptlrpc_request **request)
{
	struct obd_device     *obd = class_exp2obd(exp);
	struct ptlrpc_request *req;
	int		    rc;
	ENTRY;

	*request = NULL;
	req = ptlrpc_request_alloc(class_exp2cliimp(exp), &RQF_MDS_CLOSE);
	if (req == NULL)
		RETURN(-ENOMEM);

	mdc_set_capa_size(req, &RMF_CAPA1, op_data->op_capa1);

	rc = ptlrpc_request_pack(req, LUSTRE_MDS_VERSION, MDS_CLOSE);
	if (rc) {
		ptlrpc_request_free(req);
		RETURN(rc);
	}

	/* To avoid a livelock (bug 7034), we need to send CLOSE RPCs to a
	 * portal whose threads are not taking any DLM locks and are therefore
	 * always progressing */
	req->rq_request_portal = MDS_READPAGE_PORTAL;
	ptlrpc_at_set_req_timeout(req);

	/* Ensure that this close's handle is fixed up during replay. */
	if (likely(mod != NULL)) {
		LASSERTF(mod->mod_open_req != NULL &&
			 mod->mod_open_req->rq_type != LI_POISON,
			 "POISONED open %p!\n", mod->mod_open_req);

		mod->mod_close_req = req;

		DEBUG_REQ(D_HA, mod->mod_open_req, "matched open");
		/* We no longer want to preserve this open for replay even
		 * though the open was committed. b=3632, b=3633 */
		spin_lock(&mod->mod_open_req->rq_lock);
		mod->mod_open_req->rq_replay = 0;
		spin_unlock(&mod->mod_open_req->rq_lock);
	} else {
		 CDEBUG(D_HA, "couldn't find open req; expecting close error\n");
	}

	mdc_close_pack(req, op_data);

	req_capsule_set_size(&req->rq_pill, &RMF_MDT_MD, RCL_SERVER,
			     obd->u.cli.cl_max_mds_easize);
	req_capsule_set_size(&req->rq_pill, &RMF_LOGCOOKIES, RCL_SERVER,
			     obd->u.cli.cl_max_mds_cookiesize);

	ptlrpc_request_set_replen(req);

	mdc_get_rpc_lock(obd->u.cli.cl_close_lock, NULL);
	rc = ptlrpc_queue_wait(req);
	mdc_put_rpc_lock(obd->u.cli.cl_close_lock, NULL);

	if (req->rq_repmsg == NULL) {
		CDEBUG(D_RPCTRACE, "request failed to send: %p, %d\n", req,
		       req->rq_status);
		if (rc == 0)
			rc = req->rq_status ?: -EIO;
	} else if (rc == 0 || rc == -EAGAIN) {
		struct mdt_body *body;

		rc = lustre_msg_get_status(req->rq_repmsg);
		if (lustre_msg_get_type(req->rq_repmsg) == PTL_RPC_MSG_ERR) {
			DEBUG_REQ(D_ERROR, req, "type == PTL_RPC_MSG_ERR, err "
				  "= %d", rc);
			if (rc > 0)
				rc = -rc;
		}
		body = req_capsule_server_get(&req->rq_pill, &RMF_MDT_BODY);
		if (body == NULL)
			rc = -EPROTO;
	} else if (rc == -ESTALE) {
		/**
		 * it can be allowed error after 3633 if open was committed and
		 * server failed before close was sent. Let's check if mod
		 * exists and return no error in that case
		 */
		if (mod) {
			DEBUG_REQ(D_HA, req, "Reset ESTALE = %d", rc);
			LASSERT(mod->mod_open_req != NULL);
			if (mod->mod_open_req->rq_committed)
				rc = 0;
		}
	}

	if (mod) {
		if (rc != 0)
			mod->mod_close_req = NULL;
		/* Since now, mod is accessed through open_req only,
		 * thus close req does not keep a reference on mod anymore. */
		obd_mod_put(mod);
	}
	*request = req;
	mdc_close_handle_reply(req, op_data, rc);
	RETURN(rc);
}

int mdc_done_writing(struct obd_export *exp, struct md_op_data *op_data,
		     struct md_open_data *mod)
{
	struct obd_device     *obd = class_exp2obd(exp);
	struct ptlrpc_request *req;
	int		    rc;
	ENTRY;

	req = ptlrpc_request_alloc(class_exp2cliimp(exp),
				   &RQF_MDS_DONE_WRITING);
	if (req == NULL)
		RETURN(-ENOMEM);

	mdc_set_capa_size(req, &RMF_CAPA1, op_data->op_capa1);
	rc = ptlrpc_request_pack(req, LUSTRE_MDS_VERSION, MDS_DONE_WRITING);
	if (rc) {
		ptlrpc_request_free(req);
		RETURN(rc);
	}

	if (mod != NULL) {
		LASSERTF(mod->mod_open_req != NULL &&
			 mod->mod_open_req->rq_type != LI_POISON,
			 "POISONED setattr %p!\n", mod->mod_open_req);

		mod->mod_close_req = req;
		DEBUG_REQ(D_HA, mod->mod_open_req, "matched setattr");
		/* We no longer want to preserve this setattr for replay even
		 * though the open was committed. b=3632, b=3633 */
		spin_lock(&mod->mod_open_req->rq_lock);
		mod->mod_open_req->rq_replay = 0;
		spin_unlock(&mod->mod_open_req->rq_lock);
	}

	mdc_close_pack(req, op_data);
	ptlrpc_request_set_replen(req);

	mdc_get_rpc_lock(obd->u.cli.cl_close_lock, NULL);
	rc = ptlrpc_queue_wait(req);
	mdc_put_rpc_lock(obd->u.cli.cl_close_lock, NULL);

	if (rc == -ESTALE) {
		/**
		 * it can be allowed error after 3633 if open or setattr were
		 * committed and server failed before close was sent.
		 * Let's check if mod exists and return no error in that case
		 */
		if (mod) {
			LASSERT(mod->mod_open_req != NULL);
			if (mod->mod_open_req->rq_committed)
				rc = 0;
		}
	}

	if (mod) {
		if (rc != 0)
			mod->mod_close_req = NULL;
		/* Since now, mod is accessed through setattr req only,
		 * thus DW req does not keep a reference on mod anymore. */
		obd_mod_put(mod);
	}

	mdc_close_handle_reply(req, op_data, rc);
	ptlrpc_req_finished(req);
	RETURN(rc);
}


int mdc_readpage(struct obd_export *exp, struct md_op_data *op_data,
		 struct page **pages, struct ptlrpc_request **request)
{
	struct ptlrpc_request   *req;
	struct ptlrpc_bulk_desc *desc;
	int		      i;
	wait_queue_head_t	      waitq;
	int		      resends = 0;
	struct l_wait_info       lwi;
	int		      rc;
	ENTRY;

	*request = NULL;
	init_waitqueue_head(&waitq);

restart_bulk:
	req = ptlrpc_request_alloc(class_exp2cliimp(exp), &RQF_MDS_READPAGE);
	if (req == NULL)
		RETURN(-ENOMEM);

	mdc_set_capa_size(req, &RMF_CAPA1, op_data->op_capa1);

	rc = ptlrpc_request_pack(req, LUSTRE_MDS_VERSION, MDS_READPAGE);
	if (rc) {
		ptlrpc_request_free(req);
		RETURN(rc);
	}

	req->rq_request_portal = MDS_READPAGE_PORTAL;
	ptlrpc_at_set_req_timeout(req);

	desc = ptlrpc_prep_bulk_imp(req, op_data->op_npages, 1, BULK_PUT_SINK,
				    MDS_BULK_PORTAL);
	if (desc == NULL) {
		ptlrpc_request_free(req);
		RETURN(-ENOMEM);
	}

	/* NB req now owns desc and will free it when it gets freed */
	for (i = 0; i < op_data->op_npages; i++)
		ptlrpc_prep_bulk_page_pin(desc, pages[i], 0, PAGE_CACHE_SIZE);

	mdc_readdir_pack(req, op_data->op_offset,
			 PAGE_CACHE_SIZE * op_data->op_npages,
			 &op_data->op_fid1, op_data->op_capa1);

	ptlrpc_request_set_replen(req);
	rc = ptlrpc_queue_wait(req);
	if (rc) {
		ptlrpc_req_finished(req);
		if (rc != -ETIMEDOUT)
			RETURN(rc);

		resends++;
		if (!client_should_resend(resends, &exp->exp_obd->u.cli)) {
			CERROR("too many resend retries, returning error\n");
			RETURN(-EIO);
		}
		lwi = LWI_TIMEOUT_INTR(cfs_time_seconds(resends), NULL, NULL, NULL);
		l_wait_event(waitq, 0, &lwi);

		goto restart_bulk;
	}

	rc = sptlrpc_cli_unwrap_bulk_read(req, req->rq_bulk,
					  req->rq_bulk->bd_nob_transferred);
	if (rc < 0) {
		ptlrpc_req_finished(req);
		RETURN(rc);
	}

	if (req->rq_bulk->bd_nob_transferred & ~LU_PAGE_MASK) {
		CERROR("Unexpected # bytes transferred: %d (%ld expected)\n",
			req->rq_bulk->bd_nob_transferred,
			PAGE_CACHE_SIZE * op_data->op_npages);
		ptlrpc_req_finished(req);
		RETURN(-EPROTO);
	}

	*request = req;
	RETURN(0);
}

static int mdc_statfs(const struct lu_env *env,
		      struct obd_export *exp, struct obd_statfs *osfs,
		      __u64 max_age, __u32 flags)
{
	struct obd_device     *obd = class_exp2obd(exp);
	struct ptlrpc_request *req;
	struct obd_statfs     *msfs;
	struct obd_import     *imp = NULL;
	int		    rc;
	ENTRY;

	/*
	 * Since the request might also come from lprocfs, so we need
	 * sync this with client_disconnect_export Bug15684
	 */
	down_read(&obd->u.cli.cl_sem);
	if (obd->u.cli.cl_import)
		imp = class_import_get(obd->u.cli.cl_import);
	up_read(&obd->u.cli.cl_sem);
	if (!imp)
		RETURN(-ENODEV);

	req = ptlrpc_request_alloc_pack(imp, &RQF_MDS_STATFS,
					LUSTRE_MDS_VERSION, MDS_STATFS);
	if (req == NULL)
		GOTO(output, rc = -ENOMEM);

	ptlrpc_request_set_replen(req);

	if (flags & OBD_STATFS_NODELAY) {
		/* procfs requests not want stay in wait for avoid deadlock */
		req->rq_no_resend = 1;
		req->rq_no_delay = 1;
	}

	rc = ptlrpc_queue_wait(req);
	if (rc) {
		/* check connection error first */
		if (imp->imp_connect_error)
			rc = imp->imp_connect_error;
		GOTO(out, rc);
	}

	msfs = req_capsule_server_get(&req->rq_pill, &RMF_OBD_STATFS);
	if (msfs == NULL)
		GOTO(out, rc = -EPROTO);

	*osfs = *msfs;
	EXIT;
out:
	ptlrpc_req_finished(req);
output:
	class_import_put(imp);
	return rc;
}

static int mdc_ioc_fid2path(struct obd_export *exp, struct getinfo_fid2path *gf)
{
	__u32 keylen, vallen;
	void *key;
	int rc;

	if (gf->gf_pathlen > PATH_MAX)
		RETURN(-ENAMETOOLONG);
	if (gf->gf_pathlen < 2)
		RETURN(-EOVERFLOW);

	/* Key is KEY_FID2PATH + getinfo_fid2path description */
	keylen = cfs_size_round(sizeof(KEY_FID2PATH)) + sizeof(*gf);
	OBD_ALLOC(key, keylen);
	if (key == NULL)
		RETURN(-ENOMEM);
	memcpy(key, KEY_FID2PATH, sizeof(KEY_FID2PATH));
	memcpy(key + cfs_size_round(sizeof(KEY_FID2PATH)), gf, sizeof(*gf));

	CDEBUG(D_IOCTL, "path get "DFID" from "LPU64" #%d\n",
	       PFID(&gf->gf_fid), gf->gf_recno, gf->gf_linkno);

	if (!fid_is_sane(&gf->gf_fid))
		GOTO(out, rc = -EINVAL);

	/* Val is struct getinfo_fid2path result plus path */
	vallen = sizeof(*gf) + gf->gf_pathlen;

	rc = obd_get_info(NULL, exp, keylen, key, &vallen, gf, NULL);
	if (rc != 0 && rc != -EREMOTE)
		GOTO(out, rc);

	if (vallen <= sizeof(*gf))
		GOTO(out, rc = -EPROTO);
	else if (vallen > sizeof(*gf) + gf->gf_pathlen)
		GOTO(out, rc = -EOVERFLOW);

	CDEBUG(D_IOCTL, "path get "DFID" from "LPU64" #%d\n%s\n",
	       PFID(&gf->gf_fid), gf->gf_recno, gf->gf_linkno, gf->gf_path);

out:
	OBD_FREE(key, keylen);
	return rc;
}

static int mdc_ioc_hsm_progress(struct obd_export *exp,
				struct hsm_progress_kernel *hpk)
{
	struct obd_import		*imp = class_exp2cliimp(exp);
	struct hsm_progress_kernel	*req_hpk;
	struct ptlrpc_request		*req;
	int				 rc;
	ENTRY;

	req = ptlrpc_request_alloc_pack(imp, &RQF_MDS_HSM_PROGRESS,
					LUSTRE_MDS_VERSION, MDS_HSM_PROGRESS);
	if (req == NULL)
		GOTO(out, rc = -ENOMEM);

	mdc_pack_body(req, NULL, NULL, OBD_MD_FLRMTPERM, 0, 0, 0);

	/* Copy hsm_progress struct */
	req_hpk = req_capsule_client_get(&req->rq_pill, &RMF_MDS_HSM_PROGRESS);
	if (req_hpk == NULL)
		GOTO(out, rc = -EPROTO);

	*req_hpk = *hpk;

	ptlrpc_request_set_replen(req);

	rc = mdc_queue_wait(req);
	GOTO(out, rc);
out:
	ptlrpc_req_finished(req);
	return rc;
}

static int mdc_ioc_hsm_ct_register(struct obd_import *imp, __u32 archives)
{
	__u32			*archive_mask;
	struct ptlrpc_request	*req;
	int			 rc;
	ENTRY;

	req = ptlrpc_request_alloc_pack(imp, &RQF_MDS_HSM_CT_REGISTER,
					LUSTRE_MDS_VERSION,
					MDS_HSM_CT_REGISTER);
	if (req == NULL)
		GOTO(out, rc = -ENOMEM);

	mdc_pack_body(req, NULL, NULL, OBD_MD_FLRMTPERM, 0, 0, 0);

	/* Copy hsm_progress struct */
	archive_mask = req_capsule_client_get(&req->rq_pill,
					      &RMF_MDS_HSM_ARCHIVE);
	if (archive_mask == NULL)
		GOTO(out, rc = -EPROTO);

	*archive_mask = archives;

	ptlrpc_request_set_replen(req);

	rc = mdc_queue_wait(req);
	GOTO(out, rc);
out:
	ptlrpc_req_finished(req);
	return rc;
}

static int mdc_ioc_hsm_current_action(struct obd_export *exp,
				      struct md_op_data *op_data)
{
	struct hsm_current_action	*hca = op_data->op_data;
	struct hsm_current_action	*req_hca;
	struct ptlrpc_request		*req;
	int				 rc;
	ENTRY;

	req = ptlrpc_request_alloc(class_exp2cliimp(exp),
				   &RQF_MDS_HSM_ACTION);
	if (req == NULL)
		RETURN(-ENOMEM);

	mdc_set_capa_size(req, &RMF_CAPA1, op_data->op_capa1);

	rc = ptlrpc_request_pack(req, LUSTRE_MDS_VERSION, MDS_HSM_ACTION);
	if (rc) {
		ptlrpc_request_free(req);
		RETURN(rc);
	}

	mdc_pack_body(req, &op_data->op_fid1, op_data->op_capa1,
		      OBD_MD_FLRMTPERM, 0, op_data->op_suppgids[0], 0);

	ptlrpc_request_set_replen(req);

	rc = mdc_queue_wait(req);
	if (rc)
		GOTO(out, rc);

	req_hca = req_capsule_server_get(&req->rq_pill,
					 &RMF_MDS_HSM_CURRENT_ACTION);
	if (req_hca == NULL)
		GOTO(out, rc = -EPROTO);

	*hca = *req_hca;

	EXIT;
out:
	ptlrpc_req_finished(req);
	return rc;
}

static int mdc_ioc_hsm_ct_unregister(struct obd_import *imp)
{
	struct ptlrpc_request	*req;
	int			 rc;
	ENTRY;

	req = ptlrpc_request_alloc_pack(imp, &RQF_MDS_HSM_CT_UNREGISTER,
					LUSTRE_MDS_VERSION,
					MDS_HSM_CT_UNREGISTER);
	if (req == NULL)
		GOTO(out, rc = -ENOMEM);

	mdc_pack_body(req, NULL, NULL, OBD_MD_FLRMTPERM, 0, 0, 0);

	ptlrpc_request_set_replen(req);

	rc = mdc_queue_wait(req);
	GOTO(out, rc);
out:
	ptlrpc_req_finished(req);
	return rc;
}

static int mdc_ioc_hsm_state_get(struct obd_export *exp,
				 struct md_op_data *op_data)
{
	struct hsm_user_state	*hus = op_data->op_data;
	struct hsm_user_state	*req_hus;
	struct ptlrpc_request	*req;
	int			 rc;
	ENTRY;

	req = ptlrpc_request_alloc(class_exp2cliimp(exp),
				   &RQF_MDS_HSM_STATE_GET);
	if (req == NULL)
		RETURN(-ENOMEM);

	mdc_set_capa_size(req, &RMF_CAPA1, op_data->op_capa1);

	rc = ptlrpc_request_pack(req, LUSTRE_MDS_VERSION, MDS_HSM_STATE_GET);
	if (rc != 0) {
		ptlrpc_request_free(req);
		RETURN(rc);
	}

	mdc_pack_body(req, &op_data->op_fid1, op_data->op_capa1,
		      OBD_MD_FLRMTPERM, 0, op_data->op_suppgids[0], 0);

	ptlrpc_request_set_replen(req);

	rc = mdc_queue_wait(req);
	if (rc)
		GOTO(out, rc);

	req_hus = req_capsule_server_get(&req->rq_pill, &RMF_HSM_USER_STATE);
	if (req_hus == NULL)
		GOTO(out, rc = -EPROTO);

	*hus = *req_hus;

	EXIT;
out:
	ptlrpc_req_finished(req);
	return rc;
}

static int mdc_ioc_hsm_state_set(struct obd_export *exp,
				 struct md_op_data *op_data)
{
	struct hsm_state_set	*hss = op_data->op_data;
	struct hsm_state_set	*req_hss;
	struct ptlrpc_request	*req;
	int			 rc;
	ENTRY;

	req = ptlrpc_request_alloc(class_exp2cliimp(exp),
				   &RQF_MDS_HSM_STATE_SET);
	if (req == NULL)
		RETURN(-ENOMEM);

	mdc_set_capa_size(req, &RMF_CAPA1, op_data->op_capa1);

	rc = ptlrpc_request_pack(req, LUSTRE_MDS_VERSION, MDS_HSM_STATE_SET);
	if (rc) {
		ptlrpc_request_free(req);
		RETURN(rc);
	}

	mdc_pack_body(req, &op_data->op_fid1, op_data->op_capa1,
		      OBD_MD_FLRMTPERM, 0, op_data->op_suppgids[0], 0);

	/* Copy states */
	req_hss = req_capsule_client_get(&req->rq_pill, &RMF_HSM_STATE_SET);
	if (req_hss == NULL)
		GOTO(out, rc = -EPROTO);
	*req_hss = *hss;

	ptlrpc_request_set_replen(req);

	rc = mdc_queue_wait(req);
	GOTO(out, rc);

	EXIT;
out:
	ptlrpc_req_finished(req);
	return rc;
}

static int mdc_ioc_hsm_request(struct obd_export *exp,
			       struct hsm_user_request *hur)
{
	struct obd_import	*imp = class_exp2cliimp(exp);
	struct ptlrpc_request	*req;
	struct hsm_request	*req_hr;
	struct hsm_user_item	*req_hui;
	char			*req_opaque;
	int			 rc;
	ENTRY;

	req = ptlrpc_request_alloc(imp, &RQF_MDS_HSM_REQUEST);
	if (req == NULL)
		GOTO(out, rc = -ENOMEM);

	req_capsule_set_size(&req->rq_pill, &RMF_MDS_HSM_USER_ITEM, RCL_CLIENT,
			     hur->hur_request.hr_itemcount
			     * sizeof(struct hsm_user_item));
	req_capsule_set_size(&req->rq_pill, &RMF_GENERIC_DATA, RCL_CLIENT,
			     hur->hur_request.hr_data_len);

	rc = ptlrpc_request_pack(req, LUSTRE_MDS_VERSION, MDS_HSM_REQUEST);
	if (rc) {
		ptlrpc_request_free(req);
		RETURN(rc);
	}

	mdc_pack_body(req, NULL, NULL, OBD_MD_FLRMTPERM, 0, 0, 0);

	/* Copy hsm_request struct */
	req_hr = req_capsule_client_get(&req->rq_pill, &RMF_MDS_HSM_REQUEST);
	if (req_hr == NULL)
		GOTO(out, rc = -EPROTO);
	*req_hr = hur->hur_request;

	/* Copy hsm_user_item structs */
	req_hui = req_capsule_client_get(&req->rq_pill, &RMF_MDS_HSM_USER_ITEM);
	if (req_hui == NULL)
		GOTO(out, rc = -EPROTO);
	memcpy(req_hui, hur->hur_user_item,
	       hur->hur_request.hr_itemcount * sizeof(struct hsm_user_item));

	/* Copy opaque field */
	req_opaque = req_capsule_client_get(&req->rq_pill, &RMF_GENERIC_DATA);
	if (req_opaque == NULL)
		GOTO(out, rc = -EPROTO);
	memcpy(req_opaque, hur_data(hur), hur->hur_request.hr_data_len);

	ptlrpc_request_set_replen(req);

	rc = mdc_queue_wait(req);
	GOTO(out, rc);

out:
	ptlrpc_req_finished(req);
	return rc;
}

static struct kuc_hdr *changelog_kuc_hdr(char *buf, int len, int flags)
{
	struct kuc_hdr *lh = (struct kuc_hdr *)buf;

	LASSERT(len <= CR_MAXSIZE);

	lh->kuc_magic = KUC_MAGIC;
	lh->kuc_transport = KUC_TRANSPORT_CHANGELOG;
	lh->kuc_flags = flags;
	lh->kuc_msgtype = CL_RECORD;
	lh->kuc_msglen = len;
	return lh;
}

#define D_CHANGELOG 0

struct changelog_show {
	__u64		cs_startrec;
	__u32		cs_flags;
	struct file	*cs_fp;
	char		*cs_buf;
	struct obd_device *cs_obd;
};

static int changelog_kkuc_cb(const struct lu_env *env, struct llog_handle *llh,
			     struct llog_rec_hdr *hdr, void *data)
{
	struct changelog_show *cs = data;
	struct llog_changelog_rec *rec = (struct llog_changelog_rec *)hdr;
	struct kuc_hdr *lh;
	int len, rc;
	ENTRY;

	if (rec->cr_hdr.lrh_type != CHANGELOG_REC) {
		rc = -EINVAL;
		CERROR("%s: not a changelog rec %x/%d: rc = %d\n",
		       cs->cs_obd->obd_name, rec->cr_hdr.lrh_type,
		       rec->cr.cr_type, rc);
		RETURN(rc);
	}

	if (rec->cr.cr_index < cs->cs_startrec) {
		/* Skip entries earlier than what we are interested in */
		CDEBUG(D_CHANGELOG, "rec="LPU64" start="LPU64"\n",
		       rec->cr.cr_index, cs->cs_startrec);
		RETURN(0);
	}

	CDEBUG(D_CHANGELOG, LPU64" %02d%-5s "LPU64" 0x%x t="DFID" p="DFID
		" %.*s\n", rec->cr.cr_index, rec->cr.cr_type,
		changelog_type2str(rec->cr.cr_type), rec->cr.cr_time,
		rec->cr.cr_flags & CLF_FLAGMASK,
		PFID(&rec->cr.cr_tfid), PFID(&rec->cr.cr_pfid),
		rec->cr.cr_namelen, changelog_rec_name(&rec->cr));

	len = sizeof(*lh) + changelog_rec_size(&rec->cr) + rec->cr.cr_namelen;

	/* Set up the message */
	lh = changelog_kuc_hdr(cs->cs_buf, len, cs->cs_flags);
	memcpy(lh + 1, &rec->cr, len - sizeof(*lh));

	rc = libcfs_kkuc_msg_put(cs->cs_fp, lh);
	CDEBUG(D_CHANGELOG, "kucmsg fp %p len %d rc %d\n", cs->cs_fp, len,rc);

	RETURN(rc);
}

static int mdc_changelog_send_thread(void *csdata)
{
	struct changelog_show *cs = csdata;
	struct llog_ctxt *ctxt = NULL;
	struct llog_handle *llh = NULL;
	struct kuc_hdr *kuch;
	int rc;

	CDEBUG(D_CHANGELOG, "changelog to fp=%p start "LPU64"\n",
	       cs->cs_fp, cs->cs_startrec);

	OBD_ALLOC(cs->cs_buf, CR_MAXSIZE);
	if (cs->cs_buf == NULL)
		GOTO(out, rc = -ENOMEM);

	/* Set up the remote catalog handle */
	ctxt = llog_get_context(cs->cs_obd, LLOG_CHANGELOG_REPL_CTXT);
	if (ctxt == NULL)
		GOTO(out, rc = -ENOENT);
	rc = llog_open(NULL, ctxt, &llh, NULL, CHANGELOG_CATALOG,
		       LLOG_OPEN_EXISTS);
	if (rc) {
		CERROR("%s: fail to open changelog catalog: rc = %d\n",
		       cs->cs_obd->obd_name, rc);
		GOTO(out, rc);
	}
	rc = llog_init_handle(NULL, llh, LLOG_F_IS_CAT, NULL);
	if (rc) {
		CERROR("llog_init_handle failed %d\n", rc);
		GOTO(out, rc);
	}

	rc = llog_cat_process(NULL, llh, changelog_kkuc_cb, cs, 0, 0);

	/* Send EOF no matter what our result */
	if ((kuch = changelog_kuc_hdr(cs->cs_buf, sizeof(*kuch),
				      cs->cs_flags))) {
		kuch->kuc_msgtype = CL_EOF;
		libcfs_kkuc_msg_put(cs->cs_fp, kuch);
	}

out:
	fput(cs->cs_fp);
	if (llh)
		llog_cat_close(NULL, llh);
	if (ctxt)
		llog_ctxt_put(ctxt);
	if (cs->cs_buf)
		OBD_FREE(cs->cs_buf, CR_MAXSIZE);
	OBD_FREE_PTR(cs);
	return rc;
}

static int mdc_ioc_changelog_send(struct obd_device *obd,
				  struct ioc_changelog *icc)
{
	struct changelog_show *cs;
	int rc;

	/* Freed in mdc_changelog_send_thread */
	OBD_ALLOC_PTR(cs);
	if (!cs)
		return -ENOMEM;

	cs->cs_obd = obd;
	cs->cs_startrec = icc->icc_recno;
	/* matching fput in mdc_changelog_send_thread */
	cs->cs_fp = fget(icc->icc_id);
	cs->cs_flags = icc->icc_flags;

	/*
	 * New thread because we should return to user app before
	 * writing into our pipe
	 */
	rc = PTR_ERR(kthread_run(mdc_changelog_send_thread, cs,
				 "mdc_clg_send_thread"));
	if (!IS_ERR_VALUE(rc)) {
		CDEBUG(D_CHANGELOG, "start changelog thread\n");
		return 0;
	}

	CERROR("Failed to start changelog thread: %d\n", rc);
	OBD_FREE_PTR(cs);
	return rc;
}

static int mdc_ioc_hsm_ct_start(struct obd_export *exp,
				struct lustre_kernelcomm *lk);

static int mdc_quotacheck(struct obd_device *unused, struct obd_export *exp,
			  struct obd_quotactl *oqctl)
{
	struct client_obd       *cli = &exp->exp_obd->u.cli;
	struct ptlrpc_request   *req;
	struct obd_quotactl     *body;
	int		      rc;
	ENTRY;

	req = ptlrpc_request_alloc_pack(class_exp2cliimp(exp),
					&RQF_MDS_QUOTACHECK, LUSTRE_MDS_VERSION,
					MDS_QUOTACHECK);
	if (req == NULL)
		RETURN(-ENOMEM);

	body = req_capsule_client_get(&req->rq_pill, &RMF_OBD_QUOTACTL);
	*body = *oqctl;

	ptlrpc_request_set_replen(req);

	/* the next poll will find -ENODATA, that means quotacheck is
	 * going on */
	cli->cl_qchk_stat = -ENODATA;
	rc = ptlrpc_queue_wait(req);
	if (rc)
		cli->cl_qchk_stat = rc;
	ptlrpc_req_finished(req);
	RETURN(rc);
}

static int mdc_quota_poll_check(struct obd_export *exp,
				struct if_quotacheck *qchk)
{
	struct client_obd *cli = &exp->exp_obd->u.cli;
	int rc;
	ENTRY;

	qchk->obd_uuid = cli->cl_target_uuid;
	memcpy(qchk->obd_type, LUSTRE_MDS_NAME, strlen(LUSTRE_MDS_NAME));

	rc = cli->cl_qchk_stat;
	/* the client is not the previous one */
	if (rc == CL_NOT_QUOTACHECKED)
		rc = -EINTR;
	RETURN(rc);
}

static int mdc_quotactl(struct obd_device *unused, struct obd_export *exp,
			struct obd_quotactl *oqctl)
{
	struct ptlrpc_request   *req;
	struct obd_quotactl     *oqc;
	int		      rc;
	ENTRY;

	req = ptlrpc_request_alloc_pack(class_exp2cliimp(exp),
					&RQF_MDS_QUOTACTL, LUSTRE_MDS_VERSION,
					MDS_QUOTACTL);
	if (req == NULL)
		RETURN(-ENOMEM);

	oqc = req_capsule_client_get(&req->rq_pill, &RMF_OBD_QUOTACTL);
	*oqc = *oqctl;

	ptlrpc_request_set_replen(req);
	ptlrpc_at_set_req_timeout(req);
	req->rq_no_resend = 1;

	rc = ptlrpc_queue_wait(req);
	if (rc)
		CERROR("ptlrpc_queue_wait failed, rc: %d\n", rc);

	if (req->rq_repmsg &&
	    (oqc = req_capsule_server_get(&req->rq_pill, &RMF_OBD_QUOTACTL))) {
		*oqctl = *oqc;
	} else if (!rc) {
		CERROR ("Can't unpack obd_quotactl\n");
		rc = -EPROTO;
	}
	ptlrpc_req_finished(req);

	RETURN(rc);
}

static int mdc_ioc_swap_layouts(struct obd_export *exp,
				struct md_op_data *op_data)
{
	LIST_HEAD(cancels);
	struct ptlrpc_request	*req;
	int			 rc, count;
	struct mdc_swap_layouts *msl, *payload;
	ENTRY;

	msl = op_data->op_data;

	/* When the MDT will get the MDS_SWAP_LAYOUTS RPC the
	 * first thing it will do is to cancel the 2 layout
	 * locks hold by this client.
	 * So the client must cancel its layout locks on the 2 fids
	 * with the request RPC to avoid extra RPC round trips
	 */
	count = mdc_resource_get_unused(exp, &op_data->op_fid1, &cancels,
					LCK_CR, MDS_INODELOCK_LAYOUT);
	count += mdc_resource_get_unused(exp, &op_data->op_fid2, &cancels,
					 LCK_CR, MDS_INODELOCK_LAYOUT);

	req = ptlrpc_request_alloc(class_exp2cliimp(exp),
				   &RQF_MDS_SWAP_LAYOUTS);
	if (req == NULL) {
		ldlm_lock_list_put(&cancels, l_bl_ast, count);
		RETURN(-ENOMEM);
	}

	mdc_set_capa_size(req, &RMF_CAPA1, op_data->op_capa1);
	mdc_set_capa_size(req, &RMF_CAPA2, op_data->op_capa2);

	rc = mdc_prep_elc_req(exp, req, MDS_SWAP_LAYOUTS, &cancels, count);
	if (rc) {
		ptlrpc_request_free(req);
		RETURN(rc);
	}

	mdc_swap_layouts_pack(req, op_data);

	payload = req_capsule_client_get(&req->rq_pill, &RMF_SWAP_LAYOUTS);
	LASSERT(payload);

	*payload = *msl;

	ptlrpc_request_set_replen(req);

	rc = ptlrpc_queue_wait(req);
	if (rc)
		GOTO(out, rc);
	EXIT;

out:
	ptlrpc_req_finished(req);
	return rc;
}

static int mdc_iocontrol(unsigned int cmd, struct obd_export *exp, int len,
			 void *karg, void *uarg)
{
	struct obd_device *obd = exp->exp_obd;
	struct obd_ioctl_data *data = karg;
	struct obd_import *imp = obd->u.cli.cl_import;
	struct llog_ctxt *ctxt;
	int rc;
	ENTRY;

	if (!try_module_get(THIS_MODULE)) {
		CERROR("Can't get module. Is it alive?");
		return -EINVAL;
	}
	switch (cmd) {
	case OBD_IOC_CHANGELOG_SEND:
		rc = mdc_ioc_changelog_send(obd, karg);
		GOTO(out, rc);
	case OBD_IOC_CHANGELOG_CLEAR: {
		struct ioc_changelog *icc = karg;
		struct changelog_setinfo cs =
			{.cs_recno = icc->icc_recno, .cs_id = icc->icc_id};
		rc = obd_set_info_async(NULL, exp, strlen(KEY_CHANGELOG_CLEAR),
					KEY_CHANGELOG_CLEAR, sizeof(cs), &cs,
					NULL);
		GOTO(out, rc);
	}
	case OBD_IOC_FID2PATH:
		rc = mdc_ioc_fid2path(exp, karg);
		GOTO(out, rc);
	case LL_IOC_HSM_CT_START:
		rc = mdc_ioc_hsm_ct_start(exp, karg);
		GOTO(out, rc);
	case LL_IOC_HSM_PROGRESS:
		rc = mdc_ioc_hsm_progress(exp, karg);
		GOTO(out, rc);
	case LL_IOC_HSM_STATE_GET:
		rc = mdc_ioc_hsm_state_get(exp, karg);
		GOTO(out, rc);
	case LL_IOC_HSM_STATE_SET:
		rc = mdc_ioc_hsm_state_set(exp, karg);
	case LL_IOC_HSM_ACTION:
		rc = mdc_ioc_hsm_current_action(exp, karg);
		GOTO(out, rc);
	case LL_IOC_HSM_REQUEST:
		rc = mdc_ioc_hsm_request(exp, karg);
		GOTO(out, rc);
	case OBD_IOC_CLIENT_RECOVER:
		rc = ptlrpc_recover_import(imp, data->ioc_inlbuf1, 0);
		if (rc < 0)
			GOTO(out, rc);
		GOTO(out, rc = 0);
	case IOC_OSC_SET_ACTIVE:
		rc = ptlrpc_set_import_active(imp, data->ioc_offset);
		GOTO(out, rc);
	case OBD_IOC_PARSE: {
		ctxt = llog_get_context(exp->exp_obd, LLOG_CONFIG_REPL_CTXT);
		rc = class_config_parse_llog(NULL, ctxt, data->ioc_inlbuf1,
					     NULL);
		llog_ctxt_put(ctxt);
		GOTO(out, rc);
	}
	case OBD_IOC_LLOG_INFO:
	case OBD_IOC_LLOG_PRINT: {
		ctxt = llog_get_context(obd, LLOG_CONFIG_REPL_CTXT);
		rc = llog_ioctl(NULL, ctxt, cmd, data);
		llog_ctxt_put(ctxt);
		GOTO(out, rc);
	}
	case OBD_IOC_POLL_QUOTACHECK:
		rc = mdc_quota_poll_check(exp, (struct if_quotacheck *)karg);
		GOTO(out, rc);
	case OBD_IOC_PING_TARGET:
		rc = ptlrpc_obd_ping(obd);
		GOTO(out, rc);
	/*
	 * Normally IOC_OBD_STATFS, OBD_IOC_QUOTACTL iocontrol are handled by
	 * LMV instead of MDC. But when the cluster is upgraded from 1.8,
	 * there'd be no LMV layer thus we might be called here. Eventually
	 * this code should be removed.
	 * bz20731, LU-592.
	 */
	case IOC_OBD_STATFS: {
		struct obd_statfs stat_buf = {0};

		if (*((__u32 *) data->ioc_inlbuf2) != 0)
			GOTO(out, rc = -ENODEV);

		/* copy UUID */
		if (copy_to_user(data->ioc_pbuf2, obd2cli_tgt(obd),
				     min((int) data->ioc_plen2,
					 (int) sizeof(struct obd_uuid))))
			GOTO(out, rc = -EFAULT);

		rc = mdc_statfs(NULL, obd->obd_self_export, &stat_buf,
				cfs_time_shift_64(-OBD_STATFS_CACHE_SECONDS),
				0);
		if (rc != 0)
			GOTO(out, rc);

		if (copy_to_user(data->ioc_pbuf1, &stat_buf,
				     min((int) data->ioc_plen1,
					 (int) sizeof(stat_buf))))
			GOTO(out, rc = -EFAULT);

		GOTO(out, rc = 0);
	}
	case OBD_IOC_QUOTACTL: {
		struct if_quotactl *qctl = karg;
		struct obd_quotactl *oqctl;

		OBD_ALLOC_PTR(oqctl);
		if (!oqctl)
			RETURN(-ENOMEM);

		QCTL_COPY(oqctl, qctl);
		rc = obd_quotactl(exp, oqctl);
		if (rc == 0) {
			QCTL_COPY(qctl, oqctl);
			qctl->qc_valid = QC_MDTIDX;
			qctl->obd_uuid = obd->u.cli.cl_target_uuid;
		}
		OBD_FREE_PTR(oqctl);
		break;
	}
	case LL_IOC_GET_CONNECT_FLAGS: {
		if (copy_to_user(uarg,
				     exp_connect_flags_ptr(exp),
				     sizeof(__u64)))
			GOTO(out, rc = -EFAULT);
		else
			GOTO(out, rc = 0);
	}
	case LL_IOC_LOV_SWAP_LAYOUTS: {
		rc = mdc_ioc_swap_layouts(exp, karg);
		break;
	}
	default:
		CERROR("mdc_ioctl(): unrecognised ioctl %#x\n", cmd);
		GOTO(out, rc = -ENOTTY);
	}
out:
	module_put(THIS_MODULE);

	return rc;
}

int mdc_get_info_rpc(struct obd_export *exp,
		     obd_count keylen, void *key,
		     int vallen, void *val)
{
	struct obd_import      *imp = class_exp2cliimp(exp);
	struct ptlrpc_request  *req;
	char		   *tmp;
	int		     rc = -EINVAL;
	ENTRY;

	req = ptlrpc_request_alloc(imp, &RQF_MDS_GET_INFO);
	if (req == NULL)
		RETURN(-ENOMEM);

	req_capsule_set_size(&req->rq_pill, &RMF_GETINFO_KEY,
			     RCL_CLIENT, keylen);
	req_capsule_set_size(&req->rq_pill, &RMF_GETINFO_VALLEN,
			     RCL_CLIENT, sizeof(__u32));

	rc = ptlrpc_request_pack(req, LUSTRE_MDS_VERSION, MDS_GET_INFO);
	if (rc) {
		ptlrpc_request_free(req);
		RETURN(rc);
	}

	tmp = req_capsule_client_get(&req->rq_pill, &RMF_GETINFO_KEY);
	memcpy(tmp, key, keylen);
	tmp = req_capsule_client_get(&req->rq_pill, &RMF_GETINFO_VALLEN);
	memcpy(tmp, &vallen, sizeof(__u32));

	req_capsule_set_size(&req->rq_pill, &RMF_GETINFO_VAL,
			     RCL_SERVER, vallen);
	ptlrpc_request_set_replen(req);

	rc = ptlrpc_queue_wait(req);
	/* -EREMOTE means the get_info result is partial, and it needs to
	 * continue on another MDT, see fid2path part in lmv_iocontrol */
	if (rc == 0 || rc == -EREMOTE) {
		tmp = req_capsule_server_get(&req->rq_pill, &RMF_GETINFO_VAL);
		memcpy(val, tmp, vallen);
		if (ptlrpc_rep_need_swab(req)) {
			if (KEY_IS(KEY_FID2PATH))
				lustre_swab_fid2path(val);
		}
	}
	ptlrpc_req_finished(req);

	RETURN(rc);
}

static void lustre_swab_hai(struct hsm_action_item *h)
{
	__swab32s(&h->hai_len);
	__swab32s(&h->hai_action);
	lustre_swab_lu_fid(&h->hai_fid);
	lustre_swab_lu_fid(&h->hai_dfid);
	__swab64s(&h->hai_cookie);
	__swab64s(&h->hai_extent.offset);
	__swab64s(&h->hai_extent.length);
	__swab64s(&h->hai_gid);
}

static void lustre_swab_hal(struct hsm_action_list *h)
{
	struct hsm_action_item	*hai;
	int			 i;

	__swab32s(&h->hal_version);
	__swab32s(&h->hal_count);
	__swab32s(&h->hal_archive_id);
	__swab64s(&h->hal_flags);
	hai = hai_zero(h);
	for (i = 0; i < h->hal_count; i++) {
		lustre_swab_hai(hai);
		hai = hai_next(hai);
	}
}

static void lustre_swab_kuch(struct kuc_hdr *l)
{
	__swab16s(&l->kuc_magic);
	/* __u8 l->kuc_transport */
	__swab16s(&l->kuc_msgtype);
	__swab16s(&l->kuc_msglen);
}

static int mdc_ioc_hsm_ct_start(struct obd_export *exp,
				struct lustre_kernelcomm *lk)
{
	struct obd_import  *imp = class_exp2cliimp(exp);
	__u32		    archive = lk->lk_data;
	int		    rc = 0;

	if (lk->lk_group != KUC_GRP_HSM) {
		CERROR("Bad copytool group %d\n", lk->lk_group);
		return -EINVAL;
	}

	CDEBUG(D_HSM, "CT start r%d w%d u%d g%d f%#x\n", lk->lk_rfd, lk->lk_wfd,
	       lk->lk_uid, lk->lk_group, lk->lk_flags);

	if (lk->lk_flags & LK_FLG_STOP) {
		rc = libcfs_kkuc_group_rem(lk->lk_uid, lk->lk_group);
		/* Unregister with the coordinator */
		if (rc == 0)
			rc = mdc_ioc_hsm_ct_unregister(imp);
	} else {
		struct file *fp = fget(lk->lk_wfd);

		rc = libcfs_kkuc_group_add(fp, lk->lk_uid, lk->lk_group,
					   lk->lk_data);
		if (rc && fp)
			fput(fp);
		if (rc == 0)
			rc = mdc_ioc_hsm_ct_register(imp, archive);
	}

	return rc;
}

/**
 * Send a message to any listening copytools
 * @param val KUC message (kuc_hdr + hsm_action_list)
 * @param len total length of message
 */
static int mdc_hsm_copytool_send(int len, void *val)
{
	struct kuc_hdr		*lh = (struct kuc_hdr *)val;
	struct hsm_action_list	*hal = (struct hsm_action_list *)(lh + 1);
	int			 rc;
	ENTRY;

	if (len < sizeof(*lh) + sizeof(*hal)) {
		CERROR("Short HSM message %d < %d\n", len,
		       (int) (sizeof(*lh) + sizeof(*hal)));
		RETURN(-EPROTO);
	}
	if (lh->kuc_magic == __swab16(KUC_MAGIC)) {
		lustre_swab_kuch(lh);
		lustre_swab_hal(hal);
	} else if (lh->kuc_magic != KUC_MAGIC) {
		CERROR("Bad magic %x!=%x\n", lh->kuc_magic, KUC_MAGIC);
		RETURN(-EPROTO);
	}

	CDEBUG(D_HSM, " Received message mg=%x t=%d m=%d l=%d actions=%d "
	       "on %s\n",
	       lh->kuc_magic, lh->kuc_transport, lh->kuc_msgtype,
	       lh->kuc_msglen, hal->hal_count, hal->hal_fsname);

	/* Broadcast to HSM listeners */
	rc = libcfs_kkuc_group_put(KUC_GRP_HSM, lh);

	RETURN(rc);
}

/**
 * callback function passed to kuc for re-registering each HSM copytool
 * running on MDC, after MDT shutdown/recovery.
 * @param data archive id served by the copytool
 * @param cb_arg callback argument (obd_import)
 */
static int mdc_hsm_ct_reregister(__u32 data, void *cb_arg)
{
	struct obd_import	*imp = (struct obd_import *)cb_arg;
	__u32			 archive = data;
	int			 rc;

	CDEBUG(D_HA, "recover copytool registration to MDT (archive=%#x)\n",
	       archive);
	rc = mdc_ioc_hsm_ct_register(imp, archive);

	/* ignore error if the copytool is already registered */
	return ((rc != 0) && (rc != -EEXIST)) ? rc : 0;
}

/**
 * Re-establish all kuc contexts with MDT
 * after MDT shutdown/recovery.
 */
static int mdc_kuc_reregister(struct obd_import *imp)
{
	/* re-register HSM agents */
	return libcfs_kkuc_group_foreach(KUC_GRP_HSM, mdc_hsm_ct_reregister,
					 (void *)imp);
}

int mdc_set_info_async(const struct lu_env *env,
		       struct obd_export *exp,
		       obd_count keylen, void *key,
		       obd_count vallen, void *val,
		       struct ptlrpc_request_set *set)
{
	struct obd_import	*imp = class_exp2cliimp(exp);
	int			 rc;
	ENTRY;

	if (KEY_IS(KEY_READ_ONLY)) {
		if (vallen != sizeof(int))
			RETURN(-EINVAL);

		spin_lock(&imp->imp_lock);
		if (*((int *)val)) {
			imp->imp_connect_flags_orig |= OBD_CONNECT_RDONLY;
			imp->imp_connect_data.ocd_connect_flags |=
							OBD_CONNECT_RDONLY;
		} else {
			imp->imp_connect_flags_orig &= ~OBD_CONNECT_RDONLY;
			imp->imp_connect_data.ocd_connect_flags &=
							~OBD_CONNECT_RDONLY;
		}
		spin_unlock(&imp->imp_lock);

		rc = do_set_info_async(imp, MDS_SET_INFO, LUSTRE_MDS_VERSION,
				       keylen, key, vallen, val, set);
		RETURN(rc);
	}
	if (KEY_IS(KEY_SPTLRPC_CONF)) {
		sptlrpc_conf_client_adapt(exp->exp_obd);
		RETURN(0);
	}
	if (KEY_IS(KEY_FLUSH_CTX)) {
		sptlrpc_import_flush_my_ctx(imp);
		RETURN(0);
	}
	if (KEY_IS(KEY_MDS_CONN)) {
		/* mds-mds import */
		spin_lock(&imp->imp_lock);
		imp->imp_server_timeout = 1;
		spin_unlock(&imp->imp_lock);
		imp->imp_client->cli_request_portal = MDS_MDS_PORTAL;
		CDEBUG(D_OTHER, "%s: timeout / 2\n", exp->exp_obd->obd_name);
		RETURN(0);
	}
	if (KEY_IS(KEY_CHANGELOG_CLEAR)) {
		rc = do_set_info_async(imp, MDS_SET_INFO, LUSTRE_MDS_VERSION,
				       keylen, key, vallen, val, set);
		RETURN(rc);
	}
	if (KEY_IS(KEY_HSM_COPYTOOL_SEND)) {
		rc = mdc_hsm_copytool_send(vallen, val);
		RETURN(rc);
	}

	CERROR("Unknown key %s\n", (char *)key);
	RETURN(-EINVAL);
}

int mdc_get_info(const struct lu_env *env, struct obd_export *exp,
		 __u32 keylen, void *key, __u32 *vallen, void *val,
		 struct lov_stripe_md *lsm)
{
	int rc = -EINVAL;

	if (KEY_IS(KEY_MAX_EASIZE)) {
		int mdsize, *max_easize;

		if (*vallen != sizeof(int))
			RETURN(-EINVAL);
		mdsize = *(int*)val;
		if (mdsize > exp->exp_obd->u.cli.cl_max_mds_easize)
			exp->exp_obd->u.cli.cl_max_mds_easize = mdsize;
		max_easize = val;
		*max_easize = exp->exp_obd->u.cli.cl_max_mds_easize;
		RETURN(0);
	} else if (KEY_IS(KEY_CONN_DATA)) {
		struct obd_import *imp = class_exp2cliimp(exp);
		struct obd_connect_data *data = val;

		if (*vallen != sizeof(*data))
			RETURN(-EINVAL);

		*data = imp->imp_connect_data;
		RETURN(0);
	} else if (KEY_IS(KEY_TGT_COUNT)) {
		*((int *)val) = 1;
		RETURN(0);
	}

	rc = mdc_get_info_rpc(exp, keylen, key, *vallen, val);

	RETURN(rc);
}

static int mdc_pin(struct obd_export *exp, const struct lu_fid *fid,
		   struct obd_capa *oc, struct obd_client_handle *handle,
		   int flags)
{
	struct ptlrpc_request *req;
	struct mdt_body       *body;
	int		    rc;
	ENTRY;

	req = ptlrpc_request_alloc(class_exp2cliimp(exp), &RQF_MDS_PIN);
	if (req == NULL)
		RETURN(-ENOMEM);

	mdc_set_capa_size(req, &RMF_CAPA1, oc);

	rc = ptlrpc_request_pack(req, LUSTRE_MDS_VERSION, MDS_PIN);
	if (rc) {
		ptlrpc_request_free(req);
		RETURN(rc);
	}

	mdc_pack_body(req, fid, oc, 0, 0, -1, flags);

	ptlrpc_request_set_replen(req);

	mdc_get_rpc_lock(exp->exp_obd->u.cli.cl_rpc_lock, NULL);
	rc = ptlrpc_queue_wait(req);
	mdc_put_rpc_lock(exp->exp_obd->u.cli.cl_rpc_lock, NULL);
	if (rc) {
		CERROR("Pin failed: %d\n", rc);
		GOTO(err_out, rc);
	}

	body = req_capsule_server_get(&req->rq_pill, &RMF_MDT_BODY);
	if (body == NULL)
		GOTO(err_out, rc = -EPROTO);

	handle->och_fh = body->handle;
	handle->och_magic = OBD_CLIENT_HANDLE_MAGIC;

	handle->och_mod = obd_mod_alloc();
	if (handle->och_mod == NULL) {
		DEBUG_REQ(D_ERROR, req, "can't allocate md_open_data");
		GOTO(err_out, rc = -ENOMEM);
	}
	handle->och_mod->mod_open_req = req; /* will be dropped by unpin */

	RETURN(0);

err_out:
	ptlrpc_req_finished(req);
	RETURN(rc);
}

static int mdc_unpin(struct obd_export *exp, struct obd_client_handle *handle,
		     int flag)
{
	struct ptlrpc_request *req;
	struct mdt_body       *body;
	int		    rc;
	ENTRY;

	req = ptlrpc_request_alloc_pack(class_exp2cliimp(exp), &RQF_MDS_UNPIN,
					LUSTRE_MDS_VERSION, MDS_UNPIN);
	if (req == NULL)
		RETURN(-ENOMEM);

	body = req_capsule_client_get(&req->rq_pill, &RMF_MDT_BODY);
	body->handle = handle->och_fh;
	body->flags = flag;

	ptlrpc_request_set_replen(req);

	mdc_get_rpc_lock(exp->exp_obd->u.cli.cl_rpc_lock, NULL);
	rc = ptlrpc_queue_wait(req);
	mdc_put_rpc_lock(exp->exp_obd->u.cli.cl_rpc_lock, NULL);

	if (rc != 0)
		CERROR("Unpin failed: %d\n", rc);

	ptlrpc_req_finished(req);
	ptlrpc_req_finished(handle->och_mod->mod_open_req);

	obd_mod_put(handle->och_mod);
	RETURN(rc);
}

int mdc_sync(struct obd_export *exp, const struct lu_fid *fid,
	     struct obd_capa *oc, struct ptlrpc_request **request)
{
	struct ptlrpc_request *req;
	int		    rc;
	ENTRY;

	*request = NULL;
	req = ptlrpc_request_alloc(class_exp2cliimp(exp), &RQF_MDS_SYNC);
	if (req == NULL)
		RETURN(-ENOMEM);

	mdc_set_capa_size(req, &RMF_CAPA1, oc);

	rc = ptlrpc_request_pack(req, LUSTRE_MDS_VERSION, MDS_SYNC);
	if (rc) {
		ptlrpc_request_free(req);
		RETURN(rc);
	}

	mdc_pack_body(req, fid, oc, 0, 0, -1, 0);

	ptlrpc_request_set_replen(req);

	rc = ptlrpc_queue_wait(req);
	if (rc)
		ptlrpc_req_finished(req);
	else
		*request = req;
	RETURN(rc);
}

static int mdc_import_event(struct obd_device *obd, struct obd_import *imp,
			    enum obd_import_event event)
{
	int rc = 0;

	LASSERT(imp->imp_obd == obd);

	switch (event) {
	case IMP_EVENT_DISCON: {
#if 0
		/* XXX Pass event up to OBDs stack. used only for FLD now */
		rc = obd_notify_observer(obd, obd, OBD_NOTIFY_DISCON, NULL);
#endif
		break;
	}
	case IMP_EVENT_INACTIVE: {
		struct client_obd *cli = &obd->u.cli;
		/*
		 * Flush current sequence to make client obtain new one
		 * from server in case of disconnect/reconnect.
		 */
		if (cli->cl_seq != NULL)
			seq_client_flush(cli->cl_seq);

		rc = obd_notify_observer(obd, obd, OBD_NOTIFY_INACTIVE, NULL);
		break;
	}
	case IMP_EVENT_INVALIDATE: {
		struct ldlm_namespace *ns = obd->obd_namespace;

		ldlm_namespace_cleanup(ns, LDLM_FL_LOCAL_ONLY);

		break;
	}
	case IMP_EVENT_ACTIVE:
		rc = obd_notify_observer(obd, obd, OBD_NOTIFY_ACTIVE, NULL);
		/* restore re-establish kuc registration after reconnecting */
		if (rc == 0)
			rc = mdc_kuc_reregister(imp);
		break;
	case IMP_EVENT_OCD:
		rc = obd_notify_observer(obd, obd, OBD_NOTIFY_OCD, NULL);
		break;
	case IMP_EVENT_DEACTIVATE:
	case IMP_EVENT_ACTIVATE:
		break;
	default:
		CERROR("Unknown import event %x\n", event);
		LBUG();
	}
	RETURN(rc);
}

int mdc_fid_alloc(struct obd_export *exp, struct lu_fid *fid,
		  struct md_op_data *op_data)
{
	struct client_obd *cli = &exp->exp_obd->u.cli;
	struct lu_client_seq *seq = cli->cl_seq;
	ENTRY;
	RETURN(seq_client_alloc_fid(NULL, seq, fid));
}

struct obd_uuid *mdc_get_uuid(struct obd_export *exp) {
	struct client_obd *cli = &exp->exp_obd->u.cli;
	return &cli->cl_target_uuid;
}

/**
 * Determine whether the lock can be canceled before replaying it during
 * recovery, non zero value will be return if the lock can be canceled,
 * or zero returned for not
 */
static int mdc_cancel_for_recovery(struct ldlm_lock *lock)
{
	if (lock->l_resource->lr_type != LDLM_IBITS)
		RETURN(0);

	/* FIXME: if we ever get into a situation where there are too many
	 * opened files with open locks on a single node, then we really
	 * should replay these open locks to reget it */
	if (lock->l_policy_data.l_inodebits.bits & MDS_INODELOCK_OPEN)
		RETURN(0);

	RETURN(1);
}

static int mdc_resource_inode_free(struct ldlm_resource *res)
{
	if (res->lr_lvb_inode)
		res->lr_lvb_inode = NULL;

	return 0;
}

struct ldlm_valblock_ops inode_lvbo = {
	lvbo_free: mdc_resource_inode_free
};

static int mdc_setup(struct obd_device *obd, struct lustre_cfg *cfg)
{
	struct client_obd *cli = &obd->u.cli;
	struct lprocfs_static_vars lvars = { 0 };
	int rc;
	ENTRY;

	OBD_ALLOC(cli->cl_rpc_lock, sizeof (*cli->cl_rpc_lock));
	if (!cli->cl_rpc_lock)
		RETURN(-ENOMEM);
	mdc_init_rpc_lock(cli->cl_rpc_lock);

	ptlrpcd_addref();

	OBD_ALLOC(cli->cl_close_lock, sizeof (*cli->cl_close_lock));
	if (!cli->cl_close_lock)
		GOTO(err_rpc_lock, rc = -ENOMEM);
	mdc_init_rpc_lock(cli->cl_close_lock);

	rc = client_obd_setup(obd, cfg);
	if (rc)
		GOTO(err_close_lock, rc);
	lprocfs_mdc_init_vars(&lvars);
	lprocfs_obd_setup(obd, lvars.obd_vars);
	sptlrpc_lprocfs_cliobd_attach(obd);
	ptlrpc_lprocfs_register_obd(obd);

	ns_register_cancel(obd->obd_namespace, mdc_cancel_for_recovery);

	obd->obd_namespace->ns_lvbo = &inode_lvbo;

	rc = obd_llog_init(obd, &obd->obd_olg, obd, NULL);
	if (rc) {
		mdc_cleanup(obd);
		CERROR("failed to setup llogging subsystems\n");
	}

	RETURN(rc);

err_close_lock:
	OBD_FREE(cli->cl_close_lock, sizeof (*cli->cl_close_lock));
err_rpc_lock:
	OBD_FREE(cli->cl_rpc_lock, sizeof (*cli->cl_rpc_lock));
	ptlrpcd_decref();
	RETURN(rc);
}

/* Initialize the default and maximum LOV EA and cookie sizes.  This allows
 * us to make MDS RPCs with large enough reply buffers to hold the
 * maximum-sized (= maximum striped) EA and cookie without having to
 * calculate this (via a call into the LOV + OSCs) each time we make an RPC. */
static int mdc_init_ea_size(struct obd_export *exp, int easize,
		     int def_easize, int cookiesize)
{
	struct obd_device *obd = exp->exp_obd;
	struct client_obd *cli = &obd->u.cli;
	ENTRY;

	if (cli->cl_max_mds_easize < easize)
		cli->cl_max_mds_easize = easize;

	if (cli->cl_default_mds_easize < def_easize)
		cli->cl_default_mds_easize = def_easize;

	if (cli->cl_max_mds_cookiesize < cookiesize)
		cli->cl_max_mds_cookiesize = cookiesize;

	RETURN(0);
}

static int mdc_precleanup(struct obd_device *obd, enum obd_cleanup_stage stage)
{
	int rc = 0;
	ENTRY;

	switch (stage) {
	case OBD_CLEANUP_EARLY:
		break;
	case OBD_CLEANUP_EXPORTS:
		/* Failsafe, ok if racy */
		if (obd->obd_type->typ_refcnt <= 1)
			libcfs_kkuc_group_rem(0, KUC_GRP_HSM);

		obd_cleanup_client_import(obd);
		ptlrpc_lprocfs_unregister_obd(obd);
		lprocfs_obd_cleanup(obd);

		rc = obd_llog_finish(obd, 0);
		if (rc != 0)
			CERROR("failed to cleanup llogging subsystems\n");
		break;
	}
	RETURN(rc);
}

static int mdc_cleanup(struct obd_device *obd)
{
	struct client_obd *cli = &obd->u.cli;

	OBD_FREE(cli->cl_rpc_lock, sizeof (*cli->cl_rpc_lock));
	OBD_FREE(cli->cl_close_lock, sizeof (*cli->cl_close_lock));

	ptlrpcd_decref();

	return client_obd_cleanup(obd);
}


static int mdc_llog_init(struct obd_device *obd, struct obd_llog_group *olg,
			 struct obd_device *tgt, int *index)
{
	struct llog_ctxt	*ctxt;
	int			 rc;

	ENTRY;

	LASSERT(olg == &obd->obd_olg);

	rc = llog_setup(NULL, obd, olg, LLOG_CHANGELOG_REPL_CTXT, tgt,
			&llog_client_ops);
	if (rc)
		RETURN(rc);

	ctxt = llog_group_get_ctxt(olg, LLOG_CHANGELOG_REPL_CTXT);
	llog_initiator_connect(ctxt);
	llog_ctxt_put(ctxt);

	RETURN(0);
}

static int mdc_llog_finish(struct obd_device *obd, int count)
{
	struct llog_ctxt *ctxt;

	ENTRY;

	ctxt = llog_get_context(obd, LLOG_CHANGELOG_REPL_CTXT);
	if (ctxt)
		llog_cleanup(NULL, ctxt);

	RETURN(0);
}

static int mdc_process_config(struct obd_device *obd, obd_count len, void *buf)
{
	struct lustre_cfg *lcfg = buf;
	struct lprocfs_static_vars lvars = { 0 };
	int rc = 0;

	lprocfs_mdc_init_vars(&lvars);
	switch (lcfg->lcfg_command) {
	default:
		rc = class_process_proc_param(PARAM_MDC, lvars.obd_vars,
					      lcfg, obd);
		if (rc > 0)
			rc = 0;
		break;
	}
	return(rc);
}


/* get remote permission for current user on fid */
int mdc_get_remote_perm(struct obd_export *exp, const struct lu_fid *fid,
			struct obd_capa *oc, __u32 suppgid,
			struct ptlrpc_request **request)
{
	struct ptlrpc_request  *req;
	int		    rc;
	ENTRY;

	LASSERT(client_is_remote(exp));

	*request = NULL;
	req = ptlrpc_request_alloc(class_exp2cliimp(exp), &RQF_MDS_GETATTR);
	if (req == NULL)
		RETURN(-ENOMEM);

	mdc_set_capa_size(req, &RMF_CAPA1, oc);

	rc = ptlrpc_request_pack(req, LUSTRE_MDS_VERSION, MDS_GETATTR);
	if (rc) {
		ptlrpc_request_free(req);
		RETURN(rc);
	}

	mdc_pack_body(req, fid, oc, OBD_MD_FLRMTPERM, 0, suppgid, 0);

	req_capsule_set_size(&req->rq_pill, &RMF_ACL, RCL_SERVER,
			     sizeof(struct mdt_remote_perm));

	ptlrpc_request_set_replen(req);

	rc = ptlrpc_queue_wait(req);
	if (rc)
		ptlrpc_req_finished(req);
	else
		*request = req;
	RETURN(rc);
}

static int mdc_interpret_renew_capa(const struct lu_env *env,
				    struct ptlrpc_request *req, void *args,
				    int status)
{
	struct mdc_renew_capa_args *ra = args;
	struct mdt_body *body = NULL;
	struct lustre_capa *capa;
	ENTRY;

	if (status)
		GOTO(out, capa = ERR_PTR(status));

	body = req_capsule_server_get(&req->rq_pill, &RMF_MDT_BODY);
	if (body == NULL)
		GOTO(out, capa = ERR_PTR(-EFAULT));

	if ((body->valid & OBD_MD_FLOSSCAPA) == 0)
		GOTO(out, capa = ERR_PTR(-ENOENT));

	capa = req_capsule_server_get(&req->rq_pill, &RMF_CAPA2);
	if (!capa)
		GOTO(out, capa = ERR_PTR(-EFAULT));
	EXIT;
out:
	ra->ra_cb(ra->ra_oc, capa);
	return 0;
}

static int mdc_renew_capa(struct obd_export *exp, struct obd_capa *oc,
			  renew_capa_cb_t cb)
{
	struct ptlrpc_request *req;
	struct mdc_renew_capa_args *ra;
	ENTRY;

	req = ptlrpc_request_alloc_pack(class_exp2cliimp(exp), &RQF_MDS_GETATTR,
					LUSTRE_MDS_VERSION, MDS_GETATTR);
	if (req == NULL)
		RETURN(-ENOMEM);

	/* NB, OBD_MD_FLOSSCAPA is set here, but it doesn't necessarily mean the
	 * capa to renew is oss capa.
	 */
	mdc_pack_body(req, &oc->c_capa.lc_fid, oc, OBD_MD_FLOSSCAPA, 0, -1, 0);
	ptlrpc_request_set_replen(req);

	CLASSERT(sizeof(*ra) <= sizeof(req->rq_async_args));
	ra = ptlrpc_req_async_args(req);
	ra->ra_oc = oc;
	ra->ra_cb = cb;
	req->rq_interpret_reply = mdc_interpret_renew_capa;
	ptlrpcd_add_req(req, PDL_POLICY_LOCAL, -1);
	RETURN(0);
}

static int mdc_connect(const struct lu_env *env,
		       struct obd_export **exp,
		       struct obd_device *obd, struct obd_uuid *cluuid,
		       struct obd_connect_data *data,
		       void *localdata)
{
	struct obd_import *imp = obd->u.cli.cl_import;

	/* mds-mds import features */
	if (data && (data->ocd_connect_flags & OBD_CONNECT_MDS_MDS)) {
		spin_lock(&imp->imp_lock);
		imp->imp_server_timeout = 1;
		spin_unlock(&imp->imp_lock);
		imp->imp_client->cli_request_portal = MDS_MDS_PORTAL;
		CDEBUG(D_OTHER, "%s: Set 'mds' portal and timeout\n",
		       obd->obd_name);
	}

	return client_connect_import(env, exp, obd, cluuid, data, NULL);
}

struct obd_ops mdc_obd_ops = {
	.o_owner	    = THIS_MODULE,
	.o_setup	    = mdc_setup,
	.o_precleanup       = mdc_precleanup,
	.o_cleanup	  = mdc_cleanup,
	.o_add_conn	 = client_import_add_conn,
	.o_del_conn	 = client_import_del_conn,
	.o_connect	  = mdc_connect,
	.o_disconnect       = client_disconnect_export,
	.o_iocontrol	= mdc_iocontrol,
	.o_set_info_async   = mdc_set_info_async,
	.o_statfs	   = mdc_statfs,
	.o_pin	      = mdc_pin,
	.o_unpin	    = mdc_unpin,
	.o_fid_init	    = client_fid_init,
	.o_fid_fini	    = client_fid_fini,
	.o_fid_alloc	= mdc_fid_alloc,
	.o_import_event     = mdc_import_event,
	.o_llog_init	= mdc_llog_init,
	.o_llog_finish      = mdc_llog_finish,
	.o_get_info	 = mdc_get_info,
	.o_process_config   = mdc_process_config,
	.o_get_uuid	 = mdc_get_uuid,
	.o_quotactl	 = mdc_quotactl,
	.o_quotacheck       = mdc_quotacheck
};

struct md_ops mdc_md_ops = {
	.m_getstatus	= mdc_getstatus,
	.m_null_inode	    = mdc_null_inode,
	.m_find_cbdata      = mdc_find_cbdata,
	.m_close	    = mdc_close,
	.m_create	   = mdc_create,
	.m_done_writing     = mdc_done_writing,
	.m_enqueue	  = mdc_enqueue,
	.m_getattr	  = mdc_getattr,
	.m_getattr_name     = mdc_getattr_name,
	.m_intent_lock      = mdc_intent_lock,
	.m_link	     = mdc_link,
	.m_is_subdir	= mdc_is_subdir,
	.m_rename	   = mdc_rename,
	.m_setattr	  = mdc_setattr,
	.m_setxattr	 = mdc_setxattr,
	.m_getxattr	 = mdc_getxattr,
	.m_sync	     = mdc_sync,
	.m_readpage	 = mdc_readpage,
	.m_unlink	   = mdc_unlink,
	.m_cancel_unused    = mdc_cancel_unused,
	.m_init_ea_size     = mdc_init_ea_size,
	.m_set_lock_data    = mdc_set_lock_data,
	.m_lock_match       = mdc_lock_match,
	.m_get_lustre_md    = mdc_get_lustre_md,
	.m_free_lustre_md   = mdc_free_lustre_md,
	.m_set_open_replay_data = mdc_set_open_replay_data,
	.m_clear_open_replay_data = mdc_clear_open_replay_data,
	.m_renew_capa       = mdc_renew_capa,
	.m_unpack_capa      = mdc_unpack_capa,
	.m_get_remote_perm  = mdc_get_remote_perm,
	.m_intent_getattr_async = mdc_intent_getattr_async,
	.m_revalidate_lock      = mdc_revalidate_lock
};

int __init mdc_init(void)
{
	int rc;
	struct lprocfs_static_vars lvars = { 0 };
	lprocfs_mdc_init_vars(&lvars);

	rc = class_register_type(&mdc_obd_ops, &mdc_md_ops, lvars.module_vars,
				 LUSTRE_MDC_NAME, NULL);
	RETURN(rc);
}

static void /*__exit*/ mdc_exit(void)
{
	class_unregister_type(LUSTRE_MDC_NAME);
}

MODULE_AUTHOR("Sun Microsystems, Inc. <http://www.lustre.org/>");
MODULE_DESCRIPTION("Lustre Metadata Client");
MODULE_LICENSE("GPL");

module_init(mdc_init);
module_exit(mdc_exit);
