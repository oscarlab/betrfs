/**
 * SHA-256 routines supporting the Power 7+ Nest Accelerators driver
 *
 * Copyright (C) 2011-2012 International Business Machines Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 2 only.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 * Author: Kent Yoder <yoder1@us.ibm.com>
 */

#include <crypto/internal/hash.h>
#include <crypto/sha.h>
#include <linux/module.h>
#include <asm/vio.h>

#include "nx_csbcpb.h"
#include "nx.h"


static int nx_sha256_init(struct shash_desc *desc)
{
	struct sha256_state *sctx = shash_desc_ctx(desc);
	struct nx_crypto_ctx *nx_ctx = crypto_tfm_ctx(&desc->tfm->base);
	struct nx_sg *out_sg;

	nx_ctx_init(nx_ctx, HCOP_FC_SHA);

	memset(sctx, 0, sizeof *sctx);

	nx_ctx->ap = &nx_ctx->props[NX_PROPS_SHA256];

	NX_CPB_SET_DIGEST_SIZE(nx_ctx->csbcpb, NX_DS_SHA256);
	out_sg = nx_build_sg_list(nx_ctx->out_sg, (u8 *)sctx->state,
				  SHA256_DIGEST_SIZE, nx_ctx->ap->sglen);
	nx_ctx->op.outlen = (nx_ctx->out_sg - out_sg) * sizeof(struct nx_sg);

	return 0;
}

static int nx_sha256_update(struct shash_desc *desc, const u8 *data,
			    unsigned int len)
{
	struct sha256_state *sctx = shash_desc_ctx(desc);
	struct nx_crypto_ctx *nx_ctx = crypto_tfm_ctx(&desc->tfm->base);
	struct nx_csbcpb *csbcpb = (struct nx_csbcpb *)nx_ctx->csbcpb;
	struct nx_sg *in_sg;
	u64 to_process, leftover;
	int rc = 0;

	if (NX_CPB_FDM(csbcpb) & NX_FDM_CONTINUATION) {
		/* we've hit the nx chip previously and we're updating again,
		 * so copy over the partial digest */
		memcpy(csbcpb->cpb.sha256.input_partial_digest,
		       csbcpb->cpb.sha256.message_digest, SHA256_DIGEST_SIZE);
	}

	/* 2 cases for total data len:
	 *  1: <= SHA256_BLOCK_SIZE: copy into state, return 0
	 *  2: > SHA256_BLOCK_SIZE: process X blocks, copy in leftover
	 */
	if (len + sctx->count < SHA256_BLOCK_SIZE) {
		memcpy(sctx->buf + sctx->count, data, len);
		sctx->count += len;
		goto out;
	}

	/* to_process: the SHA256_BLOCK_SIZE data chunk to process in this
	 * update */
	to_process = (sctx->count + len) & ~(SHA256_BLOCK_SIZE - 1);
	leftover = (sctx->count + len) & (SHA256_BLOCK_SIZE - 1);

	if (sctx->count) {
		in_sg = nx_build_sg_list(nx_ctx->in_sg, (u8 *)sctx->buf,
					 sctx->count, nx_ctx->ap->sglen);
		in_sg = nx_build_sg_list(in_sg, (u8 *)data,
					 to_process - sctx->count,
					 nx_ctx->ap->sglen);
		nx_ctx->op.inlen = (nx_ctx->in_sg - in_sg) *
					sizeof(struct nx_sg);
	} else {
		in_sg = nx_build_sg_list(nx_ctx->in_sg, (u8 *)data,
					 to_process, nx_ctx->ap->sglen);
		nx_ctx->op.inlen = (nx_ctx->in_sg - in_sg) *
					sizeof(struct nx_sg);
	}

	NX_CPB_FDM(csbcpb) |= NX_FDM_INTERMEDIATE;

	if (!nx_ctx->op.inlen || !nx_ctx->op.outlen) {
		rc = -EINVAL;
		goto out;
	}

	rc = nx_hcall_sync(nx_ctx, &nx_ctx->op,
			   desc->flags & CRYPTO_TFM_REQ_MAY_SLEEP);
	if (rc)
		goto out;

	atomic_inc(&(nx_ctx->stats->sha256_ops));

	/* copy the leftover back into the state struct */
	if (leftover)
		memcpy(sctx->buf, data + len - leftover, leftover);
	sctx->count = leftover;

	csbcpb->cpb.sha256.message_bit_length += (u64)
		(csbcpb->cpb.sha256.spbc * 8);

	/* everything after the first update is continuation */
	NX_CPB_FDM(csbcpb) |= NX_FDM_CONTINUATION;
out:
	return rc;
}

static int nx_sha256_final(struct shash_desc *desc, u8 *out)
{
	struct sha256_state *sctx = shash_desc_ctx(desc);
	struct nx_crypto_ctx *nx_ctx = crypto_tfm_ctx(&desc->tfm->base);
	struct nx_csbcpb *csbcpb = (struct nx_csbcpb *)nx_ctx->csbcpb;
	struct nx_sg *in_sg, *out_sg;
	int rc;


	if (NX_CPB_FDM(csbcpb) & NX_FDM_CONTINUATION) {
		/* we've hit the nx chip previously, now we're finalizing,
		 * so copy over the partial digest */
		memcpy(csbcpb->cpb.sha256.input_partial_digest,
		       csbcpb->cpb.sha256.message_digest, SHA256_DIGEST_SIZE);
	}

	/* final is represented by continuing the operation and indicating that
	 * this is not an intermediate operation */
	NX_CPB_FDM(csbcpb) &= ~NX_FDM_INTERMEDIATE;

	csbcpb->cpb.sha256.message_bit_length += (u64)(sctx->count * 8);

	in_sg = nx_build_sg_list(nx_ctx->in_sg, (u8 *)sctx->buf,
				 sctx->count, nx_ctx->ap->sglen);
	out_sg = nx_build_sg_list(nx_ctx->out_sg, out, SHA256_DIGEST_SIZE,
				  nx_ctx->ap->sglen);
	nx_ctx->op.inlen = (nx_ctx->in_sg - in_sg) * sizeof(struct nx_sg);
	nx_ctx->op.outlen = (nx_ctx->out_sg - out_sg) * sizeof(struct nx_sg);

	if (!nx_ctx->op.outlen) {
		rc = -EINVAL;
		goto out;
	}

	rc = nx_hcall_sync(nx_ctx, &nx_ctx->op,
			   desc->flags & CRYPTO_TFM_REQ_MAY_SLEEP);
	if (rc)
		goto out;

	atomic_inc(&(nx_ctx->stats->sha256_ops));

	atomic64_add(csbcpb->cpb.sha256.message_bit_length / 8,
		     &(nx_ctx->stats->sha256_bytes));
	memcpy(out, csbcpb->cpb.sha256.message_digest, SHA256_DIGEST_SIZE);
out:
	return rc;
}

static int nx_sha256_export(struct shash_desc *desc, void *out)
{
	struct sha256_state *sctx = shash_desc_ctx(desc);
	struct nx_crypto_ctx *nx_ctx = crypto_tfm_ctx(&desc->tfm->base);
	struct nx_csbcpb *csbcpb = (struct nx_csbcpb *)nx_ctx->csbcpb;
	struct sha256_state *octx = out;

	octx->count = sctx->count +
		      (csbcpb->cpb.sha256.message_bit_length / 8);
	memcpy(octx->buf, sctx->buf, sizeof(octx->buf));

	/* if no data has been processed yet, we need to export SHA256's
	 * initial data, in case this context gets imported into a software
	 * context */
	if (csbcpb->cpb.sha256.message_bit_length)
		memcpy(octx->state, csbcpb->cpb.sha256.message_digest,
		       SHA256_DIGEST_SIZE);
	else {
		octx->state[0] = SHA256_H0;
		octx->state[1] = SHA256_H1;
		octx->state[2] = SHA256_H2;
		octx->state[3] = SHA256_H3;
		octx->state[4] = SHA256_H4;
		octx->state[5] = SHA256_H5;
		octx->state[6] = SHA256_H6;
		octx->state[7] = SHA256_H7;
	}

	return 0;
}

static int nx_sha256_import(struct shash_desc *desc, const void *in)
{
	struct sha256_state *sctx = shash_desc_ctx(desc);
	struct nx_crypto_ctx *nx_ctx = crypto_tfm_ctx(&desc->tfm->base);
	struct nx_csbcpb *csbcpb = (struct nx_csbcpb *)nx_ctx->csbcpb;
	const struct sha256_state *ictx = in;

	memcpy(sctx->buf, ictx->buf, sizeof(ictx->buf));

	sctx->count = ictx->count & 0x3f;
	csbcpb->cpb.sha256.message_bit_length = (ictx->count & ~0x3f) * 8;

	if (csbcpb->cpb.sha256.message_bit_length) {
		memcpy(csbcpb->cpb.sha256.message_digest, ictx->state,
		       SHA256_DIGEST_SIZE);

		NX_CPB_FDM(csbcpb) |= NX_FDM_CONTINUATION;
		NX_CPB_FDM(csbcpb) |= NX_FDM_INTERMEDIATE;
	}

	return 0;
}

struct shash_alg nx_shash_sha256_alg = {
	.digestsize = SHA256_DIGEST_SIZE,
	.init       = nx_sha256_init,
	.update     = nx_sha256_update,
	.final      = nx_sha256_final,
	.export     = nx_sha256_export,
	.import     = nx_sha256_import,
	.descsize   = sizeof(struct sha256_state),
	.statesize  = sizeof(struct sha256_state),
	.base       = {
		.cra_name        = "sha256",
		.cra_driver_name = "sha256-nx",
		.cra_priority    = 300,
		.cra_flags       = CRYPTO_ALG_TYPE_SHASH,
		.cra_blocksize   = SHA256_BLOCK_SIZE,
		.cra_module      = THIS_MODULE,
		.cra_ctxsize     = sizeof(struct nx_crypto_ctx),
		.cra_init        = nx_crypto_ctx_sha_init,
		.cra_exit        = nx_crypto_ctx_exit,
	}
};
