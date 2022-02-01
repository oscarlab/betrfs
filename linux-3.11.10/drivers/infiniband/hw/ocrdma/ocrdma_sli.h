/*******************************************************************
 * This file is part of the Emulex RoCE Device Driver for          *
 * RoCE (RDMA over Converged Ethernet) adapters.                   *
 * Copyright (C) 2008-2012 Emulex. All rights reserved.            *
 * EMULEX and SLI are trademarks of Emulex.                        *
 * www.emulex.com                                                  *
 *                                                                 *
 * This program is free software; you can redistribute it and/or   *
 * modify it under the terms of version 2 of the GNU General       *
 * Public License as published by the Free Software Foundation.    *
 * This program is distributed in the hope that it will be useful. *
 * ALL EXPRESS OR IMPLIED CONDITIONS, REPRESENTATIONS AND          *
 * WARRANTIES, INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,  *
 * FITNESS FOR A PARTICULAR PURPOSE, OR NON-INFRINGEMENT, ARE      *
 * DISCLAIMED, EXCEPT TO THE EXTENT THAT SUCH DISCLAIMERS ARE HELD *
 * TO BE LEGALLY INVALID.  See the GNU General Public License for  *
 * more details, a copy of which can be found in the file COPYING  *
 * included with this package.                                     *
 *
 * Contact Information:
 * linux-drivers@emulex.com
 *
 * Emulex
 * 3333 Susan Street
 * Costa Mesa, CA 92626
 *******************************************************************/

#ifndef __OCRDMA_SLI_H__
#define __OCRDMA_SLI_H__

#define Bit(_b) (1 << (_b))

#define OCRDMA_GEN1_FAMILY	0xB
#define OCRDMA_GEN2_FAMILY	0x2

#define OCRDMA_SUBSYS_ROCE 10
enum {
	OCRDMA_CMD_QUERY_CONFIG = 1,
	OCRDMA_CMD_ALLOC_PD,
	OCRDMA_CMD_DEALLOC_PD,

	OCRDMA_CMD_CREATE_AH_TBL,
	OCRDMA_CMD_DELETE_AH_TBL,

	OCRDMA_CMD_CREATE_QP,
	OCRDMA_CMD_QUERY_QP,
	OCRDMA_CMD_MODIFY_QP,
	OCRDMA_CMD_DELETE_QP,

	OCRDMA_CMD_RSVD1,
	OCRDMA_CMD_ALLOC_LKEY,
	OCRDMA_CMD_DEALLOC_LKEY,
	OCRDMA_CMD_REGISTER_NSMR,
	OCRDMA_CMD_REREGISTER_NSMR,
	OCRDMA_CMD_REGISTER_NSMR_CONT,
	OCRDMA_CMD_QUERY_NSMR,
	OCRDMA_CMD_ALLOC_MW,
	OCRDMA_CMD_QUERY_MW,

	OCRDMA_CMD_CREATE_SRQ,
	OCRDMA_CMD_QUERY_SRQ,
	OCRDMA_CMD_MODIFY_SRQ,
	OCRDMA_CMD_DELETE_SRQ,

	OCRDMA_CMD_ATTACH_MCAST,
	OCRDMA_CMD_DETACH_MCAST,

	OCRDMA_CMD_MAX
};

#define OCRDMA_SUBSYS_COMMON 1
enum {
	OCRDMA_CMD_CREATE_CQ		= 12,
	OCRDMA_CMD_CREATE_EQ		= 13,
	OCRDMA_CMD_CREATE_MQ		= 21,
	OCRDMA_CMD_GET_FW_VER		= 35,
	OCRDMA_CMD_DELETE_MQ		= 53,
	OCRDMA_CMD_DELETE_CQ		= 54,
	OCRDMA_CMD_DELETE_EQ		= 55,
	OCRDMA_CMD_GET_FW_CONFIG	= 58,
	OCRDMA_CMD_CREATE_MQ_EXT	= 90
};

enum {
	QTYPE_EQ	= 1,
	QTYPE_CQ	= 2,
	QTYPE_MCCQ	= 3
};

#define OCRDMA_MAX_SGID (8)

#define OCRDMA_MAX_QP    2048
#define OCRDMA_MAX_CQ    2048

enum {
	OCRDMA_DB_RQ_OFFSET		= 0xE0,
	OCRDMA_DB_GEN2_RQ1_OFFSET	= 0x100,
	OCRDMA_DB_GEN2_RQ2_OFFSET	= 0xC0,
	OCRDMA_DB_SQ_OFFSET		= 0x60,
	OCRDMA_DB_GEN2_SQ_OFFSET	= 0x1C0,
	OCRDMA_DB_SRQ_OFFSET		= OCRDMA_DB_RQ_OFFSET,
	OCRDMA_DB_GEN2_SRQ_OFFSET	= OCRDMA_DB_GEN2_RQ1_OFFSET,
	OCRDMA_DB_CQ_OFFSET		= 0x120,
	OCRDMA_DB_EQ_OFFSET		= OCRDMA_DB_CQ_OFFSET,
	OCRDMA_DB_MQ_OFFSET		= 0x140
};

#define OCRDMA_DB_CQ_RING_ID_MASK       0x3FF	/* bits 0 - 9 */
#define OCRDMA_DB_CQ_RING_ID_EXT_MASK  0x0C00	/* bits 10-11 of qid at 12-11 */
/* qid #2 msbits at 12-11 */
#define OCRDMA_DB_CQ_RING_ID_EXT_MASK_SHIFT  0x1
#define OCRDMA_DB_CQ_NUM_POPPED_SHIFT       (16)	/* bits 16 - 28 */
/* Rearm bit */
#define OCRDMA_DB_CQ_REARM_SHIFT        (29)	/* bit 29 */
/* solicited bit */
#define OCRDMA_DB_CQ_SOLICIT_SHIFT   (31)	/* bit 31 */

#define OCRDMA_EQ_ID_MASK		0x1FF	/* bits 0 - 8 */
#define OCRDMA_EQ_ID_EXT_MASK		0x3e00	/* bits 9-13 */
#define OCRDMA_EQ_ID_EXT_MASK_SHIFT	(2)	/* qid bits 9-13 at 11-15 */

/* Clear the interrupt for this eq */
#define OCRDMA_EQ_CLR_SHIFT			(9)	/* bit 9 */
/* Must be 1 */
#define OCRDMA_EQ_TYPE_SHIFT		(10)	/* bit 10 */
/* Number of event entries processed */
#define OCRDMA_NUM_EQE_SHIFT		(16)	/* bits 16 - 28 */
/* Rearm bit */
#define OCRDMA_REARM_SHIFT		(29)	/* bit 29 */

#define OCRDMA_MQ_ID_MASK		0x7FF	/* bits 0 - 10 */
/* Number of entries posted */
#define OCRDMA_MQ_NUM_MQE_SHIFT	(16)	/* bits 16 - 29 */

#define OCRDMA_MIN_HPAGE_SIZE (4096)

#define OCRDMA_MIN_Q_PAGE_SIZE (4096)
#define OCRDMA_MAX_Q_PAGES     (8)

/*
# 0: 4K Bytes
# 1: 8K Bytes
# 2: 16K Bytes
# 3: 32K Bytes
# 4: 64K Bytes
*/
#define OCRDMA_MAX_Q_PAGE_SIZE_CNT (5)
#define OCRDMA_Q_PAGE_BASE_SIZE (OCRDMA_MIN_Q_PAGE_SIZE * OCRDMA_MAX_Q_PAGES)

#define MAX_OCRDMA_QP_PAGES      (8)
#define OCRDMA_MAX_WQE_MEM_SIZE (MAX_OCRDMA_QP_PAGES * OCRDMA_MIN_HQ_PAGE_SIZE)

#define OCRDMA_CREATE_CQ_MAX_PAGES (4)
#define OCRDMA_DPP_CQE_SIZE (4)

#define OCRDMA_GEN2_MAX_CQE 1024
#define OCRDMA_GEN2_CQ_PAGE_SIZE 4096
#define OCRDMA_GEN2_WQE_SIZE 256
#define OCRDMA_MAX_CQE  4095
#define OCRDMA_CQ_PAGE_SIZE 16384
#define OCRDMA_WQE_SIZE 128
#define OCRDMA_WQE_STRIDE 8
#define OCRDMA_WQE_ALIGN_BYTES 16

#define MAX_OCRDMA_SRQ_PAGES MAX_OCRDMA_QP_PAGES

enum {
	OCRDMA_MCH_OPCODE_SHIFT	= 0,
	OCRDMA_MCH_OPCODE_MASK	= 0xFF,
	OCRDMA_MCH_SUBSYS_SHIFT	= 8,
	OCRDMA_MCH_SUBSYS_MASK	= 0xFF00
};

/* mailbox cmd header */
struct ocrdma_mbx_hdr {
	u32 subsys_op;
	u32 timeout;		/* in seconds */
	u32 cmd_len;
	u32 rsvd_version;
} __packed;

enum {
	OCRDMA_MBX_RSP_OPCODE_SHIFT	= 0,
	OCRDMA_MBX_RSP_OPCODE_MASK	= 0xFF,
	OCRDMA_MBX_RSP_SUBSYS_SHIFT	= 8,
	OCRDMA_MBX_RSP_SUBSYS_MASK	= 0xFF << OCRDMA_MBX_RSP_SUBSYS_SHIFT,

	OCRDMA_MBX_RSP_STATUS_SHIFT	= 0,
	OCRDMA_MBX_RSP_STATUS_MASK	= 0xFF,
	OCRDMA_MBX_RSP_ASTATUS_SHIFT	= 8,
	OCRDMA_MBX_RSP_ASTATUS_MASK	= 0xFF << OCRDMA_MBX_RSP_ASTATUS_SHIFT
};

/* mailbox cmd response */
struct ocrdma_mbx_rsp {
	u32 subsys_op;
	u32 status;
	u32 rsp_len;
	u32 add_rsp_len;
} __packed;

enum {
	OCRDMA_MQE_EMBEDDED	= 1,
	OCRDMA_MQE_NONEMBEDDED	= 0
};

struct ocrdma_mqe_sge {
	u32 pa_lo;
	u32 pa_hi;
	u32 len;
} __packed;

enum {
	OCRDMA_MQE_HDR_EMB_SHIFT	= 0,
	OCRDMA_MQE_HDR_EMB_MASK		= Bit(0),
	OCRDMA_MQE_HDR_SGE_CNT_SHIFT	= 3,
	OCRDMA_MQE_HDR_SGE_CNT_MASK	= 0x1F << OCRDMA_MQE_HDR_SGE_CNT_SHIFT,
	OCRDMA_MQE_HDR_SPECIAL_SHIFT	= 24,
	OCRDMA_MQE_HDR_SPECIAL_MASK	= 0xFF << OCRDMA_MQE_HDR_SPECIAL_SHIFT
};

struct ocrdma_mqe_hdr {
	u32 spcl_sge_cnt_emb;
	u32 pyld_len;
	u32 tag_lo;
	u32 tag_hi;
	u32 rsvd3;
} __packed;

struct ocrdma_mqe_emb_cmd {
	struct ocrdma_mbx_hdr mch;
	u8 pyld[220];
} __packed;

struct ocrdma_mqe {
	struct ocrdma_mqe_hdr hdr;
	union {
		struct ocrdma_mqe_emb_cmd emb_req;
		struct {
			struct ocrdma_mqe_sge sge[19];
		} nonemb_req;
		u8 cmd[236];
		struct ocrdma_mbx_rsp rsp;
	} u;
} __packed;

#define OCRDMA_EQ_LEN       4096
#define OCRDMA_MQ_CQ_LEN    256
#define OCRDMA_MQ_LEN       128

#define PAGE_SHIFT_4K		12
#define PAGE_SIZE_4K		(1 << PAGE_SHIFT_4K)

/* Returns number of pages spanned by the data starting at the given addr */
#define PAGES_4K_SPANNED(_address, size) \
	((u32)((((size_t)(_address) & (PAGE_SIZE_4K - 1)) +	\
			(size) + (PAGE_SIZE_4K - 1)) >> PAGE_SHIFT_4K))

struct ocrdma_delete_q_req {
	struct ocrdma_mbx_hdr req;
	u32 id;
} __packed;

struct ocrdma_pa {
	u32 lo;
	u32 hi;
} __packed;

#define MAX_OCRDMA_EQ_PAGES (8)
struct ocrdma_create_eq_req {
	struct ocrdma_mbx_hdr req;
	u32 num_pages;
	u32 valid;
	u32 cnt;
	u32 delay;
	u32 rsvd;
	struct ocrdma_pa pa[MAX_OCRDMA_EQ_PAGES];
} __packed;

enum {
	OCRDMA_CREATE_EQ_VALID	= Bit(29),
	OCRDMA_CREATE_EQ_CNT_SHIFT	= 26,
	OCRDMA_CREATE_CQ_DELAY_SHIFT	= 13,
};

struct ocrdma_create_eq_rsp {
	struct ocrdma_mbx_rsp rsp;
	u32 vector_eqid;
};

#define OCRDMA_EQ_MINOR_OTHER (0x1)

enum {
	OCRDMA_MCQE_STATUS_SHIFT	= 0,
	OCRDMA_MCQE_STATUS_MASK		= 0xFFFF,
	OCRDMA_MCQE_ESTATUS_SHIFT	= 16,
	OCRDMA_MCQE_ESTATUS_MASK	= 0xFFFF << OCRDMA_MCQE_ESTATUS_SHIFT,
	OCRDMA_MCQE_CONS_SHIFT		= 27,
	OCRDMA_MCQE_CONS_MASK		= Bit(27),
	OCRDMA_MCQE_CMPL_SHIFT		= 28,
	OCRDMA_MCQE_CMPL_MASK		= Bit(28),
	OCRDMA_MCQE_AE_SHIFT		= 30,
	OCRDMA_MCQE_AE_MASK		= Bit(30),
	OCRDMA_MCQE_VALID_SHIFT		= 31,
	OCRDMA_MCQE_VALID_MASK		= Bit(31)
};

struct ocrdma_mcqe {
	u32 status;
	u32 tag_lo;
	u32 tag_hi;
	u32 valid_ae_cmpl_cons;
} __packed;

enum {
	OCRDMA_AE_MCQE_QPVALID		= Bit(31),
	OCRDMA_AE_MCQE_QPID_MASK	= 0xFFFF,

	OCRDMA_AE_MCQE_CQVALID		= Bit(31),
	OCRDMA_AE_MCQE_CQID_MASK	= 0xFFFF,
	OCRDMA_AE_MCQE_VALID		= Bit(31),
	OCRDMA_AE_MCQE_AE		= Bit(30),
	OCRDMA_AE_MCQE_EVENT_TYPE_SHIFT	= 16,
	OCRDMA_AE_MCQE_EVENT_TYPE_MASK	=
					0xFF << OCRDMA_AE_MCQE_EVENT_TYPE_SHIFT,
	OCRDMA_AE_MCQE_EVENT_CODE_SHIFT	= 8,
	OCRDMA_AE_MCQE_EVENT_CODE_MASK	=
					0xFF << OCRDMA_AE_MCQE_EVENT_CODE_SHIFT
};
struct ocrdma_ae_mcqe {
	u32 qpvalid_qpid;
	u32 cqvalid_cqid;
	u32 evt_tag;
	u32 valid_ae_event;
} __packed;

enum {
	OCRDMA_AE_MPA_MCQE_REQ_ID_SHIFT		= 16,
	OCRDMA_AE_MPA_MCQE_REQ_ID_MASK		= 0xFFFF <<
					OCRDMA_AE_MPA_MCQE_REQ_ID_SHIFT,

	OCRDMA_AE_MPA_MCQE_EVENT_CODE_SHIFT	= 8,
	OCRDMA_AE_MPA_MCQE_EVENT_CODE_MASK	= 0xFF <<
					OCRDMA_AE_MPA_MCQE_EVENT_CODE_SHIFT,
	OCRDMA_AE_MPA_MCQE_EVENT_TYPE_SHIFT	= 16,
	OCRDMA_AE_MPA_MCQE_EVENT_TYPE_MASK	= 0xFF <<
					OCRDMA_AE_MPA_MCQE_EVENT_TYPE_SHIFT,
	OCRDMA_AE_MPA_MCQE_EVENT_AE_SHIFT	= 30,
	OCRDMA_AE_MPA_MCQE_EVENT_AE_MASK	= Bit(30),
	OCRDMA_AE_MPA_MCQE_EVENT_VALID_SHIFT	= 31,
	OCRDMA_AE_MPA_MCQE_EVENT_VALID_MASK	= Bit(31)
};

struct ocrdma_ae_mpa_mcqe {
	u32 req_id;
	u32 w1;
	u32 w2;
	u32 valid_ae_event;
} __packed;

enum {
	OCRDMA_AE_QP_MCQE_NEW_QP_STATE_SHIFT	= 0,
	OCRDMA_AE_QP_MCQE_NEW_QP_STATE_MASK	= 0xFFFF,
	OCRDMA_AE_QP_MCQE_QP_ID_SHIFT		= 16,
	OCRDMA_AE_QP_MCQE_QP_ID_MASK		= 0xFFFF <<
						OCRDMA_AE_QP_MCQE_QP_ID_SHIFT,

	OCRDMA_AE_QP_MCQE_EVENT_CODE_SHIFT	= 8,
	OCRDMA_AE_QP_MCQE_EVENT_CODE_MASK	= 0xFF <<
				OCRDMA_AE_QP_MCQE_EVENT_CODE_SHIFT,
	OCRDMA_AE_QP_MCQE_EVENT_TYPE_SHIFT	= 16,
	OCRDMA_AE_QP_MCQE_EVENT_TYPE_MASK	= 0xFF <<
				OCRDMA_AE_QP_MCQE_EVENT_TYPE_SHIFT,
	OCRDMA_AE_QP_MCQE_EVENT_AE_SHIFT	= 30,
	OCRDMA_AE_QP_MCQE_EVENT_AE_MASK		= Bit(30),
	OCRDMA_AE_QP_MCQE_EVENT_VALID_SHIFT	= 31,
	OCRDMA_AE_QP_MCQE_EVENT_VALID_MASK	= Bit(31)
};

struct ocrdma_ae_qp_mcqe {
	u32 qp_id_state;
	u32 w1;
	u32 w2;
	u32 valid_ae_event;
} __packed;

#define OCRDMA_ASYNC_EVE_CODE 0x14

enum OCRDMA_ASYNC_EVENT_TYPE {
	OCRDMA_CQ_ERROR			= 0x00,
	OCRDMA_CQ_OVERRUN_ERROR		= 0x01,
	OCRDMA_CQ_QPCAT_ERROR		= 0x02,
	OCRDMA_QP_ACCESS_ERROR		= 0x03,
	OCRDMA_QP_COMM_EST_EVENT	= 0x04,
	OCRDMA_SQ_DRAINED_EVENT		= 0x05,
	OCRDMA_DEVICE_FATAL_EVENT	= 0x08,
	OCRDMA_SRQCAT_ERROR		= 0x0E,
	OCRDMA_SRQ_LIMIT_EVENT		= 0x0F,
	OCRDMA_QP_LAST_WQE_EVENT	= 0x10
};

/* mailbox command request and responses */
enum {
	OCRDMA_MBX_QUERY_CFG_CQ_OVERFLOW_SHIFT		= 2,
	OCRDMA_MBX_QUERY_CFG_CQ_OVERFLOW_MASK		= Bit(2),
	OCRDMA_MBX_QUERY_CFG_SRQ_SUPPORTED_SHIFT	= 3,
	OCRDMA_MBX_QUERY_CFG_SRQ_SUPPORTED_MASK		= Bit(3),
	OCRDMA_MBX_QUERY_CFG_MAX_QP_SHIFT		= 8,
	OCRDMA_MBX_QUERY_CFG_MAX_QP_MASK		= 0xFFFFFF <<
				OCRDMA_MBX_QUERY_CFG_MAX_QP_SHIFT,

	OCRDMA_MBX_QUERY_CFG_MAX_PD_SHIFT		= 16,
	OCRDMA_MBX_QUERY_CFG_MAX_PD_MASK		= 0xFFFF <<
					OCRDMA_MBX_QUERY_CFG_MAX_PD_SHIFT,
	OCRDMA_MBX_QUERY_CFG_CA_ACK_DELAY_SHIFT		= 8,
	OCRDMA_MBX_QUERY_CFG_CA_ACK_DELAY_MASK		= 0xFF <<
				OCRDMA_MBX_QUERY_CFG_CA_ACK_DELAY_SHIFT,

	OCRDMA_MBX_QUERY_CFG_MAX_SEND_SGE_SHIFT		= 0,
	OCRDMA_MBX_QUERY_CFG_MAX_SEND_SGE_MASK		= 0xFFFF,
	OCRDMA_MBX_QUERY_CFG_MAX_WRITE_SGE_SHIFT	= 16,
	OCRDMA_MBX_QUERY_CFG_MAX_WRITE_SGE_MASK		= 0xFFFF <<
				OCRDMA_MBX_QUERY_CFG_MAX_WRITE_SGE_SHIFT,

	OCRDMA_MBX_QUERY_CFG_MAX_ORD_PER_QP_SHIFT	= 0,
	OCRDMA_MBX_QUERY_CFG_MAX_ORD_PER_QP_MASK	= 0xFFFF,
	OCRDMA_MBX_QUERY_CFG_MAX_IRD_PER_QP_SHIFT	= 16,
	OCRDMA_MBX_QUERY_CFG_MAX_IRD_PER_QP_MASK	= 0xFFFF <<
				OCRDMA_MBX_QUERY_CFG_MAX_IRD_PER_QP_SHIFT,

	OCRDMA_MBX_QUERY_CFG_MAX_WQE_SIZE_OFFSET	= 24,
	OCRDMA_MBX_QUERY_CFG_MAX_WQE_SIZE_MASK		= 0xFF <<
				OCRDMA_MBX_QUERY_CFG_MAX_WQE_SIZE_OFFSET,
	OCRDMA_MBX_QUERY_CFG_MAX_RQE_SIZE_OFFSET	= 16,
	OCRDMA_MBX_QUERY_CFG_MAX_RQE_SIZE_MASK		= 0xFF <<
				OCRDMA_MBX_QUERY_CFG_MAX_RQE_SIZE_OFFSET,
	OCRDMA_MBX_QUERY_CFG_MAX_DPP_CQES_OFFSET	= 0,
	OCRDMA_MBX_QUERY_CFG_MAX_DPP_CQES_MASK		= 0xFFFF <<
				OCRDMA_MBX_QUERY_CFG_MAX_DPP_CQES_OFFSET,

	OCRDMA_MBX_QUERY_CFG_MAX_SRQ_OFFSET		= 16,
	OCRDMA_MBX_QUERY_CFG_MAX_SRQ_MASK		= 0xFFFF <<
				OCRDMA_MBX_QUERY_CFG_MAX_SRQ_OFFSET,
	OCRDMA_MBX_QUERY_CFG_MAX_RPIR_QPS_OFFSET	= 0,
	OCRDMA_MBX_QUERY_CFG_MAX_RPIR_QPS_MASK		= 0xFFFF <<
				OCRDMA_MBX_QUERY_CFG_MAX_RPIR_QPS_OFFSET,

	OCRDMA_MBX_QUERY_CFG_MAX_DPP_PDS_OFFSET		= 16,
	OCRDMA_MBX_QUERY_CFG_MAX_DPP_PDS_MASK		= 0xFFFF <<
				OCRDMA_MBX_QUERY_CFG_MAX_DPP_PDS_OFFSET,
	OCRDMA_MBX_QUERY_CFG_MAX_DPP_CREDITS_OFFSET	= 0,
	OCRDMA_MBX_QUERY_CFG_MAX_DPP_CREDITS_MASK	= 0xFFFF <<
				OCRDMA_MBX_QUERY_CFG_MAX_DPP_CREDITS_OFFSET,

	OCRDMA_MBX_QUERY_CFG_MAX_DPP_QPS_OFFSET		= 0,
	OCRDMA_MBX_QUERY_CFG_MAX_DPP_QPS_MASK		= 0xFFFF <<
				OCRDMA_MBX_QUERY_CFG_MAX_DPP_QPS_OFFSET,

	OCRDMA_MBX_QUERY_CFG_MAX_WQES_PER_WQ_OFFSET	= 16,
	OCRDMA_MBX_QUERY_CFG_MAX_WQES_PER_WQ_MASK	= 0xFFFF <<
				OCRDMA_MBX_QUERY_CFG_MAX_WQES_PER_WQ_OFFSET,
	OCRDMA_MBX_QUERY_CFG_MAX_RQES_PER_RQ_OFFSET	= 0,
	OCRDMA_MBX_QUERY_CFG_MAX_RQES_PER_RQ_MASK	= 0xFFFF <<
				OCRDMA_MBX_QUERY_CFG_MAX_RQES_PER_RQ_OFFSET,

	OCRDMA_MBX_QUERY_CFG_MAX_CQ_OFFSET		= 16,
	OCRDMA_MBX_QUERY_CFG_MAX_CQ_MASK		= 0xFFFF <<
				OCRDMA_MBX_QUERY_CFG_MAX_CQ_OFFSET,
	OCRDMA_MBX_QUERY_CFG_MAX_CQES_PER_CQ_OFFSET	= 0,
	OCRDMA_MBX_QUERY_CFG_MAX_CQES_PER_CQ_MASK	= 0xFFFF <<
				OCRDMA_MBX_QUERY_CFG_MAX_CQES_PER_CQ_OFFSET,

	OCRDMA_MBX_QUERY_CFG_MAX_SRQ_RQE_OFFSET		= 16,
	OCRDMA_MBX_QUERY_CFG_MAX_SRQ_RQE_MASK		= 0xFFFF <<
				OCRDMA_MBX_QUERY_CFG_MAX_SRQ_RQE_OFFSET,
	OCRDMA_MBX_QUERY_CFG_MAX_SRQ_SGE_OFFSET		= 0,
	OCRDMA_MBX_QUERY_CFG_MAX_SRQ_SGE_MASK		= 0xFFFF <<
				OCRDMA_MBX_QUERY_CFG_MAX_SRQ_SGE_OFFSET,
};

struct ocrdma_mbx_query_config {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_rsp rsp;
	u32 qp_srq_cq_ird_ord;
	u32 max_pd_ca_ack_delay;
	u32 max_write_send_sge;
	u32 max_ird_ord_per_qp;
	u32 max_shared_ird_ord;
	u32 max_mr;
	u64 max_mr_size;
	u32 max_num_mr_pbl;
	u32 max_mw;
	u32 max_fmr;
	u32 max_pages_per_frmr;
	u32 max_mcast_group;
	u32 max_mcast_qp_attach;
	u32 max_total_mcast_qp_attach;
	u32 wqe_rqe_stride_max_dpp_cqs;
	u32 max_srq_rpir_qps;
	u32 max_dpp_pds_credits;
	u32 max_dpp_credits_pds_per_pd;
	u32 max_wqes_rqes_per_q;
	u32 max_cq_cqes_per_cq;
	u32 max_srq_rqe_sge;
} __packed;

struct ocrdma_fw_ver_rsp {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_rsp rsp;

	u8 running_ver[32];
} __packed;

struct ocrdma_fw_conf_rsp {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_rsp rsp;

	u32 config_num;
	u32 asic_revision;
	u32 phy_port;
	u32 fn_mode;
	struct {
		u32 mode;
		u32 nic_wqid_base;
		u32 nic_wq_tot;
		u32 prot_wqid_base;
		u32 prot_wq_tot;
		u32 prot_rqid_base;
		u32 prot_rqid_tot;
		u32 rsvd[6];
	} ulp[2];
	u32 fn_capabilities;
	u32 rsvd1;
	u32 rsvd2;
	u32 base_eqid;
	u32 max_eq;

} __packed;

enum {
	OCRDMA_FN_MODE_RDMA	= 0x4
};

enum {
	OCRDMA_CREATE_CQ_VER2			= 2,

	OCRDMA_CREATE_CQ_PAGE_CNT_MASK		= 0xFFFF,
	OCRDMA_CREATE_CQ_PAGE_SIZE_SHIFT	= 16,
	OCRDMA_CREATE_CQ_PAGE_SIZE_MASK		= 0xFF,

	OCRDMA_CREATE_CQ_COALESCWM_SHIFT	= 12,
	OCRDMA_CREATE_CQ_COALESCWM_MASK		= Bit(13) | Bit(12),
	OCRDMA_CREATE_CQ_FLAGS_NODELAY		= Bit(14),
	OCRDMA_CREATE_CQ_FLAGS_AUTO_VALID	= Bit(15),

	OCRDMA_CREATE_CQ_EQ_ID_MASK		= 0xFFFF,
	OCRDMA_CREATE_CQ_CQE_COUNT_MASK		= 0xFFFF
};

enum {
	OCRDMA_CREATE_CQ_VER0			= 0,
	OCRDMA_CREATE_CQ_DPP			= 1,
	OCRDMA_CREATE_CQ_TYPE_SHIFT		= 24,
	OCRDMA_CREATE_CQ_EQID_SHIFT		= 22,

	OCRDMA_CREATE_CQ_CNT_SHIFT		= 27,
	OCRDMA_CREATE_CQ_FLAGS_VALID		= Bit(29),
	OCRDMA_CREATE_CQ_FLAGS_EVENTABLE	= Bit(31),
	OCRDMA_CREATE_CQ_DEF_FLAGS		= OCRDMA_CREATE_CQ_FLAGS_VALID |
					OCRDMA_CREATE_CQ_FLAGS_EVENTABLE |
					OCRDMA_CREATE_CQ_FLAGS_NODELAY
};

struct ocrdma_create_cq_cmd {
	struct ocrdma_mbx_hdr req;
	u32 pgsz_pgcnt;
	u32 ev_cnt_flags;
	u32 eqn;
	u32 cqe_count;
	u32 rsvd6;
	struct ocrdma_pa pa[OCRDMA_CREATE_CQ_MAX_PAGES];
};

struct ocrdma_create_cq {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_create_cq_cmd cmd;
} __packed;

enum {
	OCRDMA_CREATE_CQ_RSP_CQ_ID_MASK	= 0xFFFF
};

struct ocrdma_create_cq_cmd_rsp {
	struct ocrdma_mbx_rsp rsp;
	u32 cq_id;
} __packed;

struct ocrdma_create_cq_rsp {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_create_cq_cmd_rsp rsp;
} __packed;

enum {
	OCRDMA_CREATE_MQ_V0_CQ_ID_SHIFT		= 22,
	OCRDMA_CREATE_MQ_CQ_ID_SHIFT		= 16,
	OCRDMA_CREATE_MQ_RING_SIZE_SHIFT	= 16,
	OCRDMA_CREATE_MQ_VALID			= Bit(31),
	OCRDMA_CREATE_MQ_ASYNC_CQ_VALID		= Bit(0)
};

struct ocrdma_create_mq_req {
	struct ocrdma_mbx_hdr req;
	u32 cqid_pages;
	u32 async_event_bitmap;
	u32 async_cqid_ringsize;
	u32 valid;
	u32 async_cqid_valid;
	u32 rsvd;
	struct ocrdma_pa pa[8];
} __packed;

struct ocrdma_create_mq_rsp {
	struct ocrdma_mbx_rsp rsp;
	u32 id;
} __packed;

enum {
	OCRDMA_DESTROY_CQ_QID_SHIFT			= 0,
	OCRDMA_DESTROY_CQ_QID_MASK			= 0xFFFF,
	OCRDMA_DESTROY_CQ_QID_BYPASS_FLUSH_SHIFT	= 16,
	OCRDMA_DESTROY_CQ_QID_BYPASS_FLUSH_MASK		= 0xFFFF <<
				OCRDMA_DESTROY_CQ_QID_BYPASS_FLUSH_SHIFT
};

struct ocrdma_destroy_cq {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_hdr req;

	u32 bypass_flush_qid;
} __packed;

struct ocrdma_destroy_cq_rsp {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_rsp rsp;
} __packed;

enum {
	OCRDMA_QPT_GSI	= 1,
	OCRDMA_QPT_RC	= 2,
	OCRDMA_QPT_UD	= 4,
};

enum {
	OCRDMA_CREATE_QP_REQ_PD_ID_SHIFT	= 0,
	OCRDMA_CREATE_QP_REQ_PD_ID_MASK		= 0xFFFF,
	OCRDMA_CREATE_QP_REQ_SQ_PAGE_SIZE_SHIFT	= 16,
	OCRDMA_CREATE_QP_REQ_RQ_PAGE_SIZE_SHIFT	= 19,
	OCRDMA_CREATE_QP_REQ_QPT_SHIFT		= 29,
	OCRDMA_CREATE_QP_REQ_QPT_MASK		= Bit(31) | Bit(30) | Bit(29),

	OCRDMA_CREATE_QP_REQ_MAX_RQE_SHIFT	= 0,
	OCRDMA_CREATE_QP_REQ_MAX_RQE_MASK	= 0xFFFF,
	OCRDMA_CREATE_QP_REQ_MAX_WQE_SHIFT	= 16,
	OCRDMA_CREATE_QP_REQ_MAX_WQE_MASK	= 0xFFFF <<
					OCRDMA_CREATE_QP_REQ_MAX_WQE_SHIFT,

	OCRDMA_CREATE_QP_REQ_MAX_SGE_WRITE_SHIFT	= 0,
	OCRDMA_CREATE_QP_REQ_MAX_SGE_WRITE_MASK		= 0xFFFF,
	OCRDMA_CREATE_QP_REQ_MAX_SGE_SEND_SHIFT		= 16,
	OCRDMA_CREATE_QP_REQ_MAX_SGE_SEND_MASK		= 0xFFFF <<
					OCRDMA_CREATE_QP_REQ_MAX_SGE_SEND_SHIFT,

	OCRDMA_CREATE_QP_REQ_FMR_EN_SHIFT		= 0,
	OCRDMA_CREATE_QP_REQ_FMR_EN_MASK		= Bit(0),
	OCRDMA_CREATE_QP_REQ_ZERO_LKEYEN_SHIFT		= 1,
	OCRDMA_CREATE_QP_REQ_ZERO_LKEYEN_MASK		= Bit(1),
	OCRDMA_CREATE_QP_REQ_BIND_MEMWIN_SHIFT		= 2,
	OCRDMA_CREATE_QP_REQ_BIND_MEMWIN_MASK		= Bit(2),
	OCRDMA_CREATE_QP_REQ_INB_WREN_SHIFT		= 3,
	OCRDMA_CREATE_QP_REQ_INB_WREN_MASK		= Bit(3),
	OCRDMA_CREATE_QP_REQ_INB_RDEN_SHIFT		= 4,
	OCRDMA_CREATE_QP_REQ_INB_RDEN_MASK		= Bit(4),
	OCRDMA_CREATE_QP_REQ_USE_SRQ_SHIFT		= 5,
	OCRDMA_CREATE_QP_REQ_USE_SRQ_MASK		= Bit(5),
	OCRDMA_CREATE_QP_REQ_ENABLE_RPIR_SHIFT		= 6,
	OCRDMA_CREATE_QP_REQ_ENABLE_RPIR_MASK		= Bit(6),
	OCRDMA_CREATE_QP_REQ_ENABLE_DPP_SHIFT		= 7,
	OCRDMA_CREATE_QP_REQ_ENABLE_DPP_MASK		= Bit(7),
	OCRDMA_CREATE_QP_REQ_ENABLE_DPP_CQ_SHIFT	= 8,
	OCRDMA_CREATE_QP_REQ_ENABLE_DPP_CQ_MASK		= Bit(8),
	OCRDMA_CREATE_QP_REQ_MAX_SGE_RECV_SHIFT		= 16,
	OCRDMA_CREATE_QP_REQ_MAX_SGE_RECV_MASK		= 0xFFFF <<
				OCRDMA_CREATE_QP_REQ_MAX_SGE_RECV_SHIFT,

	OCRDMA_CREATE_QP_REQ_MAX_IRD_SHIFT		= 0,
	OCRDMA_CREATE_QP_REQ_MAX_IRD_MASK		= 0xFFFF,
	OCRDMA_CREATE_QP_REQ_MAX_ORD_SHIFT		= 16,
	OCRDMA_CREATE_QP_REQ_MAX_ORD_MASK		= 0xFFFF <<
				OCRDMA_CREATE_QP_REQ_MAX_ORD_SHIFT,

	OCRDMA_CREATE_QP_REQ_NUM_RQ_PAGES_SHIFT		= 0,
	OCRDMA_CREATE_QP_REQ_NUM_RQ_PAGES_MASK		= 0xFFFF,
	OCRDMA_CREATE_QP_REQ_NUM_WQ_PAGES_SHIFT		= 16,
	OCRDMA_CREATE_QP_REQ_NUM_WQ_PAGES_MASK		= 0xFFFF <<
				OCRDMA_CREATE_QP_REQ_NUM_WQ_PAGES_SHIFT,

	OCRDMA_CREATE_QP_REQ_RQE_SIZE_SHIFT		= 0,
	OCRDMA_CREATE_QP_REQ_RQE_SIZE_MASK		= 0xFFFF,
	OCRDMA_CREATE_QP_REQ_WQE_SIZE_SHIFT		= 16,
	OCRDMA_CREATE_QP_REQ_WQE_SIZE_MASK		= 0xFFFF <<
				OCRDMA_CREATE_QP_REQ_WQE_SIZE_SHIFT,

	OCRDMA_CREATE_QP_REQ_RQ_CQID_SHIFT		= 0,
	OCRDMA_CREATE_QP_REQ_RQ_CQID_MASK		= 0xFFFF,
	OCRDMA_CREATE_QP_REQ_WQ_CQID_SHIFT		= 16,
	OCRDMA_CREATE_QP_REQ_WQ_CQID_MASK		= 0xFFFF <<
				OCRDMA_CREATE_QP_REQ_WQ_CQID_SHIFT,

	OCRDMA_CREATE_QP_REQ_DPP_CQPID_SHIFT		= 0,
	OCRDMA_CREATE_QP_REQ_DPP_CQPID_MASK		= 0xFFFF,
	OCRDMA_CREATE_QP_REQ_DPP_CREDIT_SHIFT		= 16,
	OCRDMA_CREATE_QP_REQ_DPP_CREDIT_MASK		= 0xFFFF <<
				OCRDMA_CREATE_QP_REQ_DPP_CREDIT_SHIFT
};

enum {
	OCRDMA_CREATE_QP_REQ_DPP_CREDIT_LIMIT	= 16,
	OCRDMA_CREATE_QP_RSP_DPP_PAGE_SHIFT	= 1
};

#define MAX_OCRDMA_IRD_PAGES 4

enum ocrdma_qp_flags {
	OCRDMA_QP_MW_BIND	= 1,
	OCRDMA_QP_LKEY0		= (1 << 1),
	OCRDMA_QP_FAST_REG	= (1 << 2),
	OCRDMA_QP_INB_RD	= (1 << 6),
	OCRDMA_QP_INB_WR	= (1 << 7),
};

enum ocrdma_qp_state {
	OCRDMA_QPS_RST		= 0,
	OCRDMA_QPS_INIT		= 1,
	OCRDMA_QPS_RTR		= 2,
	OCRDMA_QPS_RTS		= 3,
	OCRDMA_QPS_SQE		= 4,
	OCRDMA_QPS_SQ_DRAINING	= 5,
	OCRDMA_QPS_ERR		= 6,
	OCRDMA_QPS_SQD		= 7
};

struct ocrdma_create_qp_req {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_hdr req;

	u32 type_pgsz_pdn;
	u32 max_wqe_rqe;
	u32 max_sge_send_write;
	u32 max_sge_recv_flags;
	u32 max_ord_ird;
	u32 num_wq_rq_pages;
	u32 wqe_rqe_size;
	u32 wq_rq_cqid;
	struct ocrdma_pa wq_addr[MAX_OCRDMA_QP_PAGES];
	struct ocrdma_pa rq_addr[MAX_OCRDMA_QP_PAGES];
	u32 dpp_credits_cqid;
	u32 rpir_lkey;
	struct ocrdma_pa ird_addr[MAX_OCRDMA_IRD_PAGES];
} __packed;

enum {
	OCRDMA_CREATE_QP_RSP_QP_ID_SHIFT		= 0,
	OCRDMA_CREATE_QP_RSP_QP_ID_MASK			= 0xFFFF,

	OCRDMA_CREATE_QP_RSP_MAX_RQE_SHIFT		= 0,
	OCRDMA_CREATE_QP_RSP_MAX_RQE_MASK		= 0xFFFF,
	OCRDMA_CREATE_QP_RSP_MAX_WQE_SHIFT		= 16,
	OCRDMA_CREATE_QP_RSP_MAX_WQE_MASK		= 0xFFFF <<
				OCRDMA_CREATE_QP_RSP_MAX_WQE_SHIFT,

	OCRDMA_CREATE_QP_RSP_MAX_SGE_WRITE_SHIFT	= 0,
	OCRDMA_CREATE_QP_RSP_MAX_SGE_WRITE_MASK		= 0xFFFF,
	OCRDMA_CREATE_QP_RSP_MAX_SGE_SEND_SHIFT		= 16,
	OCRDMA_CREATE_QP_RSP_MAX_SGE_SEND_MASK		= 0xFFFF <<
				OCRDMA_CREATE_QP_RSP_MAX_SGE_SEND_SHIFT,

	OCRDMA_CREATE_QP_RSP_MAX_SGE_RECV_SHIFT		= 16,
	OCRDMA_CREATE_QP_RSP_MAX_SGE_RECV_MASK		= 0xFFFF <<
				OCRDMA_CREATE_QP_RSP_MAX_SGE_RECV_SHIFT,

	OCRDMA_CREATE_QP_RSP_MAX_IRD_SHIFT		= 0,
	OCRDMA_CREATE_QP_RSP_MAX_IRD_MASK		= 0xFFFF,
	OCRDMA_CREATE_QP_RSP_MAX_ORD_SHIFT		= 16,
	OCRDMA_CREATE_QP_RSP_MAX_ORD_MASK		= 0xFFFF <<
				OCRDMA_CREATE_QP_RSP_MAX_ORD_SHIFT,

	OCRDMA_CREATE_QP_RSP_RQ_ID_SHIFT		= 0,
	OCRDMA_CREATE_QP_RSP_RQ_ID_MASK			= 0xFFFF,
	OCRDMA_CREATE_QP_RSP_SQ_ID_SHIFT		= 16,
	OCRDMA_CREATE_QP_RSP_SQ_ID_MASK			= 0xFFFF <<
				OCRDMA_CREATE_QP_RSP_SQ_ID_SHIFT,

	OCRDMA_CREATE_QP_RSP_DPP_ENABLED_MASK		= Bit(0),
	OCRDMA_CREATE_QP_RSP_DPP_PAGE_OFFSET_SHIFT	= 1,
	OCRDMA_CREATE_QP_RSP_DPP_PAGE_OFFSET_MASK	= 0x7FFF <<
				OCRDMA_CREATE_QP_RSP_DPP_PAGE_OFFSET_SHIFT,
	OCRDMA_CREATE_QP_RSP_DPP_CREDITS_SHIFT		= 16,
	OCRDMA_CREATE_QP_RSP_DPP_CREDITS_MASK		= 0xFFFF <<
				OCRDMA_CREATE_QP_RSP_DPP_CREDITS_SHIFT,
};

struct ocrdma_create_qp_rsp {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_rsp rsp;

	u32 qp_id;
	u32 max_wqe_rqe;
	u32 max_sge_send_write;
	u32 max_sge_recv;
	u32 max_ord_ird;
	u32 sq_rq_id;
	u32 dpp_response;
} __packed;

struct ocrdma_destroy_qp {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_hdr req;
	u32 qp_id;
} __packed;

struct ocrdma_destroy_qp_rsp {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_rsp rsp;
} __packed;

enum {
	OCRDMA_MODIFY_QP_ID_SHIFT	= 0,
	OCRDMA_MODIFY_QP_ID_MASK	= 0xFFFF,

	OCRDMA_QP_PARA_QPS_VALID	= Bit(0),
	OCRDMA_QP_PARA_SQD_ASYNC_VALID	= Bit(1),
	OCRDMA_QP_PARA_PKEY_VALID	= Bit(2),
	OCRDMA_QP_PARA_QKEY_VALID	= Bit(3),
	OCRDMA_QP_PARA_PMTU_VALID	= Bit(4),
	OCRDMA_QP_PARA_ACK_TO_VALID	= Bit(5),
	OCRDMA_QP_PARA_RETRY_CNT_VALID	= Bit(6),
	OCRDMA_QP_PARA_RRC_VALID	= Bit(7),
	OCRDMA_QP_PARA_RQPSN_VALID	= Bit(8),
	OCRDMA_QP_PARA_MAX_IRD_VALID	= Bit(9),
	OCRDMA_QP_PARA_MAX_ORD_VALID	= Bit(10),
	OCRDMA_QP_PARA_RNT_VALID	= Bit(11),
	OCRDMA_QP_PARA_SQPSN_VALID	= Bit(12),
	OCRDMA_QP_PARA_DST_QPN_VALID	= Bit(13),
	OCRDMA_QP_PARA_MAX_WQE_VALID	= Bit(14),
	OCRDMA_QP_PARA_MAX_RQE_VALID	= Bit(15),
	OCRDMA_QP_PARA_SGE_SEND_VALID	= Bit(16),
	OCRDMA_QP_PARA_SGE_RECV_VALID	= Bit(17),
	OCRDMA_QP_PARA_SGE_WR_VALID	= Bit(18),
	OCRDMA_QP_PARA_INB_RDEN_VALID	= Bit(19),
	OCRDMA_QP_PARA_INB_WREN_VALID	= Bit(20),
	OCRDMA_QP_PARA_FLOW_LBL_VALID	= Bit(21),
	OCRDMA_QP_PARA_BIND_EN_VALID	= Bit(22),
	OCRDMA_QP_PARA_ZLKEY_EN_VALID	= Bit(23),
	OCRDMA_QP_PARA_FMR_EN_VALID	= Bit(24),
	OCRDMA_QP_PARA_INBAT_EN_VALID	= Bit(25),
	OCRDMA_QP_PARA_VLAN_EN_VALID	= Bit(26),

	OCRDMA_MODIFY_QP_FLAGS_RD	= Bit(0),
	OCRDMA_MODIFY_QP_FLAGS_WR	= Bit(1),
	OCRDMA_MODIFY_QP_FLAGS_SEND	= Bit(2),
	OCRDMA_MODIFY_QP_FLAGS_ATOMIC	= Bit(3)
};

enum {
	OCRDMA_QP_PARAMS_SRQ_ID_SHIFT		= 0,
	OCRDMA_QP_PARAMS_SRQ_ID_MASK		= 0xFFFF,

	OCRDMA_QP_PARAMS_MAX_RQE_SHIFT		= 0,
	OCRDMA_QP_PARAMS_MAX_RQE_MASK		= 0xFFFF,
	OCRDMA_QP_PARAMS_MAX_WQE_SHIFT		= 16,
	OCRDMA_QP_PARAMS_MAX_WQE_MASK		= 0xFFFF <<
	    OCRDMA_QP_PARAMS_MAX_WQE_SHIFT,

	OCRDMA_QP_PARAMS_MAX_SGE_WRITE_SHIFT	= 0,
	OCRDMA_QP_PARAMS_MAX_SGE_WRITE_MASK	= 0xFFFF,
	OCRDMA_QP_PARAMS_MAX_SGE_SEND_SHIFT	= 16,
	OCRDMA_QP_PARAMS_MAX_SGE_SEND_MASK	= 0xFFFF <<
					OCRDMA_QP_PARAMS_MAX_SGE_SEND_SHIFT,

	OCRDMA_QP_PARAMS_FLAGS_FMR_EN		= Bit(0),
	OCRDMA_QP_PARAMS_FLAGS_LKEY_0_EN	= Bit(1),
	OCRDMA_QP_PARAMS_FLAGS_BIND_MW_EN	= Bit(2),
	OCRDMA_QP_PARAMS_FLAGS_INBWR_EN		= Bit(3),
	OCRDMA_QP_PARAMS_FLAGS_INBRD_EN		= Bit(4),
	OCRDMA_QP_PARAMS_STATE_SHIFT		= 5,
	OCRDMA_QP_PARAMS_STATE_MASK		= Bit(5) | Bit(6) | Bit(7),
	OCRDMA_QP_PARAMS_FLAGS_SQD_ASYNC	= Bit(8),
	OCRDMA_QP_PARAMS_FLAGS_INB_ATEN		= Bit(9),
	OCRDMA_QP_PARAMS_MAX_SGE_RECV_SHIFT	= 16,
	OCRDMA_QP_PARAMS_MAX_SGE_RECV_MASK	= 0xFFFF <<
					OCRDMA_QP_PARAMS_MAX_SGE_RECV_SHIFT,

	OCRDMA_QP_PARAMS_MAX_IRD_SHIFT		= 0,
	OCRDMA_QP_PARAMS_MAX_IRD_MASK		= 0xFFFF,
	OCRDMA_QP_PARAMS_MAX_ORD_SHIFT		= 16,
	OCRDMA_QP_PARAMS_MAX_ORD_MASK		= 0xFFFF <<
					OCRDMA_QP_PARAMS_MAX_ORD_SHIFT,

	OCRDMA_QP_PARAMS_RQ_CQID_SHIFT		= 0,
	OCRDMA_QP_PARAMS_RQ_CQID_MASK		= 0xFFFF,
	OCRDMA_QP_PARAMS_WQ_CQID_SHIFT		= 16,
	OCRDMA_QP_PARAMS_WQ_CQID_MASK		= 0xFFFF <<
					OCRDMA_QP_PARAMS_WQ_CQID_SHIFT,

	OCRDMA_QP_PARAMS_RQ_PSN_SHIFT		= 0,
	OCRDMA_QP_PARAMS_RQ_PSN_MASK		= 0xFFFFFF,
	OCRDMA_QP_PARAMS_HOP_LMT_SHIFT		= 24,
	OCRDMA_QP_PARAMS_HOP_LMT_MASK		= 0xFF <<
					OCRDMA_QP_PARAMS_HOP_LMT_SHIFT,

	OCRDMA_QP_PARAMS_SQ_PSN_SHIFT		= 0,
	OCRDMA_QP_PARAMS_SQ_PSN_MASK		= 0xFFFFFF,
	OCRDMA_QP_PARAMS_TCLASS_SHIFT		= 24,
	OCRDMA_QP_PARAMS_TCLASS_MASK		= 0xFF <<
					OCRDMA_QP_PARAMS_TCLASS_SHIFT,

	OCRDMA_QP_PARAMS_DEST_QPN_SHIFT		= 0,
	OCRDMA_QP_PARAMS_DEST_QPN_MASK		= 0xFFFFFF,
	OCRDMA_QP_PARAMS_RNR_RETRY_CNT_SHIFT	= 24,
	OCRDMA_QP_PARAMS_RNR_RETRY_CNT_MASK	= 0x7 <<
					OCRDMA_QP_PARAMS_RNR_RETRY_CNT_SHIFT,
	OCRDMA_QP_PARAMS_ACK_TIMEOUT_SHIFT	= 27,
	OCRDMA_QP_PARAMS_ACK_TIMEOUT_MASK	= 0x1F <<
					OCRDMA_QP_PARAMS_ACK_TIMEOUT_SHIFT,

	OCRDMA_QP_PARAMS_PKEY_IDNEX_SHIFT	= 0,
	OCRDMA_QP_PARAMS_PKEY_INDEX_MASK	= 0xFFFF,
	OCRDMA_QP_PARAMS_PATH_MTU_SHIFT		= 18,
	OCRDMA_QP_PARAMS_PATH_MTU_MASK		= 0x3FFF <<
					OCRDMA_QP_PARAMS_PATH_MTU_SHIFT,

	OCRDMA_QP_PARAMS_FLOW_LABEL_SHIFT	= 0,
	OCRDMA_QP_PARAMS_FLOW_LABEL_MASK	= 0xFFFFF,
	OCRDMA_QP_PARAMS_SL_SHIFT		= 20,
	OCRDMA_QP_PARAMS_SL_MASK		= 0xF <<
					OCRDMA_QP_PARAMS_SL_SHIFT,
	OCRDMA_QP_PARAMS_RETRY_CNT_SHIFT	= 24,
	OCRDMA_QP_PARAMS_RETRY_CNT_MASK		= 0x7 <<
					OCRDMA_QP_PARAMS_RETRY_CNT_SHIFT,
	OCRDMA_QP_PARAMS_RNR_NAK_TIMER_SHIFT	= 27,
	OCRDMA_QP_PARAMS_RNR_NAK_TIMER_MASK	= 0x1F <<
					OCRDMA_QP_PARAMS_RNR_NAK_TIMER_SHIFT,

	OCRDMA_QP_PARAMS_DMAC_B4_TO_B5_SHIFT	= 0,
	OCRDMA_QP_PARAMS_DMAC_B4_TO_B5_MASK	= 0xFFFF,
	OCRDMA_QP_PARAMS_VLAN_SHIFT		= 16,
	OCRDMA_QP_PARAMS_VLAN_MASK		= 0xFFFF <<
					OCRDMA_QP_PARAMS_VLAN_SHIFT
};

struct ocrdma_qp_params {
	u32 id;
	u32 max_wqe_rqe;
	u32 max_sge_send_write;
	u32 max_sge_recv_flags;
	u32 max_ord_ird;
	u32 wq_rq_cqid;
	u32 hop_lmt_rq_psn;
	u32 tclass_sq_psn;
	u32 ack_to_rnr_rtc_dest_qpn;
	u32 path_mtu_pkey_indx;
	u32 rnt_rc_sl_fl;
	u8 sgid[16];
	u8 dgid[16];
	u32 dmac_b0_to_b3;
	u32 vlan_dmac_b4_to_b5;
	u32 qkey;
} __packed;


struct ocrdma_modify_qp {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_hdr req;

	struct ocrdma_qp_params params;
	u32 flags;
	u32 rdma_flags;
	u32 num_outstanding_atomic_rd;
} __packed;

enum {
	OCRDMA_MODIFY_QP_RSP_MAX_RQE_SHIFT	= 0,
	OCRDMA_MODIFY_QP_RSP_MAX_RQE_MASK	= 0xFFFF,
	OCRDMA_MODIFY_QP_RSP_MAX_WQE_SHIFT	= 16,
	OCRDMA_MODIFY_QP_RSP_MAX_WQE_MASK	= 0xFFFF <<
					OCRDMA_MODIFY_QP_RSP_MAX_WQE_SHIFT,

	OCRDMA_MODIFY_QP_RSP_MAX_IRD_SHIFT	= 0,
	OCRDMA_MODIFY_QP_RSP_MAX_IRD_MASK	= 0xFFFF,
	OCRDMA_MODIFY_QP_RSP_MAX_ORD_SHIFT	= 16,
	OCRDMA_MODIFY_QP_RSP_MAX_ORD_MASK	= 0xFFFF <<
					OCRDMA_MODIFY_QP_RSP_MAX_ORD_SHIFT
};
struct ocrdma_modify_qp_rsp {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_rsp rsp;

	u32 max_wqe_rqe;
	u32 max_ord_ird;
} __packed;

struct ocrdma_query_qp {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_hdr req;

#define OCRDMA_QUERY_UP_QP_ID_SHIFT 0
#define OCRDMA_QUERY_UP_QP_ID_MASK   0xFFFFFF
	u32 qp_id;
} __packed;

struct ocrdma_query_qp_rsp {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_rsp rsp;
	struct ocrdma_qp_params params;
} __packed;

enum {
	OCRDMA_CREATE_SRQ_PD_ID_SHIFT		= 0,
	OCRDMA_CREATE_SRQ_PD_ID_MASK		= 0xFFFF,
	OCRDMA_CREATE_SRQ_PG_SZ_SHIFT		= 16,
	OCRDMA_CREATE_SRQ_PG_SZ_MASK		= 0x3 <<
					OCRDMA_CREATE_SRQ_PG_SZ_SHIFT,

	OCRDMA_CREATE_SRQ_MAX_RQE_SHIFT		= 0,
	OCRDMA_CREATE_SRQ_MAX_SGE_RECV_SHIFT	= 16,
	OCRDMA_CREATE_SRQ_MAX_SGE_RECV_MASK	= 0xFFFF <<
					OCRDMA_CREATE_SRQ_MAX_SGE_RECV_SHIFT,

	OCRDMA_CREATE_SRQ_RQE_SIZE_SHIFT	= 0,
	OCRDMA_CREATE_SRQ_RQE_SIZE_MASK		= 0xFFFF,
	OCRDMA_CREATE_SRQ_NUM_RQ_PAGES_SHIFT	= 16,
	OCRDMA_CREATE_SRQ_NUM_RQ_PAGES_MASK	= 0xFFFF <<
					OCRDMA_CREATE_SRQ_NUM_RQ_PAGES_SHIFT
};

struct ocrdma_create_srq {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_hdr req;

	u32 pgsz_pdid;
	u32 max_sge_rqe;
	u32 pages_rqe_sz;
	struct ocrdma_pa rq_addr[MAX_OCRDMA_SRQ_PAGES];
} __packed;

enum {
	OCRDMA_CREATE_SRQ_RSP_SRQ_ID_SHIFT			= 0,
	OCRDMA_CREATE_SRQ_RSP_SRQ_ID_MASK			= 0xFFFFFF,

	OCRDMA_CREATE_SRQ_RSP_MAX_RQE_ALLOCATED_SHIFT		= 0,
	OCRDMA_CREATE_SRQ_RSP_MAX_RQE_ALLOCATED_MASK		= 0xFFFF,
	OCRDMA_CREATE_SRQ_RSP_MAX_SGE_RECV_ALLOCATED_SHIFT	= 16,
	OCRDMA_CREATE_SRQ_RSP_MAX_SGE_RECV_ALLOCATED_MASK	= 0xFFFF <<
			OCRDMA_CREATE_SRQ_RSP_MAX_SGE_RECV_ALLOCATED_SHIFT
};

struct ocrdma_create_srq_rsp {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_rsp rsp;

	u32 id;
	u32 max_sge_rqe_allocated;
} __packed;

enum {
	OCRDMA_MODIFY_SRQ_ID_SHIFT	= 0,
	OCRDMA_MODIFY_SRQ_ID_MASK	= 0xFFFFFF,

	OCRDMA_MODIFY_SRQ_MAX_RQE_SHIFT	= 0,
	OCRDMA_MODIFY_SRQ_MAX_RQE_MASK	= 0xFFFF,
	OCRDMA_MODIFY_SRQ_LIMIT_SHIFT	= 16,
	OCRDMA_MODIFY_SRQ__LIMIT_MASK	= 0xFFFF <<
					OCRDMA_MODIFY_SRQ_LIMIT_SHIFT
};

struct ocrdma_modify_srq {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_rsp rep;

	u32 id;
	u32 limit_max_rqe;
} __packed;

enum {
	OCRDMA_QUERY_SRQ_ID_SHIFT	= 0,
	OCRDMA_QUERY_SRQ_ID_MASK	= 0xFFFFFF
};

struct ocrdma_query_srq {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_rsp req;

	u32 id;
} __packed;

enum {
	OCRDMA_QUERY_SRQ_RSP_PD_ID_SHIFT	= 0,
	OCRDMA_QUERY_SRQ_RSP_PD_ID_MASK		= 0xFFFF,
	OCRDMA_QUERY_SRQ_RSP_MAX_RQE_SHIFT	= 16,
	OCRDMA_QUERY_SRQ_RSP_MAX_RQE_MASK	= 0xFFFF <<
					OCRDMA_QUERY_SRQ_RSP_MAX_RQE_SHIFT,

	OCRDMA_QUERY_SRQ_RSP_MAX_SGE_RECV_SHIFT	= 0,
	OCRDMA_QUERY_SRQ_RSP_MAX_SGE_RECV_MASK	= 0xFFFF,
	OCRDMA_QUERY_SRQ_RSP_SRQ_LIMIT_SHIFT	= 16,
	OCRDMA_QUERY_SRQ_RSP_SRQ_LIMIT_MASK	= 0xFFFF <<
					OCRDMA_QUERY_SRQ_RSP_SRQ_LIMIT_SHIFT
};

struct ocrdma_query_srq_rsp {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_rsp req;

	u32 max_rqe_pdid;
	u32 srq_lmt_max_sge;
} __packed;

enum {
	OCRDMA_DESTROY_SRQ_ID_SHIFT	= 0,
	OCRDMA_DESTROY_SRQ_ID_MASK	= 0xFFFFFF
};

struct ocrdma_destroy_srq {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_rsp req;

	u32 id;
} __packed;

enum {
	OCRDMA_ALLOC_PD_ENABLE_DPP	= BIT(16),
	OCRDMA_PD_MAX_DPP_ENABLED_QP	= 8,
	OCRDMA_DPP_PAGE_SIZE		= 4096
};

struct ocrdma_alloc_pd {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_hdr req;
	u32 enable_dpp_rsvd;
} __packed;

enum {
	OCRDMA_ALLOC_PD_RSP_DPP			= Bit(16),
	OCRDMA_ALLOC_PD_RSP_DPP_PAGE_SHIFT	= 20,
	OCRDMA_ALLOC_PD_RSP_PDID_MASK		= 0xFFFF,
};

struct ocrdma_alloc_pd_rsp {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_rsp rsp;
	u32 dpp_page_pdid;
} __packed;

struct ocrdma_dealloc_pd {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_hdr req;
	u32 id;
} __packed;

struct ocrdma_dealloc_pd_rsp {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_rsp rsp;
} __packed;

enum {
	OCRDMA_ADDR_CHECK_ENABLE	= 1,
	OCRDMA_ADDR_CHECK_DISABLE	= 0
};

enum {
	OCRDMA_ALLOC_LKEY_PD_ID_SHIFT		= 0,
	OCRDMA_ALLOC_LKEY_PD_ID_MASK		= 0xFFFF,

	OCRDMA_ALLOC_LKEY_ADDR_CHECK_SHIFT	= 0,
	OCRDMA_ALLOC_LKEY_ADDR_CHECK_MASK	= Bit(0),
	OCRDMA_ALLOC_LKEY_FMR_SHIFT		= 1,
	OCRDMA_ALLOC_LKEY_FMR_MASK		= Bit(1),
	OCRDMA_ALLOC_LKEY_REMOTE_INV_SHIFT	= 2,
	OCRDMA_ALLOC_LKEY_REMOTE_INV_MASK	= Bit(2),
	OCRDMA_ALLOC_LKEY_REMOTE_WR_SHIFT	= 3,
	OCRDMA_ALLOC_LKEY_REMOTE_WR_MASK	= Bit(3),
	OCRDMA_ALLOC_LKEY_REMOTE_RD_SHIFT	= 4,
	OCRDMA_ALLOC_LKEY_REMOTE_RD_MASK	= Bit(4),
	OCRDMA_ALLOC_LKEY_LOCAL_WR_SHIFT	= 5,
	OCRDMA_ALLOC_LKEY_LOCAL_WR_MASK		= Bit(5),
	OCRDMA_ALLOC_LKEY_REMOTE_ATOMIC_MASK	= Bit(6),
	OCRDMA_ALLOC_LKEY_REMOTE_ATOMIC_SHIFT	= 6,
	OCRDMA_ALLOC_LKEY_PBL_SIZE_SHIFT	= 16,
	OCRDMA_ALLOC_LKEY_PBL_SIZE_MASK		= 0xFFFF <<
						OCRDMA_ALLOC_LKEY_PBL_SIZE_SHIFT
};

struct ocrdma_alloc_lkey {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_hdr req;

	u32 pdid;
	u32 pbl_sz_flags;
} __packed;

struct ocrdma_alloc_lkey_rsp {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_rsp rsp;

	u32 lrkey;
	u32 num_pbl_rsvd;
} __packed;

struct ocrdma_dealloc_lkey {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_hdr req;

	u32 lkey;
	u32 rsvd_frmr;
} __packed;

struct ocrdma_dealloc_lkey_rsp {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_rsp rsp;
} __packed;

#define MAX_OCRDMA_NSMR_PBL    (u32)22
#define MAX_OCRDMA_PBL_SIZE     65536
#define MAX_OCRDMA_PBL_PER_LKEY	32767

enum {
	OCRDMA_REG_NSMR_LRKEY_INDEX_SHIFT	= 0,
	OCRDMA_REG_NSMR_LRKEY_INDEX_MASK	= 0xFFFFFF,
	OCRDMA_REG_NSMR_LRKEY_SHIFT		= 24,
	OCRDMA_REG_NSMR_LRKEY_MASK		= 0xFF <<
					OCRDMA_REG_NSMR_LRKEY_SHIFT,

	OCRDMA_REG_NSMR_PD_ID_SHIFT		= 0,
	OCRDMA_REG_NSMR_PD_ID_MASK		= 0xFFFF,
	OCRDMA_REG_NSMR_NUM_PBL_SHIFT		= 16,
	OCRDMA_REG_NSMR_NUM_PBL_MASK		= 0xFFFF <<
					OCRDMA_REG_NSMR_NUM_PBL_SHIFT,

	OCRDMA_REG_NSMR_PBE_SIZE_SHIFT		= 0,
	OCRDMA_REG_NSMR_PBE_SIZE_MASK		= 0xFFFF,
	OCRDMA_REG_NSMR_HPAGE_SIZE_SHIFT	= 16,
	OCRDMA_REG_NSMR_HPAGE_SIZE_MASK		= 0xFF <<
					OCRDMA_REG_NSMR_HPAGE_SIZE_SHIFT,
	OCRDMA_REG_NSMR_BIND_MEMWIN_SHIFT	= 24,
	OCRDMA_REG_NSMR_BIND_MEMWIN_MASK	= Bit(24),
	OCRDMA_REG_NSMR_ZB_SHIFT		= 25,
	OCRDMA_REG_NSMR_ZB_SHIFT_MASK		= Bit(25),
	OCRDMA_REG_NSMR_REMOTE_INV_SHIFT	= 26,
	OCRDMA_REG_NSMR_REMOTE_INV_MASK		= Bit(26),
	OCRDMA_REG_NSMR_REMOTE_WR_SHIFT		= 27,
	OCRDMA_REG_NSMR_REMOTE_WR_MASK		= Bit(27),
	OCRDMA_REG_NSMR_REMOTE_RD_SHIFT		= 28,
	OCRDMA_REG_NSMR_REMOTE_RD_MASK		= Bit(28),
	OCRDMA_REG_NSMR_LOCAL_WR_SHIFT		= 29,
	OCRDMA_REG_NSMR_LOCAL_WR_MASK		= Bit(29),
	OCRDMA_REG_NSMR_REMOTE_ATOMIC_SHIFT	= 30,
	OCRDMA_REG_NSMR_REMOTE_ATOMIC_MASK	= Bit(30),
	OCRDMA_REG_NSMR_LAST_SHIFT		= 31,
	OCRDMA_REG_NSMR_LAST_MASK		= Bit(31)
};

struct ocrdma_reg_nsmr {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_hdr cmd;

	u32 lrkey_key_index;
	u32 num_pbl_pdid;
	u32 flags_hpage_pbe_sz;
	u32 totlen_low;
	u32 totlen_high;
	u32 fbo_low;
	u32 fbo_high;
	u32 va_loaddr;
	u32 va_hiaddr;
	struct ocrdma_pa pbl[MAX_OCRDMA_NSMR_PBL];
} __packed;

enum {
	OCRDMA_REG_NSMR_CONT_PBL_SHIFT		= 0,
	OCRDMA_REG_NSMR_CONT_PBL_SHIFT_MASK	= 0xFFFF,
	OCRDMA_REG_NSMR_CONT_NUM_PBL_SHIFT	= 16,
	OCRDMA_REG_NSMR_CONT_NUM_PBL_MASK	= 0xFFFF <<
					OCRDMA_REG_NSMR_CONT_NUM_PBL_SHIFT,

	OCRDMA_REG_NSMR_CONT_LAST_SHIFT		= 31,
	OCRDMA_REG_NSMR_CONT_LAST_MASK		= Bit(31)
};

struct ocrdma_reg_nsmr_cont {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_hdr cmd;

	u32 lrkey;
	u32 num_pbl_offset;
	u32 last;

	struct ocrdma_pa pbl[MAX_OCRDMA_NSMR_PBL];
} __packed;

struct ocrdma_pbe {
	u32 pa_hi;
	u32 pa_lo;
} __packed;

enum {
	OCRDMA_REG_NSMR_RSP_NUM_PBL_SHIFT	= 16,
	OCRDMA_REG_NSMR_RSP_NUM_PBL_MASK	= 0xFFFF0000
};
struct ocrdma_reg_nsmr_rsp {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_rsp rsp;

	u32 lrkey;
	u32 num_pbl;
} __packed;

enum {
	OCRDMA_REG_NSMR_CONT_RSP_LRKEY_INDEX_SHIFT	= 0,
	OCRDMA_REG_NSMR_CONT_RSP_LRKEY_INDEX_MASK	= 0xFFFFFF,
	OCRDMA_REG_NSMR_CONT_RSP_LRKEY_SHIFT		= 24,
	OCRDMA_REG_NSMR_CONT_RSP_LRKEY_MASK		= 0xFF <<
					OCRDMA_REG_NSMR_CONT_RSP_LRKEY_SHIFT,

	OCRDMA_REG_NSMR_CONT_RSP_NUM_PBL_SHIFT		= 16,
	OCRDMA_REG_NSMR_CONT_RSP_NUM_PBL_MASK		= 0xFFFF <<
					OCRDMA_REG_NSMR_CONT_RSP_NUM_PBL_SHIFT
};

struct ocrdma_reg_nsmr_cont_rsp {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_rsp rsp;

	u32 lrkey_key_index;
	u32 num_pbl;
} __packed;

enum {
	OCRDMA_ALLOC_MW_PD_ID_SHIFT	= 0,
	OCRDMA_ALLOC_MW_PD_ID_MASK	= 0xFFFF
};

struct ocrdma_alloc_mw {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_hdr req;

	u32 pdid;
} __packed;

enum {
	OCRDMA_ALLOC_MW_RSP_LRKEY_INDEX_SHIFT	= 0,
	OCRDMA_ALLOC_MW_RSP_LRKEY_INDEX_MASK	= 0xFFFFFF
};

struct ocrdma_alloc_mw_rsp {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_rsp rsp;

	u32 lrkey_index;
} __packed;

struct ocrdma_attach_mcast {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_hdr req;
	u32 qp_id;
	u8 mgid[16];
	u32 mac_b0_to_b3;
	u32 vlan_mac_b4_to_b5;
} __packed;

struct ocrdma_attach_mcast_rsp {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_rsp rsp;
} __packed;

struct ocrdma_detach_mcast {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_hdr req;
	u32 qp_id;
	u8 mgid[16];
	u32 mac_b0_to_b3;
	u32 vlan_mac_b4_to_b5;
} __packed;

struct ocrdma_detach_mcast_rsp {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_rsp rsp;
} __packed;

enum {
	OCRDMA_CREATE_AH_NUM_PAGES_SHIFT	= 19,
	OCRDMA_CREATE_AH_NUM_PAGES_MASK		= 0xF <<
					OCRDMA_CREATE_AH_NUM_PAGES_SHIFT,

	OCRDMA_CREATE_AH_PAGE_SIZE_SHIFT	= 16,
	OCRDMA_CREATE_AH_PAGE_SIZE_MASK		= 0x7 <<
					OCRDMA_CREATE_AH_PAGE_SIZE_SHIFT,

	OCRDMA_CREATE_AH_ENTRY_SIZE_SHIFT	= 23,
	OCRDMA_CREATE_AH_ENTRY_SIZE_MASK	= 0x1FF <<
					OCRDMA_CREATE_AH_ENTRY_SIZE_SHIFT,
};

#define OCRDMA_AH_TBL_PAGES 8

struct ocrdma_create_ah_tbl {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_hdr req;

	u32 ah_conf;
	struct ocrdma_pa tbl_addr[8];
} __packed;

struct ocrdma_create_ah_tbl_rsp {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_rsp rsp;
	u32 ahid;
} __packed;

struct ocrdma_delete_ah_tbl {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_hdr req;
	u32 ahid;
} __packed;

struct ocrdma_delete_ah_tbl_rsp {
	struct ocrdma_mqe_hdr hdr;
	struct ocrdma_mbx_rsp rsp;
} __packed;

enum {
	OCRDMA_EQE_VALID_SHIFT		= 0,
	OCRDMA_EQE_VALID_MASK		= Bit(0),
	OCRDMA_EQE_FOR_CQE_MASK		= 0xFFFE,
	OCRDMA_EQE_RESOURCE_ID_SHIFT	= 16,
	OCRDMA_EQE_RESOURCE_ID_MASK	= 0xFFFF <<
				OCRDMA_EQE_RESOURCE_ID_SHIFT,
};

struct ocrdma_eqe {
	u32 id_valid;
} __packed;

enum OCRDMA_CQE_STATUS {
	OCRDMA_CQE_SUCCESS = 0,
	OCRDMA_CQE_LOC_LEN_ERR,
	OCRDMA_CQE_LOC_QP_OP_ERR,
	OCRDMA_CQE_LOC_EEC_OP_ERR,
	OCRDMA_CQE_LOC_PROT_ERR,
	OCRDMA_CQE_WR_FLUSH_ERR,
	OCRDMA_CQE_MW_BIND_ERR,
	OCRDMA_CQE_BAD_RESP_ERR,
	OCRDMA_CQE_LOC_ACCESS_ERR,
	OCRDMA_CQE_REM_INV_REQ_ERR,
	OCRDMA_CQE_REM_ACCESS_ERR,
	OCRDMA_CQE_REM_OP_ERR,
	OCRDMA_CQE_RETRY_EXC_ERR,
	OCRDMA_CQE_RNR_RETRY_EXC_ERR,
	OCRDMA_CQE_LOC_RDD_VIOL_ERR,
	OCRDMA_CQE_REM_INV_RD_REQ_ERR,
	OCRDMA_CQE_REM_ABORT_ERR,
	OCRDMA_CQE_INV_EECN_ERR,
	OCRDMA_CQE_INV_EEC_STATE_ERR,
	OCRDMA_CQE_FATAL_ERR,
	OCRDMA_CQE_RESP_TIMEOUT_ERR,
	OCRDMA_CQE_GENERAL_ERR
};

enum {
	/* w0 */
	OCRDMA_CQE_WQEIDX_SHIFT		= 0,
	OCRDMA_CQE_WQEIDX_MASK		= 0xFFFF,

	/* w1 */
	OCRDMA_CQE_UD_XFER_LEN_SHIFT	= 16,
	OCRDMA_CQE_PKEY_SHIFT		= 0,
	OCRDMA_CQE_PKEY_MASK		= 0xFFFF,

	/* w2 */
	OCRDMA_CQE_QPN_SHIFT		= 0,
	OCRDMA_CQE_QPN_MASK		= 0x0000FFFF,

	OCRDMA_CQE_BUFTAG_SHIFT		= 16,
	OCRDMA_CQE_BUFTAG_MASK		= 0xFFFF << OCRDMA_CQE_BUFTAG_SHIFT,

	/* w3 */
	OCRDMA_CQE_UD_STATUS_SHIFT	= 24,
	OCRDMA_CQE_UD_STATUS_MASK	= 0x7 << OCRDMA_CQE_UD_STATUS_SHIFT,
	OCRDMA_CQE_STATUS_SHIFT		= 16,
	OCRDMA_CQE_STATUS_MASK		= 0xFF << OCRDMA_CQE_STATUS_SHIFT,
	OCRDMA_CQE_VALID		= Bit(31),
	OCRDMA_CQE_INVALIDATE		= Bit(30),
	OCRDMA_CQE_QTYPE		= Bit(29),
	OCRDMA_CQE_IMM			= Bit(28),
	OCRDMA_CQE_WRITE_IMM		= Bit(27),
	OCRDMA_CQE_QTYPE_SQ		= 0,
	OCRDMA_CQE_QTYPE_RQ		= 1,
	OCRDMA_CQE_SRCQP_MASK		= 0xFFFFFF
};

struct ocrdma_cqe {
	union {
		/* w0 to w2 */
		struct {
			u32 wqeidx;
			u32 bytes_xfered;
			u32 qpn;
		} wq;
		struct {
			u32 lkey_immdt;
			u32 rxlen;
			u32 buftag_qpn;
		} rq;
		struct {
			u32 lkey_immdt;
			u32 rxlen_pkey;
			u32 buftag_qpn;
		} ud;
		struct {
			u32 word_0;
			u32 word_1;
			u32 qpn;
		} cmn;
	};
	u32 flags_status_srcqpn;	/* w3 */
} __packed;

struct ocrdma_sge {
	u32 addr_hi;
	u32 addr_lo;
	u32 lrkey;
	u32 len;
} __packed;

enum {
	OCRDMA_FLAG_SIG		= 0x1,
	OCRDMA_FLAG_INV		= 0x2,
	OCRDMA_FLAG_FENCE_L	= 0x4,
	OCRDMA_FLAG_FENCE_R	= 0x8,
	OCRDMA_FLAG_SOLICIT	= 0x10,
	OCRDMA_FLAG_IMM		= 0x20,

	/* Stag flags */
	OCRDMA_LKEY_FLAG_LOCAL_WR	= 0x1,
	OCRDMA_LKEY_FLAG_REMOTE_RD	= 0x2,
	OCRDMA_LKEY_FLAG_REMOTE_WR	= 0x4,
	OCRDMA_LKEY_FLAG_VATO		= 0x8,
};

enum OCRDMA_WQE_OPCODE {
	OCRDMA_WRITE		= 0x06,
	OCRDMA_READ		= 0x0C,
	OCRDMA_RESV0		= 0x02,
	OCRDMA_SEND		= 0x00,
	OCRDMA_CMP_SWP		= 0x14,
	OCRDMA_BIND_MW		= 0x10,
	OCRDMA_RESV1		= 0x0A,
	OCRDMA_LKEY_INV		= 0x15,
	OCRDMA_FETCH_ADD	= 0x13,
	OCRDMA_POST_RQ		= 0x12
};

enum {
	OCRDMA_TYPE_INLINE	= 0x0,
	OCRDMA_TYPE_LKEY	= 0x1,
};

enum {
	OCRDMA_WQE_OPCODE_SHIFT		= 0,
	OCRDMA_WQE_OPCODE_MASK		= 0x0000001F,
	OCRDMA_WQE_FLAGS_SHIFT		= 5,
	OCRDMA_WQE_TYPE_SHIFT		= 16,
	OCRDMA_WQE_TYPE_MASK		= 0x00030000,
	OCRDMA_WQE_SIZE_SHIFT		= 18,
	OCRDMA_WQE_SIZE_MASK		= 0xFF,
	OCRDMA_WQE_NXT_WQE_SIZE_SHIFT	= 25,

	OCRDMA_WQE_LKEY_FLAGS_SHIFT	= 0,
	OCRDMA_WQE_LKEY_FLAGS_MASK	= 0xF
};

/* header WQE for all the SQ and RQ operations */
struct ocrdma_hdr_wqe {
	u32 cw;
	union {
		u32 rsvd_tag;
		u32 rsvd_lkey_flags;
	};
	union {
		u32 immdt;
		u32 lkey;
	};
	u32 total_len;
} __packed;

struct ocrdma_ewqe_ud_hdr {
	u32 rsvd_dest_qpn;
	u32 qkey;
	u32 rsvd_ahid;
	u32 rsvd;
} __packed;

struct ocrdma_eth_basic {
	u8 dmac[6];
	u8 smac[6];
	__be16 eth_type;
} __packed;

struct ocrdma_eth_vlan {
	u8 dmac[6];
	u8 smac[6];
	__be16 eth_type;
	__be16 vlan_tag;
#define OCRDMA_ROCE_ETH_TYPE 0x8915
	__be16 roce_eth_type;
} __packed;

struct ocrdma_grh {
	__be32	tclass_flow;
	__be32	pdid_hoplimit;
	u8	sgid[16];
	u8	dgid[16];
	u16	rsvd;
} __packed;

#define OCRDMA_AV_VALID		Bit(0)
#define OCRDMA_AV_VLAN_VALID	Bit(1)

struct ocrdma_av {
	struct ocrdma_eth_vlan eth_hdr;
	struct ocrdma_grh grh;
	u32 valid;
} __packed;

#endif				/* __OCRDMA_SLI_H__ */
