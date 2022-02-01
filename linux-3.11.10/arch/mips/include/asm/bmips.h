/*
 * This file is subject to the terms and conditions of the GNU General Public
 * License.  See the file "COPYING" in the main directory of this archive
 * for more details.
 *
 * Copyright (C) 2011 by Kevin Cernekee (cernekee@gmail.com)
 *
 * Definitions for BMIPS processors
 */
#ifndef _ASM_BMIPS_H
#define _ASM_BMIPS_H

#include <linux/compiler.h>
#include <linux/linkage.h>
#include <asm/addrspace.h>
#include <asm/mipsregs.h>
#include <asm/hazards.h>

/* NOTE: the CBR register returns a PA, and it can be above 0xff00_0000 */
#define BMIPS_GET_CBR()			((void __iomem *)(CKSEG1 | \
					 (unsigned long) \
					 ((read_c0_brcm_cbr() >> 18) << 18)))

#define BMIPS_RAC_CONFIG		0x00000000
#define BMIPS_RAC_ADDRESS_RANGE		0x00000004
#define BMIPS_RAC_CONFIG_1		0x00000008
#define BMIPS_L2_CONFIG			0x0000000c
#define BMIPS_LMB_CONTROL		0x0000001c
#define BMIPS_SYSTEM_BASE		0x00000020
#define BMIPS_PERF_GLOBAL_CONTROL	0x00020000
#define BMIPS_PERF_CONTROL_0		0x00020004
#define BMIPS_PERF_CONTROL_1		0x00020008
#define BMIPS_PERF_COUNTER_0		0x00020010
#define BMIPS_PERF_COUNTER_1		0x00020014
#define BMIPS_PERF_COUNTER_2		0x00020018
#define BMIPS_PERF_COUNTER_3		0x0002001c
#define BMIPS_RELO_VECTOR_CONTROL_0	0x00030000
#define BMIPS_RELO_VECTOR_CONTROL_1	0x00038000

#define BMIPS_NMI_RESET_VEC		0x80000000
#define BMIPS_WARM_RESTART_VEC		0x80000380

#define ZSCM_REG_BASE			0x97000000

#if !defined(__ASSEMBLY__)

#include <linux/cpumask.h>
#include <asm/r4kcache.h>

extern struct plat_smp_ops bmips_smp_ops;
extern char bmips_reset_nmi_vec;
extern char bmips_reset_nmi_vec_end;
extern char bmips_smp_movevec;
extern char bmips_smp_int_vec;
extern char bmips_smp_int_vec_end;

extern int bmips_smp_enabled;
extern int bmips_cpu_offset;
extern cpumask_t bmips_booted_mask;

extern void bmips_ebase_setup(void);
extern asmlinkage void plat_wired_tlb_setup(void);

static inline unsigned long bmips_read_zscm_reg(unsigned int offset)
{
	unsigned long ret;

	__asm__ __volatile__(
		".set push\n"
		".set noreorder\n"
		"cache %1, 0(%2)\n"
		"sync\n"
		"_ssnop\n"
		"_ssnop\n"
		"_ssnop\n"
		"_ssnop\n"
		"_ssnop\n"
		"_ssnop\n"
		"_ssnop\n"
		"mfc0 %0, $28, 3\n"
		"_ssnop\n"
		".set pop\n"
		: "=&r" (ret)
		: "i" (Index_Load_Tag_S), "r" (ZSCM_REG_BASE + offset)
		: "memory");
	return ret;
}

static inline void bmips_write_zscm_reg(unsigned int offset, unsigned long data)
{
	__asm__ __volatile__(
		".set push\n"
		".set noreorder\n"
		"mtc0 %0, $28, 3\n"
		"_ssnop\n"
		"_ssnop\n"
		"_ssnop\n"
		"cache %1, 0(%2)\n"
		"_ssnop\n"
		"_ssnop\n"
		"_ssnop\n"
		: /* no outputs */
		: "r" (data),
		  "i" (Index_Store_Tag_S), "r" (ZSCM_REG_BASE + offset)
		: "memory");
}

#endif /* !defined(__ASSEMBLY__) */

#endif /* _ASM_BMIPS_H */
