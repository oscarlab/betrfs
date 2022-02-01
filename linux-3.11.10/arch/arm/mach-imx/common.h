/*
 * Copyright 2004-2013 Freescale Semiconductor, Inc. All Rights Reserved.
 */

/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

#ifndef __ASM_ARCH_MXC_COMMON_H__
#define __ASM_ARCH_MXC_COMMON_H__

#include <linux/reboot.h>

struct platform_device;
struct pt_regs;
struct clk;
enum mxc_cpu_pwr_mode;

extern void mx1_map_io(void);
extern void mx21_map_io(void);
extern void mx25_map_io(void);
extern void mx27_map_io(void);
extern void mx31_map_io(void);
extern void mx35_map_io(void);
extern void mx51_map_io(void);
extern void mx53_map_io(void);
extern void imx1_init_early(void);
extern void imx21_init_early(void);
extern void imx25_init_early(void);
extern void imx27_init_early(void);
extern void imx31_init_early(void);
extern void imx35_init_early(void);
extern void imx51_init_early(void);
extern void imx53_init_early(void);
extern void mxc_init_irq(void __iomem *);
extern void tzic_init_irq(void __iomem *);
extern void mx1_init_irq(void);
extern void mx21_init_irq(void);
extern void mx25_init_irq(void);
extern void mx27_init_irq(void);
extern void mx31_init_irq(void);
extern void mx35_init_irq(void);
extern void mx51_init_irq(void);
extern void mx53_init_irq(void);
extern void imx1_soc_init(void);
extern void imx21_soc_init(void);
extern void imx25_soc_init(void);
extern void imx27_soc_init(void);
extern void imx31_soc_init(void);
extern void imx35_soc_init(void);
extern void imx51_soc_init(void);
extern void imx51_init_late(void);
extern void imx53_init_late(void);
extern void epit_timer_init(void __iomem *base, int irq);
extern void mxc_timer_init(void __iomem *, int);
extern int mx1_clocks_init(unsigned long fref);
extern int mx21_clocks_init(unsigned long lref, unsigned long fref);
extern int mx25_clocks_init(void);
extern int mx27_clocks_init(unsigned long fref);
extern int mx31_clocks_init(unsigned long fref);
extern int mx35_clocks_init(void);
extern int mx51_clocks_init(unsigned long ckil, unsigned long osc,
			unsigned long ckih1, unsigned long ckih2);
extern int mx53_clocks_init(unsigned long ckil, unsigned long osc,
			unsigned long ckih1, unsigned long ckih2);
extern int mx25_clocks_init_dt(void);
extern int mx27_clocks_init_dt(void);
extern int mx31_clocks_init_dt(void);
extern int mx51_clocks_init_dt(void);
extern int mx53_clocks_init_dt(void);
extern struct platform_device *mxc_register_gpio(char *name, int id,
	resource_size_t iobase, resource_size_t iosize, int irq, int irq_high);
extern void mxc_set_cpu_type(unsigned int type);
extern void mxc_restart(enum reboot_mode, const char *);
extern void mxc_arch_reset_init(void __iomem *);
extern void mxc_arch_reset_init_dt(void);
extern int mx53_revision(void);
extern int imx6q_revision(void);
extern int mx53_display_revision(void);
extern void imx_set_aips(void __iomem *);
extern int mxc_device_init(void);

enum mxc_cpu_pwr_mode {
	WAIT_CLOCKED,		/* wfi only */
	WAIT_UNCLOCKED,		/* WAIT */
	WAIT_UNCLOCKED_POWER_OFF,	/* WAIT + SRPG */
	STOP_POWER_ON,		/* just STOP */
	STOP_POWER_OFF,		/* STOP + SRPG */
};

enum mx3_cpu_pwr_mode {
	MX3_RUN,
	MX3_WAIT,
	MX3_DOZE,
	MX3_SLEEP,
};

extern void mx3_cpu_lp_set(enum mx3_cpu_pwr_mode mode);
extern void imx_print_silicon_rev(const char *cpu, int srev);

void avic_handle_irq(struct pt_regs *);
void tzic_handle_irq(struct pt_regs *);

#define imx1_handle_irq avic_handle_irq
#define imx21_handle_irq avic_handle_irq
#define imx25_handle_irq avic_handle_irq
#define imx27_handle_irq avic_handle_irq
#define imx31_handle_irq avic_handle_irq
#define imx35_handle_irq avic_handle_irq
#define imx51_handle_irq tzic_handle_irq
#define imx53_handle_irq tzic_handle_irq

extern void imx_enable_cpu(int cpu, bool enable);
extern void imx_set_cpu_jump(int cpu, void *jump_addr);
extern u32 imx_get_cpu_arg(int cpu);
extern void imx_set_cpu_arg(int cpu, u32 arg);
extern void v7_cpu_resume(void);
#ifdef CONFIG_SMP
extern void v7_secondary_startup(void);
extern void imx_scu_map_io(void);
extern void imx_smp_prepare(void);
extern void imx_scu_standby_enable(void);
#else
static inline void imx_scu_map_io(void) {}
static inline void imx_smp_prepare(void) {}
static inline void imx_scu_standby_enable(void) {}
#endif
extern void imx_src_init(void);
extern void imx_src_prepare_restart(void);
extern void imx_gpc_init(void);
extern void imx_gpc_pre_suspend(void);
extern void imx_gpc_post_resume(void);
extern void imx_gpc_mask_all(void);
extern void imx_gpc_restore_all(void);
extern void imx_anatop_init(void);
extern void imx_anatop_pre_suspend(void);
extern void imx_anatop_post_resume(void);
extern void imx_anatop_usb_chrg_detect_disable(void);
extern u32 imx_anatop_get_digprog(void);
extern int imx6q_set_lpm(enum mxc_cpu_pwr_mode mode);
extern void imx6q_set_chicken_bit(void);

extern void imx_cpu_die(unsigned int cpu);
extern int imx_cpu_kill(unsigned int cpu);

#ifdef CONFIG_PM
extern void imx6q_pm_init(void);
extern void imx51_pm_init(void);
extern void imx53_pm_init(void);
#else
static inline void imx6q_pm_init(void) {}
static inline void imx51_pm_init(void) {}
static inline void imx53_pm_init(void) {}
#endif

#ifdef CONFIG_NEON
extern int mx51_neon_fixup(void);
#else
static inline int mx51_neon_fixup(void) { return 0; }
#endif

extern struct smp_operations imx_smp_ops;

#endif
