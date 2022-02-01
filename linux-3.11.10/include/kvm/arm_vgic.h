/*
 * Copyright (C) 2012 ARM Ltd.
 * Author: Marc Zyngier <marc.zyngier@arm.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

#ifndef __ASM_ARM_KVM_VGIC_H
#define __ASM_ARM_KVM_VGIC_H

#include <linux/kernel.h>
#include <linux/kvm.h>
#include <linux/irqreturn.h>
#include <linux/spinlock.h>
#include <linux/types.h>
#include <linux/irqchip/arm-gic.h>

#define VGIC_NR_IRQS		128
#define VGIC_NR_SGIS		16
#define VGIC_NR_PPIS		16
#define VGIC_NR_PRIVATE_IRQS	(VGIC_NR_SGIS + VGIC_NR_PPIS)
#define VGIC_NR_SHARED_IRQS	(VGIC_NR_IRQS - VGIC_NR_PRIVATE_IRQS)
#define VGIC_MAX_CPUS		KVM_MAX_VCPUS
#define VGIC_MAX_LRS		(1 << 6)

/* Sanity checks... */
#if (VGIC_MAX_CPUS > 8)
#error	Invalid number of CPU interfaces
#endif

#if (VGIC_NR_IRQS & 31)
#error "VGIC_NR_IRQS must be a multiple of 32"
#endif

#if (VGIC_NR_IRQS > 1024)
#error "VGIC_NR_IRQS must be <= 1024"
#endif

/*
 * The GIC distributor registers describing interrupts have two parts:
 * - 32 per-CPU interrupts (SGI + PPI)
 * - a bunch of shared interrupts (SPI)
 */
struct vgic_bitmap {
	union {
		u32 reg[VGIC_NR_PRIVATE_IRQS / 32];
		DECLARE_BITMAP(reg_ul, VGIC_NR_PRIVATE_IRQS);
	} percpu[VGIC_MAX_CPUS];
	union {
		u32 reg[VGIC_NR_SHARED_IRQS / 32];
		DECLARE_BITMAP(reg_ul, VGIC_NR_SHARED_IRQS);
	} shared;
};

struct vgic_bytemap {
	u32 percpu[VGIC_MAX_CPUS][VGIC_NR_PRIVATE_IRQS / 4];
	u32 shared[VGIC_NR_SHARED_IRQS  / 4];
};

struct vgic_dist {
#ifdef CONFIG_KVM_ARM_VGIC
	spinlock_t		lock;
	bool			ready;

	/* Virtual control interface mapping */
	void __iomem		*vctrl_base;

	/* Distributor and vcpu interface mapping in the guest */
	phys_addr_t		vgic_dist_base;
	phys_addr_t		vgic_cpu_base;

	/* Distributor enabled */
	u32			enabled;

	/* Interrupt enabled (one bit per IRQ) */
	struct vgic_bitmap	irq_enabled;

	/* Interrupt 'pin' level */
	struct vgic_bitmap	irq_state;

	/* Level-triggered interrupt in progress */
	struct vgic_bitmap	irq_active;

	/* Interrupt priority. Not used yet. */
	struct vgic_bytemap	irq_priority;

	/* Level/edge triggered */
	struct vgic_bitmap	irq_cfg;

	/* Source CPU per SGI and target CPU */
	u8			irq_sgi_sources[VGIC_MAX_CPUS][VGIC_NR_SGIS];

	/* Target CPU for each IRQ */
	u8			irq_spi_cpu[VGIC_NR_SHARED_IRQS];
	struct vgic_bitmap	irq_spi_target[VGIC_MAX_CPUS];

	/* Bitmap indicating which CPU has something pending */
	unsigned long		irq_pending_on_cpu;
#endif
};

struct vgic_cpu {
#ifdef CONFIG_KVM_ARM_VGIC
	/* per IRQ to LR mapping */
	u8		vgic_irq_lr_map[VGIC_NR_IRQS];

	/* Pending interrupts on this VCPU */
	DECLARE_BITMAP(	pending_percpu, VGIC_NR_PRIVATE_IRQS);
	DECLARE_BITMAP(	pending_shared, VGIC_NR_SHARED_IRQS);

	/* Bitmap of used/free list registers */
	DECLARE_BITMAP(	lr_used, VGIC_MAX_LRS);

	/* Number of list registers on this CPU */
	int		nr_lr;

	/* CPU vif control registers for world switch */
	u32		vgic_hcr;
	u32		vgic_vmcr;
	u32		vgic_misr;	/* Saved only */
	u32		vgic_eisr[2];	/* Saved only */
	u32		vgic_elrsr[2];	/* Saved only */
	u32		vgic_apr;
	u32		vgic_lr[VGIC_MAX_LRS];
#endif
};

#define LR_EMPTY	0xff

struct kvm;
struct kvm_vcpu;
struct kvm_run;
struct kvm_exit_mmio;

#ifdef CONFIG_KVM_ARM_VGIC
int kvm_vgic_set_addr(struct kvm *kvm, unsigned long type, u64 addr);
int kvm_vgic_hyp_init(void);
int kvm_vgic_init(struct kvm *kvm);
int kvm_vgic_create(struct kvm *kvm);
int kvm_vgic_vcpu_init(struct kvm_vcpu *vcpu);
void kvm_vgic_flush_hwstate(struct kvm_vcpu *vcpu);
void kvm_vgic_sync_hwstate(struct kvm_vcpu *vcpu);
int kvm_vgic_inject_irq(struct kvm *kvm, int cpuid, unsigned int irq_num,
			bool level);
int kvm_vgic_vcpu_pending_irq(struct kvm_vcpu *vcpu);
bool vgic_handle_mmio(struct kvm_vcpu *vcpu, struct kvm_run *run,
		      struct kvm_exit_mmio *mmio);

#define irqchip_in_kernel(k)	(!!((k)->arch.vgic.vctrl_base))
#define vgic_initialized(k)	((k)->arch.vgic.ready)

#else
static inline int kvm_vgic_hyp_init(void)
{
	return 0;
}

static inline int kvm_vgic_set_addr(struct kvm *kvm, unsigned long type, u64 addr)
{
	return 0;
}

static inline int kvm_vgic_init(struct kvm *kvm)
{
	return 0;
}

static inline int kvm_vgic_create(struct kvm *kvm)
{
	return 0;
}

static inline int kvm_vgic_vcpu_init(struct kvm_vcpu *vcpu)
{
	return 0;
}

static inline void kvm_vgic_flush_hwstate(struct kvm_vcpu *vcpu) {}
static inline void kvm_vgic_sync_hwstate(struct kvm_vcpu *vcpu) {}

static inline int kvm_vgic_inject_irq(struct kvm *kvm, int cpuid,
				      unsigned int irq_num, bool level)
{
	return 0;
}

static inline int kvm_vgic_vcpu_pending_irq(struct kvm_vcpu *vcpu)
{
	return 0;
}

static inline bool vgic_handle_mmio(struct kvm_vcpu *vcpu, struct kvm_run *run,
				    struct kvm_exit_mmio *mmio)
{
	return false;
}

static inline int irqchip_in_kernel(struct kvm *kvm)
{
	return 0;
}

static inline bool vgic_initialized(struct kvm *kvm)
{
	return true;
}
#endif

#endif
