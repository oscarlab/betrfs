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

#include <linux/cpu.h>
#include <linux/kvm.h>
#include <linux/kvm_host.h>
#include <linux/interrupt.h>
#include <linux/io.h>
#include <linux/of.h>
#include <linux/of_address.h>
#include <linux/of_irq.h>

#include <linux/irqchip/arm-gic.h>

#include <asm/kvm_emulate.h>
#include <asm/kvm_arm.h>
#include <asm/kvm_mmu.h>

/*
 * How the whole thing works (courtesy of Christoffer Dall):
 *
 * - At any time, the dist->irq_pending_on_cpu is the oracle that knows if
 *   something is pending
 * - VGIC pending interrupts are stored on the vgic.irq_state vgic
 *   bitmap (this bitmap is updated by both user land ioctls and guest
 *   mmio ops, and other in-kernel peripherals such as the
 *   arch. timers) and indicate the 'wire' state.
 * - Every time the bitmap changes, the irq_pending_on_cpu oracle is
 *   recalculated
 * - To calculate the oracle, we need info for each cpu from
 *   compute_pending_for_cpu, which considers:
 *   - PPI: dist->irq_state & dist->irq_enable
 *   - SPI: dist->irq_state & dist->irq_enable & dist->irq_spi_target
 *   - irq_spi_target is a 'formatted' version of the GICD_ICFGR
 *     registers, stored on each vcpu. We only keep one bit of
 *     information per interrupt, making sure that only one vcpu can
 *     accept the interrupt.
 * - The same is true when injecting an interrupt, except that we only
 *   consider a single interrupt at a time. The irq_spi_cpu array
 *   contains the target CPU for each SPI.
 *
 * The handling of level interrupts adds some extra complexity. We
 * need to track when the interrupt has been EOIed, so we can sample
 * the 'line' again. This is achieved as such:
 *
 * - When a level interrupt is moved onto a vcpu, the corresponding
 *   bit in irq_active is set. As long as this bit is set, the line
 *   will be ignored for further interrupts. The interrupt is injected
 *   into the vcpu with the GICH_LR_EOI bit set (generate a
 *   maintenance interrupt on EOI).
 * - When the interrupt is EOIed, the maintenance interrupt fires,
 *   and clears the corresponding bit in irq_active. This allow the
 *   interrupt line to be sampled again.
 */

#define VGIC_ADDR_UNDEF		(-1)
#define IS_VGIC_ADDR_UNDEF(_x)  ((_x) == VGIC_ADDR_UNDEF)

/* Physical address of vgic virtual cpu interface */
static phys_addr_t vgic_vcpu_base;

/* Virtual control interface base address */
static void __iomem *vgic_vctrl_base;

static struct device_node *vgic_node;

#define ACCESS_READ_VALUE	(1 << 0)
#define ACCESS_READ_RAZ		(0 << 0)
#define ACCESS_READ_MASK(x)	((x) & (1 << 0))
#define ACCESS_WRITE_IGNORED	(0 << 1)
#define ACCESS_WRITE_SETBIT	(1 << 1)
#define ACCESS_WRITE_CLEARBIT	(2 << 1)
#define ACCESS_WRITE_VALUE	(3 << 1)
#define ACCESS_WRITE_MASK(x)	((x) & (3 << 1))

static void vgic_retire_disabled_irqs(struct kvm_vcpu *vcpu);
static void vgic_update_state(struct kvm *kvm);
static void vgic_kick_vcpus(struct kvm *kvm);
static void vgic_dispatch_sgi(struct kvm_vcpu *vcpu, u32 reg);
static u32 vgic_nr_lr;

static unsigned int vgic_maint_irq;

static u32 *vgic_bitmap_get_reg(struct vgic_bitmap *x,
				int cpuid, u32 offset)
{
	offset >>= 2;
	if (!offset)
		return x->percpu[cpuid].reg;
	else
		return x->shared.reg + offset - 1;
}

static int vgic_bitmap_get_irq_val(struct vgic_bitmap *x,
				   int cpuid, int irq)
{
	if (irq < VGIC_NR_PRIVATE_IRQS)
		return test_bit(irq, x->percpu[cpuid].reg_ul);

	return test_bit(irq - VGIC_NR_PRIVATE_IRQS, x->shared.reg_ul);
}

static void vgic_bitmap_set_irq_val(struct vgic_bitmap *x, int cpuid,
				    int irq, int val)
{
	unsigned long *reg;

	if (irq < VGIC_NR_PRIVATE_IRQS) {
		reg = x->percpu[cpuid].reg_ul;
	} else {
		reg =  x->shared.reg_ul;
		irq -= VGIC_NR_PRIVATE_IRQS;
	}

	if (val)
		set_bit(irq, reg);
	else
		clear_bit(irq, reg);
}

static unsigned long *vgic_bitmap_get_cpu_map(struct vgic_bitmap *x, int cpuid)
{
	if (unlikely(cpuid >= VGIC_MAX_CPUS))
		return NULL;
	return x->percpu[cpuid].reg_ul;
}

static unsigned long *vgic_bitmap_get_shared_map(struct vgic_bitmap *x)
{
	return x->shared.reg_ul;
}

static u32 *vgic_bytemap_get_reg(struct vgic_bytemap *x, int cpuid, u32 offset)
{
	offset >>= 2;
	BUG_ON(offset > (VGIC_NR_IRQS / 4));
	if (offset < 4)
		return x->percpu[cpuid] + offset;
	else
		return x->shared + offset - 8;
}

#define VGIC_CFG_LEVEL	0
#define VGIC_CFG_EDGE	1

static bool vgic_irq_is_edge(struct kvm_vcpu *vcpu, int irq)
{
	struct vgic_dist *dist = &vcpu->kvm->arch.vgic;
	int irq_val;

	irq_val = vgic_bitmap_get_irq_val(&dist->irq_cfg, vcpu->vcpu_id, irq);
	return irq_val == VGIC_CFG_EDGE;
}

static int vgic_irq_is_enabled(struct kvm_vcpu *vcpu, int irq)
{
	struct vgic_dist *dist = &vcpu->kvm->arch.vgic;

	return vgic_bitmap_get_irq_val(&dist->irq_enabled, vcpu->vcpu_id, irq);
}

static int vgic_irq_is_active(struct kvm_vcpu *vcpu, int irq)
{
	struct vgic_dist *dist = &vcpu->kvm->arch.vgic;

	return vgic_bitmap_get_irq_val(&dist->irq_active, vcpu->vcpu_id, irq);
}

static void vgic_irq_set_active(struct kvm_vcpu *vcpu, int irq)
{
	struct vgic_dist *dist = &vcpu->kvm->arch.vgic;

	vgic_bitmap_set_irq_val(&dist->irq_active, vcpu->vcpu_id, irq, 1);
}

static void vgic_irq_clear_active(struct kvm_vcpu *vcpu, int irq)
{
	struct vgic_dist *dist = &vcpu->kvm->arch.vgic;

	vgic_bitmap_set_irq_val(&dist->irq_active, vcpu->vcpu_id, irq, 0);
}

static int vgic_dist_irq_is_pending(struct kvm_vcpu *vcpu, int irq)
{
	struct vgic_dist *dist = &vcpu->kvm->arch.vgic;

	return vgic_bitmap_get_irq_val(&dist->irq_state, vcpu->vcpu_id, irq);
}

static void vgic_dist_irq_set(struct kvm_vcpu *vcpu, int irq)
{
	struct vgic_dist *dist = &vcpu->kvm->arch.vgic;

	vgic_bitmap_set_irq_val(&dist->irq_state, vcpu->vcpu_id, irq, 1);
}

static void vgic_dist_irq_clear(struct kvm_vcpu *vcpu, int irq)
{
	struct vgic_dist *dist = &vcpu->kvm->arch.vgic;

	vgic_bitmap_set_irq_val(&dist->irq_state, vcpu->vcpu_id, irq, 0);
}

static void vgic_cpu_irq_set(struct kvm_vcpu *vcpu, int irq)
{
	if (irq < VGIC_NR_PRIVATE_IRQS)
		set_bit(irq, vcpu->arch.vgic_cpu.pending_percpu);
	else
		set_bit(irq - VGIC_NR_PRIVATE_IRQS,
			vcpu->arch.vgic_cpu.pending_shared);
}

static void vgic_cpu_irq_clear(struct kvm_vcpu *vcpu, int irq)
{
	if (irq < VGIC_NR_PRIVATE_IRQS)
		clear_bit(irq, vcpu->arch.vgic_cpu.pending_percpu);
	else
		clear_bit(irq - VGIC_NR_PRIVATE_IRQS,
			  vcpu->arch.vgic_cpu.pending_shared);
}

static u32 mmio_data_read(struct kvm_exit_mmio *mmio, u32 mask)
{
	return *((u32 *)mmio->data) & mask;
}

static void mmio_data_write(struct kvm_exit_mmio *mmio, u32 mask, u32 value)
{
	*((u32 *)mmio->data) = value & mask;
}

/**
 * vgic_reg_access - access vgic register
 * @mmio:   pointer to the data describing the mmio access
 * @reg:    pointer to the virtual backing of vgic distributor data
 * @offset: least significant 2 bits used for word offset
 * @mode:   ACCESS_ mode (see defines above)
 *
 * Helper to make vgic register access easier using one of the access
 * modes defined for vgic register access
 * (read,raz,write-ignored,setbit,clearbit,write)
 */
static void vgic_reg_access(struct kvm_exit_mmio *mmio, u32 *reg,
			    phys_addr_t offset, int mode)
{
	int word_offset = (offset & 3) * 8;
	u32 mask = (1UL << (mmio->len * 8)) - 1;
	u32 regval;

	/*
	 * Any alignment fault should have been delivered to the guest
	 * directly (ARM ARM B3.12.7 "Prioritization of aborts").
	 */

	if (reg) {
		regval = *reg;
	} else {
		BUG_ON(mode != (ACCESS_READ_RAZ | ACCESS_WRITE_IGNORED));
		regval = 0;
	}

	if (mmio->is_write) {
		u32 data = mmio_data_read(mmio, mask) << word_offset;
		switch (ACCESS_WRITE_MASK(mode)) {
		case ACCESS_WRITE_IGNORED:
			return;

		case ACCESS_WRITE_SETBIT:
			regval |= data;
			break;

		case ACCESS_WRITE_CLEARBIT:
			regval &= ~data;
			break;

		case ACCESS_WRITE_VALUE:
			regval = (regval & ~(mask << word_offset)) | data;
			break;
		}
		*reg = regval;
	} else {
		switch (ACCESS_READ_MASK(mode)) {
		case ACCESS_READ_RAZ:
			regval = 0;
			/* fall through */

		case ACCESS_READ_VALUE:
			mmio_data_write(mmio, mask, regval >> word_offset);
		}
	}
}

static bool handle_mmio_misc(struct kvm_vcpu *vcpu,
			     struct kvm_exit_mmio *mmio, phys_addr_t offset)
{
	u32 reg;
	u32 word_offset = offset & 3;

	switch (offset & ~3) {
	case 0:			/* CTLR */
		reg = vcpu->kvm->arch.vgic.enabled;
		vgic_reg_access(mmio, &reg, word_offset,
				ACCESS_READ_VALUE | ACCESS_WRITE_VALUE);
		if (mmio->is_write) {
			vcpu->kvm->arch.vgic.enabled = reg & 1;
			vgic_update_state(vcpu->kvm);
			return true;
		}
		break;

	case 4:			/* TYPER */
		reg  = (atomic_read(&vcpu->kvm->online_vcpus) - 1) << 5;
		reg |= (VGIC_NR_IRQS >> 5) - 1;
		vgic_reg_access(mmio, &reg, word_offset,
				ACCESS_READ_VALUE | ACCESS_WRITE_IGNORED);
		break;

	case 8:			/* IIDR */
		reg = 0x4B00043B;
		vgic_reg_access(mmio, &reg, word_offset,
				ACCESS_READ_VALUE | ACCESS_WRITE_IGNORED);
		break;
	}

	return false;
}

static bool handle_mmio_raz_wi(struct kvm_vcpu *vcpu,
			       struct kvm_exit_mmio *mmio, phys_addr_t offset)
{
	vgic_reg_access(mmio, NULL, offset,
			ACCESS_READ_RAZ | ACCESS_WRITE_IGNORED);
	return false;
}

static bool handle_mmio_set_enable_reg(struct kvm_vcpu *vcpu,
				       struct kvm_exit_mmio *mmio,
				       phys_addr_t offset)
{
	u32 *reg = vgic_bitmap_get_reg(&vcpu->kvm->arch.vgic.irq_enabled,
				       vcpu->vcpu_id, offset);
	vgic_reg_access(mmio, reg, offset,
			ACCESS_READ_VALUE | ACCESS_WRITE_SETBIT);
	if (mmio->is_write) {
		vgic_update_state(vcpu->kvm);
		return true;
	}

	return false;
}

static bool handle_mmio_clear_enable_reg(struct kvm_vcpu *vcpu,
					 struct kvm_exit_mmio *mmio,
					 phys_addr_t offset)
{
	u32 *reg = vgic_bitmap_get_reg(&vcpu->kvm->arch.vgic.irq_enabled,
				       vcpu->vcpu_id, offset);
	vgic_reg_access(mmio, reg, offset,
			ACCESS_READ_VALUE | ACCESS_WRITE_CLEARBIT);
	if (mmio->is_write) {
		if (offset < 4) /* Force SGI enabled */
			*reg |= 0xffff;
		vgic_retire_disabled_irqs(vcpu);
		vgic_update_state(vcpu->kvm);
		return true;
	}

	return false;
}

static bool handle_mmio_set_pending_reg(struct kvm_vcpu *vcpu,
					struct kvm_exit_mmio *mmio,
					phys_addr_t offset)
{
	u32 *reg = vgic_bitmap_get_reg(&vcpu->kvm->arch.vgic.irq_state,
				       vcpu->vcpu_id, offset);
	vgic_reg_access(mmio, reg, offset,
			ACCESS_READ_VALUE | ACCESS_WRITE_SETBIT);
	if (mmio->is_write) {
		vgic_update_state(vcpu->kvm);
		return true;
	}

	return false;
}

static bool handle_mmio_clear_pending_reg(struct kvm_vcpu *vcpu,
					  struct kvm_exit_mmio *mmio,
					  phys_addr_t offset)
{
	u32 *reg = vgic_bitmap_get_reg(&vcpu->kvm->arch.vgic.irq_state,
				       vcpu->vcpu_id, offset);
	vgic_reg_access(mmio, reg, offset,
			ACCESS_READ_VALUE | ACCESS_WRITE_CLEARBIT);
	if (mmio->is_write) {
		vgic_update_state(vcpu->kvm);
		return true;
	}

	return false;
}

static bool handle_mmio_priority_reg(struct kvm_vcpu *vcpu,
				     struct kvm_exit_mmio *mmio,
				     phys_addr_t offset)
{
	u32 *reg = vgic_bytemap_get_reg(&vcpu->kvm->arch.vgic.irq_priority,
					vcpu->vcpu_id, offset);
	vgic_reg_access(mmio, reg, offset,
			ACCESS_READ_VALUE | ACCESS_WRITE_VALUE);
	return false;
}

#define GICD_ITARGETSR_SIZE	32
#define GICD_CPUTARGETS_BITS	8
#define GICD_IRQS_PER_ITARGETSR	(GICD_ITARGETSR_SIZE / GICD_CPUTARGETS_BITS)
static u32 vgic_get_target_reg(struct kvm *kvm, int irq)
{
	struct vgic_dist *dist = &kvm->arch.vgic;
	struct kvm_vcpu *vcpu;
	int i, c;
	unsigned long *bmap;
	u32 val = 0;

	irq -= VGIC_NR_PRIVATE_IRQS;

	kvm_for_each_vcpu(c, vcpu, kvm) {
		bmap = vgic_bitmap_get_shared_map(&dist->irq_spi_target[c]);
		for (i = 0; i < GICD_IRQS_PER_ITARGETSR; i++)
			if (test_bit(irq + i, bmap))
				val |= 1 << (c + i * 8);
	}

	return val;
}

static void vgic_set_target_reg(struct kvm *kvm, u32 val, int irq)
{
	struct vgic_dist *dist = &kvm->arch.vgic;
	struct kvm_vcpu *vcpu;
	int i, c;
	unsigned long *bmap;
	u32 target;

	irq -= VGIC_NR_PRIVATE_IRQS;

	/*
	 * Pick the LSB in each byte. This ensures we target exactly
	 * one vcpu per IRQ. If the byte is null, assume we target
	 * CPU0.
	 */
	for (i = 0; i < GICD_IRQS_PER_ITARGETSR; i++) {
		int shift = i * GICD_CPUTARGETS_BITS;
		target = ffs((val >> shift) & 0xffU);
		target = target ? (target - 1) : 0;
		dist->irq_spi_cpu[irq + i] = target;
		kvm_for_each_vcpu(c, vcpu, kvm) {
			bmap = vgic_bitmap_get_shared_map(&dist->irq_spi_target[c]);
			if (c == target)
				set_bit(irq + i, bmap);
			else
				clear_bit(irq + i, bmap);
		}
	}
}

static bool handle_mmio_target_reg(struct kvm_vcpu *vcpu,
				   struct kvm_exit_mmio *mmio,
				   phys_addr_t offset)
{
	u32 reg;

	/* We treat the banked interrupts targets as read-only */
	if (offset < 32) {
		u32 roreg = 1 << vcpu->vcpu_id;
		roreg |= roreg << 8;
		roreg |= roreg << 16;

		vgic_reg_access(mmio, &roreg, offset,
				ACCESS_READ_VALUE | ACCESS_WRITE_IGNORED);
		return false;
	}

	reg = vgic_get_target_reg(vcpu->kvm, offset & ~3U);
	vgic_reg_access(mmio, &reg, offset,
			ACCESS_READ_VALUE | ACCESS_WRITE_VALUE);
	if (mmio->is_write) {
		vgic_set_target_reg(vcpu->kvm, reg, offset & ~3U);
		vgic_update_state(vcpu->kvm);
		return true;
	}

	return false;
}

static u32 vgic_cfg_expand(u16 val)
{
	u32 res = 0;
	int i;

	/*
	 * Turn a 16bit value like abcd...mnop into a 32bit word
	 * a0b0c0d0...m0n0o0p0, which is what the HW cfg register is.
	 */
	for (i = 0; i < 16; i++)
		res |= ((val >> i) & VGIC_CFG_EDGE) << (2 * i + 1);

	return res;
}

static u16 vgic_cfg_compress(u32 val)
{
	u16 res = 0;
	int i;

	/*
	 * Turn a 32bit word a0b0c0d0...m0n0o0p0 into 16bit value like
	 * abcd...mnop which is what we really care about.
	 */
	for (i = 0; i < 16; i++)
		res |= ((val >> (i * 2 + 1)) & VGIC_CFG_EDGE) << i;

	return res;
}

/*
 * The distributor uses 2 bits per IRQ for the CFG register, but the
 * LSB is always 0. As such, we only keep the upper bit, and use the
 * two above functions to compress/expand the bits
 */
static bool handle_mmio_cfg_reg(struct kvm_vcpu *vcpu,
				struct kvm_exit_mmio *mmio, phys_addr_t offset)
{
	u32 val;
	u32 *reg = vgic_bitmap_get_reg(&vcpu->kvm->arch.vgic.irq_cfg,
				       vcpu->vcpu_id, offset >> 1);
	if (offset & 2)
		val = *reg >> 16;
	else
		val = *reg & 0xffff;

	val = vgic_cfg_expand(val);
	vgic_reg_access(mmio, &val, offset,
			ACCESS_READ_VALUE | ACCESS_WRITE_VALUE);
	if (mmio->is_write) {
		if (offset < 4) {
			*reg = ~0U; /* Force PPIs/SGIs to 1 */
			return false;
		}

		val = vgic_cfg_compress(val);
		if (offset & 2) {
			*reg &= 0xffff;
			*reg |= val << 16;
		} else {
			*reg &= 0xffff << 16;
			*reg |= val;
		}
	}

	return false;
}

static bool handle_mmio_sgi_reg(struct kvm_vcpu *vcpu,
				struct kvm_exit_mmio *mmio, phys_addr_t offset)
{
	u32 reg;
	vgic_reg_access(mmio, &reg, offset,
			ACCESS_READ_RAZ | ACCESS_WRITE_VALUE);
	if (mmio->is_write) {
		vgic_dispatch_sgi(vcpu, reg);
		vgic_update_state(vcpu->kvm);
		return true;
	}

	return false;
}

/*
 * I would have liked to use the kvm_bus_io_*() API instead, but it
 * cannot cope with banked registers (only the VM pointer is passed
 * around, and we need the vcpu). One of these days, someone please
 * fix it!
 */
struct mmio_range {
	phys_addr_t base;
	unsigned long len;
	bool (*handle_mmio)(struct kvm_vcpu *vcpu, struct kvm_exit_mmio *mmio,
			    phys_addr_t offset);
};

static const struct mmio_range vgic_ranges[] = {
	{
		.base		= GIC_DIST_CTRL,
		.len		= 12,
		.handle_mmio	= handle_mmio_misc,
	},
	{
		.base		= GIC_DIST_IGROUP,
		.len		= VGIC_NR_IRQS / 8,
		.handle_mmio	= handle_mmio_raz_wi,
	},
	{
		.base		= GIC_DIST_ENABLE_SET,
		.len		= VGIC_NR_IRQS / 8,
		.handle_mmio	= handle_mmio_set_enable_reg,
	},
	{
		.base		= GIC_DIST_ENABLE_CLEAR,
		.len		= VGIC_NR_IRQS / 8,
		.handle_mmio	= handle_mmio_clear_enable_reg,
	},
	{
		.base		= GIC_DIST_PENDING_SET,
		.len		= VGIC_NR_IRQS / 8,
		.handle_mmio	= handle_mmio_set_pending_reg,
	},
	{
		.base		= GIC_DIST_PENDING_CLEAR,
		.len		= VGIC_NR_IRQS / 8,
		.handle_mmio	= handle_mmio_clear_pending_reg,
	},
	{
		.base		= GIC_DIST_ACTIVE_SET,
		.len		= VGIC_NR_IRQS / 8,
		.handle_mmio	= handle_mmio_raz_wi,
	},
	{
		.base		= GIC_DIST_ACTIVE_CLEAR,
		.len		= VGIC_NR_IRQS / 8,
		.handle_mmio	= handle_mmio_raz_wi,
	},
	{
		.base		= GIC_DIST_PRI,
		.len		= VGIC_NR_IRQS,
		.handle_mmio	= handle_mmio_priority_reg,
	},
	{
		.base		= GIC_DIST_TARGET,
		.len		= VGIC_NR_IRQS,
		.handle_mmio	= handle_mmio_target_reg,
	},
	{
		.base		= GIC_DIST_CONFIG,
		.len		= VGIC_NR_IRQS / 4,
		.handle_mmio	= handle_mmio_cfg_reg,
	},
	{
		.base		= GIC_DIST_SOFTINT,
		.len		= 4,
		.handle_mmio	= handle_mmio_sgi_reg,
	},
	{}
};

static const
struct mmio_range *find_matching_range(const struct mmio_range *ranges,
				       struct kvm_exit_mmio *mmio,
				       phys_addr_t base)
{
	const struct mmio_range *r = ranges;
	phys_addr_t addr = mmio->phys_addr - base;

	while (r->len) {
		if (addr >= r->base &&
		    (addr + mmio->len) <= (r->base + r->len))
			return r;
		r++;
	}

	return NULL;
}

/**
 * vgic_handle_mmio - handle an in-kernel MMIO access
 * @vcpu:	pointer to the vcpu performing the access
 * @run:	pointer to the kvm_run structure
 * @mmio:	pointer to the data describing the access
 *
 * returns true if the MMIO access has been performed in kernel space,
 * and false if it needs to be emulated in user space.
 */
bool vgic_handle_mmio(struct kvm_vcpu *vcpu, struct kvm_run *run,
		      struct kvm_exit_mmio *mmio)
{
	const struct mmio_range *range;
	struct vgic_dist *dist = &vcpu->kvm->arch.vgic;
	unsigned long base = dist->vgic_dist_base;
	bool updated_state;
	unsigned long offset;

	if (!irqchip_in_kernel(vcpu->kvm) ||
	    mmio->phys_addr < base ||
	    (mmio->phys_addr + mmio->len) > (base + KVM_VGIC_V2_DIST_SIZE))
		return false;

	/* We don't support ldrd / strd or ldm / stm to the emulated vgic */
	if (mmio->len > 4) {
		kvm_inject_dabt(vcpu, mmio->phys_addr);
		return true;
	}

	range = find_matching_range(vgic_ranges, mmio, base);
	if (unlikely(!range || !range->handle_mmio)) {
		pr_warn("Unhandled access %d %08llx %d\n",
			mmio->is_write, mmio->phys_addr, mmio->len);
		return false;
	}

	spin_lock(&vcpu->kvm->arch.vgic.lock);
	offset = mmio->phys_addr - range->base - base;
	updated_state = range->handle_mmio(vcpu, mmio, offset);
	spin_unlock(&vcpu->kvm->arch.vgic.lock);
	kvm_prepare_mmio(run, mmio);
	kvm_handle_mmio_return(vcpu, run);

	if (updated_state)
		vgic_kick_vcpus(vcpu->kvm);

	return true;
}

static void vgic_dispatch_sgi(struct kvm_vcpu *vcpu, u32 reg)
{
	struct kvm *kvm = vcpu->kvm;
	struct vgic_dist *dist = &kvm->arch.vgic;
	int nrcpus = atomic_read(&kvm->online_vcpus);
	u8 target_cpus;
	int sgi, mode, c, vcpu_id;

	vcpu_id = vcpu->vcpu_id;

	sgi = reg & 0xf;
	target_cpus = (reg >> 16) & 0xff;
	mode = (reg >> 24) & 3;

	switch (mode) {
	case 0:
		if (!target_cpus)
			return;

	case 1:
		target_cpus = ((1 << nrcpus) - 1) & ~(1 << vcpu_id) & 0xff;
		break;

	case 2:
		target_cpus = 1 << vcpu_id;
		break;
	}

	kvm_for_each_vcpu(c, vcpu, kvm) {
		if (target_cpus & 1) {
			/* Flag the SGI as pending */
			vgic_dist_irq_set(vcpu, sgi);
			dist->irq_sgi_sources[c][sgi] |= 1 << vcpu_id;
			kvm_debug("SGI%d from CPU%d to CPU%d\n", sgi, vcpu_id, c);
		}

		target_cpus >>= 1;
	}
}

static int compute_pending_for_cpu(struct kvm_vcpu *vcpu)
{
	struct vgic_dist *dist = &vcpu->kvm->arch.vgic;
	unsigned long *pending, *enabled, *pend_percpu, *pend_shared;
	unsigned long pending_private, pending_shared;
	int vcpu_id;

	vcpu_id = vcpu->vcpu_id;
	pend_percpu = vcpu->arch.vgic_cpu.pending_percpu;
	pend_shared = vcpu->arch.vgic_cpu.pending_shared;

	pending = vgic_bitmap_get_cpu_map(&dist->irq_state, vcpu_id);
	enabled = vgic_bitmap_get_cpu_map(&dist->irq_enabled, vcpu_id);
	bitmap_and(pend_percpu, pending, enabled, VGIC_NR_PRIVATE_IRQS);

	pending = vgic_bitmap_get_shared_map(&dist->irq_state);
	enabled = vgic_bitmap_get_shared_map(&dist->irq_enabled);
	bitmap_and(pend_shared, pending, enabled, VGIC_NR_SHARED_IRQS);
	bitmap_and(pend_shared, pend_shared,
		   vgic_bitmap_get_shared_map(&dist->irq_spi_target[vcpu_id]),
		   VGIC_NR_SHARED_IRQS);

	pending_private = find_first_bit(pend_percpu, VGIC_NR_PRIVATE_IRQS);
	pending_shared = find_first_bit(pend_shared, VGIC_NR_SHARED_IRQS);
	return (pending_private < VGIC_NR_PRIVATE_IRQS ||
		pending_shared < VGIC_NR_SHARED_IRQS);
}

/*
 * Update the interrupt state and determine which CPUs have pending
 * interrupts. Must be called with distributor lock held.
 */
static void vgic_update_state(struct kvm *kvm)
{
	struct vgic_dist *dist = &kvm->arch.vgic;
	struct kvm_vcpu *vcpu;
	int c;

	if (!dist->enabled) {
		set_bit(0, &dist->irq_pending_on_cpu);
		return;
	}

	kvm_for_each_vcpu(c, vcpu, kvm) {
		if (compute_pending_for_cpu(vcpu)) {
			pr_debug("CPU%d has pending interrupts\n", c);
			set_bit(c, &dist->irq_pending_on_cpu);
		}
	}
}

#define LR_CPUID(lr)	\
	(((lr) & GICH_LR_PHYSID_CPUID) >> GICH_LR_PHYSID_CPUID_SHIFT)
#define MK_LR_PEND(src, irq)	\
	(GICH_LR_PENDING_BIT | ((src) << GICH_LR_PHYSID_CPUID_SHIFT) | (irq))

/*
 * An interrupt may have been disabled after being made pending on the
 * CPU interface (the classic case is a timer running while we're
 * rebooting the guest - the interrupt would kick as soon as the CPU
 * interface gets enabled, with deadly consequences).
 *
 * The solution is to examine already active LRs, and check the
 * interrupt is still enabled. If not, just retire it.
 */
static void vgic_retire_disabled_irqs(struct kvm_vcpu *vcpu)
{
	struct vgic_cpu *vgic_cpu = &vcpu->arch.vgic_cpu;
	int lr;

	for_each_set_bit(lr, vgic_cpu->lr_used, vgic_cpu->nr_lr) {
		int irq = vgic_cpu->vgic_lr[lr] & GICH_LR_VIRTUALID;

		if (!vgic_irq_is_enabled(vcpu, irq)) {
			vgic_cpu->vgic_irq_lr_map[irq] = LR_EMPTY;
			clear_bit(lr, vgic_cpu->lr_used);
			vgic_cpu->vgic_lr[lr] &= ~GICH_LR_STATE;
			if (vgic_irq_is_active(vcpu, irq))
				vgic_irq_clear_active(vcpu, irq);
		}
	}
}

/*
 * Queue an interrupt to a CPU virtual interface. Return true on success,
 * or false if it wasn't possible to queue it.
 */
static bool vgic_queue_irq(struct kvm_vcpu *vcpu, u8 sgi_source_id, int irq)
{
	struct vgic_cpu *vgic_cpu = &vcpu->arch.vgic_cpu;
	int lr;

	/* Sanitize the input... */
	BUG_ON(sgi_source_id & ~7);
	BUG_ON(sgi_source_id && irq >= VGIC_NR_SGIS);
	BUG_ON(irq >= VGIC_NR_IRQS);

	kvm_debug("Queue IRQ%d\n", irq);

	lr = vgic_cpu->vgic_irq_lr_map[irq];

	/* Do we have an active interrupt for the same CPUID? */
	if (lr != LR_EMPTY &&
	    (LR_CPUID(vgic_cpu->vgic_lr[lr]) == sgi_source_id)) {
		kvm_debug("LR%d piggyback for IRQ%d %x\n",
			  lr, irq, vgic_cpu->vgic_lr[lr]);
		BUG_ON(!test_bit(lr, vgic_cpu->lr_used));
		vgic_cpu->vgic_lr[lr] |= GICH_LR_PENDING_BIT;
		return true;
	}

	/* Try to use another LR for this interrupt */
	lr = find_first_zero_bit((unsigned long *)vgic_cpu->lr_used,
			       vgic_cpu->nr_lr);
	if (lr >= vgic_cpu->nr_lr)
		return false;

	kvm_debug("LR%d allocated for IRQ%d %x\n", lr, irq, sgi_source_id);
	vgic_cpu->vgic_lr[lr] = MK_LR_PEND(sgi_source_id, irq);
	vgic_cpu->vgic_irq_lr_map[irq] = lr;
	set_bit(lr, vgic_cpu->lr_used);

	if (!vgic_irq_is_edge(vcpu, irq))
		vgic_cpu->vgic_lr[lr] |= GICH_LR_EOI;

	return true;
}

static bool vgic_queue_sgi(struct kvm_vcpu *vcpu, int irq)
{
	struct vgic_dist *dist = &vcpu->kvm->arch.vgic;
	unsigned long sources;
	int vcpu_id = vcpu->vcpu_id;
	int c;

	sources = dist->irq_sgi_sources[vcpu_id][irq];

	for_each_set_bit(c, &sources, VGIC_MAX_CPUS) {
		if (vgic_queue_irq(vcpu, c, irq))
			clear_bit(c, &sources);
	}

	dist->irq_sgi_sources[vcpu_id][irq] = sources;

	/*
	 * If the sources bitmap has been cleared it means that we
	 * could queue all the SGIs onto link registers (see the
	 * clear_bit above), and therefore we are done with them in
	 * our emulated gic and can get rid of them.
	 */
	if (!sources) {
		vgic_dist_irq_clear(vcpu, irq);
		vgic_cpu_irq_clear(vcpu, irq);
		return true;
	}

	return false;
}

static bool vgic_queue_hwirq(struct kvm_vcpu *vcpu, int irq)
{
	if (vgic_irq_is_active(vcpu, irq))
		return true; /* level interrupt, already queued */

	if (vgic_queue_irq(vcpu, 0, irq)) {
		if (vgic_irq_is_edge(vcpu, irq)) {
			vgic_dist_irq_clear(vcpu, irq);
			vgic_cpu_irq_clear(vcpu, irq);
		} else {
			vgic_irq_set_active(vcpu, irq);
		}

		return true;
	}

	return false;
}

/*
 * Fill the list registers with pending interrupts before running the
 * guest.
 */
static void __kvm_vgic_flush_hwstate(struct kvm_vcpu *vcpu)
{
	struct vgic_cpu *vgic_cpu = &vcpu->arch.vgic_cpu;
	struct vgic_dist *dist = &vcpu->kvm->arch.vgic;
	int i, vcpu_id;
	int overflow = 0;

	vcpu_id = vcpu->vcpu_id;

	/*
	 * We may not have any pending interrupt, or the interrupts
	 * may have been serviced from another vcpu. In all cases,
	 * move along.
	 */
	if (!kvm_vgic_vcpu_pending_irq(vcpu)) {
		pr_debug("CPU%d has no pending interrupt\n", vcpu_id);
		goto epilog;
	}

	/* SGIs */
	for_each_set_bit(i, vgic_cpu->pending_percpu, VGIC_NR_SGIS) {
		if (!vgic_queue_sgi(vcpu, i))
			overflow = 1;
	}

	/* PPIs */
	for_each_set_bit_from(i, vgic_cpu->pending_percpu, VGIC_NR_PRIVATE_IRQS) {
		if (!vgic_queue_hwirq(vcpu, i))
			overflow = 1;
	}

	/* SPIs */
	for_each_set_bit(i, vgic_cpu->pending_shared, VGIC_NR_SHARED_IRQS) {
		if (!vgic_queue_hwirq(vcpu, i + VGIC_NR_PRIVATE_IRQS))
			overflow = 1;
	}

epilog:
	if (overflow) {
		vgic_cpu->vgic_hcr |= GICH_HCR_UIE;
	} else {
		vgic_cpu->vgic_hcr &= ~GICH_HCR_UIE;
		/*
		 * We're about to run this VCPU, and we've consumed
		 * everything the distributor had in store for
		 * us. Claim we don't have anything pending. We'll
		 * adjust that if needed while exiting.
		 */
		clear_bit(vcpu_id, &dist->irq_pending_on_cpu);
	}
}

static bool vgic_process_maintenance(struct kvm_vcpu *vcpu)
{
	struct vgic_cpu *vgic_cpu = &vcpu->arch.vgic_cpu;
	bool level_pending = false;

	kvm_debug("MISR = %08x\n", vgic_cpu->vgic_misr);

	if (vgic_cpu->vgic_misr & GICH_MISR_EOI) {
		/*
		 * Some level interrupts have been EOIed. Clear their
		 * active bit.
		 */
		int lr, irq;

		for_each_set_bit(lr, (unsigned long *)vgic_cpu->vgic_eisr,
				 vgic_cpu->nr_lr) {
			irq = vgic_cpu->vgic_lr[lr] & GICH_LR_VIRTUALID;

			vgic_irq_clear_active(vcpu, irq);
			vgic_cpu->vgic_lr[lr] &= ~GICH_LR_EOI;

			/* Any additional pending interrupt? */
			if (vgic_dist_irq_is_pending(vcpu, irq)) {
				vgic_cpu_irq_set(vcpu, irq);
				level_pending = true;
			} else {
				vgic_cpu_irq_clear(vcpu, irq);
			}

			/*
			 * Despite being EOIed, the LR may not have
			 * been marked as empty.
			 */
			set_bit(lr, (unsigned long *)vgic_cpu->vgic_elrsr);
			vgic_cpu->vgic_lr[lr] &= ~GICH_LR_ACTIVE_BIT;
		}
	}

	if (vgic_cpu->vgic_misr & GICH_MISR_U)
		vgic_cpu->vgic_hcr &= ~GICH_HCR_UIE;

	return level_pending;
}

/*
 * Sync back the VGIC state after a guest run. The distributor lock is
 * needed so we don't get preempted in the middle of the state processing.
 */
static void __kvm_vgic_sync_hwstate(struct kvm_vcpu *vcpu)
{
	struct vgic_cpu *vgic_cpu = &vcpu->arch.vgic_cpu;
	struct vgic_dist *dist = &vcpu->kvm->arch.vgic;
	int lr, pending;
	bool level_pending;

	level_pending = vgic_process_maintenance(vcpu);

	/* Clear mappings for empty LRs */
	for_each_set_bit(lr, (unsigned long *)vgic_cpu->vgic_elrsr,
			 vgic_cpu->nr_lr) {
		int irq;

		if (!test_and_clear_bit(lr, vgic_cpu->lr_used))
			continue;

		irq = vgic_cpu->vgic_lr[lr] & GICH_LR_VIRTUALID;

		BUG_ON(irq >= VGIC_NR_IRQS);
		vgic_cpu->vgic_irq_lr_map[irq] = LR_EMPTY;
	}

	/* Check if we still have something up our sleeve... */
	pending = find_first_zero_bit((unsigned long *)vgic_cpu->vgic_elrsr,
				      vgic_cpu->nr_lr);
	if (level_pending || pending < vgic_cpu->nr_lr)
		set_bit(vcpu->vcpu_id, &dist->irq_pending_on_cpu);
}

void kvm_vgic_flush_hwstate(struct kvm_vcpu *vcpu)
{
	struct vgic_dist *dist = &vcpu->kvm->arch.vgic;

	if (!irqchip_in_kernel(vcpu->kvm))
		return;

	spin_lock(&dist->lock);
	__kvm_vgic_flush_hwstate(vcpu);
	spin_unlock(&dist->lock);
}

void kvm_vgic_sync_hwstate(struct kvm_vcpu *vcpu)
{
	struct vgic_dist *dist = &vcpu->kvm->arch.vgic;

	if (!irqchip_in_kernel(vcpu->kvm))
		return;

	spin_lock(&dist->lock);
	__kvm_vgic_sync_hwstate(vcpu);
	spin_unlock(&dist->lock);
}

int kvm_vgic_vcpu_pending_irq(struct kvm_vcpu *vcpu)
{
	struct vgic_dist *dist = &vcpu->kvm->arch.vgic;

	if (!irqchip_in_kernel(vcpu->kvm))
		return 0;

	return test_bit(vcpu->vcpu_id, &dist->irq_pending_on_cpu);
}

static void vgic_kick_vcpus(struct kvm *kvm)
{
	struct kvm_vcpu *vcpu;
	int c;

	/*
	 * We've injected an interrupt, time to find out who deserves
	 * a good kick...
	 */
	kvm_for_each_vcpu(c, vcpu, kvm) {
		if (kvm_vgic_vcpu_pending_irq(vcpu))
			kvm_vcpu_kick(vcpu);
	}
}

static int vgic_validate_injection(struct kvm_vcpu *vcpu, int irq, int level)
{
	int is_edge = vgic_irq_is_edge(vcpu, irq);
	int state = vgic_dist_irq_is_pending(vcpu, irq);

	/*
	 * Only inject an interrupt if:
	 * - edge triggered and we have a rising edge
	 * - level triggered and we change level
	 */
	if (is_edge)
		return level > state;
	else
		return level != state;
}

static bool vgic_update_irq_state(struct kvm *kvm, int cpuid,
				  unsigned int irq_num, bool level)
{
	struct vgic_dist *dist = &kvm->arch.vgic;
	struct kvm_vcpu *vcpu;
	int is_edge, is_level;
	int enabled;
	bool ret = true;

	spin_lock(&dist->lock);

	vcpu = kvm_get_vcpu(kvm, cpuid);
	is_edge = vgic_irq_is_edge(vcpu, irq_num);
	is_level = !is_edge;

	if (!vgic_validate_injection(vcpu, irq_num, level)) {
		ret = false;
		goto out;
	}

	if (irq_num >= VGIC_NR_PRIVATE_IRQS) {
		cpuid = dist->irq_spi_cpu[irq_num - VGIC_NR_PRIVATE_IRQS];
		vcpu = kvm_get_vcpu(kvm, cpuid);
	}

	kvm_debug("Inject IRQ%d level %d CPU%d\n", irq_num, level, cpuid);

	if (level)
		vgic_dist_irq_set(vcpu, irq_num);
	else
		vgic_dist_irq_clear(vcpu, irq_num);

	enabled = vgic_irq_is_enabled(vcpu, irq_num);

	if (!enabled) {
		ret = false;
		goto out;
	}

	if (is_level && vgic_irq_is_active(vcpu, irq_num)) {
		/*
		 * Level interrupt in progress, will be picked up
		 * when EOId.
		 */
		ret = false;
		goto out;
	}

	if (level) {
		vgic_cpu_irq_set(vcpu, irq_num);
		set_bit(cpuid, &dist->irq_pending_on_cpu);
	}

out:
	spin_unlock(&dist->lock);

	return ret;
}

/**
 * kvm_vgic_inject_irq - Inject an IRQ from a device to the vgic
 * @kvm:     The VM structure pointer
 * @cpuid:   The CPU for PPIs
 * @irq_num: The IRQ number that is assigned to the device
 * @level:   Edge-triggered:  true:  to trigger the interrupt
 *			      false: to ignore the call
 *	     Level-sensitive  true:  activates an interrupt
 *			      false: deactivates an interrupt
 *
 * The GIC is not concerned with devices being active-LOW or active-HIGH for
 * level-sensitive interrupts.  You can think of the level parameter as 1
 * being HIGH and 0 being LOW and all devices being active-HIGH.
 */
int kvm_vgic_inject_irq(struct kvm *kvm, int cpuid, unsigned int irq_num,
			bool level)
{
	if (vgic_update_irq_state(kvm, cpuid, irq_num, level))
		vgic_kick_vcpus(kvm);

	return 0;
}

static irqreturn_t vgic_maintenance_handler(int irq, void *data)
{
	/*
	 * We cannot rely on the vgic maintenance interrupt to be
	 * delivered synchronously. This means we can only use it to
	 * exit the VM, and we perform the handling of EOIed
	 * interrupts on the exit path (see vgic_process_maintenance).
	 */
	return IRQ_HANDLED;
}

int kvm_vgic_vcpu_init(struct kvm_vcpu *vcpu)
{
	struct vgic_cpu *vgic_cpu = &vcpu->arch.vgic_cpu;
	struct vgic_dist *dist = &vcpu->kvm->arch.vgic;
	int i;

	if (!irqchip_in_kernel(vcpu->kvm))
		return 0;

	if (vcpu->vcpu_id >= VGIC_MAX_CPUS)
		return -EBUSY;

	for (i = 0; i < VGIC_NR_IRQS; i++) {
		if (i < VGIC_NR_PPIS)
			vgic_bitmap_set_irq_val(&dist->irq_enabled,
						vcpu->vcpu_id, i, 1);
		if (i < VGIC_NR_PRIVATE_IRQS)
			vgic_bitmap_set_irq_val(&dist->irq_cfg,
						vcpu->vcpu_id, i, VGIC_CFG_EDGE);

		vgic_cpu->vgic_irq_lr_map[i] = LR_EMPTY;
	}

	/*
	 * By forcing VMCR to zero, the GIC will restore the binary
	 * points to their reset values. Anything else resets to zero
	 * anyway.
	 */
	vgic_cpu->vgic_vmcr = 0;

	vgic_cpu->nr_lr = vgic_nr_lr;
	vgic_cpu->vgic_hcr = GICH_HCR_EN; /* Get the show on the road... */

	return 0;
}

static void vgic_init_maintenance_interrupt(void *info)
{
	enable_percpu_irq(vgic_maint_irq, 0);
}

static int vgic_cpu_notify(struct notifier_block *self,
			   unsigned long action, void *cpu)
{
	switch (action) {
	case CPU_STARTING:
	case CPU_STARTING_FROZEN:
		vgic_init_maintenance_interrupt(NULL);
		break;
	case CPU_DYING:
	case CPU_DYING_FROZEN:
		disable_percpu_irq(vgic_maint_irq);
		break;
	}

	return NOTIFY_OK;
}

static struct notifier_block vgic_cpu_nb = {
	.notifier_call = vgic_cpu_notify,
};

int kvm_vgic_hyp_init(void)
{
	int ret;
	struct resource vctrl_res;
	struct resource vcpu_res;

	vgic_node = of_find_compatible_node(NULL, NULL, "arm,cortex-a15-gic");
	if (!vgic_node) {
		kvm_err("error: no compatible vgic node in DT\n");
		return -ENODEV;
	}

	vgic_maint_irq = irq_of_parse_and_map(vgic_node, 0);
	if (!vgic_maint_irq) {
		kvm_err("error getting vgic maintenance irq from DT\n");
		ret = -ENXIO;
		goto out;
	}

	ret = request_percpu_irq(vgic_maint_irq, vgic_maintenance_handler,
				 "vgic", kvm_get_running_vcpus());
	if (ret) {
		kvm_err("Cannot register interrupt %d\n", vgic_maint_irq);
		goto out;
	}

	ret = register_cpu_notifier(&vgic_cpu_nb);
	if (ret) {
		kvm_err("Cannot register vgic CPU notifier\n");
		goto out_free_irq;
	}

	ret = of_address_to_resource(vgic_node, 2, &vctrl_res);
	if (ret) {
		kvm_err("Cannot obtain VCTRL resource\n");
		goto out_free_irq;
	}

	vgic_vctrl_base = of_iomap(vgic_node, 2);
	if (!vgic_vctrl_base) {
		kvm_err("Cannot ioremap VCTRL\n");
		ret = -ENOMEM;
		goto out_free_irq;
	}

	vgic_nr_lr = readl_relaxed(vgic_vctrl_base + GICH_VTR);
	vgic_nr_lr = (vgic_nr_lr & 0x3f) + 1;

	ret = create_hyp_io_mappings(vgic_vctrl_base,
				     vgic_vctrl_base + resource_size(&vctrl_res),
				     vctrl_res.start);
	if (ret) {
		kvm_err("Cannot map VCTRL into hyp\n");
		goto out_unmap;
	}

	kvm_info("%s@%llx IRQ%d\n", vgic_node->name,
		 vctrl_res.start, vgic_maint_irq);
	on_each_cpu(vgic_init_maintenance_interrupt, NULL, 1);

	if (of_address_to_resource(vgic_node, 3, &vcpu_res)) {
		kvm_err("Cannot obtain VCPU resource\n");
		ret = -ENXIO;
		goto out_unmap;
	}
	vgic_vcpu_base = vcpu_res.start;

	goto out;

out_unmap:
	iounmap(vgic_vctrl_base);
out_free_irq:
	free_percpu_irq(vgic_maint_irq, kvm_get_running_vcpus());
out:
	of_node_put(vgic_node);
	return ret;
}

int kvm_vgic_init(struct kvm *kvm)
{
	int ret = 0, i;

	mutex_lock(&kvm->lock);

	if (vgic_initialized(kvm))
		goto out;

	if (IS_VGIC_ADDR_UNDEF(kvm->arch.vgic.vgic_dist_base) ||
	    IS_VGIC_ADDR_UNDEF(kvm->arch.vgic.vgic_cpu_base)) {
		kvm_err("Need to set vgic cpu and dist addresses first\n");
		ret = -ENXIO;
		goto out;
	}

	ret = kvm_phys_addr_ioremap(kvm, kvm->arch.vgic.vgic_cpu_base,
				    vgic_vcpu_base, KVM_VGIC_V2_CPU_SIZE);
	if (ret) {
		kvm_err("Unable to remap VGIC CPU to VCPU\n");
		goto out;
	}

	for (i = VGIC_NR_PRIVATE_IRQS; i < VGIC_NR_IRQS; i += 4)
		vgic_set_target_reg(kvm, 0, i);

	kvm_timer_init(kvm);
	kvm->arch.vgic.ready = true;
out:
	mutex_unlock(&kvm->lock);
	return ret;
}

int kvm_vgic_create(struct kvm *kvm)
{
	int ret = 0;

	mutex_lock(&kvm->lock);

	if (atomic_read(&kvm->online_vcpus) || kvm->arch.vgic.vctrl_base) {
		ret = -EEXIST;
		goto out;
	}

	spin_lock_init(&kvm->arch.vgic.lock);
	kvm->arch.vgic.vctrl_base = vgic_vctrl_base;
	kvm->arch.vgic.vgic_dist_base = VGIC_ADDR_UNDEF;
	kvm->arch.vgic.vgic_cpu_base = VGIC_ADDR_UNDEF;

out:
	mutex_unlock(&kvm->lock);
	return ret;
}

static bool vgic_ioaddr_overlap(struct kvm *kvm)
{
	phys_addr_t dist = kvm->arch.vgic.vgic_dist_base;
	phys_addr_t cpu = kvm->arch.vgic.vgic_cpu_base;

	if (IS_VGIC_ADDR_UNDEF(dist) || IS_VGIC_ADDR_UNDEF(cpu))
		return 0;
	if ((dist <= cpu && dist + KVM_VGIC_V2_DIST_SIZE > cpu) ||
	    (cpu <= dist && cpu + KVM_VGIC_V2_CPU_SIZE > dist))
		return -EBUSY;
	return 0;
}

static int vgic_ioaddr_assign(struct kvm *kvm, phys_addr_t *ioaddr,
			      phys_addr_t addr, phys_addr_t size)
{
	int ret;

	if (!IS_VGIC_ADDR_UNDEF(*ioaddr))
		return -EEXIST;
	if (addr + size < addr)
		return -EINVAL;

	ret = vgic_ioaddr_overlap(kvm);
	if (ret)
		return ret;
	*ioaddr = addr;
	return ret;
}

int kvm_vgic_set_addr(struct kvm *kvm, unsigned long type, u64 addr)
{
	int r = 0;
	struct vgic_dist *vgic = &kvm->arch.vgic;

	if (addr & ~KVM_PHYS_MASK)
		return -E2BIG;

	if (addr & (SZ_4K - 1))
		return -EINVAL;

	mutex_lock(&kvm->lock);
	switch (type) {
	case KVM_VGIC_V2_ADDR_TYPE_DIST:
		r = vgic_ioaddr_assign(kvm, &vgic->vgic_dist_base,
				       addr, KVM_VGIC_V2_DIST_SIZE);
		break;
	case KVM_VGIC_V2_ADDR_TYPE_CPU:
		r = vgic_ioaddr_assign(kvm, &vgic->vgic_cpu_base,
				       addr, KVM_VGIC_V2_CPU_SIZE);
		break;
	default:
		r = -ENODEV;
	}

	mutex_unlock(&kvm->lock);
	return r;
}
