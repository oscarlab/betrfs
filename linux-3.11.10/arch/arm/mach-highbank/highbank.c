/*
 * Copyright 2010-2011 Calxeda, Inc.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms and conditions of the GNU General Public License,
 * version 2, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#include <linux/clk.h>
#include <linux/clkdev.h>
#include <linux/clocksource.h>
#include <linux/dma-mapping.h>
#include <linux/io.h>
#include <linux/irq.h>
#include <linux/irqchip.h>
#include <linux/irqdomain.h>
#include <linux/of.h>
#include <linux/of_irq.h>
#include <linux/of_platform.h>
#include <linux/of_address.h>
#include <linux/smp.h>
#include <linux/amba/bus.h>
#include <linux/clk-provider.h>

#include <asm/cacheflush.h>
#include <asm/cputype.h>
#include <asm/smp_plat.h>
#include <asm/hardware/cache-l2x0.h>
#include <asm/mach/arch.h>
#include <asm/mach/map.h>
#include <asm/mach/time.h>

#include "core.h"
#include "sysregs.h"

void __iomem *sregs_base;
void __iomem *scu_base_addr;

static void __init highbank_scu_map_io(void)
{
	unsigned long base;

	/* Get SCU base */
	asm("mrc p15, 4, %0, c15, c0, 0" : "=r" (base));

	scu_base_addr = ioremap(base, SZ_4K);
}

#define HB_JUMP_TABLE_PHYS(cpu)		(0x40 + (0x10 * (cpu)))
#define HB_JUMP_TABLE_VIRT(cpu)		phys_to_virt(HB_JUMP_TABLE_PHYS(cpu))

void highbank_set_cpu_jump(int cpu, void *jump_addr)
{
	cpu = MPIDR_AFFINITY_LEVEL(cpu_logical_map(cpu), 0);
	writel(virt_to_phys(jump_addr), HB_JUMP_TABLE_VIRT(cpu));
	__cpuc_flush_dcache_area(HB_JUMP_TABLE_VIRT(cpu), 16);
	outer_clean_range(HB_JUMP_TABLE_PHYS(cpu),
			  HB_JUMP_TABLE_PHYS(cpu) + 15);
}

#ifdef CONFIG_CACHE_L2X0
static void highbank_l2x0_disable(void)
{
	/* Disable PL310 L2 Cache controller */
	highbank_smc1(0x102, 0x0);
}
#endif

static void __init highbank_init_irq(void)
{
	irqchip_init();

	if (of_find_compatible_node(NULL, NULL, "arm,cortex-a9"))
		highbank_scu_map_io();

#ifdef CONFIG_CACHE_L2X0
	/* Enable PL310 L2 Cache controller */
	highbank_smc1(0x102, 0x1);
	l2x0_of_init(0, ~0UL);
	outer_cache.disable = highbank_l2x0_disable;
#endif
}

static void __init highbank_timer_init(void)
{
	struct device_node *np;

	/* Map system registers */
	np = of_find_compatible_node(NULL, NULL, "calxeda,hb-sregs");
	sregs_base = of_iomap(np, 0);
	WARN_ON(!sregs_base);

	of_clk_init(NULL);

	clocksource_of_init();
}

static void highbank_power_off(void)
{
	highbank_set_pwr_shutdown();

	while (1)
		cpu_do_idle();
}

static int highbank_platform_notifier(struct notifier_block *nb,
				  unsigned long event, void *__dev)
{
	struct resource *res;
	int reg = -1;
	u32 val;
	struct device *dev = __dev;

	if (event != BUS_NOTIFY_ADD_DEVICE)
		return NOTIFY_DONE;

	if (of_device_is_compatible(dev->of_node, "calxeda,hb-ahci"))
		reg = 0xc;
	else if (of_device_is_compatible(dev->of_node, "calxeda,hb-sdhci"))
		reg = 0x18;
	else if (of_device_is_compatible(dev->of_node, "arm,pl330"))
		reg = 0x20;
	else if (of_device_is_compatible(dev->of_node, "calxeda,hb-xgmac")) {
		res = platform_get_resource(to_platform_device(dev),
					    IORESOURCE_MEM, 0);
		if (res) {
			if (res->start == 0xfff50000)
				reg = 0;
			else if (res->start == 0xfff51000)
				reg = 4;
		}
	}

	if (reg < 0)
		return NOTIFY_DONE;

	if (of_property_read_bool(dev->of_node, "dma-coherent")) {
		val = readl(sregs_base + reg);
		writel(val | 0xff01, sregs_base + reg);
		set_dma_ops(dev, &arm_coherent_dma_ops);
	}

	return NOTIFY_OK;
}

static struct notifier_block highbank_amba_nb = {
	.notifier_call = highbank_platform_notifier,
};

static struct notifier_block highbank_platform_nb = {
	.notifier_call = highbank_platform_notifier,
};

static void __init highbank_init(void)
{
	pm_power_off = highbank_power_off;
	highbank_pm_init();

	bus_register_notifier(&platform_bus_type, &highbank_platform_nb);
	bus_register_notifier(&amba_bustype, &highbank_amba_nb);

	of_platform_populate(NULL, of_default_bus_match_table, NULL, NULL);
}

static const char *highbank_match[] __initconst = {
	"calxeda,highbank",
	"calxeda,ecx-2000",
	NULL,
};

DT_MACHINE_START(HIGHBANK, "Highbank")
	.smp		= smp_ops(highbank_smp_ops),
	.init_irq	= highbank_init_irq,
	.init_time	= highbank_timer_init,
	.init_machine	= highbank_init,
	.dt_compat	= highbank_match,
	.restart	= highbank_restart,
MACHINE_END
