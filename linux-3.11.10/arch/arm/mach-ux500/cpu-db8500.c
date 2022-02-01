/*
 * Copyright (C) 2008-2009 ST-Ericsson SA
 *
 * Author: Srinidhi KASAGAR <srinidhi.kasagar@stericsson.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2, as
 * published by the Free Software Foundation.
 *
 */
#include <linux/types.h>
#include <linux/init.h>
#include <linux/device.h>
#include <linux/amba/bus.h>
#include <linux/interrupt.h>
#include <linux/irq.h>
#include <linux/platform_device.h>
#include <linux/io.h>
#include <linux/mfd/abx500/ab8500.h>
#include <linux/mfd/dbx500-prcmu.h>
#include <linux/of.h>
#include <linux/of_platform.h>
#include <linux/regulator/machine.h>
#include <linux/platform_data/pinctrl-nomadik.h>
#include <linux/random.h>

#include <asm/pmu.h>
#include <asm/mach/map.h>
#include <asm/mach/arch.h>

#include "setup.h"
#include "devices.h"
#include "irqs.h"

#include "devices-db8500.h"
#include "ste-dma40-db8500.h"
#include "db8500-regs.h"
#include "board-mop500.h"
#include "id.h"

/* minimum static i/o mapping required to boot U8500 platforms */
static struct map_desc u8500_uart_io_desc[] __initdata = {
	__IO_DEV_DESC(U8500_UART0_BASE, SZ_4K),
	__IO_DEV_DESC(U8500_UART2_BASE, SZ_4K),
};
/*  U8500 and U9540 common io_desc */
static struct map_desc u8500_common_io_desc[] __initdata = {
	/* SCU base also covers GIC CPU BASE and TWD with its 4K page */
	__IO_DEV_DESC(U8500_SCU_BASE, SZ_4K),
	__IO_DEV_DESC(U8500_GIC_DIST_BASE, SZ_4K),
	__IO_DEV_DESC(U8500_L2CC_BASE, SZ_4K),
	__IO_DEV_DESC(U8500_MTU0_BASE, SZ_4K),
	__IO_DEV_DESC(U8500_BACKUPRAM0_BASE, SZ_8K),

	__IO_DEV_DESC(U8500_CLKRST1_BASE, SZ_4K),
	__IO_DEV_DESC(U8500_CLKRST2_BASE, SZ_4K),
	__IO_DEV_DESC(U8500_CLKRST3_BASE, SZ_4K),
	__IO_DEV_DESC(U8500_CLKRST5_BASE, SZ_4K),
	__IO_DEV_DESC(U8500_CLKRST6_BASE, SZ_4K),

	__IO_DEV_DESC(U8500_GPIO0_BASE, SZ_4K),
	__IO_DEV_DESC(U8500_GPIO1_BASE, SZ_4K),
	__IO_DEV_DESC(U8500_GPIO2_BASE, SZ_4K),
	__IO_DEV_DESC(U8500_GPIO3_BASE, SZ_4K),
};

/* U8500 IO map specific description */
static struct map_desc u8500_io_desc[] __initdata = {
	__IO_DEV_DESC(U8500_PRCMU_BASE, SZ_4K),
	__IO_DEV_DESC(U8500_PRCMU_TCDM_BASE, SZ_4K),

};

/* U9540 IO map specific description */
static struct map_desc u9540_io_desc[] __initdata = {
	__IO_DEV_DESC(U8500_PRCMU_BASE, SZ_4K + SZ_8K),
	__IO_DEV_DESC(U8500_PRCMU_TCDM_BASE, SZ_4K + SZ_8K),
};

void __init u8500_map_io(void)
{
	/*
	 * Map the UARTs early so that the DEBUG_LL stuff continues to work.
	 */
	iotable_init(u8500_uart_io_desc, ARRAY_SIZE(u8500_uart_io_desc));

	ux500_map_io();

	iotable_init(u8500_common_io_desc, ARRAY_SIZE(u8500_common_io_desc));

	if (cpu_is_ux540_family())
		iotable_init(u9540_io_desc, ARRAY_SIZE(u9540_io_desc));
	else
		iotable_init(u8500_io_desc, ARRAY_SIZE(u8500_io_desc));
}

static struct resource db8500_pmu_resources[] = {
	[0] = {
		.start		= IRQ_DB8500_PMU,
		.end		= IRQ_DB8500_PMU,
		.flags		= IORESOURCE_IRQ,
	},
};

/*
 * The PMU IRQ lines of two cores are wired together into a single interrupt.
 * Bounce the interrupt to the other core if it's not ours.
 */
static irqreturn_t db8500_pmu_handler(int irq, void *dev, irq_handler_t handler)
{
	irqreturn_t ret = handler(irq, dev);
	int other = !smp_processor_id();

	if (ret == IRQ_NONE && cpu_online(other))
		irq_set_affinity(irq, cpumask_of(other));

	/*
	 * We should be able to get away with the amount of IRQ_NONEs we give,
	 * while still having the spurious IRQ detection code kick in if the
	 * interrupt really starts hitting spuriously.
	 */
	return ret;
}

struct arm_pmu_platdata db8500_pmu_platdata = {
	.handle_irq		= db8500_pmu_handler,
};

static struct platform_device db8500_pmu_device = {
	.name			= "arm-pmu",
	.id			= -1,
	.num_resources		= ARRAY_SIZE(db8500_pmu_resources),
	.resource		= db8500_pmu_resources,
	.dev.platform_data	= &db8500_pmu_platdata,
};

static struct platform_device *platform_devs[] __initdata = {
	&u8500_dma40_device,
	&db8500_pmu_device,
};

static resource_size_t __initdata db8500_gpio_base[] = {
	U8500_GPIOBANK0_BASE,
	U8500_GPIOBANK1_BASE,
	U8500_GPIOBANK2_BASE,
	U8500_GPIOBANK3_BASE,
	U8500_GPIOBANK4_BASE,
	U8500_GPIOBANK5_BASE,
	U8500_GPIOBANK6_BASE,
	U8500_GPIOBANK7_BASE,
	U8500_GPIOBANK8_BASE,
};

static void __init db8500_add_gpios(struct device *parent)
{
	struct nmk_gpio_platform_data pdata = {
		.supports_sleepmode = true,
	};

	dbx500_add_gpios(parent, ARRAY_AND_SIZE(db8500_gpio_base),
			 IRQ_DB8500_GPIO0, &pdata);
	dbx500_add_pinctrl(parent, "pinctrl-db8500", U8500_PRCMU_BASE);
}

static int usb_db8500_dma_cfg[] = {
	DB8500_DMA_DEV38_USB_OTG_IEP_AND_OEP_1_9,
	DB8500_DMA_DEV37_USB_OTG_IEP_AND_OEP_2_10,
	DB8500_DMA_DEV36_USB_OTG_IEP_AND_OEP_3_11,
	DB8500_DMA_DEV19_USB_OTG_IEP_AND_OEP_4_12,
	DB8500_DMA_DEV18_USB_OTG_IEP_AND_OEP_5_13,
	DB8500_DMA_DEV17_USB_OTG_IEP_AND_OEP_6_14,
	DB8500_DMA_DEV16_USB_OTG_IEP_AND_OEP_7_15,
	DB8500_DMA_DEV39_USB_OTG_IEP_AND_OEP_8
};

static const char *db8500_read_soc_id(void)
{
	void __iomem *uid = __io_address(U8500_BB_UID_BASE);

	/* Throw these device-specific numbers into the entropy pool */
	add_device_randomness(uid, 0x14);
	return kasprintf(GFP_KERNEL, "%08x%08x%08x%08x%08x",
			 readl((u32 *)uid+0),
			 readl((u32 *)uid+1), readl((u32 *)uid+2),
			 readl((u32 *)uid+3), readl((u32 *)uid+4));
}

static struct device * __init db8500_soc_device_init(void)
{
	const char *soc_id = db8500_read_soc_id();

	return ux500_soc_device_init(soc_id);
}

/*
 * This function is called from the board init
 */
struct device * __init u8500_init_devices(void)
{
	struct device *parent;
	int i;

	parent = db8500_soc_device_init();

	db8500_add_rtc(parent);
	db8500_add_gpios(parent);
	db8500_add_usb(parent, usb_db8500_dma_cfg, usb_db8500_dma_cfg);

	for (i = 0; i < ARRAY_SIZE(platform_devs); i++)
		platform_devs[i]->dev.parent = parent;

	platform_add_devices(platform_devs, ARRAY_SIZE(platform_devs));

	return parent;
}

#ifdef CONFIG_MACH_UX500_DT
static struct of_dev_auxdata u8500_auxdata_lookup[] __initdata = {
	/* Requires call-back bindings. */
	OF_DEV_AUXDATA("arm,cortex-a9-pmu", 0, "arm-pmu", &db8500_pmu_platdata),
	/* Requires DMA bindings. */
	OF_DEV_AUXDATA("arm,pl011", 0x80120000, "uart0", NULL),
	OF_DEV_AUXDATA("arm,pl011", 0x80121000, "uart1", NULL),
	OF_DEV_AUXDATA("arm,pl011", 0x80007000, "uart2", NULL),
	OF_DEV_AUXDATA("arm,pl022", 0x80002000, "ssp0",  &ssp0_plat),
	OF_DEV_AUXDATA("arm,pl18x", 0x80126000, "sdi0",  &mop500_sdi0_data),
	OF_DEV_AUXDATA("arm,pl18x", 0x80118000, "sdi1",  &mop500_sdi1_data),
	OF_DEV_AUXDATA("arm,pl18x", 0x80005000, "sdi2",  &mop500_sdi2_data),
	OF_DEV_AUXDATA("arm,pl18x", 0x80114000, "sdi4",  &mop500_sdi4_data),
	/* Requires clock name bindings. */
	OF_DEV_AUXDATA("st,nomadik-gpio", 0x8012e000, "gpio.0", NULL),
	OF_DEV_AUXDATA("st,nomadik-gpio", 0x8012e080, "gpio.1", NULL),
	OF_DEV_AUXDATA("st,nomadik-gpio", 0x8000e000, "gpio.2", NULL),
	OF_DEV_AUXDATA("st,nomadik-gpio", 0x8000e080, "gpio.3", NULL),
	OF_DEV_AUXDATA("st,nomadik-gpio", 0x8000e100, "gpio.4", NULL),
	OF_DEV_AUXDATA("st,nomadik-gpio", 0x8000e180, "gpio.5", NULL),
	OF_DEV_AUXDATA("st,nomadik-gpio", 0x8011e000, "gpio.6", NULL),
	OF_DEV_AUXDATA("st,nomadik-gpio", 0x8011e080, "gpio.7", NULL),
	OF_DEV_AUXDATA("st,nomadik-gpio", 0xa03fe000, "gpio.8", NULL),
	OF_DEV_AUXDATA("st,nomadik-i2c", 0x80004000, "nmk-i2c.0", NULL),
	OF_DEV_AUXDATA("st,nomadik-i2c", 0x80122000, "nmk-i2c.1", NULL),
	OF_DEV_AUXDATA("st,nomadik-i2c", 0x80128000, "nmk-i2c.2", NULL),
	OF_DEV_AUXDATA("st,nomadik-i2c", 0x80110000, "nmk-i2c.3", NULL),
	OF_DEV_AUXDATA("st,nomadik-i2c", 0x8012a000, "nmk-i2c.4", NULL),
	OF_DEV_AUXDATA("stericsson,db8500-musb", 0xa03e0000, "musb-ux500.0", NULL),
	OF_DEV_AUXDATA("stericsson,db8500-prcmu", 0x80157000, "db8500-prcmu",
			&db8500_prcmu_pdata),
	OF_DEV_AUXDATA("smsc,lan9115", 0x50000000, "smsc911x.0", NULL),
	OF_DEV_AUXDATA("stericsson,ux500-cryp", 0xa03cb000, "cryp1", NULL),
	OF_DEV_AUXDATA("stericsson,ux500-hash", 0xa03c2000, "hash1", NULL),
	OF_DEV_AUXDATA("stericsson,snd-soc-mop500", 0, "snd-soc-mop500.0",
			NULL),
	/* Requires device name bindings. */
	OF_DEV_AUXDATA("stericsson,db8500-pinctrl", U8500_PRCMU_BASE,
		"pinctrl-db8500", NULL),
	/* Requires clock name and DMA bindings. */
	OF_DEV_AUXDATA("stericsson,ux500-msp-i2s", 0x80123000,
		"ux500-msp-i2s.0", &msp0_platform_data),
	OF_DEV_AUXDATA("stericsson,ux500-msp-i2s", 0x80124000,
		"ux500-msp-i2s.1", &msp1_platform_data),
	OF_DEV_AUXDATA("stericsson,ux500-msp-i2s", 0x80117000,
		"ux500-msp-i2s.2", &msp2_platform_data),
	OF_DEV_AUXDATA("stericsson,ux500-msp-i2s", 0x80125000,
		"ux500-msp-i2s.3", &msp3_platform_data),
	/* Requires clock name bindings and channel address lookup table. */
	OF_DEV_AUXDATA("stericsson,db8500-dma40", 0x801C0000, "dma40.0", NULL),
	{},
};

static struct of_dev_auxdata u8540_auxdata_lookup[] __initdata = {
	/* Requires DMA bindings. */
	OF_DEV_AUXDATA("arm,pl011", 0x80120000, "uart0", NULL),
	OF_DEV_AUXDATA("arm,pl011", 0x80121000, "uart1", NULL),
	OF_DEV_AUXDATA("arm,pl011", 0x80007000, "uart2", NULL),
	OF_DEV_AUXDATA("stericsson,db8500-prcmu", 0x80157000, "db8500-prcmu",
			&db8500_prcmu_pdata),
	{},
};

static const struct of_device_id u8500_local_bus_nodes[] = {
	/* only create devices below soc node */
	{ .compatible = "stericsson,db8500", },
	{ .compatible = "stericsson,db8500-prcmu", },
	{ .compatible = "simple-bus"},
	{ },
};

static void __init u8500_init_machine(void)
{
	struct device *parent = db8500_soc_device_init();

	/* Pinmaps must be in place before devices register */
	if (of_machine_is_compatible("st-ericsson,mop500"))
		mop500_pinmaps_init();
	else if (of_machine_is_compatible("calaosystems,snowball-a9500")) {
		snowball_pinmaps_init();
	} else if (of_machine_is_compatible("st-ericsson,hrefv60+"))
		hrefv60_pinmaps_init();
	else if (of_machine_is_compatible("st-ericsson,ccu9540")) {}
		/* TODO: Add pinmaps for ccu9540 board. */

	/* automatically probe child nodes of dbx5x0 devices */
	if (of_machine_is_compatible("st-ericsson,u8540"))
		of_platform_populate(NULL, u8500_local_bus_nodes,
				     u8540_auxdata_lookup, parent);
	else
		of_platform_populate(NULL, u8500_local_bus_nodes,
				     u8500_auxdata_lookup, parent);
}

static const char * stericsson_dt_platform_compat[] = {
	"st-ericsson,u8500",
	"st-ericsson,u8540",
	"st-ericsson,u9500",
	"st-ericsson,u9540",
	NULL,
};

DT_MACHINE_START(U8500_DT, "ST-Ericsson Ux5x0 platform (Device Tree Support)")
	.smp            = smp_ops(ux500_smp_ops),
	.map_io		= u8500_map_io,
	.init_irq	= ux500_init_irq,
	/* we re-use nomadik timer here */
	.init_time	= ux500_timer_init,
	.init_machine	= u8500_init_machine,
	.init_late	= NULL,
	.dt_compat      = stericsson_dt_platform_compat,
MACHINE_END

#endif
