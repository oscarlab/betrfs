/*
 * r8a7778 processor support
 *
 * Copyright (C) 2013  Renesas Solutions Corp.
 * Copyright (C) 2013  Kuninori Morimoto <kuninori.morimoto.gx@renesas.com>
 * Copyright (C) 2013  Cogent Embedded, Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 2 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

#include <linux/kernel.h>
#include <linux/io.h>
#include <linux/irqchip/arm-gic.h>
#include <linux/of.h>
#include <linux/of_platform.h>
#include <linux/platform_data/gpio-rcar.h>
#include <linux/platform_data/irq-renesas-intc-irqpin.h>
#include <linux/platform_device.h>
#include <linux/irqchip.h>
#include <linux/serial_sci.h>
#include <linux/sh_timer.h>
#include <linux/pm_runtime.h>
#include <linux/usb/phy.h>
#include <linux/usb/hcd.h>
#include <linux/usb/ehci_pdriver.h>
#include <linux/usb/ohci_pdriver.h>
#include <linux/dma-mapping.h>
#include <mach/irqs.h>
#include <mach/r8a7778.h>
#include <mach/common.h>
#include <asm/mach/arch.h>
#include <asm/hardware/cache-l2x0.h>

/* SCIF */
#define SCIF_INFO(baseaddr, irq)				\
{								\
	.mapbase	= baseaddr,				\
	.flags		= UPF_BOOT_AUTOCONF | UPF_IOREMAP,	\
	.scscr		= SCSCR_RE | SCSCR_TE | SCSCR_CKE1,	\
	.scbrr_algo_id	= SCBRR_ALGO_2,				\
	.type		= PORT_SCIF,				\
	.irqs		= SCIx_IRQ_MUXED(irq),			\
}

static struct plat_sci_port scif_platform_data[] = {
	SCIF_INFO(0xffe40000, gic_iid(0x66)),
	SCIF_INFO(0xffe41000, gic_iid(0x67)),
	SCIF_INFO(0xffe42000, gic_iid(0x68)),
	SCIF_INFO(0xffe43000, gic_iid(0x69)),
	SCIF_INFO(0xffe44000, gic_iid(0x6a)),
	SCIF_INFO(0xffe45000, gic_iid(0x6b)),
};

/* TMU */
static struct resource sh_tmu0_resources[] = {
	DEFINE_RES_MEM(0xffd80008, 12),
	DEFINE_RES_IRQ(gic_iid(0x40)),
};

static struct sh_timer_config sh_tmu0_platform_data = {
	.name			= "TMU00",
	.channel_offset		= 0x4,
	.timer_bit		= 0,
	.clockevent_rating	= 200,
};

static struct resource sh_tmu1_resources[] = {
	DEFINE_RES_MEM(0xffd80014, 12),
	DEFINE_RES_IRQ(gic_iid(0x41)),
};

static struct sh_timer_config sh_tmu1_platform_data = {
	.name			= "TMU01",
	.channel_offset		= 0x10,
	.timer_bit		= 1,
	.clocksource_rating	= 200,
};

#define r8a7778_register_tmu(idx)			\
	platform_device_register_resndata(		\
		&platform_bus, "sh_tmu", idx,		\
		sh_tmu##idx##_resources,		\
		ARRAY_SIZE(sh_tmu##idx##_resources),	\
		&sh_tmu##idx##_platform_data,		\
		sizeof(sh_tmu##idx##_platform_data))

/* USB PHY */
static struct resource usb_phy_resources[] __initdata = {
	DEFINE_RES_MEM(0xffe70800, 0x100),
	DEFINE_RES_MEM(0xffe76000, 0x100),
};

void __init r8a7778_add_usb_phy_device(struct rcar_phy_platform_data *pdata)
{
	platform_device_register_resndata(&platform_bus, "rcar_usb_phy", -1,
					  usb_phy_resources,
					  ARRAY_SIZE(usb_phy_resources),
					  pdata, sizeof(*pdata));
}

/* USB */
static struct usb_phy *phy;

static int usb_power_on(struct platform_device *pdev)
{
	if (IS_ERR(phy))
		return PTR_ERR(phy);

	pm_runtime_enable(&pdev->dev);
	pm_runtime_get_sync(&pdev->dev);

	usb_phy_init(phy);

	return 0;
}

static void usb_power_off(struct platform_device *pdev)
{
	if (IS_ERR(phy))
		return;

	usb_phy_shutdown(phy);

	pm_runtime_put_sync(&pdev->dev);
	pm_runtime_disable(&pdev->dev);
}

static int ehci_init_internal_buffer(struct usb_hcd *hcd)
{
	/*
	 * Below are recommended values from the datasheet;
	 * see [USB :: Setting of EHCI Internal Buffer].
	 */
	/* EHCI IP internal buffer setting */
	iowrite32(0x00ff0040, hcd->regs + 0x0094);
	/* EHCI IP internal buffer enable */
	iowrite32(0x00000001, hcd->regs + 0x009C);

	return 0;
}

static struct usb_ehci_pdata ehci_pdata __initdata = {
	.power_on	= usb_power_on,
	.power_off	= usb_power_off,
	.power_suspend	= usb_power_off,
	.pre_setup	= ehci_init_internal_buffer,
};

static struct resource ehci_resources[] __initdata = {
	DEFINE_RES_MEM(0xffe70000, 0x400),
	DEFINE_RES_IRQ(gic_iid(0x4c)),
};

static struct usb_ohci_pdata ohci_pdata __initdata = {
	.power_on	= usb_power_on,
	.power_off	= usb_power_off,
	.power_suspend	= usb_power_off,
};

static struct resource ohci_resources[] __initdata = {
	DEFINE_RES_MEM(0xffe70400, 0x400),
	DEFINE_RES_IRQ(gic_iid(0x4c)),
};

#define USB_PLATFORM_INFO(hci)					\
static struct platform_device_info hci##_info __initdata = {	\
	.parent		= &platform_bus,			\
	.name		= #hci "-platform",			\
	.id		= -1,					\
	.res		= hci##_resources,			\
	.num_res	= ARRAY_SIZE(hci##_resources),		\
	.data		= &hci##_pdata,				\
	.size_data	= sizeof(hci##_pdata),			\
	.dma_mask	= DMA_BIT_MASK(32),			\
}

USB_PLATFORM_INFO(ehci);
USB_PLATFORM_INFO(ohci);

/* Ether */
static struct resource ether_resources[] = {
	DEFINE_RES_MEM(0xfde00000, 0x400),
	DEFINE_RES_IRQ(gic_iid(0x89)),
};

void __init r8a7778_add_ether_device(struct sh_eth_plat_data *pdata)
{
	platform_device_register_resndata(&platform_bus, "r8a777x-ether", -1,
					  ether_resources,
					  ARRAY_SIZE(ether_resources),
					  pdata, sizeof(*pdata));
}

/* PFC/GPIO */
static struct resource pfc_resources[] = {
	DEFINE_RES_MEM(0xfffc0000, 0x118),
};

#define R8A7778_GPIO(idx)						\
static struct resource r8a7778_gpio##idx##_resources[] = {		\
	DEFINE_RES_MEM(0xffc40000 + 0x1000 * (idx), 0x30),		\
	DEFINE_RES_IRQ(gic_iid(0x87)),					\
};									\
									\
static struct gpio_rcar_config r8a7778_gpio##idx##_platform_data = {	\
	.gpio_base	= 32 * (idx),					\
	.irq_base	= GPIO_IRQ_BASE(idx),				\
	.number_of_pins	= 32,						\
	.pctl_name	= "pfc-r8a7778",				\
}

R8A7778_GPIO(0);
R8A7778_GPIO(1);
R8A7778_GPIO(2);
R8A7778_GPIO(3);
R8A7778_GPIO(4);

#define r8a7778_register_gpio(idx)				\
	platform_device_register_resndata(			\
		&platform_bus, "gpio_rcar", idx,		\
		r8a7778_gpio##idx##_resources,			\
		ARRAY_SIZE(r8a7778_gpio##idx##_resources),	\
		&r8a7778_gpio##idx##_platform_data,		\
		sizeof(r8a7778_gpio##idx##_platform_data))

void __init r8a7778_pinmux_init(void)
{
	platform_device_register_simple(
		"pfc-r8a7778", -1,
		pfc_resources,
		ARRAY_SIZE(pfc_resources));

	r8a7778_register_gpio(0);
	r8a7778_register_gpio(1);
	r8a7778_register_gpio(2);
	r8a7778_register_gpio(3);
	r8a7778_register_gpio(4);
};

/* SDHI */
static struct resource sdhi_resources[] = {
	/* SDHI0 */
	DEFINE_RES_MEM(0xFFE4C000, 0x100),
	DEFINE_RES_IRQ(gic_iid(0x77)),
	/* SDHI1 */
	DEFINE_RES_MEM(0xFFE4D000, 0x100),
	DEFINE_RES_IRQ(gic_iid(0x78)),
	/* SDHI2 */
	DEFINE_RES_MEM(0xFFE4F000, 0x100),
	DEFINE_RES_IRQ(gic_iid(0x76)),
};

void __init r8a7778_sdhi_init(int id,
			      struct sh_mobile_sdhi_info *info)
{
	BUG_ON(id < 0 || id > 2);

	platform_device_register_resndata(
		&platform_bus, "sh_mobile_sdhi", id,
		sdhi_resources + (2 * id), 2,
		info, sizeof(*info));
}

/* I2C */
static struct resource i2c_resources[] __initdata = {
	/* I2C0 */
	DEFINE_RES_MEM(0xffc70000, 0x1000),
	DEFINE_RES_IRQ(gic_iid(0x63)),
	/* I2C1 */
	DEFINE_RES_MEM(0xffc71000, 0x1000),
	DEFINE_RES_IRQ(gic_iid(0x6e)),
	/* I2C2 */
	DEFINE_RES_MEM(0xffc72000, 0x1000),
	DEFINE_RES_IRQ(gic_iid(0x6c)),
	/* I2C3 */
	DEFINE_RES_MEM(0xffc73000, 0x1000),
	DEFINE_RES_IRQ(gic_iid(0x6d)),
};

void __init r8a7778_add_i2c_device(int id)
{
	BUG_ON(id < 0 || id > 3);

	platform_device_register_simple(
		"i2c-rcar", id,
		i2c_resources + (2 * id), 2);
}

/* HSPI */
static struct resource hspi_resources[] __initdata = {
	/* HSPI0 */
	DEFINE_RES_MEM(0xfffc7000, 0x18),
	DEFINE_RES_IRQ(gic_iid(0x5f)),
	/* HSPI1 */
	DEFINE_RES_MEM(0xfffc8000, 0x18),
	DEFINE_RES_IRQ(gic_iid(0x74)),
	/* HSPI2 */
	DEFINE_RES_MEM(0xfffc6000, 0x18),
	DEFINE_RES_IRQ(gic_iid(0x75)),
};

void __init r8a7778_add_hspi_device(int id)
{
	BUG_ON(id < 0 || id > 2);

	platform_device_register_simple(
		"sh-hspi", id,
		hspi_resources + (2 * id), 2);
}

/* MMC */
static struct resource mmc_resources[] __initdata = {
	DEFINE_RES_MEM(0xffe4e000, 0x100),
	DEFINE_RES_IRQ(gic_iid(0x5d)),
};

void __init r8a7778_add_mmc_device(struct sh_mmcif_plat_data *info)
{
	platform_device_register_resndata(
		&platform_bus, "sh_mmcif", -1,
		mmc_resources, ARRAY_SIZE(mmc_resources),
		info, sizeof(*info));
}

void __init r8a7778_add_standard_devices(void)
{
	int i;

#ifdef CONFIG_CACHE_L2X0
	void __iomem *base = ioremap_nocache(0xf0100000, 0x1000);
	if (base) {
		/*
		 * Early BRESP enable, Shared attribute override enable, 64K*16way
		 * don't call iounmap(base)
		 */
		l2x0_init(base, 0x40470000, 0x82000fff);
	}
#endif

	for (i = 0; i < ARRAY_SIZE(scif_platform_data); i++)
		platform_device_register_data(&platform_bus, "sh-sci", i,
					      &scif_platform_data[i],
					      sizeof(struct plat_sci_port));

	r8a7778_register_tmu(0);
	r8a7778_register_tmu(1);
}

void __init r8a7778_init_late(void)
{
	phy = usb_get_phy(USB_PHY_TYPE_USB2);

	platform_device_register_full(&ehci_info);
	platform_device_register_full(&ohci_info);
}

static struct renesas_intc_irqpin_config irqpin_platform_data = {
	.irq_base = irq_pin(0), /* IRQ0 -> IRQ3 */
	.sense_bitfield_width = 2,
};

static struct resource irqpin_resources[] = {
	DEFINE_RES_MEM(0xfe78001c, 4), /* ICR1 */
	DEFINE_RES_MEM(0xfe780010, 4), /* INTPRI */
	DEFINE_RES_MEM(0xfe780024, 4), /* INTREQ */
	DEFINE_RES_MEM(0xfe780044, 4), /* INTMSK0 */
	DEFINE_RES_MEM(0xfe780064, 4), /* INTMSKCLR0 */
	DEFINE_RES_IRQ(gic_iid(0x3b)), /* IRQ0 */
	DEFINE_RES_IRQ(gic_iid(0x3c)), /* IRQ1 */
	DEFINE_RES_IRQ(gic_iid(0x3d)), /* IRQ2 */
	DEFINE_RES_IRQ(gic_iid(0x3e)), /* IRQ3 */
};

void __init r8a7778_init_irq_extpin(int irlm)
{
	void __iomem *icr0 = ioremap_nocache(0xfe780000, PAGE_SIZE);
	unsigned long tmp;

	if (!icr0) {
		pr_warn("r8a7778: unable to setup external irq pin mode\n");
		return;
	}

	tmp = ioread32(icr0);
	if (irlm)
		tmp |= 1 << 23; /* IRQ0 -> IRQ3 as individual pins */
	else
		tmp &= ~(1 << 23); /* IRL mode - not supported */
	tmp |= (1 << 21); /* LVLMODE = 1 */
	iowrite32(tmp, icr0);
	iounmap(icr0);

	if (irlm)
		platform_device_register_resndata(
			&platform_bus, "renesas_intc_irqpin", -1,
			irqpin_resources, ARRAY_SIZE(irqpin_resources),
			&irqpin_platform_data, sizeof(irqpin_platform_data));
}

#define INT2SMSKCR0	0x82288 /* 0xfe782288 */
#define INT2SMSKCR1	0x8228c /* 0xfe78228c */

#define INT2NTSR0	0x00018 /* 0xfe700018 */
#define INT2NTSR1	0x0002c /* 0xfe70002c */
static void __init r8a7778_init_irq_common(void)
{
	void __iomem *base = ioremap_nocache(0xfe700000, 0x00100000);

	BUG_ON(!base);

	/* route all interrupts to ARM */
	__raw_writel(0x73ffffff, base + INT2NTSR0);
	__raw_writel(0xffffffff, base + INT2NTSR1);

	/* unmask all known interrupts in INTCS2 */
	__raw_writel(0x08330773, base + INT2SMSKCR0);
	__raw_writel(0x00311110, base + INT2SMSKCR1);

	iounmap(base);
}

void __init r8a7778_init_irq(void)
{
	void __iomem *gic_dist_base;
	void __iomem *gic_cpu_base;

	gic_dist_base = ioremap_nocache(0xfe438000, PAGE_SIZE);
	gic_cpu_base  = ioremap_nocache(0xfe430000, PAGE_SIZE);
	BUG_ON(!gic_dist_base || !gic_cpu_base);

	/* use GIC to handle interrupts */
	gic_init(0, 29, gic_dist_base, gic_cpu_base);

	r8a7778_init_irq_common();
}

void __init r8a7778_init_delay(void)
{
	shmobile_setup_delay(800, 1, 3); /* Cortex-A9 @ 800MHz */
}

#ifdef CONFIG_USE_OF
void __init r8a7778_init_irq_dt(void)
{
	irqchip_init();
	r8a7778_init_irq_common();
}

static const struct of_dev_auxdata r8a7778_auxdata_lookup[] __initconst = {
	{},
};

void __init r8a7778_add_standard_devices_dt(void)
{
	of_platform_populate(NULL, of_default_bus_match_table,
			     r8a7778_auxdata_lookup, NULL);
}

static const char *r8a7778_compat_dt[] __initdata = {
	"renesas,r8a7778",
	NULL,
};

DT_MACHINE_START(R8A7778_DT, "Generic R8A7778 (Flattened Device Tree)")
	.init_early	= r8a7778_init_delay,
	.init_irq	= r8a7778_init_irq_dt,
	.init_machine	= r8a7778_add_standard_devices_dt,
	.init_time	= shmobile_timer_init,
	.dt_compat	= r8a7778_compat_dt,
	.init_late      = r8a7778_init_late,
MACHINE_END

#endif /* CONFIG_USE_OF */
