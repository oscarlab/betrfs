/*
 *
 * Copyright (C) 2010 Eric Bénard <eric@eukrea.com>
 *
 * based on board-mx51_babbage.c which is
 * Copyright 2009 Freescale Semiconductor, Inc. All Rights Reserved.
 * Copyright (C) 2009-2010 Amit Kucheria <amit.kucheria@canonical.com>
 *
 * The code contained herein is licensed under the GNU General Public
 * License. You may obtain a copy of the GNU General Public License
 * Version 2 or later at the following locations:
 *
 * http://www.opensource.org/licenses/gpl-license.html
 * http://www.gnu.org/copyleft/gpl.html
 */

#include <linux/init.h>
#include <linux/platform_device.h>
#include <linux/i2c.h>
#include <linux/i2c/tsc2007.h>
#include <linux/gpio.h>
#include <linux/delay.h>
#include <linux/io.h>
#include <linux/interrupt.h>
#include <linux/i2c-gpio.h>
#include <linux/spi/spi.h>
#include <linux/can/platform/mcp251x.h>

#include <asm/setup.h>
#include <asm/mach-types.h>
#include <asm/mach/arch.h>
#include <asm/mach/time.h>

#include "common.h"
#include "devices-imx51.h"
#include "eukrea-baseboards.h"
#include "hardware.h"
#include "iomux-mx51.h"

#define USBH1_RST		IMX_GPIO_NR(2, 28)
#define ETH_RST			IMX_GPIO_NR(2, 31)
#define TSC2007_IRQGPIO_REV2	IMX_GPIO_NR(3, 12)
#define TSC2007_IRQGPIO_REV3	IMX_GPIO_NR(4, 0)
#define CAN_IRQGPIO		IMX_GPIO_NR(1, 1)
#define CAN_RST			IMX_GPIO_NR(4, 15)
#define CAN_NCS			IMX_GPIO_NR(4, 24)
#define CAN_RXOBF_REV2		IMX_GPIO_NR(1, 4)
#define CAN_RXOBF_REV3		IMX_GPIO_NR(3, 12)
#define CAN_RX1BF		IMX_GPIO_NR(1, 6)
#define CAN_TXORTS		IMX_GPIO_NR(1, 7)
#define CAN_TX1RTS		IMX_GPIO_NR(1, 8)
#define CAN_TX2RTS		IMX_GPIO_NR(1, 9)
#define I2C_SCL			IMX_GPIO_NR(4, 16)
#define I2C_SDA			IMX_GPIO_NR(4, 17)

/* USB_CTRL_1 */
#define MX51_USB_CTRL_1_OFFSET		0x10
#define MX51_USB_CTRL_UH1_EXT_CLK_EN	(1 << 25)

#define	MX51_USB_PLLDIV_12_MHZ		0x00
#define	MX51_USB_PLL_DIV_19_2_MHZ	0x01
#define	MX51_USB_PLL_DIV_24_MHZ		0x02

static iomux_v3_cfg_t eukrea_cpuimx51sd_pads[] = {
	/* UART1 */
	MX51_PAD_UART1_RXD__UART1_RXD,
	MX51_PAD_UART1_TXD__UART1_TXD,
	MX51_PAD_UART1_RTS__UART1_RTS,
	MX51_PAD_UART1_CTS__UART1_CTS,

	/* USB HOST1 */
	MX51_PAD_USBH1_CLK__USBH1_CLK,
	MX51_PAD_USBH1_DIR__USBH1_DIR,
	MX51_PAD_USBH1_NXT__USBH1_NXT,
	MX51_PAD_USBH1_DATA0__USBH1_DATA0,
	MX51_PAD_USBH1_DATA1__USBH1_DATA1,
	MX51_PAD_USBH1_DATA2__USBH1_DATA2,
	MX51_PAD_USBH1_DATA3__USBH1_DATA3,
	MX51_PAD_USBH1_DATA4__USBH1_DATA4,
	MX51_PAD_USBH1_DATA5__USBH1_DATA5,
	MX51_PAD_USBH1_DATA6__USBH1_DATA6,
	MX51_PAD_USBH1_DATA7__USBH1_DATA7,
	MX51_PAD_USBH1_STP__USBH1_STP,
	MX51_PAD_EIM_CS3__GPIO2_28,		/* PHY nRESET */

	/* FEC */
	MX51_PAD_EIM_DTACK__GPIO2_31,		/* PHY nRESET */

	/* HSI2C */
	MX51_PAD_I2C1_CLK__GPIO4_16,
	MX51_PAD_I2C1_DAT__GPIO4_17,

	/* I2C1 */
	MX51_PAD_SD2_CMD__I2C1_SCL,
	MX51_PAD_SD2_CLK__I2C1_SDA,

	/* CAN */
	MX51_PAD_CSPI1_MOSI__ECSPI1_MOSI,
	MX51_PAD_CSPI1_MISO__ECSPI1_MISO,
	MX51_PAD_CSPI1_SCLK__ECSPI1_SCLK,
	MX51_PAD_CSPI1_SS0__GPIO4_24,		/* nCS */
	MX51_PAD_CSI2_PIXCLK__GPIO4_15,		/* nReset */
	MX51_PAD_GPIO1_1__GPIO1_1,		/* IRQ */
	MX51_PAD_GPIO1_4__GPIO1_4,		/* Control signals */
	MX51_PAD_GPIO1_6__GPIO1_6,
	MX51_PAD_GPIO1_7__GPIO1_7,
	MX51_PAD_GPIO1_8__GPIO1_8,
	MX51_PAD_GPIO1_9__GPIO1_9,

	/* Touchscreen */
	/* IRQ */
	NEW_PAD_CTRL(MX51_PAD_GPIO_NAND__GPIO_NAND, PAD_CTL_PUS_22K_UP |
			PAD_CTL_PKE | PAD_CTL_SRE_FAST |
			PAD_CTL_DSE_HIGH | PAD_CTL_PUE | PAD_CTL_HYS),
	NEW_PAD_CTRL(MX51_PAD_NANDF_D8__GPIO4_0, PAD_CTL_PUS_22K_UP |
			PAD_CTL_PKE | PAD_CTL_SRE_FAST |
			PAD_CTL_DSE_HIGH | PAD_CTL_PUE | PAD_CTL_HYS),
};

static const struct imxuart_platform_data uart_pdata __initconst = {
	.flags = IMXUART_HAVE_RTSCTS,
};

static int tsc2007_get_pendown_state(void)
{
	if (mx51_revision() < IMX_CHIP_REVISION_3_0)
		return !gpio_get_value(TSC2007_IRQGPIO_REV2);
	else
		return !gpio_get_value(TSC2007_IRQGPIO_REV3);
}

static struct tsc2007_platform_data tsc2007_info = {
	.model			= 2007,
	.x_plate_ohms		= 180,
	.get_pendown_state	= tsc2007_get_pendown_state,
};

static struct i2c_board_info eukrea_cpuimx51sd_i2c_devices[] = {
	{
		I2C_BOARD_INFO("pcf8563", 0x51),
	}, {
		I2C_BOARD_INFO("tsc2007", 0x49),
		.platform_data	= &tsc2007_info,
	},
};

static const struct mxc_nand_platform_data
		eukrea_cpuimx51sd_nand_board_info __initconst = {
	.width		= 1,
	.hw_ecc		= 1,
	.flash_bbt	= 1,
};

/* This function is board specific as the bit mask for the plldiv will also
be different for other Freescale SoCs, thus a common bitmask is not
possible and cannot get place in /plat-mxc/ehci.c.*/
static int initialize_otg_port(struct platform_device *pdev)
{
	u32 v;
	void __iomem *usb_base;
	void __iomem *usbother_base;

	usb_base = ioremap(MX51_USB_OTG_BASE_ADDR, SZ_4K);
	if (!usb_base)
		return -ENOMEM;
	usbother_base = usb_base + MX5_USBOTHER_REGS_OFFSET;

	/* Set the PHY clock to 19.2MHz */
	v = __raw_readl(usbother_base + MXC_USB_PHY_CTR_FUNC2_OFFSET);
	v &= ~MX5_USB_UTMI_PHYCTRL1_PLLDIV_MASK;
	v |= MX51_USB_PLL_DIV_19_2_MHZ;
	__raw_writel(v, usbother_base + MXC_USB_PHY_CTR_FUNC2_OFFSET);
	iounmap(usb_base);

	mdelay(10);

	return mx51_initialize_usb_hw(0, MXC_EHCI_INTERNAL_PHY);
}

static int initialize_usbh1_port(struct platform_device *pdev)
{
	u32 v;
	void __iomem *usb_base;
	void __iomem *usbother_base;

	usb_base = ioremap(MX51_USB_OTG_BASE_ADDR, SZ_4K);
	if (!usb_base)
		return -ENOMEM;
	usbother_base = usb_base + MX5_USBOTHER_REGS_OFFSET;

	/* The clock for the USBH1 ULPI port will come from the PHY. */
	v = __raw_readl(usbother_base + MX51_USB_CTRL_1_OFFSET);
	__raw_writel(v | MX51_USB_CTRL_UH1_EXT_CLK_EN,
			usbother_base + MX51_USB_CTRL_1_OFFSET);
	iounmap(usb_base);

	mdelay(10);

	return mx51_initialize_usb_hw(1, MXC_EHCI_POWER_PINS_ENABLED |
			MXC_EHCI_ITC_NO_THRESHOLD);
}

static const struct mxc_usbh_platform_data dr_utmi_config __initconst = {
	.init		= initialize_otg_port,
	.portsc	= MXC_EHCI_UTMI_16BIT,
};

static const struct fsl_usb2_platform_data usb_pdata __initconst = {
	.operating_mode	= FSL_USB2_DR_DEVICE,
	.phy_mode	= FSL_USB2_PHY_UTMI_WIDE,
};

static const struct mxc_usbh_platform_data usbh1_config __initconst = {
	.init		= initialize_usbh1_port,
	.portsc	= MXC_EHCI_MODE_ULPI,
};

static bool otg_mode_host __initdata;

static int __init eukrea_cpuimx51sd_otg_mode(char *options)
{
	if (!strcmp(options, "host"))
		otg_mode_host = true;
	else if (!strcmp(options, "device"))
		otg_mode_host = false;
	else
		pr_info("otg_mode neither \"host\" nor \"device\". "
			"Defaulting to device\n");
	return 1;
}
__setup("otg_mode=", eukrea_cpuimx51sd_otg_mode);

static struct i2c_gpio_platform_data pdata = {
	.sda_pin		= I2C_SDA,
	.sda_is_open_drain	= 0,
	.scl_pin		= I2C_SCL,
	.scl_is_open_drain	= 0,
	.udelay			= 2,
};

static struct platform_device hsi2c_gpio_device = {
	.name			= "i2c-gpio",
	.id			= 0,
	.dev.platform_data	= &pdata,
};

static struct mcp251x_platform_data mcp251x_info = {
	.oscillator_frequency = 24E6,
};

static struct spi_board_info cpuimx51sd_spi_device[] = {
	{
		.modalias        = "mcp2515",
		.max_speed_hz    = 10000000,
		.bus_num         = 0,
		.mode		= SPI_MODE_0,
		.chip_select     = 0,
		.platform_data   = &mcp251x_info,
		/* irq number is run-time assigned */
	},
};

static int cpuimx51sd_spi1_cs[] = {
	CAN_NCS,
};

static const struct spi_imx_master cpuimx51sd_ecspi1_pdata __initconst = {
	.chipselect	= cpuimx51sd_spi1_cs,
	.num_chipselect	= ARRAY_SIZE(cpuimx51sd_spi1_cs),
};

static struct platform_device *rev2_platform_devices[] __initdata = {
	&hsi2c_gpio_device,
};

static const struct imxi2c_platform_data cpuimx51sd_i2c_data __initconst = {
	.bitrate = 100000,
};

static void __init eukrea_cpuimx51sd_init(void)
{
	imx51_soc_init();

	mxc_iomux_v3_setup_multiple_pads(eukrea_cpuimx51sd_pads,
					ARRAY_SIZE(eukrea_cpuimx51sd_pads));

	imx51_add_imx_uart(0, &uart_pdata);
	imx51_add_mxc_nand(&eukrea_cpuimx51sd_nand_board_info);
	imx51_add_imx2_wdt(0);

	gpio_request(ETH_RST, "eth_rst");
	gpio_set_value(ETH_RST, 1);
	imx51_add_fec(NULL);

	gpio_request(CAN_IRQGPIO, "can_irq");
	gpio_direction_input(CAN_IRQGPIO);
	gpio_free(CAN_IRQGPIO);
	gpio_request(CAN_NCS, "can_ncs");
	gpio_direction_output(CAN_NCS, 1);
	gpio_free(CAN_NCS);
	gpio_request(CAN_RST, "can_rst");
	gpio_direction_output(CAN_RST, 0);
	msleep(20);
	gpio_set_value(CAN_RST, 1);
	imx51_add_ecspi(0, &cpuimx51sd_ecspi1_pdata);
	cpuimx51sd_spi_device[0].irq = gpio_to_irq(CAN_IRQGPIO);
	spi_register_board_info(cpuimx51sd_spi_device,
				ARRAY_SIZE(cpuimx51sd_spi_device));

	if (mx51_revision() < IMX_CHIP_REVISION_3_0) {
		eukrea_cpuimx51sd_i2c_devices[1].irq =
			gpio_to_irq(TSC2007_IRQGPIO_REV2),
		platform_add_devices(rev2_platform_devices,
			ARRAY_SIZE(rev2_platform_devices));
		gpio_request(TSC2007_IRQGPIO_REV2, "tsc2007_irq");
		gpio_direction_input(TSC2007_IRQGPIO_REV2);
		gpio_free(TSC2007_IRQGPIO_REV2);
	} else {
		eukrea_cpuimx51sd_i2c_devices[1].irq =
			gpio_to_irq(TSC2007_IRQGPIO_REV3),
		imx51_add_imx_i2c(0, &cpuimx51sd_i2c_data);
		gpio_request(TSC2007_IRQGPIO_REV3, "tsc2007_irq");
		gpio_direction_input(TSC2007_IRQGPIO_REV3);
		gpio_free(TSC2007_IRQGPIO_REV3);
	}

	i2c_register_board_info(0, eukrea_cpuimx51sd_i2c_devices,
			ARRAY_SIZE(eukrea_cpuimx51sd_i2c_devices));

	if (otg_mode_host)
		imx51_add_mxc_ehci_otg(&dr_utmi_config);
	else {
		initialize_otg_port(NULL);
		imx51_add_fsl_usb2_udc(&usb_pdata);
	}

	gpio_request(USBH1_RST, "usb_rst");
	gpio_direction_output(USBH1_RST, 0);
	msleep(20);
	gpio_set_value(USBH1_RST, 1);
	imx51_add_mxc_ehci_hs(1, &usbh1_config);

#ifdef CONFIG_MACH_EUKREA_MBIMXSD51_BASEBOARD
	eukrea_mbimxsd51_baseboard_init();
#endif
}

static void __init eukrea_cpuimx51sd_timer_init(void)
{
	mx51_clocks_init(32768, 24000000, 22579200, 0);
}

MACHINE_START(EUKREA_CPUIMX51SD, "Eukrea CPUIMX51SD")
	/* Maintainer: Eric Bénard <eric@eukrea.com> */
	.atag_offset = 0x100,
	.map_io = mx51_map_io,
	.init_early = imx51_init_early,
	.init_irq = mx51_init_irq,
	.handle_irq = imx51_handle_irq,
	.init_time	= eukrea_cpuimx51sd_timer_init,
	.init_machine = eukrea_cpuimx51sd_init,
	.init_late	= imx51_init_late,
	.restart	= mxc_restart,
MACHINE_END
