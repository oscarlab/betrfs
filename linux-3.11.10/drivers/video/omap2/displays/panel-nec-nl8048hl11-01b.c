/*
 * Support for NEC-nl8048hl11-01b panel driver
 *
 * Copyright (C) 2010 Texas Instruments Inc.
 * Author: Erik Gilling <konkers@android.com>
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published by
 * the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <linux/module.h>
#include <linux/delay.h>
#include <linux/spi/spi.h>
#include <linux/fb.h>
#include <linux/gpio.h>

#include <video/omapdss.h>
#include <video/omap-panel-data.h>

#define LCD_XRES		800
#define LCD_YRES		480
/*
 * NEC PIX Clock Ratings
 * MIN:21.8MHz TYP:23.8MHz MAX:25.7MHz
 */
#define LCD_PIXEL_CLOCK		23800

static const struct {
	unsigned char addr;
	unsigned char dat;
} nec_8048_init_seq[] = {
	{ 3, 0x01 }, { 0, 0x00 }, { 1, 0x01 }, { 4, 0x00 }, { 5, 0x14 },
	{ 6, 0x24 }, { 16, 0xD7 }, { 17, 0x00 }, { 18, 0x00 }, { 19, 0x55 },
	{ 20, 0x01 }, { 21, 0x70 }, { 22, 0x1E }, { 23, 0x25 },	{ 24, 0x25 },
	{ 25, 0x02 }, { 26, 0x02 }, { 27, 0xA0 }, { 32, 0x2F }, { 33, 0x0F },
	{ 34, 0x0F }, { 35, 0x0F }, { 36, 0x0F }, { 37, 0x0F },	{ 38, 0x0F },
	{ 39, 0x00 }, { 40, 0x02 }, { 41, 0x02 }, { 42, 0x02 },	{ 43, 0x0F },
	{ 44, 0x0F }, { 45, 0x0F }, { 46, 0x0F }, { 47, 0x0F },	{ 48, 0x0F },
	{ 49, 0x0F }, { 50, 0x00 }, { 51, 0x02 }, { 52, 0x02 }, { 53, 0x02 },
	{ 80, 0x0C }, { 83, 0x42 }, { 84, 0x42 }, { 85, 0x41 },	{ 86, 0x14 },
	{ 89, 0x88 }, { 90, 0x01 }, { 91, 0x00 }, { 92, 0x02 },	{ 93, 0x0C },
	{ 94, 0x1C }, { 95, 0x27 }, { 98, 0x49 }, { 99, 0x27 }, { 102, 0x76 },
	{ 103, 0x27 }, { 112, 0x01 }, { 113, 0x0E }, { 114, 0x02 },
	{ 115, 0x0C }, { 118, 0x0C }, { 121, 0x30 }, { 130, 0x00 },
	{ 131, 0x00 }, { 132, 0xFC }, { 134, 0x00 }, { 136, 0x00 },
	{ 138, 0x00 }, { 139, 0x00 }, { 140, 0x00 }, { 141, 0xFC },
	{ 143, 0x00 }, { 145, 0x00 }, { 147, 0x00 }, { 148, 0x00 },
	{ 149, 0x00 }, { 150, 0xFC }, { 152, 0x00 }, { 154, 0x00 },
	{ 156, 0x00 }, { 157, 0x00 }, { 2, 0x00 },
};

/*
 * NEC NL8048HL11-01B  Manual
 * defines HFB, HSW, HBP, VFP, VSW, VBP as shown below
 */

static struct omap_video_timings nec_8048_panel_timings = {
	/* 800 x 480 @ 60 Hz  Reduced blanking VESA CVT 0.31M3-R */
	.x_res		= LCD_XRES,
	.y_res		= LCD_YRES,
	.pixel_clock	= LCD_PIXEL_CLOCK,
	.hfp		= 6,
	.hsw		= 1,
	.hbp		= 4,
	.vfp		= 3,
	.vsw		= 1,
	.vbp		= 4,

	.vsync_level	= OMAPDSS_SIG_ACTIVE_LOW,
	.hsync_level	= OMAPDSS_SIG_ACTIVE_LOW,
	.data_pclk_edge	= OMAPDSS_DRIVE_SIG_RISING_EDGE,
	.de_level	= OMAPDSS_SIG_ACTIVE_HIGH,
	.sync_pclk_edge	= OMAPDSS_DRIVE_SIG_RISING_EDGE,
};

static inline struct panel_nec_nl8048_data
*get_panel_data(const struct omap_dss_device *dssdev)
{
	return (struct panel_nec_nl8048_data *) dssdev->data;
}

static int nec_8048_panel_probe(struct omap_dss_device *dssdev)
{
	struct panel_nec_nl8048_data *pd = get_panel_data(dssdev);
	int r;

	if (!pd)
		return -EINVAL;

	dssdev->panel.timings = nec_8048_panel_timings;

	if (gpio_is_valid(pd->qvga_gpio)) {
		r = devm_gpio_request_one(dssdev->dev, pd->qvga_gpio,
				GPIOF_OUT_INIT_HIGH, "lcd QVGA");
		if (r)
			return r;
	}

	if (gpio_is_valid(pd->res_gpio)) {
		r = devm_gpio_request_one(dssdev->dev, pd->res_gpio,
				GPIOF_OUT_INIT_LOW, "lcd RES");
		if (r)
			return r;
	}

	return 0;
}

static void nec_8048_panel_remove(struct omap_dss_device *dssdev)
{
}

static int nec_8048_panel_power_on(struct omap_dss_device *dssdev)
{
	struct panel_nec_nl8048_data *pd = get_panel_data(dssdev);
	int r;

	if (dssdev->state == OMAP_DSS_DISPLAY_ACTIVE)
		return 0;

	omapdss_dpi_set_timings(dssdev, &dssdev->panel.timings);
	omapdss_dpi_set_data_lines(dssdev, dssdev->phy.dpi.data_lines);

	r = omapdss_dpi_display_enable(dssdev);
	if (r)
		goto err0;

	if (gpio_is_valid(pd->res_gpio))
		gpio_set_value_cansleep(pd->res_gpio, 1);

	return 0;

err0:
	return r;
}

static void nec_8048_panel_power_off(struct omap_dss_device *dssdev)
{
	struct panel_nec_nl8048_data *pd = get_panel_data(dssdev);

	if (dssdev->state != OMAP_DSS_DISPLAY_ACTIVE)
		return;

	if (gpio_is_valid(pd->res_gpio))
		gpio_set_value_cansleep(pd->res_gpio, 0);

	omapdss_dpi_display_disable(dssdev);
}

static int nec_8048_panel_enable(struct omap_dss_device *dssdev)
{
	int r;

	r = nec_8048_panel_power_on(dssdev);
	if (r)
		return r;

	dssdev->state = OMAP_DSS_DISPLAY_ACTIVE;

	return 0;
}

static void nec_8048_panel_disable(struct omap_dss_device *dssdev)
{
	nec_8048_panel_power_off(dssdev);

	dssdev->state = OMAP_DSS_DISPLAY_DISABLED;
}

static int nec_8048_recommended_bpp(struct omap_dss_device *dssdev)
{
	return 16;
}

static struct omap_dss_driver nec_8048_driver = {
	.probe			= nec_8048_panel_probe,
	.remove			= nec_8048_panel_remove,
	.enable			= nec_8048_panel_enable,
	.disable		= nec_8048_panel_disable,
	.get_recommended_bpp	= nec_8048_recommended_bpp,

	.driver		= {
		.name		= "NEC_8048_panel",
		.owner		= THIS_MODULE,
	},
};

static int nec_8048_spi_send(struct spi_device *spi, unsigned char reg_addr,
			unsigned char reg_data)
{
	int ret = 0;
	unsigned int cmd = 0, data = 0;

	cmd = 0x0000 | reg_addr; /* register address write */
	data = 0x0100 | reg_data ; /* register data write */
	data = (cmd << 16) | data;

	ret = spi_write(spi, (unsigned char *)&data, 4);
	if (ret)
		pr_err("error in spi_write %x\n", data);

	return ret;
}

static int init_nec_8048_wvga_lcd(struct spi_device *spi)
{
	unsigned int i;
	/* Initialization Sequence */
	/* nec_8048_spi_send(spi, REG, VAL) */
	for (i = 0; i < (ARRAY_SIZE(nec_8048_init_seq) - 1); i++)
		nec_8048_spi_send(spi, nec_8048_init_seq[i].addr,
				nec_8048_init_seq[i].dat);
	udelay(20);
	nec_8048_spi_send(spi, nec_8048_init_seq[i].addr,
				nec_8048_init_seq[i].dat);
	return 0;
}

static int nec_8048_spi_probe(struct spi_device *spi)
{
	spi->mode = SPI_MODE_0;
	spi->bits_per_word = 32;
	spi_setup(spi);

	init_nec_8048_wvga_lcd(spi);

	return omap_dss_register_driver(&nec_8048_driver);
}

static int nec_8048_spi_remove(struct spi_device *spi)
{
	omap_dss_unregister_driver(&nec_8048_driver);

	return 0;
}

#ifdef CONFIG_PM_SLEEP

static int nec_8048_spi_suspend(struct device *dev)
{
	struct spi_device *spi = to_spi_device(dev);

	nec_8048_spi_send(spi, 2, 0x01);
	mdelay(40);

	return 0;
}

static int nec_8048_spi_resume(struct device *dev)
{
	struct spi_device *spi = to_spi_device(dev);

	/* reinitialize the panel */
	spi_setup(spi);
	nec_8048_spi_send(spi, 2, 0x00);
	init_nec_8048_wvga_lcd(spi);

	return 0;
}

static SIMPLE_DEV_PM_OPS(nec_8048_spi_pm_ops, nec_8048_spi_suspend,
		nec_8048_spi_resume);
#define NEC_8048_SPI_PM_OPS (&nec_8048_spi_pm_ops)
#else
#define NEC_8048_SPI_PM_OPS NULL
#endif

static struct spi_driver nec_8048_spi_driver = {
	.probe		= nec_8048_spi_probe,
	.remove		= nec_8048_spi_remove,
	.driver		= {
		.name	= "nec_8048_spi",
		.owner	= THIS_MODULE,
		.pm	= NEC_8048_SPI_PM_OPS,
	},
};

module_spi_driver(nec_8048_spi_driver);

MODULE_AUTHOR("Erik Gilling <konkers@android.com>");
MODULE_DESCRIPTION("NEC-nl8048hl11-01b Driver");
MODULE_LICENSE("GPL");
