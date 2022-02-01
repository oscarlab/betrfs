/* linux/arch/arm/mach-s5p64x0/dev-audio.c
 *
 * Copyright (c) 2010 Samsung Electronics Co. Ltd
 *	Jaswinder Singh <jassi.brar@samsung.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
*/

#include <linux/platform_device.h>
#include <linux/dma-mapping.h>
#include <linux/gpio.h>

#include <plat/gpio-cfg.h>
#include <linux/platform_data/asoc-s3c.h>

#include <mach/map.h>
#include <mach/dma.h>
#include <mach/irqs.h>

static int s5p6440_cfg_i2s(struct platform_device *pdev)
{
	switch (pdev->id) {
	case 0:
		s3c_gpio_cfgpin_range(S5P6440_GPC(4), 2, S3C_GPIO_SFN(5));
		s3c_gpio_cfgpin(S5P6440_GPC(7), S3C_GPIO_SFN(5));
		s3c_gpio_cfgpin_range(S5P6440_GPH(6), 4, S3C_GPIO_SFN(5));
		break;
	default:
		printk(KERN_ERR "Invalid Device %d\n", pdev->id);
		return -EINVAL;
	}

	return 0;
}

static struct s3c_audio_pdata s5p6440_i2s_pdata = {
	.cfg_gpio = s5p6440_cfg_i2s,
	.type = {
		.i2s = {
			.quirks = QUIRK_PRI_6CHAN,
		},
	},
};

static struct resource s5p64x0_i2s0_resource[] = {
	[0] = DEFINE_RES_MEM(S5P64X0_PA_I2S, SZ_256),
	[1] = DEFINE_RES_DMA(DMACH_I2S0_TX),
	[2] = DEFINE_RES_DMA(DMACH_I2S0_RX),
};

struct platform_device s5p6440_device_iis = {
	.name		= "samsung-i2s",
	.id		= 0,
	.num_resources	= ARRAY_SIZE(s5p64x0_i2s0_resource),
	.resource	= s5p64x0_i2s0_resource,
	.dev = {
		.platform_data = &s5p6440_i2s_pdata,
	},
};

static int s5p6450_cfg_i2s(struct platform_device *pdev)
{
	switch (pdev->id) {
	case 0:
		s3c_gpio_cfgpin_range(S5P6450_GPR(4), 5, S3C_GPIO_SFN(5));
		s3c_gpio_cfgpin_range(S5P6450_GPR(13), 2, S3C_GPIO_SFN(5));
		break;
	case 1:
		s3c_gpio_cfgpin(S5P6440_GPB(4), S3C_GPIO_SFN(5));
		s3c_gpio_cfgpin_range(S5P6450_GPC(0), 4, S3C_GPIO_SFN(5));
		break;
	case 2:
		s3c_gpio_cfgpin_range(S5P6450_GPK(0), 5, S3C_GPIO_SFN(5));
		break;
	default:
		printk(KERN_ERR "Invalid Device %d\n", pdev->id);
		return -EINVAL;
	}

	return 0;
}

static struct s3c_audio_pdata s5p6450_i2s0_pdata = {
	.cfg_gpio = s5p6450_cfg_i2s,
	.type = {
		.i2s = {
			.quirks = QUIRK_PRI_6CHAN,
		},
	},
};

struct platform_device s5p6450_device_iis0 = {
	.name		= "samsung-i2s",
	.id		= 0,
	.num_resources	= ARRAY_SIZE(s5p64x0_i2s0_resource),
	.resource	= s5p64x0_i2s0_resource,
	.dev = {
		.platform_data = &s5p6450_i2s0_pdata,
	},
};

static struct s3c_audio_pdata s5p6450_i2s_pdata = {
	.cfg_gpio = s5p6450_cfg_i2s,
};

static struct resource s5p6450_i2s1_resource[] = {
	[0] = DEFINE_RES_MEM(S5P6450_PA_I2S1, SZ_256),
	[1] = DEFINE_RES_DMA(DMACH_I2S1_TX),
	[2] = DEFINE_RES_DMA(DMACH_I2S1_RX),
};

struct platform_device s5p6450_device_iis1 = {
	.name		= "samsung-i2s",
	.id		= 1,
	.num_resources	= ARRAY_SIZE(s5p6450_i2s1_resource),
	.resource	= s5p6450_i2s1_resource,
	.dev = {
		.platform_data = &s5p6450_i2s_pdata,
	},
};

static struct resource s5p6450_i2s2_resource[] = {
	[0] = DEFINE_RES_MEM(S5P6450_PA_I2S2, SZ_256),
	[1] = DEFINE_RES_DMA(DMACH_I2S2_TX),
	[2] = DEFINE_RES_DMA(DMACH_I2S2_RX),
};

struct platform_device s5p6450_device_iis2 = {
	.name		= "samsung-i2s",
	.id		= 2,
	.num_resources	= ARRAY_SIZE(s5p6450_i2s2_resource),
	.resource	= s5p6450_i2s2_resource,
	.dev = {
		.platform_data = &s5p6450_i2s_pdata,
	},
};

/* PCM Controller platform_devices */

static int s5p6440_pcm_cfg_gpio(struct platform_device *pdev)
{
	switch (pdev->id) {
	case 0:
		s3c_gpio_cfgpin_range(S5P6440_GPR(6), 3, S3C_GPIO_SFN(2));
		s3c_gpio_cfgpin_range(S5P6440_GPR(13), 2, S3C_GPIO_SFN(2));
		break;

	default:
		printk(KERN_DEBUG "Invalid PCM Controller number!");
		return -EINVAL;
	}

	return 0;
}

static struct s3c_audio_pdata s5p6440_pcm_pdata = {
	.cfg_gpio = s5p6440_pcm_cfg_gpio,
};

static struct resource s5p6440_pcm0_resource[] = {
	[0] = DEFINE_RES_MEM(S5P64X0_PA_PCM, SZ_256),
	[1] = DEFINE_RES_DMA(DMACH_PCM0_TX),
	[2] = DEFINE_RES_DMA(DMACH_PCM0_RX),
};

struct platform_device s5p6440_device_pcm = {
	.name		= "samsung-pcm",
	.id		= 0,
	.num_resources	= ARRAY_SIZE(s5p6440_pcm0_resource),
	.resource	= s5p6440_pcm0_resource,
	.dev = {
		.platform_data = &s5p6440_pcm_pdata,
	},
};
