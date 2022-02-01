/*
 * Copyright (C) ST-Ericsson SA 2010
 *
 * License terms: GNU General Public License (GPL), version 2
 */

#include <linux/platform_device.h>
#include <linux/init.h>
#include <linux/gpio.h>
#include <linux/platform_data/pinctrl-nomadik.h>
#include <linux/platform_data/dma-ste-dma40.h>

#include "devices.h"
#include "irqs.h"
#include <linux/platform_data/asoc-ux500-msp.h>

#include "ste-dma40-db8500.h"
#include "board-mop500.h"
#include "devices-db8500.h"
#include "pins-db8500.h"

static struct stedma40_chan_cfg msp0_dma_rx = {
	.high_priority = true,
	.dir = DMA_DEV_TO_MEM,
	.dev_type = DB8500_DMA_DEV31_MSP0_SLIM0_CH0,
};

static struct stedma40_chan_cfg msp0_dma_tx = {
	.high_priority = true,
	.dir = DMA_MEM_TO_DEV,
	.dev_type = DB8500_DMA_DEV31_MSP0_SLIM0_CH0,
};

struct msp_i2s_platform_data msp0_platform_data = {
	.id = MSP_I2S_0,
	.msp_i2s_dma_rx = &msp0_dma_rx,
	.msp_i2s_dma_tx = &msp0_dma_tx,
};

static struct stedma40_chan_cfg msp1_dma_rx = {
	.high_priority = true,
	.dir = DMA_DEV_TO_MEM,
	.dev_type = DB8500_DMA_DEV30_MSP3,
};

static struct stedma40_chan_cfg msp1_dma_tx = {
	.high_priority = true,
	.dir = DMA_MEM_TO_DEV,
	.dev_type = DB8500_DMA_DEV30_MSP1,
};

struct msp_i2s_platform_data msp1_platform_data = {
	.id = MSP_I2S_1,
	.msp_i2s_dma_rx = NULL,
	.msp_i2s_dma_tx = &msp1_dma_tx,
};

static struct stedma40_chan_cfg msp2_dma_rx = {
	.high_priority = true,
	.dir = DMA_DEV_TO_MEM,
	.dev_type = DB8500_DMA_DEV14_MSP2,
};

static struct stedma40_chan_cfg msp2_dma_tx = {
	.high_priority = true,
	.dir = DMA_MEM_TO_DEV,
	.dev_type = DB8500_DMA_DEV14_MSP2,
	.use_fixed_channel = true,
	.phy_channel = 1,
};

static struct platform_device *db8500_add_msp_i2s(struct device *parent,
			int id,
			resource_size_t base, int irq,
			struct msp_i2s_platform_data *pdata)
{
	struct platform_device *pdev;
	struct resource res[] = {
		DEFINE_RES_MEM(base, SZ_4K),
		DEFINE_RES_IRQ(irq),
	};

	pr_info("Register platform-device 'ux500-msp-i2s', id %d, irq %d\n",
		id, irq);
	pdev = platform_device_register_resndata(parent, "ux500-msp-i2s", id,
						res, ARRAY_SIZE(res),
						pdata, sizeof(*pdata));
	if (!pdev) {
		pr_err("Failed to register platform-device 'ux500-msp-i2s.%d'!\n",
			id);
		return NULL;
	}

	return pdev;
}

/* Platform device for ASoC MOP500 machine */
static struct platform_device snd_soc_mop500 = {
	.name = "snd-soc-mop500",
	.id = 0,
	.dev = {
		.platform_data = NULL,
	},
};

struct msp_i2s_platform_data msp2_platform_data = {
	.id = MSP_I2S_2,
	.msp_i2s_dma_rx = &msp2_dma_rx,
	.msp_i2s_dma_tx = &msp2_dma_tx,
};

struct msp_i2s_platform_data msp3_platform_data = {
	.id		= MSP_I2S_3,
	.msp_i2s_dma_rx	= &msp1_dma_rx,
	.msp_i2s_dma_tx	= NULL,
};

void mop500_audio_init(struct device *parent)
{
	pr_info("%s: Register platform-device 'snd-soc-mop500'.\n", __func__);
	platform_device_register(&snd_soc_mop500);

	pr_info("Initialize MSP I2S-devices.\n");
	db8500_add_msp_i2s(parent, 0, U8500_MSP0_BASE, IRQ_DB8500_MSP0,
			   &msp0_platform_data);
	db8500_add_msp_i2s(parent, 1, U8500_MSP1_BASE, IRQ_DB8500_MSP1,
			   &msp1_platform_data);
	db8500_add_msp_i2s(parent, 2, U8500_MSP2_BASE, IRQ_DB8500_MSP2,
			   &msp2_platform_data);
	db8500_add_msp_i2s(parent, 3, U8500_MSP3_BASE, IRQ_DB8500_MSP1,
			   &msp3_platform_data);
}
