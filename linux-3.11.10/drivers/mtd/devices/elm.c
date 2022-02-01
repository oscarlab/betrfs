/*
 * Error Location Module
 *
 * Copyright (C) 2012 Texas Instruments Incorporated - http://www.ti.com/
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 */

#include <linux/platform_device.h>
#include <linux/module.h>
#include <linux/interrupt.h>
#include <linux/io.h>
#include <linux/of.h>
#include <linux/pm_runtime.h>
#include <linux/platform_data/elm.h>

#define ELM_IRQSTATUS			0x018
#define ELM_IRQENABLE			0x01c
#define ELM_LOCATION_CONFIG		0x020
#define ELM_PAGE_CTRL			0x080
#define ELM_SYNDROME_FRAGMENT_0		0x400
#define ELM_SYNDROME_FRAGMENT_6		0x418
#define ELM_LOCATION_STATUS		0x800
#define ELM_ERROR_LOCATION_0		0x880

/* ELM Interrupt Status Register */
#define INTR_STATUS_PAGE_VALID		BIT(8)

/* ELM Interrupt Enable Register */
#define INTR_EN_PAGE_MASK		BIT(8)

/* ELM Location Configuration Register */
#define ECC_BCH_LEVEL_MASK		0x3

/* ELM syndrome */
#define ELM_SYNDROME_VALID		BIT(16)

/* ELM_LOCATION_STATUS Register */
#define ECC_CORRECTABLE_MASK		BIT(8)
#define ECC_NB_ERRORS_MASK		0x1f

/* ELM_ERROR_LOCATION_0-15 Registers */
#define ECC_ERROR_LOCATION_MASK		0x1fff

#define ELM_ECC_SIZE			0x7ff

#define SYNDROME_FRAGMENT_REG_SIZE	0x40
#define ERROR_LOCATION_SIZE		0x100

struct elm_info {
	struct device *dev;
	void __iomem *elm_base;
	struct completion elm_completion;
	struct list_head list;
	enum bch_ecc bch_type;
};

static LIST_HEAD(elm_devices);

static void elm_write_reg(struct elm_info *info, int offset, u32 val)
{
	writel(val, info->elm_base + offset);
}

static u32 elm_read_reg(struct elm_info *info, int offset)
{
	return readl(info->elm_base + offset);
}

/**
 * elm_config - Configure ELM module
 * @dev:	ELM device
 * @bch_type:	Type of BCH ecc
 */
int elm_config(struct device *dev, enum bch_ecc bch_type)
{
	u32 reg_val;
	struct elm_info *info = dev_get_drvdata(dev);

	if (!info) {
		dev_err(dev, "Unable to configure elm - device not probed?\n");
		return -ENODEV;
	}

	reg_val = (bch_type & ECC_BCH_LEVEL_MASK) | (ELM_ECC_SIZE << 16);
	elm_write_reg(info, ELM_LOCATION_CONFIG, reg_val);
	info->bch_type = bch_type;

	return 0;
}
EXPORT_SYMBOL(elm_config);

/**
 * elm_configure_page_mode - Enable/Disable page mode
 * @info:	elm info
 * @index:	index number of syndrome fragment vector
 * @enable:	enable/disable flag for page mode
 *
 * Enable page mode for syndrome fragment index
 */
static void elm_configure_page_mode(struct elm_info *info, int index,
		bool enable)
{
	u32 reg_val;

	reg_val = elm_read_reg(info, ELM_PAGE_CTRL);
	if (enable)
		reg_val |= BIT(index);	/* enable page mode */
	else
		reg_val &= ~BIT(index);	/* disable page mode */

	elm_write_reg(info, ELM_PAGE_CTRL, reg_val);
}

/**
 * elm_load_syndrome - Load ELM syndrome reg
 * @info:	elm info
 * @err_vec:	elm error vectors
 * @ecc:	buffer with calculated ecc
 *
 * Load syndrome fragment registers with calculated ecc in reverse order.
 */
static void elm_load_syndrome(struct elm_info *info,
		struct elm_errorvec *err_vec, u8 *ecc)
{
	int i, offset;
	u32 val;

	for (i = 0; i < ERROR_VECTOR_MAX; i++) {

		/* Check error reported */
		if (err_vec[i].error_reported) {
			elm_configure_page_mode(info, i, true);
			offset = ELM_SYNDROME_FRAGMENT_0 +
				SYNDROME_FRAGMENT_REG_SIZE * i;

			/* BCH8 */
			if (info->bch_type) {

				/* syndrome fragment 0 = ecc[9-12B] */
				val = cpu_to_be32(*(u32 *) &ecc[9]);
				elm_write_reg(info, offset, val);

				/* syndrome fragment 1 = ecc[5-8B] */
				offset += 4;
				val = cpu_to_be32(*(u32 *) &ecc[5]);
				elm_write_reg(info, offset, val);

				/* syndrome fragment 2 = ecc[1-4B] */
				offset += 4;
				val = cpu_to_be32(*(u32 *) &ecc[1]);
				elm_write_reg(info, offset, val);

				/* syndrome fragment 3 = ecc[0B] */
				offset += 4;
				val = ecc[0];
				elm_write_reg(info, offset, val);
			} else {
				/* syndrome fragment 0 = ecc[20-52b] bits */
				val = (cpu_to_be32(*(u32 *) &ecc[3]) >> 4) |
					((ecc[2] & 0xf) << 28);
				elm_write_reg(info, offset, val);

				/* syndrome fragment 1 = ecc[0-20b] bits */
				offset += 4;
				val = cpu_to_be32(*(u32 *) &ecc[0]) >> 12;
				elm_write_reg(info, offset, val);
			}
		}

		/* Update ecc pointer with ecc byte size */
		ecc += info->bch_type ? BCH8_SIZE : BCH4_SIZE;
	}
}

/**
 * elm_start_processing - start elm syndrome processing
 * @info:	elm info
 * @err_vec:	elm error vectors
 *
 * Set syndrome valid bit for syndrome fragment registers for which
 * elm syndrome fragment registers are loaded. This enables elm module
 * to start processing syndrome vectors.
 */
static void elm_start_processing(struct elm_info *info,
		struct elm_errorvec *err_vec)
{
	int i, offset;
	u32 reg_val;

	/*
	 * Set syndrome vector valid, so that ELM module
	 * will process it for vectors error is reported
	 */
	for (i = 0; i < ERROR_VECTOR_MAX; i++) {
		if (err_vec[i].error_reported) {
			offset = ELM_SYNDROME_FRAGMENT_6 +
				SYNDROME_FRAGMENT_REG_SIZE * i;
			reg_val = elm_read_reg(info, offset);
			reg_val |= ELM_SYNDROME_VALID;
			elm_write_reg(info, offset, reg_val);
		}
	}
}

/**
 * elm_error_correction - locate correctable error position
 * @info:	elm info
 * @err_vec:	elm error vectors
 *
 * On completion of processing by elm module, error location status
 * register updated with correctable/uncorrectable error information.
 * In case of correctable errors, number of errors located from
 * elm location status register & read the positions from
 * elm error location register.
 */
static void elm_error_correction(struct elm_info *info,
		struct elm_errorvec *err_vec)
{
	int i, j, errors = 0;
	int offset;
	u32 reg_val;

	for (i = 0; i < ERROR_VECTOR_MAX; i++) {

		/* Check error reported */
		if (err_vec[i].error_reported) {
			offset = ELM_LOCATION_STATUS + ERROR_LOCATION_SIZE * i;
			reg_val = elm_read_reg(info, offset);

			/* Check correctable error or not */
			if (reg_val & ECC_CORRECTABLE_MASK) {
				offset = ELM_ERROR_LOCATION_0 +
					ERROR_LOCATION_SIZE * i;

				/* Read count of correctable errors */
				err_vec[i].error_count = reg_val &
					ECC_NB_ERRORS_MASK;

				/* Update the error locations in error vector */
				for (j = 0; j < err_vec[i].error_count; j++) {

					reg_val = elm_read_reg(info, offset);
					err_vec[i].error_loc[j] = reg_val &
						ECC_ERROR_LOCATION_MASK;

					/* Update error location register */
					offset += 4;
				}

				errors += err_vec[i].error_count;
			} else {
				err_vec[i].error_uncorrectable = true;
			}

			/* Clearing interrupts for processed error vectors */
			elm_write_reg(info, ELM_IRQSTATUS, BIT(i));

			/* Disable page mode */
			elm_configure_page_mode(info, i, false);
		}
	}
}

/**
 * elm_decode_bch_error_page - Locate error position
 * @dev:	device pointer
 * @ecc_calc:	calculated ECC bytes from GPMC
 * @err_vec:	elm error vectors
 *
 * Called with one or more error reported vectors & vectors with
 * error reported is updated in err_vec[].error_reported
 */
void elm_decode_bch_error_page(struct device *dev, u8 *ecc_calc,
		struct elm_errorvec *err_vec)
{
	struct elm_info *info = dev_get_drvdata(dev);
	u32 reg_val;

	/* Enable page mode interrupt */
	reg_val = elm_read_reg(info, ELM_IRQSTATUS);
	elm_write_reg(info, ELM_IRQSTATUS, reg_val & INTR_STATUS_PAGE_VALID);
	elm_write_reg(info, ELM_IRQENABLE, INTR_EN_PAGE_MASK);

	/* Load valid ecc byte to syndrome fragment register */
	elm_load_syndrome(info, err_vec, ecc_calc);

	/* Enable syndrome processing for which syndrome fragment is updated */
	elm_start_processing(info, err_vec);

	/* Wait for ELM module to finish locating error correction */
	wait_for_completion(&info->elm_completion);

	/* Disable page mode interrupt */
	reg_val = elm_read_reg(info, ELM_IRQENABLE);
	elm_write_reg(info, ELM_IRQENABLE, reg_val & ~INTR_EN_PAGE_MASK);
	elm_error_correction(info, err_vec);
}
EXPORT_SYMBOL(elm_decode_bch_error_page);

static irqreturn_t elm_isr(int this_irq, void *dev_id)
{
	u32 reg_val;
	struct elm_info *info = dev_id;

	reg_val = elm_read_reg(info, ELM_IRQSTATUS);

	/* All error vectors processed */
	if (reg_val & INTR_STATUS_PAGE_VALID) {
		elm_write_reg(info, ELM_IRQSTATUS,
				reg_val & INTR_STATUS_PAGE_VALID);
		complete(&info->elm_completion);
		return IRQ_HANDLED;
	}

	return IRQ_NONE;
}

static int elm_probe(struct platform_device *pdev)
{
	int ret = 0;
	struct resource *res, *irq;
	struct elm_info *info;

	info = devm_kzalloc(&pdev->dev, sizeof(*info), GFP_KERNEL);
	if (!info) {
		dev_err(&pdev->dev, "failed to allocate memory\n");
		return -ENOMEM;
	}

	info->dev = &pdev->dev;

	irq = platform_get_resource(pdev, IORESOURCE_IRQ, 0);
	if (!irq) {
		dev_err(&pdev->dev, "no irq resource defined\n");
		return -ENODEV;
	}

	res = platform_get_resource(pdev, IORESOURCE_MEM, 0);
	if (!res) {
		dev_err(&pdev->dev, "no memory resource defined\n");
		return -ENODEV;
	}

	info->elm_base = devm_request_and_ioremap(&pdev->dev, res);
	if (!info->elm_base)
		return -EADDRNOTAVAIL;

	ret = devm_request_irq(&pdev->dev, irq->start, elm_isr, 0,
			pdev->name, info);
	if (ret) {
		dev_err(&pdev->dev, "failure requesting irq %i\n", irq->start);
		return ret;
	}

	pm_runtime_enable(&pdev->dev);
	if (pm_runtime_get_sync(&pdev->dev)) {
		ret = -EINVAL;
		pm_runtime_disable(&pdev->dev);
		dev_err(&pdev->dev, "can't enable clock\n");
		return ret;
	}

	init_completion(&info->elm_completion);
	INIT_LIST_HEAD(&info->list);
	list_add(&info->list, &elm_devices);
	platform_set_drvdata(pdev, info);
	return ret;
}

static int elm_remove(struct platform_device *pdev)
{
	pm_runtime_put_sync(&pdev->dev);
	pm_runtime_disable(&pdev->dev);
	platform_set_drvdata(pdev, NULL);
	return 0;
}

#ifdef CONFIG_OF
static const struct of_device_id elm_of_match[] = {
	{ .compatible = "ti,am3352-elm" },
	{},
};
MODULE_DEVICE_TABLE(of, elm_of_match);
#endif

static struct platform_driver elm_driver = {
	.driver	= {
		.name	= "elm",
		.owner	= THIS_MODULE,
		.of_match_table = of_match_ptr(elm_of_match),
	},
	.probe	= elm_probe,
	.remove	= elm_remove,
};

module_platform_driver(elm_driver);

MODULE_DESCRIPTION("ELM driver for BCH error correction");
MODULE_AUTHOR("Texas Instruments");
MODULE_ALIAS("platform: elm");
MODULE_LICENSE("GPL v2");
