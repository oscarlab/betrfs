/*
 * Core driver interface for TI TPS65090 PMIC family
 *
 * Copyright (C) 2012 NVIDIA Corporation
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 *
 */

#ifndef __LINUX_MFD_TPS65090_H
#define __LINUX_MFD_TPS65090_H

#include <linux/irq.h>
#include <linux/regmap.h>

/* TPS65090 IRQs */
enum {
	TPS65090_IRQ_INTERRUPT,
	TPS65090_IRQ_VAC_STATUS_CHANGE,
	TPS65090_IRQ_VSYS_STATUS_CHANGE,
	TPS65090_IRQ_BAT_STATUS_CHANGE,
	TPS65090_IRQ_CHARGING_STATUS_CHANGE,
	TPS65090_IRQ_CHARGING_COMPLETE,
	TPS65090_IRQ_OVERLOAD_DCDC1,
	TPS65090_IRQ_OVERLOAD_DCDC2,
	TPS65090_IRQ_OVERLOAD_DCDC3,
	TPS65090_IRQ_OVERLOAD_FET1,
	TPS65090_IRQ_OVERLOAD_FET2,
	TPS65090_IRQ_OVERLOAD_FET3,
	TPS65090_IRQ_OVERLOAD_FET4,
	TPS65090_IRQ_OVERLOAD_FET5,
	TPS65090_IRQ_OVERLOAD_FET6,
	TPS65090_IRQ_OVERLOAD_FET7,
};

/* TPS65090 Regulator ID */
enum {
	TPS65090_REGULATOR_DCDC1,
	TPS65090_REGULATOR_DCDC2,
	TPS65090_REGULATOR_DCDC3,
	TPS65090_REGULATOR_FET1,
	TPS65090_REGULATOR_FET2,
	TPS65090_REGULATOR_FET3,
	TPS65090_REGULATOR_FET4,
	TPS65090_REGULATOR_FET5,
	TPS65090_REGULATOR_FET6,
	TPS65090_REGULATOR_FET7,
	TPS65090_REGULATOR_LDO1,
	TPS65090_REGULATOR_LDO2,

	/* Last entry for maximum ID */
	TPS65090_REGULATOR_MAX,
};

struct tps65090 {
	struct device		*dev;
	struct regmap		*rmap;
	struct regmap_irq_chip_data *irq_data;
};

/*
 * struct tps65090_regulator_plat_data
 *
 * @reg_init_data: The regulator init data.
 * @enable_ext_control: Enable extrenal control or not. Only available for
 *     DCDC1, DCDC2 and DCDC3.
 * @gpio: Gpio number if external control is enabled and controlled through
 *     gpio.
 */
struct tps65090_regulator_plat_data {
	struct regulator_init_data *reg_init_data;
	bool enable_ext_control;
	int gpio;
};

struct tps65090_platform_data {
	int irq_base;

	char **supplied_to;
	size_t num_supplicants;
	int enable_low_current_chrg;

	struct tps65090_regulator_plat_data *reg_pdata[TPS65090_REGULATOR_MAX];
};

/*
 * NOTE: the functions below are not intended for use outside
 * of the TPS65090 sub-device drivers
 */
static inline int tps65090_write(struct device *dev, int reg, uint8_t val)
{
	struct tps65090 *tps = dev_get_drvdata(dev);

	return regmap_write(tps->rmap, reg, val);
}

static inline int tps65090_read(struct device *dev, int reg, uint8_t *val)
{
	struct tps65090 *tps = dev_get_drvdata(dev);
	unsigned int temp_val;
	int ret;

	ret = regmap_read(tps->rmap, reg, &temp_val);
	if (!ret)
		*val = temp_val;
	return ret;
}

static inline int tps65090_set_bits(struct device *dev, int reg,
		uint8_t bit_num)
{
	struct tps65090 *tps = dev_get_drvdata(dev);

	return regmap_update_bits(tps->rmap, reg, BIT(bit_num), ~0u);
}

static inline int tps65090_clr_bits(struct device *dev, int reg,
		uint8_t bit_num)
{
	struct tps65090 *tps = dev_get_drvdata(dev);

	return regmap_update_bits(tps->rmap, reg, BIT(bit_num), 0u);
}

#endif /*__LINUX_MFD_TPS65090_H */
