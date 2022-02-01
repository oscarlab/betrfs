/*
 * exynos_thermal.h - Samsung EXYNOS TMU (Thermal Management Unit)
 *
 *  Copyright (C) 2011 Samsung Electronics
 *  Donggeun Kim <dg77.kim@samsung.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

#ifndef _LINUX_EXYNOS_THERMAL_H
#define _LINUX_EXYNOS_THERMAL_H
#include <linux/cpu_cooling.h>

enum calibration_type {
	TYPE_ONE_POINT_TRIMMING,
	TYPE_TWO_POINT_TRIMMING,
	TYPE_NONE,
};

enum soc_type {
	SOC_ARCH_EXYNOS4210 = 1,
	SOC_ARCH_EXYNOS,
};
/**
 * struct freq_clip_table
 * @freq_clip_max: maximum frequency allowed for this cooling state.
 * @temp_level: Temperature level at which the temperature clipping will
 *	happen.
 * @mask_val: cpumask of the allowed cpu's where the clipping will take place.
 *
 * This structure is required to be filled and passed to the
 * cpufreq_cooling_unregister function.
 */
struct freq_clip_table {
	unsigned int freq_clip_max;
	unsigned int temp_level;
	const struct cpumask *mask_val;
};

/**
 * struct exynos_tmu_platform_data
 * @threshold: basic temperature for generating interrupt
 *	       25 <= threshold <= 125 [unit: degree Celsius]
 * @threshold_falling: differntial value for setting threshold
 *		       of temperature falling interrupt.
 * @trigger_levels: array for each interrupt levels
 *	[unit: degree Celsius]
 *	0: temperature for trigger_level0 interrupt
 *	   condition for trigger_level0 interrupt:
 *		current temperature > threshold + trigger_levels[0]
 *	1: temperature for trigger_level1 interrupt
 *	   condition for trigger_level1 interrupt:
 *		current temperature > threshold + trigger_levels[1]
 *	2: temperature for trigger_level2 interrupt
 *	   condition for trigger_level2 interrupt:
 *		current temperature > threshold + trigger_levels[2]
 *	3: temperature for trigger_level3 interrupt
 *	   condition for trigger_level3 interrupt:
 *		current temperature > threshold + trigger_levels[3]
 * @trigger_level0_en:
 *	1 = enable trigger_level0 interrupt,
 *	0 = disable trigger_level0 interrupt
 * @trigger_level1_en:
 *	1 = enable trigger_level1 interrupt,
 *	0 = disable trigger_level1 interrupt
 * @trigger_level2_en:
 *	1 = enable trigger_level2 interrupt,
 *	0 = disable trigger_level2 interrupt
 * @trigger_level3_en:
 *	1 = enable trigger_level3 interrupt,
 *	0 = disable trigger_level3 interrupt
 * @gain: gain of amplifier in the positive-TC generator block
 *	0 <= gain <= 15
 * @reference_voltage: reference voltage of amplifier
 *	in the positive-TC generator block
 *	0 <= reference_voltage <= 31
 * @noise_cancel_mode: noise cancellation mode
 *	000, 100, 101, 110 and 111 can be different modes
 * @type: determines the type of SOC
 * @efuse_value: platform defined fuse value
 * @cal_type: calibration type for temperature
 * @freq_clip_table: Table representing frequency reduction percentage.
 * @freq_tab_count: Count of the above table as frequency reduction may
 *	applicable to only some of the trigger levels.
 *
 * This structure is required for configuration of exynos_tmu driver.
 */
struct exynos_tmu_platform_data {
	u8 threshold;
	u8 threshold_falling;
	u8 trigger_levels[4];
	bool trigger_level0_en;
	bool trigger_level1_en;
	bool trigger_level2_en;
	bool trigger_level3_en;

	u8 gain;
	u8 reference_voltage;
	u8 noise_cancel_mode;
	u32 efuse_value;

	enum calibration_type cal_type;
	enum soc_type type;
	struct freq_clip_table freq_tab[4];
	unsigned int freq_tab_count;
};
#endif /* _LINUX_EXYNOS_THERMAL_H */
