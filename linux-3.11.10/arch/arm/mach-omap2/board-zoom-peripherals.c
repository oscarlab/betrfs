/*
 * Copyright (C) 2009 Texas Instruments Inc.
 *
 * Modified from mach-omap2/board-zoom2.c
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

#include <linux/kernel.h>
#include <linux/init.h>
#include <linux/platform_device.h>
#include <linux/input.h>
#include <linux/input/matrix_keypad.h>
#include <linux/gpio.h>
#include <linux/i2c/twl.h>
#include <linux/regulator/machine.h>
#include <linux/regulator/fixed.h>
#include <linux/wl12xx.h>
#include <linux/mmc/host.h>
#include <linux/platform_data/gpio-omap.h>
#include <linux/platform_data/omap-twl4030.h>
#include <linux/usb/phy.h>
#include <linux/pwm.h>
#include <linux/leds_pwm.h>
#include <linux/pwm_backlight.h>

#include <asm/mach-types.h>
#include <asm/mach/arch.h>
#include <asm/mach/map.h>

#include "common.h"

#include "board-zoom.h"

#include "mux.h"
#include "hsmmc.h"
#include "common-board-devices.h"

#define OMAP_ZOOM_WLAN_PMENA_GPIO	(101)
#define OMAP_ZOOM_TSC2004_IRQ_GPIO	(153)
#define OMAP_ZOOM_WLAN_IRQ_GPIO		(162)

/* Zoom2 has Qwerty keyboard*/
static uint32_t board_keymap[] = {
	KEY(0, 0, KEY_E),
	KEY(0, 1, KEY_R),
	KEY(0, 2, KEY_T),
	KEY(0, 3, KEY_HOME),
	KEY(0, 6, KEY_I),
	KEY(0, 7, KEY_LEFTSHIFT),
	KEY(1, 0, KEY_D),
	KEY(1, 1, KEY_F),
	KEY(1, 2, KEY_G),
	KEY(1, 3, KEY_SEND),
	KEY(1, 6, KEY_K),
	KEY(1, 7, KEY_ENTER),
	KEY(2, 0, KEY_X),
	KEY(2, 1, KEY_C),
	KEY(2, 2, KEY_V),
	KEY(2, 3, KEY_END),
	KEY(2, 6, KEY_DOT),
	KEY(2, 7, KEY_CAPSLOCK),
	KEY(3, 0, KEY_Z),
	KEY(3, 1, KEY_KPPLUS),
	KEY(3, 2, KEY_B),
	KEY(3, 3, KEY_F1),
	KEY(3, 6, KEY_O),
	KEY(3, 7, KEY_SPACE),
	KEY(4, 0, KEY_W),
	KEY(4, 1, KEY_Y),
	KEY(4, 2, KEY_U),
	KEY(4, 3, KEY_F2),
	KEY(4, 4, KEY_VOLUMEUP),
	KEY(4, 6, KEY_L),
	KEY(4, 7, KEY_LEFT),
	KEY(5, 0, KEY_S),
	KEY(5, 1, KEY_H),
	KEY(5, 2, KEY_J),
	KEY(5, 3, KEY_F3),
	KEY(5, 4, KEY_UNKNOWN),
	KEY(5, 5, KEY_VOLUMEDOWN),
	KEY(5, 6, KEY_M),
	KEY(5, 7, KEY_RIGHT),
	KEY(6, 0, KEY_Q),
	KEY(6, 1, KEY_A),
	KEY(6, 2, KEY_N),
	KEY(6, 3, KEY_BACKSPACE),
	KEY(6, 6, KEY_P),
	KEY(6, 7, KEY_UP),
	KEY(7, 0, KEY_PROG1),	/*MACRO 1 <User defined> */
	KEY(7, 1, KEY_PROG2),	/*MACRO 2 <User defined> */
	KEY(7, 2, KEY_PROG3),	/*MACRO 3 <User defined> */
	KEY(7, 3, KEY_PROG4),	/*MACRO 4 <User defined> */
	KEY(7, 6, KEY_SELECT),
	KEY(7, 7, KEY_DOWN)
};

static struct matrix_keymap_data board_map_data = {
	.keymap			= board_keymap,
	.keymap_size		= ARRAY_SIZE(board_keymap),
};

static struct twl4030_keypad_data zoom_kp_twl4030_data = {
	.keymap_data	= &board_map_data,
	.rows		= 8,
	.cols		= 8,
	.rep		= 1,
};

static struct regulator_consumer_supply zoom_vmmc1_supply[] = {
	REGULATOR_SUPPLY("vmmc", "omap_hsmmc.0"),
};

static struct regulator_consumer_supply zoom_vsim_supply[] = {
	REGULATOR_SUPPLY("vmmc_aux", "omap_hsmmc.0"),
};

static struct regulator_consumer_supply zoom_vmmc2_supply[] = {
	REGULATOR_SUPPLY("vmmc", "omap_hsmmc.1"),
};

static struct regulator_consumer_supply zoom_vmmc3_supply[] = {
	REGULATOR_SUPPLY("vmmc", "omap_hsmmc.2"),
};

/* VMMC1 for OMAP VDD_MMC1 (i/o) and MMC1 card */
static struct regulator_init_data zoom_vmmc1 = {
	.constraints = {
		.min_uV			= 1850000,
		.max_uV			= 3150000,
		.valid_modes_mask	= REGULATOR_MODE_NORMAL
					| REGULATOR_MODE_STANDBY,
		.valid_ops_mask		= REGULATOR_CHANGE_VOLTAGE
					| REGULATOR_CHANGE_MODE
					| REGULATOR_CHANGE_STATUS,
	},
	.num_consumer_supplies  = ARRAY_SIZE(zoom_vmmc1_supply),
	.consumer_supplies      = zoom_vmmc1_supply,
};

/* VMMC2 for MMC2 card */
static struct regulator_init_data zoom_vmmc2 = {
	.constraints = {
		.min_uV			= 1850000,
		.max_uV			= 1850000,
		.apply_uV		= true,
		.valid_modes_mask	= REGULATOR_MODE_NORMAL
					| REGULATOR_MODE_STANDBY,
		.valid_ops_mask		= REGULATOR_CHANGE_MODE
					| REGULATOR_CHANGE_STATUS,
	},
	.num_consumer_supplies  = ARRAY_SIZE(zoom_vmmc2_supply),
	.consumer_supplies      = zoom_vmmc2_supply,
};

/* VSIM for OMAP VDD_MMC1A (i/o for DAT4..DAT7) */
static struct regulator_init_data zoom_vsim = {
	.constraints = {
		.min_uV			= 1800000,
		.max_uV			= 3000000,
		.valid_modes_mask	= REGULATOR_MODE_NORMAL
					| REGULATOR_MODE_STANDBY,
		.valid_ops_mask		= REGULATOR_CHANGE_VOLTAGE
					| REGULATOR_CHANGE_MODE
					| REGULATOR_CHANGE_STATUS,
	},
	.num_consumer_supplies  = ARRAY_SIZE(zoom_vsim_supply),
	.consumer_supplies      = zoom_vsim_supply,
};

static struct regulator_init_data zoom_vmmc3 = {
	.constraints = {
		.valid_ops_mask	= REGULATOR_CHANGE_STATUS,
	},
	.num_consumer_supplies	= ARRAY_SIZE(zoom_vmmc3_supply),
	.consumer_supplies	= zoom_vmmc3_supply,
};

static struct fixed_voltage_config zoom_vwlan = {
	.supply_name		= "vwl1271",
	.microvolts		= 1800000, /* 1.8V */
	.gpio			= OMAP_ZOOM_WLAN_PMENA_GPIO,
	.startup_delay		= 70000, /* 70msec */
	.enable_high		= 1,
	.enabled_at_boot	= 0,
	.init_data		= &zoom_vmmc3,
};

static struct platform_device omap_vwlan_device = {
	.name		= "reg-fixed-voltage",
	.id		= 1,
	.dev = {
		.platform_data	= &zoom_vwlan,
	},
};

static struct pwm_lookup zoom_pwm_lookup[] = {
	PWM_LOOKUP("twl-pwm", 0, "leds_pwm", "zoom::keypad"),
	PWM_LOOKUP("twl-pwm", 1, "pwm-backlight", "backlight"),
};

static struct led_pwm zoom_pwm_leds[] = {
	{
		.name		= "zoom::keypad",
		.max_brightness	= 127,
		.pwm_period_ns	= 7812500,
	},
};

static struct led_pwm_platform_data zoom_pwm_data = {
	.num_leds	= ARRAY_SIZE(zoom_pwm_leds),
	.leds		= zoom_pwm_leds,
};

static struct platform_device zoom_leds_pwm = {
	.name	= "leds_pwm",
	.id	= -1,
	.dev	= {
		.platform_data = &zoom_pwm_data,
	},
};

static struct platform_pwm_backlight_data zoom_backlight_data = {
	.pwm_id = 1,
	.max_brightness = 127,
	.dft_brightness = 127,
	.pwm_period_ns = 7812500,
};

static struct platform_device zoom_backlight_pwm = {
	.name   = "pwm-backlight",
	.id     = -1,
	.dev    = {
		.platform_data = &zoom_backlight_data,
	},
};

static struct platform_device *zoom_devices[] __initdata = {
	&omap_vwlan_device,
	&zoom_leds_pwm,
	&zoom_backlight_pwm,
};

static struct wl12xx_platform_data omap_zoom_wlan_data __initdata = {
	.board_ref_clock = WL12XX_REFCLOCK_26, /* 26 MHz */
};

static struct omap2_hsmmc_info mmc[] = {
	{
		.name		= "external",
		.mmc		= 1,
		.caps		= MMC_CAP_4_BIT_DATA,
		.gpio_wp	= -EINVAL,
		.power_saving	= true,
		.deferred	= true,
	},
	{
		.name		= "internal",
		.mmc		= 2,
		.caps		= MMC_CAP_4_BIT_DATA | MMC_CAP_8_BIT_DATA,
		.gpio_cd	= -EINVAL,
		.gpio_wp	= -EINVAL,
		.nonremovable	= true,
		.power_saving	= true,
	},
	{
		.name		= "wl1271",
		.mmc		= 3,
		.caps		= MMC_CAP_4_BIT_DATA | MMC_CAP_POWER_OFF_CARD,
		.gpio_wp	= -EINVAL,
		.gpio_cd	= -EINVAL,
		.nonremovable	= true,
	},
	{}      /* Terminator */
};

static struct omap_tw4030_pdata omap_twl4030_audio_data = {
	.voice_connected = true,
	.custom_routing	= true,

	.has_hs		= OMAP_TWL4030_LEFT | OMAP_TWL4030_RIGHT,
	.has_hf		= OMAP_TWL4030_LEFT | OMAP_TWL4030_RIGHT,

	.has_mainmic	= true,
	.has_submic	= true,
	.has_hsmic	= true,
	.has_linein	= OMAP_TWL4030_LEFT | OMAP_TWL4030_RIGHT,
};

static int zoom_twl_gpio_setup(struct device *dev,
		unsigned gpio, unsigned ngpio)
{
	/* gpio + 0 is "mmc0_cd" (input/IRQ) */
	mmc[0].gpio_cd = gpio + 0;
	omap_hsmmc_late_init(mmc);

	/* Audio setup */
	omap_twl4030_audio_data.jack_detect = gpio + 2;
	omap_twl4030_audio_init("Zoom2", &omap_twl4030_audio_data);

	return 0;
}

static struct twl4030_gpio_platform_data zoom_gpio_data = {
	.setup		= zoom_twl_gpio_setup,
};

static struct twl4030_platform_data zoom_twldata = {
	/* platform_data for children goes here */
	.gpio		= &zoom_gpio_data,
	.keypad		= &zoom_kp_twl4030_data,
	.vmmc1          = &zoom_vmmc1,
	.vmmc2          = &zoom_vmmc2,
	.vsim           = &zoom_vsim,
};

static int __init omap_i2c_init(void)
{
	omap3_pmic_get_config(&zoom_twldata,
			TWL_COMMON_PDATA_USB | TWL_COMMON_PDATA_BCI |
			TWL_COMMON_PDATA_MADC | TWL_COMMON_PDATA_AUDIO,
			TWL_COMMON_REGULATOR_VDAC | TWL_COMMON_REGULATOR_VPLL2);

	if (machine_is_omap_zoom2())
		zoom_twldata.audio->codec->ramp_delay_value = 3; /* 161 ms */

	omap_pmic_init(1, 2400, "twl5030", 7 + OMAP_INTC_START, &zoom_twldata);
	omap_register_i2c_bus(2, 400, NULL, 0);
	omap_register_i2c_bus(3, 400, NULL, 0);
	return 0;
}

static void enable_board_wakeup_source(void)
{
	/* T2 interrupt line (keypad) */
	omap_mux_init_signal("sys_nirq",
		OMAP_WAKEUP_EN | OMAP_PIN_INPUT_PULLUP);
}

void __init zoom_peripherals_init(void)
{
	int ret;

	omap_zoom_wlan_data.irq = gpio_to_irq(OMAP_ZOOM_WLAN_IRQ_GPIO);
	ret = wl12xx_set_platform_data(&omap_zoom_wlan_data);

	if (ret)
		pr_err("error setting wl12xx data: %d\n", ret);

	omap_hsmmc_init(mmc);
	omap_i2c_init();
	pwm_add_table(zoom_pwm_lookup, ARRAY_SIZE(zoom_pwm_lookup));
	platform_add_devices(zoom_devices, ARRAY_SIZE(zoom_devices));
	usb_bind_phy("musb-hdrc.0.auto", 0, "twl4030_usb");
	usb_musb_init(NULL);
	enable_board_wakeup_source();
	omap_serial_init();
}
