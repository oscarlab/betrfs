/* Copyright (c) 2010-2011, Code Aurora Forum. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 and
 * only version 2 as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 */

#include <linux/module.h>
#include <linux/init.h>
#include <linux/kernel.h>
#include <linux/errno.h>
#include <linux/slab.h>
#include <linux/input.h>
#include <linux/interrupt.h>
#include <linux/platform_device.h>
#include <linux/log2.h>

#include <linux/mfd/pm8xxx/core.h>
#include <linux/input/pmic8xxx-pwrkey.h>

#define PON_CNTL_1 0x1C
#define PON_CNTL_PULL_UP BIT(7)
#define PON_CNTL_TRIG_DELAY_MASK (0x7)

/**
 * struct pmic8xxx_pwrkey - pmic8xxx pwrkey information
 * @key_press_irq: key press irq number
 */
struct pmic8xxx_pwrkey {
	struct input_dev *pwr;
	int key_press_irq;
};

static irqreturn_t pwrkey_press_irq(int irq, void *_pwrkey)
{
	struct pmic8xxx_pwrkey *pwrkey = _pwrkey;

	input_report_key(pwrkey->pwr, KEY_POWER, 1);
	input_sync(pwrkey->pwr);

	return IRQ_HANDLED;
}

static irqreturn_t pwrkey_release_irq(int irq, void *_pwrkey)
{
	struct pmic8xxx_pwrkey *pwrkey = _pwrkey;

	input_report_key(pwrkey->pwr, KEY_POWER, 0);
	input_sync(pwrkey->pwr);

	return IRQ_HANDLED;
}

#ifdef CONFIG_PM_SLEEP
static int pmic8xxx_pwrkey_suspend(struct device *dev)
{
	struct pmic8xxx_pwrkey *pwrkey = dev_get_drvdata(dev);

	if (device_may_wakeup(dev))
		enable_irq_wake(pwrkey->key_press_irq);

	return 0;
}

static int pmic8xxx_pwrkey_resume(struct device *dev)
{
	struct pmic8xxx_pwrkey *pwrkey = dev_get_drvdata(dev);

	if (device_may_wakeup(dev))
		disable_irq_wake(pwrkey->key_press_irq);

	return 0;
}
#endif

static SIMPLE_DEV_PM_OPS(pm8xxx_pwr_key_pm_ops,
		pmic8xxx_pwrkey_suspend, pmic8xxx_pwrkey_resume);

static int pmic8xxx_pwrkey_probe(struct platform_device *pdev)
{
	struct input_dev *pwr;
	int key_release_irq = platform_get_irq(pdev, 0);
	int key_press_irq = platform_get_irq(pdev, 1);
	int err;
	unsigned int delay;
	u8 pon_cntl;
	struct pmic8xxx_pwrkey *pwrkey;
	const struct pm8xxx_pwrkey_platform_data *pdata =
					dev_get_platdata(&pdev->dev);

	if (!pdata) {
		dev_err(&pdev->dev, "power key platform data not supplied\n");
		return -EINVAL;
	}

	if (pdata->kpd_trigger_delay_us > 62500) {
		dev_err(&pdev->dev, "invalid power key trigger delay\n");
		return -EINVAL;
	}

	pwrkey = kzalloc(sizeof(*pwrkey), GFP_KERNEL);
	if (!pwrkey)
		return -ENOMEM;

	pwr = input_allocate_device();
	if (!pwr) {
		dev_dbg(&pdev->dev, "Can't allocate power button\n");
		err = -ENOMEM;
		goto free_pwrkey;
	}

	input_set_capability(pwr, EV_KEY, KEY_POWER);

	pwr->name = "pmic8xxx_pwrkey";
	pwr->phys = "pmic8xxx_pwrkey/input0";
	pwr->dev.parent = &pdev->dev;

	delay = (pdata->kpd_trigger_delay_us << 10) / USEC_PER_SEC;
	delay = 1 + ilog2(delay);

	err = pm8xxx_readb(pdev->dev.parent, PON_CNTL_1, &pon_cntl);
	if (err < 0) {
		dev_err(&pdev->dev, "failed reading PON_CNTL_1 err=%d\n", err);
		goto free_input_dev;
	}

	pon_cntl &= ~PON_CNTL_TRIG_DELAY_MASK;
	pon_cntl |= (delay & PON_CNTL_TRIG_DELAY_MASK);
	if (pdata->pull_up)
		pon_cntl |= PON_CNTL_PULL_UP;
	else
		pon_cntl &= ~PON_CNTL_PULL_UP;

	err = pm8xxx_writeb(pdev->dev.parent, PON_CNTL_1, pon_cntl);
	if (err < 0) {
		dev_err(&pdev->dev, "failed writing PON_CNTL_1 err=%d\n", err);
		goto free_input_dev;
	}

	err = input_register_device(pwr);
	if (err) {
		dev_dbg(&pdev->dev, "Can't register power key: %d\n", err);
		goto free_input_dev;
	}

	pwrkey->key_press_irq = key_press_irq;
	pwrkey->pwr = pwr;

	platform_set_drvdata(pdev, pwrkey);

	err = request_irq(key_press_irq, pwrkey_press_irq,
		IRQF_TRIGGER_RISING, "pmic8xxx_pwrkey_press", pwrkey);
	if (err < 0) {
		dev_dbg(&pdev->dev, "Can't get %d IRQ for pwrkey: %d\n",
				 key_press_irq, err);
		goto unreg_input_dev;
	}

	err = request_irq(key_release_irq, pwrkey_release_irq,
		 IRQF_TRIGGER_RISING, "pmic8xxx_pwrkey_release", pwrkey);
	if (err < 0) {
		dev_dbg(&pdev->dev, "Can't get %d IRQ for pwrkey: %d\n",
				 key_release_irq, err);

		goto free_press_irq;
	}

	device_init_wakeup(&pdev->dev, pdata->wakeup);

	return 0;

free_press_irq:
	free_irq(key_press_irq, pwrkey);
unreg_input_dev:
	input_unregister_device(pwr);
	pwr = NULL;
free_input_dev:
	input_free_device(pwr);
free_pwrkey:
	kfree(pwrkey);
	return err;
}

static int pmic8xxx_pwrkey_remove(struct platform_device *pdev)
{
	struct pmic8xxx_pwrkey *pwrkey = platform_get_drvdata(pdev);
	int key_release_irq = platform_get_irq(pdev, 0);
	int key_press_irq = platform_get_irq(pdev, 1);

	device_init_wakeup(&pdev->dev, 0);

	free_irq(key_press_irq, pwrkey);
	free_irq(key_release_irq, pwrkey);
	input_unregister_device(pwrkey->pwr);
	kfree(pwrkey);

	return 0;
}

static struct platform_driver pmic8xxx_pwrkey_driver = {
	.probe		= pmic8xxx_pwrkey_probe,
	.remove		= pmic8xxx_pwrkey_remove,
	.driver		= {
		.name	= PM8XXX_PWRKEY_DEV_NAME,
		.owner	= THIS_MODULE,
		.pm	= &pm8xxx_pwr_key_pm_ops,
	},
};
module_platform_driver(pmic8xxx_pwrkey_driver);

MODULE_ALIAS("platform:pmic8xxx_pwrkey");
MODULE_DESCRIPTION("PMIC8XXX Power Key driver");
MODULE_LICENSE("GPL v2");
MODULE_AUTHOR("Trilok Soni <tsoni@codeaurora.org>");
