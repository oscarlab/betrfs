/*
 *
 * Copyright (C) 2007 Google, Inc.
 * Copyright (c) 2009-2012, The Linux Foundation. All rights reserved.
 *
 * This software is licensed under the terms of the GNU General Public
 * License version 2, as published by the Free Software Foundation, and
 * may be copied, distributed, and modified under those terms.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 */

#include <linux/clocksource.h>
#include <linux/clockchips.h>
#include <linux/init.h>
#include <linux/interrupt.h>
#include <linux/irq.h>
#include <linux/io.h>
#include <linux/of.h>
#include <linux/of_address.h>
#include <linux/of_irq.h>
#include <linux/sched_clock.h>

#include <asm/mach/time.h>
#include <asm/localtimer.h>

#include "common.h"

#define TIMER_MATCH_VAL			0x0000
#define TIMER_COUNT_VAL			0x0004
#define TIMER_ENABLE			0x0008
#define TIMER_ENABLE_CLR_ON_MATCH_EN	BIT(1)
#define TIMER_ENABLE_EN			BIT(0)
#define TIMER_CLEAR			0x000C
#define DGT_CLK_CTL			0x10
#define DGT_CLK_CTL_DIV_4		0x3
#define TIMER_STS_GPT0_CLR_PEND		BIT(10)

#define GPT_HZ 32768

#define MSM_DGT_SHIFT 5

static void __iomem *event_base;
static void __iomem *sts_base;

static irqreturn_t msm_timer_interrupt(int irq, void *dev_id)
{
	struct clock_event_device *evt = *(struct clock_event_device **)dev_id;
	/* Stop the timer tick */
	if (evt->mode == CLOCK_EVT_MODE_ONESHOT) {
		u32 ctrl = readl_relaxed(event_base + TIMER_ENABLE);
		ctrl &= ~TIMER_ENABLE_EN;
		writel_relaxed(ctrl, event_base + TIMER_ENABLE);
	}
	evt->event_handler(evt);
	return IRQ_HANDLED;
}

static int msm_timer_set_next_event(unsigned long cycles,
				    struct clock_event_device *evt)
{
	u32 ctrl = readl_relaxed(event_base + TIMER_ENABLE);

	ctrl &= ~TIMER_ENABLE_EN;
	writel_relaxed(ctrl, event_base + TIMER_ENABLE);

	writel_relaxed(ctrl, event_base + TIMER_CLEAR);
	writel_relaxed(cycles, event_base + TIMER_MATCH_VAL);

	if (sts_base)
		while (readl_relaxed(sts_base) & TIMER_STS_GPT0_CLR_PEND)
			cpu_relax();

	writel_relaxed(ctrl | TIMER_ENABLE_EN, event_base + TIMER_ENABLE);
	return 0;
}

static void msm_timer_set_mode(enum clock_event_mode mode,
			      struct clock_event_device *evt)
{
	u32 ctrl;

	ctrl = readl_relaxed(event_base + TIMER_ENABLE);
	ctrl &= ~(TIMER_ENABLE_EN | TIMER_ENABLE_CLR_ON_MATCH_EN);

	switch (mode) {
	case CLOCK_EVT_MODE_RESUME:
	case CLOCK_EVT_MODE_PERIODIC:
		break;
	case CLOCK_EVT_MODE_ONESHOT:
		/* Timer is enabled in set_next_event */
		break;
	case CLOCK_EVT_MODE_UNUSED:
	case CLOCK_EVT_MODE_SHUTDOWN:
		break;
	}
	writel_relaxed(ctrl, event_base + TIMER_ENABLE);
}

static struct clock_event_device msm_clockevent = {
	.name		= "gp_timer",
	.features	= CLOCK_EVT_FEAT_ONESHOT,
	.rating		= 200,
	.set_next_event	= msm_timer_set_next_event,
	.set_mode	= msm_timer_set_mode,
};

static union {
	struct clock_event_device *evt;
	struct clock_event_device * __percpu *percpu_evt;
} msm_evt;

static void __iomem *source_base;

static notrace cycle_t msm_read_timer_count(struct clocksource *cs)
{
	return readl_relaxed(source_base + TIMER_COUNT_VAL);
}

static notrace cycle_t msm_read_timer_count_shift(struct clocksource *cs)
{
	/*
	 * Shift timer count down by a constant due to unreliable lower bits
	 * on some targets.
	 */
	return msm_read_timer_count(cs) >> MSM_DGT_SHIFT;
}

static struct clocksource msm_clocksource = {
	.name	= "dg_timer",
	.rating	= 300,
	.read	= msm_read_timer_count,
	.mask	= CLOCKSOURCE_MASK(32),
	.flags	= CLOCK_SOURCE_IS_CONTINUOUS,
};

#ifdef CONFIG_LOCAL_TIMERS
static int msm_local_timer_setup(struct clock_event_device *evt)
{
	/* Use existing clock_event for cpu 0 */
	if (!smp_processor_id())
		return 0;

	evt->irq = msm_clockevent.irq;
	evt->name = "local_timer";
	evt->features = msm_clockevent.features;
	evt->rating = msm_clockevent.rating;
	evt->set_mode = msm_timer_set_mode;
	evt->set_next_event = msm_timer_set_next_event;

	*__this_cpu_ptr(msm_evt.percpu_evt) = evt;
	clockevents_config_and_register(evt, GPT_HZ, 4, 0xf0000000);
	enable_percpu_irq(evt->irq, IRQ_TYPE_EDGE_RISING);
	return 0;
}

static void msm_local_timer_stop(struct clock_event_device *evt)
{
	evt->set_mode(CLOCK_EVT_MODE_UNUSED, evt);
	disable_percpu_irq(evt->irq);
}

static struct local_timer_ops msm_local_timer_ops = {
	.setup	= msm_local_timer_setup,
	.stop	= msm_local_timer_stop,
};
#endif /* CONFIG_LOCAL_TIMERS */

static notrace u32 msm_sched_clock_read(void)
{
	return msm_clocksource.read(&msm_clocksource);
}

static void __init msm_timer_init(u32 dgt_hz, int sched_bits, int irq,
				  bool percpu)
{
	struct clock_event_device *ce = &msm_clockevent;
	struct clocksource *cs = &msm_clocksource;
	int res;

	ce->cpumask = cpumask_of(0);
	ce->irq = irq;

	clockevents_config_and_register(ce, GPT_HZ, 4, 0xffffffff);
	if (percpu) {
		msm_evt.percpu_evt = alloc_percpu(struct clock_event_device *);
		if (!msm_evt.percpu_evt) {
			pr_err("memory allocation failed for %s\n", ce->name);
			goto err;
		}
		*__this_cpu_ptr(msm_evt.percpu_evt) = ce;
		res = request_percpu_irq(ce->irq, msm_timer_interrupt,
					 ce->name, msm_evt.percpu_evt);
		if (!res) {
			enable_percpu_irq(ce->irq, IRQ_TYPE_EDGE_RISING);
#ifdef CONFIG_LOCAL_TIMERS
			local_timer_register(&msm_local_timer_ops);
#endif
		}
	} else {
		msm_evt.evt = ce;
		res = request_irq(ce->irq, msm_timer_interrupt,
				  IRQF_TIMER | IRQF_NOBALANCING |
				  IRQF_TRIGGER_RISING, ce->name, &msm_evt.evt);
	}

	if (res)
		pr_err("request_irq failed for %s\n", ce->name);
err:
	writel_relaxed(TIMER_ENABLE_EN, source_base + TIMER_ENABLE);
	res = clocksource_register_hz(cs, dgt_hz);
	if (res)
		pr_err("clocksource_register failed\n");
	setup_sched_clock(msm_sched_clock_read, sched_bits, dgt_hz);
}

#ifdef CONFIG_OF
static const struct of_device_id msm_timer_match[] __initconst = {
	{ .compatible = "qcom,kpss-timer" },
	{ .compatible = "qcom,scss-timer" },
	{ },
};

void __init msm_dt_timer_init(void)
{
	struct device_node *np;
	u32 freq;
	int irq;
	struct resource res;
	u32 percpu_offset;
	void __iomem *base;
	void __iomem *cpu0_base;

	np = of_find_matching_node(NULL, msm_timer_match);
	if (!np) {
		pr_err("Can't find msm timer DT node\n");
		return;
	}

	base = of_iomap(np, 0);
	if (!base) {
		pr_err("Failed to map event base\n");
		return;
	}

	/* We use GPT0 for the clockevent */
	irq = irq_of_parse_and_map(np, 1);
	if (irq <= 0) {
		pr_err("Can't get irq\n");
		return;
	}

	/* We use CPU0's DGT for the clocksource */
	if (of_property_read_u32(np, "cpu-offset", &percpu_offset))
		percpu_offset = 0;

	if (of_address_to_resource(np, 0, &res)) {
		pr_err("Failed to parse DGT resource\n");
		return;
	}

	cpu0_base = ioremap(res.start + percpu_offset, resource_size(&res));
	if (!cpu0_base) {
		pr_err("Failed to map source base\n");
		return;
	}

	if (of_property_read_u32(np, "clock-frequency", &freq)) {
		pr_err("Unknown frequency\n");
		return;
	}
	of_node_put(np);

	event_base = base + 0x4;
	sts_base = base + 0x88;
	source_base = cpu0_base + 0x24;
	freq /= 4;
	writel_relaxed(DGT_CLK_CTL_DIV_4, source_base + DGT_CLK_CTL);

	msm_timer_init(freq, 32, irq, !!percpu_offset);
}
#endif

static int __init msm_timer_map(phys_addr_t addr, u32 event, u32 source,
				u32 sts)
{
	void __iomem *base;

	base = ioremap(addr, SZ_256);
	if (!base) {
		pr_err("Failed to map timer base\n");
		return -ENOMEM;
	}
	event_base = base + event;
	source_base = base + source;
	if (sts)
		sts_base = base + sts;

	return 0;
}

void __init msm7x01_timer_init(void)
{
	struct clocksource *cs = &msm_clocksource;

	if (msm_timer_map(0xc0100000, 0x0, 0x10, 0x0))
		return;
	cs->read = msm_read_timer_count_shift;
	cs->mask = CLOCKSOURCE_MASK((32 - MSM_DGT_SHIFT));
	/* 600 KHz */
	msm_timer_init(19200000 >> MSM_DGT_SHIFT, 32 - MSM_DGT_SHIFT, 7,
			false);
}

void __init msm7x30_timer_init(void)
{
	if (msm_timer_map(0xc0100000, 0x4, 0x24, 0x80))
		return;
	msm_timer_init(24576000 / 4, 32, 1, false);
}

void __init qsd8x50_timer_init(void)
{
	if (msm_timer_map(0xAC100000, 0x0, 0x10, 0x34))
		return;
	msm_timer_init(19200000 / 4, 32, 7, false);
}
