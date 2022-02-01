/*
 * ci.h - common structures, functions, and macros of the ChipIdea driver
 *
 * Copyright (C) 2008 Chipidea - MIPS Technologies, Inc. All rights reserved.
 *
 * Author: David Lopo
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

#ifndef __DRIVERS_USB_CHIPIDEA_CI_H
#define __DRIVERS_USB_CHIPIDEA_CI_H

#include <linux/list.h>
#include <linux/irqreturn.h>
#include <linux/usb.h>
#include <linux/usb/gadget.h>

/******************************************************************************
 * DEFINE
 *****************************************************************************/
#define TD_PAGE_COUNT      5
#define CI_HDRC_PAGE_SIZE  4096ul /* page size for TD's */
#define ENDPT_MAX          32

/******************************************************************************
 * STRUCTURES
 *****************************************************************************/
/**
 * struct ci_hw_ep - endpoint representation
 * @ep: endpoint structure for gadget drivers
 * @dir: endpoint direction (TX/RX)
 * @num: endpoint number
 * @type: endpoint type
 * @name: string description of the endpoint
 * @qh: queue head for this endpoint
 * @wedge: is the endpoint wedged
 * @ci: pointer to the controller
 * @lock: pointer to controller's spinlock
 * @td_pool: pointer to controller's TD pool
 */
struct ci_hw_ep {
	struct usb_ep				ep;
	u8					dir;
	u8					num;
	u8					type;
	char					name[16];
	struct {
		struct list_head	queue;
		struct ci_hw_qh		*ptr;
		dma_addr_t		dma;
	}					qh;
	int					wedge;

	/* global resources */
	struct ci_hdrc				*ci;
	spinlock_t				*lock;
	struct dma_pool				*td_pool;
	struct td_node				*pending_td;
};

enum ci_role {
	CI_ROLE_HOST = 0,
	CI_ROLE_GADGET,
	CI_ROLE_END,
};

/**
 * struct ci_role_driver - host/gadget role driver
 * start: start this role
 * stop: stop this role
 * irq: irq handler for this role
 * name: role name string (host/gadget)
 */
struct ci_role_driver {
	int		(*start)(struct ci_hdrc *);
	void		(*stop)(struct ci_hdrc *);
	irqreturn_t	(*irq)(struct ci_hdrc *);
	const char	*name;
};

/**
 * struct hw_bank - hardware register mapping representation
 * @lpm: set if the device is LPM capable
 * @phys: physical address of the controller's registers
 * @abs: absolute address of the beginning of register window
 * @cap: capability registers
 * @op: operational registers
 * @size: size of the register window
 * @regmap: register lookup table
 */
struct hw_bank {
	unsigned	lpm;
	resource_size_t	phys;
	void __iomem	*abs;
	void __iomem	*cap;
	void __iomem	*op;
	size_t		size;
	void __iomem	**regmap;
};

/**
 * struct ci_hdrc - chipidea device representation
 * @dev: pointer to parent device
 * @lock: access synchronization
 * @hw_bank: hardware register mapping
 * @irq: IRQ number
 * @roles: array of supported roles for this controller
 * @role: current role
 * @is_otg: if the device is otg-capable
 * @work: work for role changing
 * @wq: workqueue thread
 * @qh_pool: allocation pool for queue heads
 * @td_pool: allocation pool for transfer descriptors
 * @gadget: device side representation for peripheral controller
 * @driver: gadget driver
 * @hw_ep_max: total number of endpoints supported by hardware
 * @ci_hw_ep: array of endpoints
 * @ep0_dir: ep0 direction
 * @ep0out: pointer to ep0 OUT endpoint
 * @ep0in: pointer to ep0 IN endpoint
 * @status: ep0 status request
 * @setaddr: if we should set the address on status completion
 * @address: usb address received from the host
 * @remote_wakeup: host-enabled remote wakeup
 * @suspended: suspended by host
 * @test_mode: the selected test mode
 * @platdata: platform specific information supplied by parent device
 * @vbus_active: is VBUS active
 * @transceiver: pointer to USB PHY, if any
 * @hcd: pointer to usb_hcd for ehci host driver
 * @debugfs: root dentry for this controller in debugfs
 */
struct ci_hdrc {
	struct device			*dev;
	spinlock_t			lock;
	struct hw_bank			hw_bank;
	int				irq;
	struct ci_role_driver		*roles[CI_ROLE_END];
	enum ci_role			role;
	bool				is_otg;
	struct work_struct		work;
	struct workqueue_struct		*wq;

	struct dma_pool			*qh_pool;
	struct dma_pool			*td_pool;

	struct usb_gadget		gadget;
	struct usb_gadget_driver	*driver;
	unsigned			hw_ep_max;
	struct ci_hw_ep			ci_hw_ep[ENDPT_MAX];
	u32				ep0_dir;
	struct ci_hw_ep			*ep0out, *ep0in;

	struct usb_request		*status;
	bool				setaddr;
	u8				address;
	u8				remote_wakeup;
	u8				suspended;
	u8				test_mode;

	struct ci_hdrc_platform_data	*platdata;
	int				vbus_active;
	/* FIXME: some day, we'll not use global phy */
	bool				global_phy;
	struct usb_phy			*transceiver;
	struct usb_hcd			*hcd;
	struct dentry			*debugfs;
};

static inline struct ci_role_driver *ci_role(struct ci_hdrc *ci)
{
	BUG_ON(ci->role >= CI_ROLE_END || !ci->roles[ci->role]);
	return ci->roles[ci->role];
}

static inline int ci_role_start(struct ci_hdrc *ci, enum ci_role role)
{
	int ret;

	if (role >= CI_ROLE_END)
		return -EINVAL;

	if (!ci->roles[role])
		return -ENXIO;

	ret = ci->roles[role]->start(ci);
	if (!ret)
		ci->role = role;
	return ret;
}

static inline void ci_role_stop(struct ci_hdrc *ci)
{
	enum ci_role role = ci->role;

	if (role == CI_ROLE_END)
		return;

	ci->role = CI_ROLE_END;

	ci->roles[role]->stop(ci);
}

/******************************************************************************
 * REGISTERS
 *****************************************************************************/
/* register size */
#define REG_BITS   (32)

/* register indices */
enum ci_hw_regs {
	CAP_CAPLENGTH,
	CAP_HCCPARAMS,
	CAP_DCCPARAMS,
	CAP_TESTMODE,
	CAP_LAST = CAP_TESTMODE,
	OP_USBCMD,
	OP_USBSTS,
	OP_USBINTR,
	OP_DEVICEADDR,
	OP_ENDPTLISTADDR,
	OP_PORTSC,
	OP_DEVLC,
	OP_OTGSC,
	OP_USBMODE,
	OP_ENDPTSETUPSTAT,
	OP_ENDPTPRIME,
	OP_ENDPTFLUSH,
	OP_ENDPTSTAT,
	OP_ENDPTCOMPLETE,
	OP_ENDPTCTRL,
	/* endptctrl1..15 follow */
	OP_LAST = OP_ENDPTCTRL + ENDPT_MAX / 2,
};

/**
 * hw_read: reads from a hw register
 * @reg:  register index
 * @mask: bitfield mask
 *
 * This function returns register contents
 */
static inline u32 hw_read(struct ci_hdrc *ci, enum ci_hw_regs reg, u32 mask)
{
	return ioread32(ci->hw_bank.regmap[reg]) & mask;
}

/**
 * hw_write: writes to a hw register
 * @reg:  register index
 * @mask: bitfield mask
 * @data: new value
 */
static inline void hw_write(struct ci_hdrc *ci, enum ci_hw_regs reg,
			    u32 mask, u32 data)
{
	if (~mask)
		data = (ioread32(ci->hw_bank.regmap[reg]) & ~mask)
			| (data & mask);

	iowrite32(data, ci->hw_bank.regmap[reg]);
}

/**
 * hw_test_and_clear: tests & clears a hw register
 * @reg:  register index
 * @mask: bitfield mask
 *
 * This function returns register contents
 */
static inline u32 hw_test_and_clear(struct ci_hdrc *ci, enum ci_hw_regs reg,
				    u32 mask)
{
	u32 val = ioread32(ci->hw_bank.regmap[reg]) & mask;

	iowrite32(val, ci->hw_bank.regmap[reg]);
	return val;
}

/**
 * hw_test_and_write: tests & writes a hw register
 * @reg:  register index
 * @mask: bitfield mask
 * @data: new value
 *
 * This function returns register contents
 */
static inline u32 hw_test_and_write(struct ci_hdrc *ci, enum ci_hw_regs reg,
				    u32 mask, u32 data)
{
	u32 val = hw_read(ci, reg, ~0);

	hw_write(ci, reg, mask, data);
	return (val & mask) >> __ffs(mask);
}

int hw_device_reset(struct ci_hdrc *ci, u32 mode);

int hw_port_test_set(struct ci_hdrc *ci, u8 mode);

u8 hw_port_test_get(struct ci_hdrc *ci);

#endif	/* __DRIVERS_USB_CHIPIDEA_CI_H */
