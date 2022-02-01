/*
 * include/net/dsa.h - Driver for Distributed Switch Architecture switch chips
 * Copyright (c) 2008-2009 Marvell Semiconductor
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 */

#ifndef __LINUX_NET_DSA_H
#define __LINUX_NET_DSA_H

#include <linux/if_ether.h>
#include <linux/list.h>
#include <linux/timer.h>
#include <linux/workqueue.h>

#define DSA_MAX_SWITCHES	4
#define DSA_MAX_PORTS		12

struct dsa_chip_data {
	/*
	 * How to access the switch configuration registers.
	 */
	struct device	*mii_bus;
	int		sw_addr;

	/*
	 * The names of the switch's ports.  Use "cpu" to
	 * designate the switch port that the cpu is connected to,
	 * "dsa" to indicate that this port is a DSA link to
	 * another switch, NULL to indicate the port is unused,
	 * or any other string to indicate this is a physical port.
	 */
	char		*port_names[DSA_MAX_PORTS];

	/*
	 * An array (with nr_chips elements) of which element [a]
	 * indicates which port on this switch should be used to
	 * send packets to that are destined for switch a.  Can be
	 * NULL if there is only one switch chip.
	 */
	s8		*rtable;
};

struct dsa_platform_data {
	/*
	 * Reference to a Linux network interface that connects
	 * to the root switch chip of the tree.
	 */
	struct device	*netdev;

	/*
	 * Info structs describing each of the switch chips
	 * connected via this network interface.
	 */
	int		nr_chips;
	struct dsa_chip_data	*chip;
};

struct dsa_switch_tree {
	/*
	 * Configuration data for the platform device that owns
	 * this dsa switch tree instance.
	 */
	struct dsa_platform_data	*pd;

	/*
	 * Reference to network device to use, and which tagging
	 * protocol to use.
	 */
	struct net_device	*master_netdev;
	__be16			tag_protocol;

	/*
	 * The switch and port to which the CPU is attached.
	 */
	s8			cpu_switch;
	s8			cpu_port;

	/*
	 * Link state polling.
	 */
	int			link_poll_needed;
	struct work_struct	link_poll_work;
	struct timer_list	link_poll_timer;

	/*
	 * Data for the individual switch chips.
	 */
	struct dsa_switch	*ds[DSA_MAX_SWITCHES];
};

struct dsa_switch {
	/*
	 * Parent switch tree, and switch index.
	 */
	struct dsa_switch_tree	*dst;
	int			index;

	/*
	 * Configuration data for this switch.
	 */
	struct dsa_chip_data	*pd;

	/*
	 * The used switch driver.
	 */
	struct dsa_switch_driver	*drv;

	/*
	 * Reference to mii bus to use.
	 */
	struct mii_bus		*master_mii_bus;

	/*
	 * Slave mii_bus and devices for the individual ports.
	 */
	u32			dsa_port_mask;
	u32			phys_port_mask;
	struct mii_bus		*slave_mii_bus;
	struct net_device	*ports[DSA_MAX_PORTS];
};

static inline bool dsa_is_cpu_port(struct dsa_switch *ds, int p)
{
	return !!(ds->index == ds->dst->cpu_switch && p == ds->dst->cpu_port);
}

static inline u8 dsa_upstream_port(struct dsa_switch *ds)
{
	struct dsa_switch_tree *dst = ds->dst;

	/*
	 * If this is the root switch (i.e. the switch that connects
	 * to the CPU), return the cpu port number on this switch.
	 * Else return the (DSA) port number that connects to the
	 * switch that is one hop closer to the cpu.
	 */
	if (dst->cpu_switch == ds->index)
		return dst->cpu_port;
	else
		return ds->pd->rtable[dst->cpu_switch];
}

struct dsa_switch_driver {
	struct list_head	list;

	__be16			tag_protocol;
	int			priv_size;

	/*
	 * Probing and setup.
	 */
	char	*(*probe)(struct mii_bus *bus, int sw_addr);
	int	(*setup)(struct dsa_switch *ds);
	int	(*set_addr)(struct dsa_switch *ds, u8 *addr);

	/*
	 * Access to the switch's PHY registers.
	 */
	int	(*phy_read)(struct dsa_switch *ds, int port, int regnum);
	int	(*phy_write)(struct dsa_switch *ds, int port,
			     int regnum, u16 val);

	/*
	 * Link state polling and IRQ handling.
	 */
	void	(*poll_link)(struct dsa_switch *ds);

	/*
	 * ethtool hardware statistics.
	 */
	void	(*get_strings)(struct dsa_switch *ds, int port, uint8_t *data);
	void	(*get_ethtool_stats)(struct dsa_switch *ds,
				     int port, uint64_t *data);
	int	(*get_sset_count)(struct dsa_switch *ds);
};

void register_switch_driver(struct dsa_switch_driver *type);
void unregister_switch_driver(struct dsa_switch_driver *type);

/*
 * The original DSA tag format and some other tag formats have no
 * ethertype, which means that we need to add a little hack to the
 * networking receive path to make sure that received frames get
 * the right ->protocol assigned to them when one of those tag
 * formats is in use.
 */
static inline bool dsa_uses_dsa_tags(struct dsa_switch_tree *dst)
{
	return !!(dst->tag_protocol == htons(ETH_P_DSA));
}

static inline bool dsa_uses_trailer_tags(struct dsa_switch_tree *dst)
{
	return !!(dst->tag_protocol == htons(ETH_P_TRAILER));
}

#endif
