/****************************************************************************
 * Driver for Solarflare Solarstorm network controllers and boards
 * Copyright 2005-2006 Fen Systems Ltd.
 * Copyright 2006-2010 Solarflare Communications Inc.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published
 * by the Free Software Foundation, incorporated herein by reference.
 */

#ifndef EFX_EFX_H
#define EFX_EFX_H

#include "net_driver.h"
#include "filter.h"

/* Solarstorm controllers use BAR 0 for I/O space and BAR 2(&3) for memory */
#define EFX_MEM_BAR 2

/* TX */
extern int efx_probe_tx_queue(struct efx_tx_queue *tx_queue);
extern void efx_remove_tx_queue(struct efx_tx_queue *tx_queue);
extern void efx_init_tx_queue(struct efx_tx_queue *tx_queue);
extern void efx_init_tx_queue_core_txq(struct efx_tx_queue *tx_queue);
extern void efx_fini_tx_queue(struct efx_tx_queue *tx_queue);
extern void efx_release_tx_buffers(struct efx_tx_queue *tx_queue);
extern netdev_tx_t
efx_hard_start_xmit(struct sk_buff *skb, struct net_device *net_dev);
extern netdev_tx_t
efx_enqueue_skb(struct efx_tx_queue *tx_queue, struct sk_buff *skb);
extern void efx_xmit_done(struct efx_tx_queue *tx_queue, unsigned int index);
extern int efx_setup_tc(struct net_device *net_dev, u8 num_tc);
extern unsigned int efx_tx_max_skb_descs(struct efx_nic *efx);

/* RX */
extern void efx_rx_config_page_split(struct efx_nic *efx);
extern int efx_probe_rx_queue(struct efx_rx_queue *rx_queue);
extern void efx_remove_rx_queue(struct efx_rx_queue *rx_queue);
extern void efx_init_rx_queue(struct efx_rx_queue *rx_queue);
extern void efx_fini_rx_queue(struct efx_rx_queue *rx_queue);
extern void efx_fast_push_rx_descriptors(struct efx_rx_queue *rx_queue);
extern void efx_rx_slow_fill(unsigned long context);
extern void __efx_rx_packet(struct efx_channel *channel);
extern void efx_rx_packet(struct efx_rx_queue *rx_queue,
			  unsigned int index, unsigned int n_frags,
			  unsigned int len, u16 flags);
static inline void efx_rx_flush_packet(struct efx_channel *channel)
{
	if (channel->rx_pkt_n_frags)
		__efx_rx_packet(channel);
}
extern void efx_schedule_slow_fill(struct efx_rx_queue *rx_queue);

#define EFX_MAX_DMAQ_SIZE 4096UL
#define EFX_DEFAULT_DMAQ_SIZE 1024UL
#define EFX_MIN_DMAQ_SIZE 512UL

#define EFX_MAX_EVQ_SIZE 16384UL
#define EFX_MIN_EVQ_SIZE 512UL

/* Maximum number of TCP segments we support for soft-TSO */
#define EFX_TSO_MAX_SEGS	100

/* The smallest [rt]xq_entries that the driver supports.  RX minimum
 * is a bit arbitrary.  For TX, we must have space for at least 2
 * TSO skbs.
 */
#define EFX_RXQ_MIN_ENT		128U
#define EFX_TXQ_MIN_ENT(efx)	(2 * efx_tx_max_skb_descs(efx))

/* Filters */
extern int efx_probe_filters(struct efx_nic *efx);
extern void efx_restore_filters(struct efx_nic *efx);
extern void efx_remove_filters(struct efx_nic *efx);
extern void efx_filter_update_rx_scatter(struct efx_nic *efx);
extern s32 efx_filter_insert_filter(struct efx_nic *efx,
				    struct efx_filter_spec *spec,
				    bool replace);
extern int efx_filter_remove_id_safe(struct efx_nic *efx,
				     enum efx_filter_priority priority,
				     u32 filter_id);
extern int efx_filter_get_filter_safe(struct efx_nic *efx,
				      enum efx_filter_priority priority,
				      u32 filter_id, struct efx_filter_spec *);
extern void efx_filter_clear_rx(struct efx_nic *efx,
				enum efx_filter_priority priority);
extern u32 efx_filter_count_rx_used(struct efx_nic *efx,
				    enum efx_filter_priority priority);
extern u32 efx_filter_get_rx_id_limit(struct efx_nic *efx);
extern s32 efx_filter_get_rx_ids(struct efx_nic *efx,
				 enum efx_filter_priority priority,
				 u32 *buf, u32 size);
#ifdef CONFIG_RFS_ACCEL
extern int efx_filter_rfs(struct net_device *net_dev, const struct sk_buff *skb,
			  u16 rxq_index, u32 flow_id);
extern bool __efx_filter_rfs_expire(struct efx_nic *efx, unsigned quota);
static inline void efx_filter_rfs_expire(struct efx_channel *channel)
{
	if (channel->rfs_filters_added >= 60 &&
	    __efx_filter_rfs_expire(channel->efx, 100))
		channel->rfs_filters_added -= 60;
}
#define efx_filter_rfs_enabled() 1
#else
static inline void efx_filter_rfs_expire(struct efx_channel *channel) {}
#define efx_filter_rfs_enabled() 0
#endif

/* Channels */
extern int efx_channel_dummy_op_int(struct efx_channel *channel);
extern void efx_channel_dummy_op_void(struct efx_channel *channel);
extern void efx_process_channel_now(struct efx_channel *channel);
extern int
efx_realloc_channels(struct efx_nic *efx, u32 rxq_entries, u32 txq_entries);

/* Ports */
extern int efx_reconfigure_port(struct efx_nic *efx);
extern int __efx_reconfigure_port(struct efx_nic *efx);

/* Ethtool support */
extern const struct ethtool_ops efx_ethtool_ops;

/* Reset handling */
extern int efx_reset(struct efx_nic *efx, enum reset_type method);
extern void efx_reset_down(struct efx_nic *efx, enum reset_type method);
extern int efx_reset_up(struct efx_nic *efx, enum reset_type method, bool ok);
extern int efx_try_recovery(struct efx_nic *efx);

/* Global */
extern void efx_schedule_reset(struct efx_nic *efx, enum reset_type type);
extern int efx_init_irq_moderation(struct efx_nic *efx, unsigned int tx_usecs,
				   unsigned int rx_usecs, bool rx_adaptive,
				   bool rx_may_override_tx);
extern void efx_get_irq_moderation(struct efx_nic *efx, unsigned int *tx_usecs,
				   unsigned int *rx_usecs, bool *rx_adaptive);

/* Dummy PHY ops for PHY drivers */
extern int efx_port_dummy_op_int(struct efx_nic *efx);
extern void efx_port_dummy_op_void(struct efx_nic *efx);


/* MTD */
#ifdef CONFIG_SFC_MTD
extern int efx_mtd_probe(struct efx_nic *efx);
extern void efx_mtd_rename(struct efx_nic *efx);
extern void efx_mtd_remove(struct efx_nic *efx);
#else
static inline int efx_mtd_probe(struct efx_nic *efx) { return 0; }
static inline void efx_mtd_rename(struct efx_nic *efx) {}
static inline void efx_mtd_remove(struct efx_nic *efx) {}
#endif

static inline void efx_schedule_channel(struct efx_channel *channel)
{
	netif_vdbg(channel->efx, intr, channel->efx->net_dev,
		   "channel %d scheduling NAPI poll on CPU%d\n",
		   channel->channel, raw_smp_processor_id());
	channel->work_pending = true;

	napi_schedule(&channel->napi_str);
}

static inline void efx_schedule_channel_irq(struct efx_channel *channel)
{
	channel->event_test_cpu = raw_smp_processor_id();
	efx_schedule_channel(channel);
}

extern void efx_link_status_changed(struct efx_nic *efx);
extern void efx_link_set_advertising(struct efx_nic *efx, u32);
extern void efx_link_set_wanted_fc(struct efx_nic *efx, u8);

static inline void efx_device_detach_sync(struct efx_nic *efx)
{
	struct net_device *dev = efx->net_dev;

	/* Lock/freeze all TX queues so that we can be sure the
	 * TX scheduler is stopped when we're done and before
	 * netif_device_present() becomes false.
	 */
	netif_tx_lock_bh(dev);
	netif_device_detach(dev);
	netif_tx_unlock_bh(dev);
}

#endif /* EFX_EFX_H */
