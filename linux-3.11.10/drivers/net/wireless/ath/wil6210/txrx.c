/*
 * Copyright (c) 2012 Qualcomm Atheros, Inc.
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <linux/etherdevice.h>
#include <net/ieee80211_radiotap.h>
#include <linux/if_arp.h>
#include <linux/moduleparam.h>

#include "wil6210.h"
#include "wmi.h"
#include "txrx.h"
#include "trace.h"

static bool rtap_include_phy_info;
module_param(rtap_include_phy_info, bool, S_IRUGO);
MODULE_PARM_DESC(rtap_include_phy_info,
		 " Include PHY info in the radiotap header, default - no");

static inline int wil_vring_is_empty(struct vring *vring)
{
	return vring->swhead == vring->swtail;
}

static inline u32 wil_vring_next_tail(struct vring *vring)
{
	return (vring->swtail + 1) % vring->size;
}

static inline void wil_vring_advance_head(struct vring *vring, int n)
{
	vring->swhead = (vring->swhead + n) % vring->size;
}

static inline int wil_vring_is_full(struct vring *vring)
{
	return wil_vring_next_tail(vring) == vring->swhead;
}
/*
 * Available space in Tx Vring
 */
static inline int wil_vring_avail_tx(struct vring *vring)
{
	u32 swhead = vring->swhead;
	u32 swtail = vring->swtail;
	int used = (vring->size + swhead - swtail) % vring->size;

	return vring->size - used - 1;
}

static int wil_vring_alloc(struct wil6210_priv *wil, struct vring *vring)
{
	struct device *dev = wil_to_dev(wil);
	size_t sz = vring->size * sizeof(vring->va[0]);
	uint i;

	BUILD_BUG_ON(sizeof(vring->va[0]) != 32);

	vring->swhead = 0;
	vring->swtail = 0;
	vring->ctx = kzalloc(vring->size * sizeof(vring->ctx[0]), GFP_KERNEL);
	if (!vring->ctx) {
		vring->va = NULL;
		return -ENOMEM;
	}
	/*
	 * vring->va should be aligned on its size rounded up to power of 2
	 * This is granted by the dma_alloc_coherent
	 */
	vring->va = dma_alloc_coherent(dev, sz, &vring->pa, GFP_KERNEL);
	if (!vring->va) {
		kfree(vring->ctx);
		vring->ctx = NULL;
		return -ENOMEM;
	}
	/* initially, all descriptors are SW owned
	 * For Tx and Rx, ownership bit is at the same location, thus
	 * we can use any
	 */
	for (i = 0; i < vring->size; i++) {
		volatile struct vring_tx_desc *_d = &(vring->va[i].tx);
		_d->dma.status = TX_DMA_STATUS_DU;
	}

	wil_dbg_misc(wil, "vring[%d] 0x%p:0x%016llx 0x%p\n", vring->size,
		     vring->va, (unsigned long long)vring->pa, vring->ctx);

	return 0;
}

static void wil_vring_free(struct wil6210_priv *wil, struct vring *vring,
			   int tx)
{
	struct device *dev = wil_to_dev(wil);
	size_t sz = vring->size * sizeof(vring->va[0]);

	while (!wil_vring_is_empty(vring)) {
		dma_addr_t pa;
		struct sk_buff *skb;
		u16 dmalen;

		if (tx) {
			struct vring_tx_desc dd, *d = &dd;
			volatile struct vring_tx_desc *_d =
					&vring->va[vring->swtail].tx;

			*d = *_d;
			pa = wil_desc_addr(&d->dma.addr);
			dmalen = le16_to_cpu(d->dma.length);
			skb = vring->ctx[vring->swtail];
			if (skb) {
				dma_unmap_single(dev, pa, dmalen,
						 DMA_TO_DEVICE);
				dev_kfree_skb_any(skb);
				vring->ctx[vring->swtail] = NULL;
			} else {
				dma_unmap_page(dev, pa, dmalen,
					       DMA_TO_DEVICE);
			}
			vring->swtail = wil_vring_next_tail(vring);
		} else { /* rx */
			struct vring_rx_desc dd, *d = &dd;
			volatile struct vring_rx_desc *_d =
					&vring->va[vring->swtail].rx;

			*d = *_d;
			pa = wil_desc_addr(&d->dma.addr);
			dmalen = le16_to_cpu(d->dma.length);
			skb = vring->ctx[vring->swhead];
			dma_unmap_single(dev, pa, dmalen, DMA_FROM_DEVICE);
			kfree_skb(skb);
			wil_vring_advance_head(vring, 1);
		}
	}
	dma_free_coherent(dev, sz, (void *)vring->va, vring->pa);
	kfree(vring->ctx);
	vring->pa = 0;
	vring->va = NULL;
	vring->ctx = NULL;
}

/**
 * Allocate one skb for Rx VRING
 *
 * Safe to call from IRQ
 */
static int wil_vring_alloc_skb(struct wil6210_priv *wil, struct vring *vring,
			       u32 i, int headroom)
{
	struct device *dev = wil_to_dev(wil);
	unsigned int sz = RX_BUF_LEN;
	struct vring_rx_desc dd, *d = &dd;
	volatile struct vring_rx_desc *_d = &(vring->va[i].rx);
	dma_addr_t pa;

	/* TODO align */
	struct sk_buff *skb = dev_alloc_skb(sz + headroom);
	if (unlikely(!skb))
		return -ENOMEM;

	skb_reserve(skb, headroom);
	skb_put(skb, sz);

	pa = dma_map_single(dev, skb->data, skb->len, DMA_FROM_DEVICE);
	if (unlikely(dma_mapping_error(dev, pa))) {
		kfree_skb(skb);
		return -ENOMEM;
	}

	d->dma.d0 = BIT(9) | RX_DMA_D0_CMD_DMA_IT;
	wil_desc_addr_set(&d->dma.addr, pa);
	/* ip_length don't care */
	/* b11 don't care */
	/* error don't care */
	d->dma.status = 0; /* BIT(0) should be 0 for HW_OWNED */
	d->dma.length = cpu_to_le16(sz);
	*_d = *d;
	vring->ctx[i] = skb;

	return 0;
}

/**
 * Adds radiotap header
 *
 * Any error indicated as "Bad FCS"
 *
 * Vendor data for 04:ce:14-1 (Wilocity-1) consists of:
 *  - Rx descriptor: 32 bytes
 *  - Phy info
 */
static void wil_rx_add_radiotap_header(struct wil6210_priv *wil,
				       struct sk_buff *skb)
{
	struct wireless_dev *wdev = wil->wdev;
	struct wil6210_rtap {
		struct ieee80211_radiotap_header rthdr;
		/* fields should be in the order of bits in rthdr.it_present */
		/* flags */
		u8 flags;
		/* channel */
		__le16 chnl_freq __aligned(2);
		__le16 chnl_flags;
		/* MCS */
		u8 mcs_present;
		u8 mcs_flags;
		u8 mcs_index;
	} __packed;
	struct wil6210_rtap_vendor {
		struct wil6210_rtap rtap;
		/* vendor */
		u8 vendor_oui[3] __aligned(2);
		u8 vendor_ns;
		__le16 vendor_skip;
		u8 vendor_data[0];
	} __packed;
	struct vring_rx_desc *d = wil_skb_rxdesc(skb);
	struct wil6210_rtap_vendor *rtap_vendor;
	int rtap_len = sizeof(struct wil6210_rtap);
	int phy_length = 0; /* phy info header size, bytes */
	static char phy_data[128];
	struct ieee80211_channel *ch = wdev->preset_chandef.chan;

	if (rtap_include_phy_info) {
		rtap_len = sizeof(*rtap_vendor) + sizeof(*d);
		/* calculate additional length */
		if (d->dma.status & RX_DMA_STATUS_PHY_INFO) {
			/**
			 * PHY info starts from 8-byte boundary
			 * there are 8-byte lines, last line may be partially
			 * written (HW bug), thus FW configures for last line
			 * to be excessive. Driver skips this last line.
			 */
			int len = min_t(int, 8 + sizeof(phy_data),
					wil_rxdesc_phy_length(d));
			if (len > 8) {
				void *p = skb_tail_pointer(skb);
				void *pa = PTR_ALIGN(p, 8);
				if (skb_tailroom(skb) >= len + (pa - p)) {
					phy_length = len - 8;
					memcpy(phy_data, pa, phy_length);
				}
			}
		}
		rtap_len += phy_length;
	}

	if (skb_headroom(skb) < rtap_len &&
	    pskb_expand_head(skb, rtap_len, 0, GFP_ATOMIC)) {
		wil_err(wil, "Unable to expand headrom to %d\n", rtap_len);
		return;
	}

	rtap_vendor = (void *)skb_push(skb, rtap_len);
	memset(rtap_vendor, 0, rtap_len);

	rtap_vendor->rtap.rthdr.it_version = PKTHDR_RADIOTAP_VERSION;
	rtap_vendor->rtap.rthdr.it_len = cpu_to_le16(rtap_len);
	rtap_vendor->rtap.rthdr.it_present = cpu_to_le32(
			(1 << IEEE80211_RADIOTAP_FLAGS) |
			(1 << IEEE80211_RADIOTAP_CHANNEL) |
			(1 << IEEE80211_RADIOTAP_MCS));
	if (d->dma.status & RX_DMA_STATUS_ERROR)
		rtap_vendor->rtap.flags |= IEEE80211_RADIOTAP_F_BADFCS;

	rtap_vendor->rtap.chnl_freq = cpu_to_le16(ch ? ch->center_freq : 58320);
	rtap_vendor->rtap.chnl_flags = cpu_to_le16(0);

	rtap_vendor->rtap.mcs_present = IEEE80211_RADIOTAP_MCS_HAVE_MCS;
	rtap_vendor->rtap.mcs_flags = 0;
	rtap_vendor->rtap.mcs_index = wil_rxdesc_mcs(d);

	if (rtap_include_phy_info) {
		rtap_vendor->rtap.rthdr.it_present |= cpu_to_le32(1 <<
				IEEE80211_RADIOTAP_VENDOR_NAMESPACE);
		/* OUI for Wilocity 04:ce:14 */
		rtap_vendor->vendor_oui[0] = 0x04;
		rtap_vendor->vendor_oui[1] = 0xce;
		rtap_vendor->vendor_oui[2] = 0x14;
		rtap_vendor->vendor_ns = 1;
		/* Rx descriptor + PHY data  */
		rtap_vendor->vendor_skip = cpu_to_le16(sizeof(*d) +
						       phy_length);
		memcpy(rtap_vendor->vendor_data, (void *)d, sizeof(*d));
		memcpy(rtap_vendor->vendor_data + sizeof(*d), phy_data,
		       phy_length);
	}
}

/*
 * Fast swap in place between 2 registers
 */
static void wil_swap_u16(u16 *a, u16 *b)
{
	*a ^= *b;
	*b ^= *a;
	*a ^= *b;
}

static void wil_swap_ethaddr(void *data)
{
	struct ethhdr *eth = data;
	u16 *s = (u16 *)eth->h_source;
	u16 *d = (u16 *)eth->h_dest;

	wil_swap_u16(s++, d++);
	wil_swap_u16(s++, d++);
	wil_swap_u16(s, d);
}

/**
 * reap 1 frame from @swhead
 *
 * Rx descriptor copied to skb->cb
 *
 * Safe to call from IRQ
 */
static struct sk_buff *wil_vring_reap_rx(struct wil6210_priv *wil,
					 struct vring *vring)
{
	struct device *dev = wil_to_dev(wil);
	struct net_device *ndev = wil_to_ndev(wil);
	volatile struct vring_rx_desc *_d;
	struct vring_rx_desc *d;
	struct sk_buff *skb;
	dma_addr_t pa;
	unsigned int sz = RX_BUF_LEN;
	u16 dmalen;
	u8 ftype;
	u8 ds_bits;

	BUILD_BUG_ON(sizeof(struct vring_rx_desc) > sizeof(skb->cb));

	if (wil_vring_is_empty(vring))
		return NULL;

	_d = &(vring->va[vring->swhead].rx);
	if (!(_d->dma.status & RX_DMA_STATUS_DU)) {
		/* it is not error, we just reached end of Rx done area */
		return NULL;
	}

	skb = vring->ctx[vring->swhead];
	d = wil_skb_rxdesc(skb);
	*d = *_d;
	pa = wil_desc_addr(&d->dma.addr);
	vring->ctx[vring->swhead] = NULL;
	wil_vring_advance_head(vring, 1);

	dma_unmap_single(dev, pa, sz, DMA_FROM_DEVICE);
	dmalen = le16_to_cpu(d->dma.length);

	trace_wil6210_rx(vring->swhead, d);
	wil_dbg_txrx(wil, "Rx[%3d] : %d bytes\n", vring->swhead, dmalen);
	wil_hex_dump_txrx("Rx ", DUMP_PREFIX_NONE, 32, 4,
			  (const void *)d, sizeof(*d), false);

	if (dmalen > sz) {
		wil_err(wil, "Rx size too large: %d bytes!\n", dmalen);
		kfree_skb(skb);
		return NULL;
	}
	skb_trim(skb, dmalen);

	wil_hex_dump_txrx("Rx ", DUMP_PREFIX_OFFSET, 16, 1,
			  skb->data, skb_headlen(skb), false);


	wil->stats.last_mcs_rx = wil_rxdesc_mcs(d);

	/* use radiotap header only if required */
	if (ndev->type == ARPHRD_IEEE80211_RADIOTAP)
		wil_rx_add_radiotap_header(wil, skb);

	/* no extra checks if in sniffer mode */
	if (ndev->type != ARPHRD_ETHER)
		return skb;
	/*
	 * Non-data frames may be delivered through Rx DMA channel (ex: BAR)
	 * Driver should recognize it by frame type, that is found
	 * in Rx descriptor. If type is not data, it is 802.11 frame as is
	 */
	ftype = wil_rxdesc_ftype(d) << 2;
	if (ftype != IEEE80211_FTYPE_DATA) {
		wil_dbg_txrx(wil, "Non-data frame ftype 0x%08x\n", ftype);
		/* TODO: process it */
		kfree_skb(skb);
		return NULL;
	}

	if (skb->len < ETH_HLEN) {
		wil_err(wil, "Short frame, len = %d\n", skb->len);
		/* TODO: process it (i.e. BAR) */
		kfree_skb(skb);
		return NULL;
	}

	ds_bits = wil_rxdesc_ds_bits(d);
	if (ds_bits == 1) {
		/*
		 * HW bug - in ToDS mode, i.e. Rx on AP side,
		 * addresses get swapped
		 */
		wil_swap_ethaddr(skb->data);
	}

	return skb;
}

/**
 * allocate and fill up to @count buffers in rx ring
 * buffers posted at @swtail
 */
static int wil_rx_refill(struct wil6210_priv *wil, int count)
{
	struct net_device *ndev = wil_to_ndev(wil);
	struct vring *v = &wil->vring_rx;
	u32 next_tail;
	int rc = 0;
	int headroom = ndev->type == ARPHRD_IEEE80211_RADIOTAP ?
			WIL6210_RTAP_SIZE : 0;

	for (; next_tail = wil_vring_next_tail(v),
			(next_tail != v->swhead) && (count-- > 0);
			v->swtail = next_tail) {
		rc = wil_vring_alloc_skb(wil, v, v->swtail, headroom);
		if (rc) {
			wil_err(wil, "Error %d in wil_rx_refill[%d]\n",
				rc, v->swtail);
			break;
		}
	}
	iowrite32(v->swtail, wil->csr + HOSTADDR(v->hwtail));

	return rc;
}

/*
 * Pass Rx packet to the netif. Update statistics.
 * Called in softirq context (NAPI poll).
 */
static void wil_netif_rx_any(struct sk_buff *skb, struct net_device *ndev)
{
	int rc;
	unsigned int len = skb->len;

	skb_orphan(skb);

	rc = netif_receive_skb(skb);

	if (likely(rc == NET_RX_SUCCESS)) {
		ndev->stats.rx_packets++;
		ndev->stats.rx_bytes += len;

	} else {
		ndev->stats.rx_dropped++;
	}
}

/**
 * Proceed all completed skb's from Rx VRING
 *
 * Safe to call from NAPI poll, i.e. softirq with interrupts enabled
 */
void wil_rx_handle(struct wil6210_priv *wil, int *quota)
{
	struct net_device *ndev = wil_to_ndev(wil);
	struct vring *v = &wil->vring_rx;
	struct sk_buff *skb;

	if (!v->va) {
		wil_err(wil, "Rx IRQ while Rx not yet initialized\n");
		return;
	}
	wil_dbg_txrx(wil, "%s()\n", __func__);
	while ((*quota > 0) && (NULL != (skb = wil_vring_reap_rx(wil, v)))) {
		(*quota)--;

		if (wil->wdev->iftype == NL80211_IFTYPE_MONITOR) {
			skb->dev = ndev;
			skb_reset_mac_header(skb);
			skb->ip_summed = CHECKSUM_UNNECESSARY;
			skb->pkt_type = PACKET_OTHERHOST;
			skb->protocol = htons(ETH_P_802_2);

		} else {
			skb->protocol = eth_type_trans(skb, ndev);
		}

		wil_netif_rx_any(skb, ndev);
	}
	wil_rx_refill(wil, v->size);
}

int wil_rx_init(struct wil6210_priv *wil)
{
	struct vring *vring = &wil->vring_rx;
	int rc;

	vring->size = WIL6210_RX_RING_SIZE;
	rc = wil_vring_alloc(wil, vring);
	if (rc)
		return rc;

	rc = wmi_rx_chain_add(wil, vring);
	if (rc)
		goto err_free;

	rc = wil_rx_refill(wil, vring->size);
	if (rc)
		goto err_free;

	return 0;
 err_free:
	wil_vring_free(wil, vring, 0);

	return rc;
}

void wil_rx_fini(struct wil6210_priv *wil)
{
	struct vring *vring = &wil->vring_rx;

	if (vring->va)
		wil_vring_free(wil, vring, 0);
}

int wil_vring_init_tx(struct wil6210_priv *wil, int id, int size,
		      int cid, int tid)
{
	int rc;
	struct wmi_vring_cfg_cmd cmd = {
		.action = cpu_to_le32(WMI_VRING_CMD_ADD),
		.vring_cfg = {
			.tx_sw_ring = {
				.max_mpdu_size = cpu_to_le16(TX_BUF_LEN),
				.ring_size = cpu_to_le16(size),
			},
			.ringid = id,
			.cidxtid = (cid & 0xf) | ((tid & 0xf) << 4),
			.encap_trans_type = WMI_VRING_ENC_TYPE_802_3,
			.mac_ctrl = 0,
			.to_resolution = 0,
			.agg_max_wsize = 16,
			.schd_params = {
				.priority = cpu_to_le16(0),
				.timeslot_us = cpu_to_le16(0xfff),
			},
		},
	};
	struct {
		struct wil6210_mbox_hdr_wmi wmi;
		struct wmi_vring_cfg_done_event cmd;
	} __packed reply;
	struct vring *vring = &wil->vring_tx[id];

	if (vring->va) {
		wil_err(wil, "Tx ring [%d] already allocated\n", id);
		rc = -EINVAL;
		goto out;
	}

	vring->size = size;
	rc = wil_vring_alloc(wil, vring);
	if (rc)
		goto out;

	cmd.vring_cfg.tx_sw_ring.ring_mem_base = cpu_to_le64(vring->pa);

	rc = wmi_call(wil, WMI_VRING_CFG_CMDID, &cmd, sizeof(cmd),
		      WMI_VRING_CFG_DONE_EVENTID, &reply, sizeof(reply), 100);
	if (rc)
		goto out_free;

	if (reply.cmd.status != WMI_FW_STATUS_SUCCESS) {
		wil_err(wil, "Tx config failed, status 0x%02x\n",
			reply.cmd.status);
		rc = -EINVAL;
		goto out_free;
	}
	vring->hwtail = le32_to_cpu(reply.cmd.tx_vring_tail_ptr);

	return 0;
 out_free:
	wil_vring_free(wil, vring, 1);
 out:

	return rc;
}

void wil_vring_fini_tx(struct wil6210_priv *wil, int id)
{
	struct vring *vring = &wil->vring_tx[id];

	if (!vring->va)
		return;

	wil_vring_free(wil, vring, 1);
}

static struct vring *wil_find_tx_vring(struct wil6210_priv *wil,
				       struct sk_buff *skb)
{
	struct vring *v = &wil->vring_tx[0];

	if (v->va)
		return v;

	return NULL;
}

static int wil_tx_desc_map(struct vring_tx_desc *d, dma_addr_t pa, u32 len,
			   int vring_index)
{
	wil_desc_addr_set(&d->dma.addr, pa);
	d->dma.ip_length = 0;
	/* 0..6: mac_length; 7:ip_version 0-IP6 1-IP4*/
	d->dma.b11 = 0/*14 | BIT(7)*/;
	d->dma.error = 0;
	d->dma.status = 0; /* BIT(0) should be 0 for HW_OWNED */
	d->dma.length = cpu_to_le16((u16)len);
	d->dma.d0 = (vring_index << DMA_CFG_DESC_TX_0_QID_POS);
	d->mac.d[0] = 0;
	d->mac.d[1] = 0;
	d->mac.d[2] = 0;
	d->mac.ucode_cmd = 0;
	/* use dst index 0 */
	d->mac.d[1] |= BIT(MAC_CFG_DESC_TX_1_DST_INDEX_EN_POS) |
		       (0 << MAC_CFG_DESC_TX_1_DST_INDEX_POS);
	/* translation type:  0 - bypass; 1 - 802.3; 2 - native wifi */
	d->mac.d[2] = BIT(MAC_CFG_DESC_TX_2_SNAP_HDR_INSERTION_EN_POS) |
		      (1 << MAC_CFG_DESC_TX_2_L2_TRANSLATION_TYPE_POS);

	return 0;
}

static int wil_tx_vring(struct wil6210_priv *wil, struct vring *vring,
			struct sk_buff *skb)
{
	struct device *dev = wil_to_dev(wil);
	struct vring_tx_desc dd, *d = &dd;
	volatile struct vring_tx_desc *_d;
	u32 swhead = vring->swhead;
	int avail = wil_vring_avail_tx(vring);
	int nr_frags = skb_shinfo(skb)->nr_frags;
	uint f;
	int vring_index = vring - wil->vring_tx;
	uint i = swhead;
	dma_addr_t pa;

	wil_dbg_txrx(wil, "%s()\n", __func__);

	if (avail < vring->size/8)
		netif_tx_stop_all_queues(wil_to_ndev(wil));
	if (avail < 1 + nr_frags) {
		wil_err(wil, "Tx ring full. No space for %d fragments\n",
			1 + nr_frags);
		return -ENOMEM;
	}
	_d = &(vring->va[i].tx);

	/* FIXME FW can accept only unicast frames for the peer */
	memcpy(skb->data, wil->dst_addr[vring_index], ETH_ALEN);

	pa = dma_map_single(dev, skb->data,
			skb_headlen(skb), DMA_TO_DEVICE);

	wil_dbg_txrx(wil, "Tx skb %d bytes %p -> %#08llx\n", skb_headlen(skb),
		     skb->data, (unsigned long long)pa);
	wil_hex_dump_txrx("Tx ", DUMP_PREFIX_OFFSET, 16, 1,
			  skb->data, skb_headlen(skb), false);

	if (unlikely(dma_mapping_error(dev, pa)))
		return -EINVAL;
	/* 1-st segment */
	wil_tx_desc_map(d, pa, skb_headlen(skb), vring_index);
	d->mac.d[2] |= ((nr_frags + 1) <<
		       MAC_CFG_DESC_TX_2_NUM_OF_DESCRIPTORS_POS);
	if (nr_frags)
		*_d = *d;

	/* middle segments */
	for (f = 0; f < nr_frags; f++) {
		const struct skb_frag_struct *frag =
				&skb_shinfo(skb)->frags[f];
		int len = skb_frag_size(frag);
		i = (swhead + f + 1) % vring->size;
		_d = &(vring->va[i].tx);
		pa = skb_frag_dma_map(dev, frag, 0, skb_frag_size(frag),
				DMA_TO_DEVICE);
		if (unlikely(dma_mapping_error(dev, pa)))
			goto dma_error;
		wil_tx_desc_map(d, pa, len, vring_index);
		vring->ctx[i] = NULL;
		*_d = *d;
	}
	/* for the last seg only */
	d->dma.d0 |= BIT(DMA_CFG_DESC_TX_0_CMD_EOP_POS);
	d->dma.d0 |= BIT(DMA_CFG_DESC_TX_0_CMD_MARK_WB_POS);
	d->dma.d0 |= BIT(DMA_CFG_DESC_TX_0_CMD_DMA_IT_POS);
	*_d = *d;

	wil_hex_dump_txrx("Tx ", DUMP_PREFIX_NONE, 32, 4,
			  (const void *)d, sizeof(*d), false);

	/* advance swhead */
	wil_vring_advance_head(vring, nr_frags + 1);
	wil_dbg_txrx(wil, "Tx swhead %d -> %d\n", swhead, vring->swhead);
	trace_wil6210_tx(vring_index, swhead, skb->len, nr_frags);
	iowrite32(vring->swhead, wil->csr + HOSTADDR(vring->hwtail));
	/* hold reference to skb
	 * to prevent skb release before accounting
	 * in case of immediate "tx done"
	 */
	vring->ctx[i] = skb_get(skb);

	return 0;
 dma_error:
	/* unmap what we have mapped */
	/* Note: increment @f to operate with positive index */
	for (f++; f > 0; f--) {
		u16 dmalen;

		i = (swhead + f) % vring->size;
		_d = &(vring->va[i].tx);
		*d = *_d;
		_d->dma.status = TX_DMA_STATUS_DU;
		pa = wil_desc_addr(&d->dma.addr);
		dmalen = le16_to_cpu(d->dma.length);
		if (vring->ctx[i])
			dma_unmap_single(dev, pa, dmalen, DMA_TO_DEVICE);
		else
			dma_unmap_page(dev, pa, dmalen, DMA_TO_DEVICE);
	}

	return -EINVAL;
}


netdev_tx_t wil_start_xmit(struct sk_buff *skb, struct net_device *ndev)
{
	struct wil6210_priv *wil = ndev_to_wil(ndev);
	struct vring *vring;
	int rc;

	wil_dbg_txrx(wil, "%s()\n", __func__);
	if (!test_bit(wil_status_fwready, &wil->status)) {
		wil_err(wil, "FW not ready\n");
		goto drop;
	}
	if (!test_bit(wil_status_fwconnected, &wil->status)) {
		wil_err(wil, "FW not connected\n");
		goto drop;
	}
	if (wil->wdev->iftype == NL80211_IFTYPE_MONITOR) {
		wil_err(wil, "Xmit in monitor mode not supported\n");
		goto drop;
	}

	/* find vring */
	vring = wil_find_tx_vring(wil, skb);
	if (!vring) {
		wil_err(wil, "No Tx VRING available\n");
		goto drop;
	}
	/* set up vring entry */
	rc = wil_tx_vring(wil, vring, skb);

	switch (rc) {
	case 0:
		/* statistics will be updated on the tx_complete */
		dev_kfree_skb_any(skb);
		return NETDEV_TX_OK;
	case -ENOMEM:
		return NETDEV_TX_BUSY;
	default:
		break; /* goto drop; */
	}
 drop:
	ndev->stats.tx_dropped++;
	dev_kfree_skb_any(skb);

	return NET_XMIT_DROP;
}

/**
 * Clean up transmitted skb's from the Tx VRING
 *
 * Return number of descriptors cleared
 *
 * Safe to call from IRQ
 */
int wil_tx_complete(struct wil6210_priv *wil, int ringid)
{
	struct net_device *ndev = wil_to_ndev(wil);
	struct device *dev = wil_to_dev(wil);
	struct vring *vring = &wil->vring_tx[ringid];
	int done = 0;

	if (!vring->va) {
		wil_err(wil, "Tx irq[%d]: vring not initialized\n", ringid);
		return 0;
	}

	wil_dbg_txrx(wil, "%s(%d)\n", __func__, ringid);

	while (!wil_vring_is_empty(vring)) {
		volatile struct vring_tx_desc *_d =
					      &vring->va[vring->swtail].tx;
		struct vring_tx_desc dd, *d = &dd;
		dma_addr_t pa;
		struct sk_buff *skb;
		u16 dmalen;

		*d = *_d;

		if (!(d->dma.status & TX_DMA_STATUS_DU))
			break;

		dmalen = le16_to_cpu(d->dma.length);
		trace_wil6210_tx_done(ringid, vring->swtail, dmalen,
				      d->dma.error);
		wil_dbg_txrx(wil,
			     "Tx[%3d] : %d bytes, status 0x%02x err 0x%02x\n",
			     vring->swtail, dmalen, d->dma.status,
			     d->dma.error);
		wil_hex_dump_txrx("TxC ", DUMP_PREFIX_NONE, 32, 4,
				  (const void *)d, sizeof(*d), false);

		pa = wil_desc_addr(&d->dma.addr);
		skb = vring->ctx[vring->swtail];
		if (skb) {
			if (d->dma.error == 0) {
				ndev->stats.tx_packets++;
				ndev->stats.tx_bytes += skb->len;
			} else {
				ndev->stats.tx_errors++;
			}

			dma_unmap_single(dev, pa, dmalen, DMA_TO_DEVICE);
			dev_kfree_skb_any(skb);
			vring->ctx[vring->swtail] = NULL;
		} else {
			dma_unmap_page(dev, pa, dmalen, DMA_TO_DEVICE);
		}
		d->dma.addr.addr_low = 0;
		d->dma.addr.addr_high = 0;
		d->dma.length = 0;
		d->dma.status = TX_DMA_STATUS_DU;
		vring->swtail = wil_vring_next_tail(vring);
		done++;
	}
	if (wil_vring_avail_tx(vring) > vring->size/4)
		netif_tx_wake_all_queues(wil_to_ndev(wil));

	return done;
}
