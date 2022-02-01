/* -----------------------------------------------------------------------------
 * Copyright (c) 2011 Ozmo Inc
 * Released under the GNU General Public License Version 2 (GPLv2).
 * -----------------------------------------------------------------------------
 */
#include <linux/init.h>
#include <linux/module.h>
#include <linux/timer.h>
#include <linux/sched.h>
#include <linux/netdevice.h>
#include <linux/errno.h>
#include <linux/ieee80211.h>
#include "ozconfig.h"
#include "ozprotocol.h"
#include "ozeltbuf.h"
#include "ozpd.h"
#include "ozproto.h"
#include "ozusbsvc.h"
#include "oztrace.h"
#include "ozappif.h"
#include <asm/unaligned.h>
#include <linux/uaccess.h>
#include <net/psnap.h>
/*------------------------------------------------------------------------------
 */
#define OZ_CF_CONN_SUCCESS	1
#define OZ_CF_CONN_FAILURE	2

#define OZ_DO_STOP		1
#define OZ_DO_SLEEP		2

/* States of the timer.
 */
#define OZ_TIMER_IDLE		0
#define OZ_TIMER_SET		1
#define OZ_TIMER_IN_HANDLER	2

#define OZ_MAX_TIMER_POOL_SIZE	16

/*------------------------------------------------------------------------------
 */
struct oz_binding {
	struct packet_type ptype;
	char name[OZ_MAX_BINDING_LEN];
	struct oz_binding *next;
};

struct oz_timer {
	struct list_head link;
	struct oz_pd *pd;
	unsigned long due_time;
	int type;
};
/*------------------------------------------------------------------------------
 * Static external variables.
 */
static DEFINE_SPINLOCK(g_polling_lock);
static LIST_HEAD(g_pd_list);
static struct oz_binding *g_binding ;
static DEFINE_SPINLOCK(g_binding_lock);
static struct sk_buff_head g_rx_queue;
static u8 g_session_id;
static u16 g_apps = 0x1;
static int g_processing_rx;
static struct timer_list g_timer;
static struct oz_timer *g_cur_timer;
static struct list_head *g_timer_pool;
static int g_timer_pool_count;
static int g_timer_state = OZ_TIMER_IDLE;
static LIST_HEAD(g_timer_list);
/*------------------------------------------------------------------------------
 */
static void oz_protocol_timer_start(void);
/*------------------------------------------------------------------------------
 * Context: softirq-serialized
 */
static u8 oz_get_new_session_id(u8 exclude)
{
	if (++g_session_id == 0)
		g_session_id = 1;
	if (g_session_id == exclude) {
		if (++g_session_id == 0)
			g_session_id = 1;
	}
	return g_session_id;
}
/*------------------------------------------------------------------------------
 * Context: softirq-serialized
 */
static void oz_send_conn_rsp(struct oz_pd *pd, u8 status)
{
	struct sk_buff *skb;
	struct net_device *dev = pd->net_dev;
	struct oz_hdr *oz_hdr;
	struct oz_elt *elt;
	struct oz_elt_connect_rsp *body;
	int sz = sizeof(struct oz_hdr) + sizeof(struct oz_elt) +
			sizeof(struct oz_elt_connect_rsp);
	skb = alloc_skb(sz + OZ_ALLOCATED_SPACE(dev), GFP_ATOMIC);
	if (skb == NULL)
		return;
	skb_reserve(skb, LL_RESERVED_SPACE(dev));
	skb_reset_network_header(skb);
	oz_hdr = (struct oz_hdr *)skb_put(skb, sz);
	elt = (struct oz_elt *)(oz_hdr+1);
	body = (struct oz_elt_connect_rsp *)(elt+1);
	skb->dev = dev;
	skb->protocol = htons(OZ_ETHERTYPE);
	/* Fill in device header */
	if (dev_hard_header(skb, dev, OZ_ETHERTYPE, pd->mac_addr,
			dev->dev_addr, skb->len) < 0) {
		kfree_skb(skb);
		return;
	}
	oz_hdr->control = (OZ_PROTOCOL_VERSION<<OZ_VERSION_SHIFT);
	oz_hdr->last_pkt_num = 0;
	put_unaligned(0, &oz_hdr->pkt_num);
	elt->type = OZ_ELT_CONNECT_RSP;
	elt->length = sizeof(struct oz_elt_connect_rsp);
	memset(body, 0, sizeof(struct oz_elt_connect_rsp));
	body->status = status;
	if (status == 0) {
		body->mode = pd->mode;
		body->session_id = pd->session_id;
		put_unaligned(cpu_to_le16(pd->total_apps), &body->apps);
	}
	oz_trace("TX: OZ_ELT_CONNECT_RSP %d", status);
	dev_queue_xmit(skb);
	return;
}
/*------------------------------------------------------------------------------
 * Context: softirq-serialized
 */
static void pd_set_keepalive(struct oz_pd *pd, u8 kalive)
{
	unsigned long keep_alive = kalive & OZ_KALIVE_VALUE_MASK;

	switch (kalive & OZ_KALIVE_TYPE_MASK) {
	case OZ_KALIVE_SPECIAL:
		pd->keep_alive_j =
			oz_ms_to_jiffies(keep_alive * 1000*60*60*24*20);
		break;
	case OZ_KALIVE_SECS:
		pd->keep_alive_j = oz_ms_to_jiffies(keep_alive*1000);
		break;
	case OZ_KALIVE_MINS:
		pd->keep_alive_j = oz_ms_to_jiffies(keep_alive*1000*60);
		break;
	case OZ_KALIVE_HOURS:
		pd->keep_alive_j = oz_ms_to_jiffies(keep_alive*1000*60*60);
		break;
	default:
		pd->keep_alive_j = 0;
	}
	oz_trace("Keepalive = %lu jiffies\n", pd->keep_alive_j);
}
/*------------------------------------------------------------------------------
 * Context: softirq-serialized
 */
static void pd_set_presleep(struct oz_pd *pd, u8 presleep)
{
	if (presleep)
		pd->presleep_j = oz_ms_to_jiffies(presleep*100);
	else
		pd->presleep_j = OZ_PRESLEEP_TOUT_J;
	oz_trace("Presleep time = %lu jiffies\n", pd->presleep_j);
}
/*------------------------------------------------------------------------------
 * Context: softirq-serialized
 */
static struct oz_pd *oz_connect_req(struct oz_pd *cur_pd, struct oz_elt *elt,
			const u8 *pd_addr, struct net_device *net_dev)
{
	struct oz_pd *pd;
	struct oz_elt_connect_req *body =
			(struct oz_elt_connect_req *)(elt+1);
	u8 rsp_status = OZ_STATUS_SUCCESS;
	u8 stop_needed = 0;
	u16 new_apps = g_apps;
	struct net_device *old_net_dev = NULL;
	struct oz_pd *free_pd = NULL;
	if (cur_pd) {
		pd = cur_pd;
		spin_lock_bh(&g_polling_lock);
	} else {
		struct oz_pd *pd2 = NULL;
		struct list_head *e;
		pd = oz_pd_alloc(pd_addr);
		if (pd == NULL)
			return NULL;
		pd->last_rx_time_j = jiffies;
		spin_lock_bh(&g_polling_lock);
		list_for_each(e, &g_pd_list) {
			pd2 = container_of(e, struct oz_pd, link);
			if (memcmp(pd2->mac_addr, pd_addr, ETH_ALEN) == 0) {
				free_pd = pd;
				pd = pd2;
				break;
			}
		}
		if (pd != pd2)
			list_add_tail(&pd->link, &g_pd_list);
	}
	if (pd == NULL) {
		spin_unlock_bh(&g_polling_lock);
		return NULL;
	}
	if (pd->net_dev != net_dev) {
		old_net_dev = pd->net_dev;
		dev_hold(net_dev);
		pd->net_dev = net_dev;
	}
	oz_trace("Host vendor: %d\n", body->host_vendor);
	pd->max_tx_size = OZ_MAX_TX_SIZE;
	pd->mode = body->mode;
	pd->pd_info = body->pd_info;
	if (pd->mode & OZ_F_ISOC_NO_ELTS) {
		pd->ms_per_isoc = body->ms_per_isoc;
		if (!pd->ms_per_isoc)
			pd->ms_per_isoc = 4;

		switch (body->ms_isoc_latency & OZ_LATENCY_MASK) {
		case OZ_ONE_MS_LATENCY:
			pd->isoc_latency = (body->ms_isoc_latency &
					~OZ_LATENCY_MASK) / pd->ms_per_isoc;
			break;
		case OZ_TEN_MS_LATENCY:
			pd->isoc_latency = ((body->ms_isoc_latency &
				~OZ_LATENCY_MASK) * 10) / pd->ms_per_isoc;
			break;
		default:
			pd->isoc_latency = OZ_MAX_TX_QUEUE_ISOC;
		}
	}
	if (body->max_len_div16)
		pd->max_tx_size = ((u16)body->max_len_div16)<<4;
	oz_trace("Max frame:%u Ms per isoc:%u\n",
		pd->max_tx_size, pd->ms_per_isoc);
	pd->max_stream_buffering = 3*1024;
	pd->timeout_time_j = jiffies + OZ_CONNECTION_TOUT_J;
	pd->pulse_period_j = OZ_QUANTUM_J;
	pd_set_presleep(pd, body->presleep);
	pd_set_keepalive(pd, body->keep_alive);

	new_apps &= le16_to_cpu(get_unaligned(&body->apps));
	if ((new_apps & 0x1) && (body->session_id)) {
		if (pd->session_id) {
			if (pd->session_id != body->session_id) {
				rsp_status = OZ_STATUS_SESSION_MISMATCH;
				goto done;
			}
		} else {
			new_apps &= ~0x1;  /* Resume not permitted */
			pd->session_id =
				oz_get_new_session_id(body->session_id);
		}
	} else {
		if (pd->session_id && !body->session_id) {
			rsp_status = OZ_STATUS_SESSION_TEARDOWN;
			stop_needed = 1;
		} else {
			new_apps &= ~0x1;  /* Resume not permitted */
			pd->session_id =
				oz_get_new_session_id(body->session_id);
		}
	}
done:
	if (rsp_status == OZ_STATUS_SUCCESS) {
		u16 start_apps = new_apps & ~pd->total_apps & ~0x1;
		u16 stop_apps = pd->total_apps & ~new_apps & ~0x1;
		u16 resume_apps = new_apps & pd->paused_apps  & ~0x1;
		spin_unlock_bh(&g_polling_lock);
		oz_pd_set_state(pd, OZ_PD_S_CONNECTED);
		oz_timer_delete(pd, OZ_TIMER_STOP);
		oz_trace("new_apps=0x%x total_apps=0x%x paused_apps=0x%x\n",
			new_apps, pd->total_apps, pd->paused_apps);
		if (start_apps) {
			if (oz_services_start(pd, start_apps, 0))
				rsp_status = OZ_STATUS_TOO_MANY_PDS;
		}
		if (resume_apps)
			if (oz_services_start(pd, resume_apps, 1))
				rsp_status = OZ_STATUS_TOO_MANY_PDS;
		if (stop_apps)
			oz_services_stop(pd, stop_apps, 0);
		oz_pd_request_heartbeat(pd);
	} else {
		spin_unlock_bh(&g_polling_lock);
	}
	oz_send_conn_rsp(pd, rsp_status);
	if (rsp_status != OZ_STATUS_SUCCESS) {
		if (stop_needed)
			oz_pd_stop(pd);
		oz_pd_put(pd);
		pd = NULL;
	}
	if (old_net_dev)
		dev_put(old_net_dev);
	if (free_pd)
		oz_pd_destroy(free_pd);
	return pd;
}
/*------------------------------------------------------------------------------
 * Context: softirq-serialized
 */
static void oz_add_farewell(struct oz_pd *pd, u8 ep_num, u8 index,
			const u8 *report, u8 len)
{
	struct oz_farewell *f;
	struct oz_farewell *f2;
	int found = 0;
	f = kmalloc(sizeof(struct oz_farewell) + len - 1, GFP_ATOMIC);
	if (!f)
		return;
	f->ep_num = ep_num;
	f->index = index;
	memcpy(f->report, report, len);
	oz_trace("RX: Adding farewell report\n");
	spin_lock(&g_polling_lock);
	list_for_each_entry(f2, &pd->farewell_list, link) {
		if ((f2->ep_num == ep_num) && (f2->index == index)) {
			found = 1;
			list_del(&f2->link);
			break;
		}
	}
	list_add_tail(&f->link, &pd->farewell_list);
	spin_unlock(&g_polling_lock);
	if (found)
		kfree(f2);
}
/*------------------------------------------------------------------------------
 * Context: softirq-serialized
 */
static void oz_rx_frame(struct sk_buff *skb)
{
	u8 *mac_hdr;
	u8 *src_addr;
	struct oz_elt *elt;
	int length;
	struct oz_pd *pd = NULL;
	struct oz_hdr *oz_hdr = (struct oz_hdr *)skb_network_header(skb);
	int dup = 0;
	u32 pkt_num;

	oz_trace2(OZ_TRACE_RX_FRAMES,
		"RX frame PN=0x%x LPN=0x%x control=0x%x\n",
		oz_hdr->pkt_num, oz_hdr->last_pkt_num, oz_hdr->control);
	mac_hdr = skb_mac_header(skb);
	src_addr = &mac_hdr[ETH_ALEN] ;
	length = skb->len;

	/* Check the version field */
	if (oz_get_prot_ver(oz_hdr->control) != OZ_PROTOCOL_VERSION) {
		oz_trace("Incorrect protocol version: %d\n",
			oz_get_prot_ver(oz_hdr->control));
		goto done;
	}

	pkt_num = le32_to_cpu(get_unaligned(&oz_hdr->pkt_num));

	pd = oz_pd_find(src_addr);
	if (pd) {
		pd->last_rx_time_j = jiffies;
		oz_timer_add(pd, OZ_TIMER_TOUT,
			pd->last_rx_time_j + pd->presleep_j, 1);
		if (pkt_num != pd->last_rx_pkt_num) {
			pd->last_rx_pkt_num = pkt_num;
		} else {
			dup = 1;
			oz_trace("Duplicate frame\n");
		}
	}

	if (pd && !dup && ((pd->mode & OZ_MODE_MASK) == OZ_MODE_TRIGGERED)) {
		oz_trace2(OZ_TRACE_RX_FRAMES, "Received TRIGGER Frame\n");
		pd->last_sent_frame = &pd->tx_queue;
		if (oz_hdr->control & OZ_F_ACK) {
			/* Retire completed frames */
			oz_retire_tx_frames(pd, oz_hdr->last_pkt_num);
		}
		if ((oz_hdr->control & OZ_F_ACK_REQUESTED) &&
				(pd->state == OZ_PD_S_CONNECTED)) {
			int backlog = pd->nb_queued_frames;
			pd->trigger_pkt_num = pkt_num;
			/* Send queued frames */
			oz_send_queued_frames(pd, backlog);
		}
	}

	length -= sizeof(struct oz_hdr);
	elt = (struct oz_elt *)((u8 *)oz_hdr + sizeof(struct oz_hdr));

	while (length >= sizeof(struct oz_elt)) {
		length -= sizeof(struct oz_elt) + elt->length;
		if (length < 0)
			break;
		switch (elt->type) {
		case OZ_ELT_CONNECT_REQ:
			oz_trace("RX: OZ_ELT_CONNECT_REQ\n");
			pd = oz_connect_req(pd, elt, src_addr, skb->dev);
			break;
		case OZ_ELT_DISCONNECT:
			oz_trace("RX: OZ_ELT_DISCONNECT\n");
			if (pd)
				oz_pd_sleep(pd);
			break;
		case OZ_ELT_UPDATE_PARAM_REQ: {
				struct oz_elt_update_param *body =
					(struct oz_elt_update_param *)(elt + 1);
				oz_trace("RX: OZ_ELT_UPDATE_PARAM_REQ\n");
				if (pd && (pd->state & OZ_PD_S_CONNECTED)) {
					spin_lock(&g_polling_lock);
					pd_set_keepalive(pd, body->keepalive);
					pd_set_presleep(pd, body->presleep);
					spin_unlock(&g_polling_lock);
				}
			}
			break;
		case OZ_ELT_FAREWELL_REQ: {
				struct oz_elt_farewell *body =
					(struct oz_elt_farewell *)(elt + 1);
				oz_trace("RX: OZ_ELT_FAREWELL_REQ\n");
				oz_add_farewell(pd, body->ep_num,
					body->index, body->report,
					elt->length + 1 - sizeof(*body));
			}
			break;
		case OZ_ELT_APP_DATA:
			if (pd && (pd->state & OZ_PD_S_CONNECTED)) {
				struct oz_app_hdr *app_hdr =
					(struct oz_app_hdr *)(elt+1);
				if (dup)
					break;
				oz_handle_app_elt(pd, app_hdr->app_id, elt);
			}
			break;
		default:
			oz_trace("RX: Unknown elt %02x\n", elt->type);
		}
		elt = oz_next_elt(elt);
	}
done:
	if (pd)
		oz_pd_put(pd);
	consume_skb(skb);
}
/*------------------------------------------------------------------------------
 * Context: process
 */
void oz_protocol_term(void)
{
	struct list_head *chain;
	del_timer_sync(&g_timer);
	/* Walk the list of bindings and remove each one.
	 */
	spin_lock_bh(&g_binding_lock);
	while (g_binding) {
		struct oz_binding *b = g_binding;
		g_binding = b->next;
		spin_unlock_bh(&g_binding_lock);
		dev_remove_pack(&b->ptype);
		if (b->ptype.dev)
			dev_put(b->ptype.dev);
		kfree(b);
		spin_lock_bh(&g_binding_lock);
	}
	spin_unlock_bh(&g_binding_lock);
	/* Walk the list of PDs and stop each one. This causes the PD to be
	 * removed from the list so we can just pull each one from the head
	 * of the list.
	 */
	spin_lock_bh(&g_polling_lock);
	while (!list_empty(&g_pd_list)) {
		struct oz_pd *pd =
			list_first_entry(&g_pd_list, struct oz_pd, link);
		oz_pd_get(pd);
		spin_unlock_bh(&g_polling_lock);
		oz_pd_stop(pd);
		oz_pd_put(pd);
		spin_lock_bh(&g_polling_lock);
	}
	chain = g_timer_pool;
	g_timer_pool = NULL;
	spin_unlock_bh(&g_polling_lock);
	while (chain) {
		struct oz_timer *t = container_of(chain, struct oz_timer, link);
		chain = chain->next;
		kfree(t);
	}
	oz_trace("Protocol stopped\n");
}
/*------------------------------------------------------------------------------
 * Context: softirq
 */
static void oz_pd_handle_timer(struct oz_pd *pd, int type)
{
	switch (type) {
	case OZ_TIMER_TOUT:
		oz_pd_sleep(pd);
		break;
	case OZ_TIMER_STOP:
		oz_pd_stop(pd);
		break;
	case OZ_TIMER_HEARTBEAT: {
			u16 apps = 0;
			spin_lock_bh(&g_polling_lock);
			pd->heartbeat_requested = 0;
			if (pd->state & OZ_PD_S_CONNECTED)
				apps = pd->total_apps;
			spin_unlock_bh(&g_polling_lock);
			if (apps)
				oz_pd_heartbeat(pd, apps);
		}
		break;
	}
}
/*------------------------------------------------------------------------------
 * Context: softirq
 */
static void oz_protocol_timer(unsigned long arg)
{
	struct oz_timer *t;
	struct oz_timer *t2;
	struct oz_pd *pd;
	spin_lock_bh(&g_polling_lock);
	if (!g_cur_timer) {
		/* This happens if we remove the current timer but can't stop
		 * the timer from firing. In this case just get out.
		 */
		spin_unlock_bh(&g_polling_lock);
		return;
	}
	g_timer_state = OZ_TIMER_IN_HANDLER;
	t = g_cur_timer;
	g_cur_timer = NULL;
	list_del(&t->link);
	spin_unlock_bh(&g_polling_lock);
	do {
		pd = t->pd;
		oz_pd_handle_timer(pd, t->type);
		spin_lock_bh(&g_polling_lock);
		if (g_timer_pool_count < OZ_MAX_TIMER_POOL_SIZE) {
			t->link.next = g_timer_pool;
			g_timer_pool = &t->link;
			g_timer_pool_count++;
			t = NULL;
		}
		if (!list_empty(&g_timer_list)) {
			t2 =  container_of(g_timer_list.next,
				struct oz_timer, link);
			if (time_before_eq(t2->due_time, jiffies))
				list_del(&t2->link);
			else
				t2 = NULL;
		} else {
			t2 = NULL;
		}
		spin_unlock_bh(&g_polling_lock);
		oz_pd_put(pd);
		kfree(t);
		t = t2;
	} while (t);
	g_timer_state = OZ_TIMER_IDLE;
	oz_protocol_timer_start();
}
/*------------------------------------------------------------------------------
 * Context: softirq
 */
static void oz_protocol_timer_start(void)
{
	spin_lock_bh(&g_polling_lock);
	if (!list_empty(&g_timer_list)) {
		g_cur_timer =
			container_of(g_timer_list.next, struct oz_timer, link);
		if (g_timer_state == OZ_TIMER_SET) {
			mod_timer(&g_timer, g_cur_timer->due_time);
		} else {
			g_timer.expires = g_cur_timer->due_time;
			g_timer.function = oz_protocol_timer;
			g_timer.data = 0;
			add_timer(&g_timer);
		}
		g_timer_state = OZ_TIMER_SET;
	} else {
		oz_trace("No queued timers\n");
	}
	spin_unlock_bh(&g_polling_lock);
}
/*------------------------------------------------------------------------------
 * Context: softirq or process
 */
void oz_timer_add(struct oz_pd *pd, int type, unsigned long due_time,
		int remove)
{
	struct list_head *e;
	struct oz_timer *t = NULL;
	int restart_needed = 0;
	spin_lock(&g_polling_lock);
	if (remove) {
		list_for_each(e, &g_timer_list) {
			t = container_of(e, struct oz_timer, link);
			if ((t->pd == pd) && (t->type == type)) {
				if (g_cur_timer == t) {
					restart_needed = 1;
					g_cur_timer = NULL;
				}
				list_del(e);
				break;
			}
			t = NULL;
		}
	}
	if (!t) {
		if (g_timer_pool) {
			t = container_of(g_timer_pool, struct oz_timer, link);
			g_timer_pool = g_timer_pool->next;
			g_timer_pool_count--;
		} else {
			t = kmalloc(sizeof(struct oz_timer), GFP_ATOMIC);
		}
		if (t) {
			t->pd = pd;
			t->type = type;
			oz_pd_get(pd);
		}
	}
	if (t) {
		struct oz_timer *t2;
		t->due_time = due_time;
		list_for_each(e, &g_timer_list) {
			t2 = container_of(e, struct oz_timer, link);
			if (time_before(due_time, t2->due_time)) {
				if (t2 == g_cur_timer) {
					g_cur_timer = NULL;
					restart_needed = 1;
				}
				break;
			}
		}
		list_add_tail(&t->link, e);
	}
	if (g_timer_state == OZ_TIMER_IDLE)
		restart_needed = 1;
	else if (g_timer_state == OZ_TIMER_IN_HANDLER)
		restart_needed = 0;
	spin_unlock(&g_polling_lock);
	if (restart_needed)
		oz_protocol_timer_start();
}
/*------------------------------------------------------------------------------
 * Context: softirq or process
 */
void oz_timer_delete(struct oz_pd *pd, int type)
{
	struct list_head *chain = NULL;
	struct oz_timer *t;
	struct oz_timer *n;
	int restart_needed = 0;
	int release = 0;
	spin_lock(&g_polling_lock);
	list_for_each_entry_safe(t, n, &g_timer_list, link) {
		if ((t->pd == pd) && ((type == 0) || (t->type == type))) {
			if (g_cur_timer == t) {
				restart_needed = 1;
				g_cur_timer = NULL;
				del_timer(&g_timer);
			}
			list_del(&t->link);
			release++;
			if (g_timer_pool_count < OZ_MAX_TIMER_POOL_SIZE) {
				t->link.next = g_timer_pool;
				g_timer_pool = &t->link;
				g_timer_pool_count++;
			} else {
				t->link.next = chain;
				chain = &t->link;
			}
			if (type)
				break;
		}
	}
	if (g_timer_state == OZ_TIMER_IN_HANDLER)
		restart_needed = 0;
	else if (restart_needed)
		g_timer_state = OZ_TIMER_IDLE;
	spin_unlock(&g_polling_lock);
	if (restart_needed)
		oz_protocol_timer_start();
	while (release--)
		oz_pd_put(pd);
	while (chain) {
		t = container_of(chain, struct oz_timer, link);
		chain = chain->next;
		kfree(t);
	}
}
/*------------------------------------------------------------------------------
 * Context: softirq or process
 */
void oz_pd_request_heartbeat(struct oz_pd *pd)
{
	unsigned long now = jiffies;
	unsigned long t;
	spin_lock(&g_polling_lock);
	if (pd->heartbeat_requested) {
		spin_unlock(&g_polling_lock);
		return;
	}
	if (pd->pulse_period_j)
		t = ((now / pd->pulse_period_j) + 1) * pd->pulse_period_j;
	else
		t = now + 1;
	pd->heartbeat_requested = 1;
	spin_unlock(&g_polling_lock);
	oz_timer_add(pd, OZ_TIMER_HEARTBEAT, t, 0);
}
/*------------------------------------------------------------------------------
 * Context: softirq or process
 */
struct oz_pd *oz_pd_find(const u8 *mac_addr)
{
	struct oz_pd *pd;
	struct list_head *e;
	spin_lock_bh(&g_polling_lock);
	list_for_each(e, &g_pd_list) {
		pd = container_of(e, struct oz_pd, link);
		if (memcmp(pd->mac_addr, mac_addr, ETH_ALEN) == 0) {
			atomic_inc(&pd->ref_count);
			spin_unlock_bh(&g_polling_lock);
			return pd;
		}
	}
	spin_unlock_bh(&g_polling_lock);
	return NULL;
}
/*------------------------------------------------------------------------------
 * Context: process
 */
void oz_app_enable(int app_id, int enable)
{
	if (app_id <= OZ_APPID_MAX) {
		spin_lock_bh(&g_polling_lock);
		if (enable)
			g_apps |= (1<<app_id);
		else
			g_apps &= ~(1<<app_id);
		spin_unlock_bh(&g_polling_lock);
	}
}
/*------------------------------------------------------------------------------
 * Context: softirq
 */
static int oz_pkt_recv(struct sk_buff *skb, struct net_device *dev,
		struct packet_type *pt, struct net_device *orig_dev)
{
	skb = skb_share_check(skb, GFP_ATOMIC);
	if (skb == NULL)
		return 0;
	spin_lock_bh(&g_rx_queue.lock);
	if (g_processing_rx) {
		/* We already hold the lock so use __ variant.
		 */
		__skb_queue_head(&g_rx_queue, skb);
		spin_unlock_bh(&g_rx_queue.lock);
	} else {
		g_processing_rx = 1;
		do {

			spin_unlock_bh(&g_rx_queue.lock);
			oz_rx_frame(skb);
			spin_lock_bh(&g_rx_queue.lock);
			if (skb_queue_empty(&g_rx_queue)) {
				g_processing_rx = 0;
				spin_unlock_bh(&g_rx_queue.lock);
				break;
			}
			/* We already hold the lock so use __ variant.
			 */
			skb = __skb_dequeue(&g_rx_queue);
		} while (1);
	}
	return 0;
}
/*------------------------------------------------------------------------------
 * Context: process
 */
void oz_binding_add(char *net_dev)
{
	struct oz_binding *binding;

	binding = kmalloc(sizeof(struct oz_binding), GFP_KERNEL);
	if (binding) {
		binding->ptype.type = __constant_htons(OZ_ETHERTYPE);
		binding->ptype.func = oz_pkt_recv;
		memcpy(binding->name, net_dev, OZ_MAX_BINDING_LEN);
		if (net_dev && *net_dev) {
			oz_trace("Adding binding: %s\n", net_dev);
			binding->ptype.dev =
				dev_get_by_name(&init_net, net_dev);
			if (binding->ptype.dev == NULL) {
				oz_trace("Netdev %s not found\n", net_dev);
				kfree(binding);
				binding = NULL;
			}
		} else {
			oz_trace("Binding to all netcards\n");
			binding->ptype.dev = NULL;
		}
		if (binding) {
			dev_add_pack(&binding->ptype);
			spin_lock_bh(&g_binding_lock);
			binding->next = g_binding;
			g_binding = binding;
			spin_unlock_bh(&g_binding_lock);
		}
	}
}
/*------------------------------------------------------------------------------
 * Context: process
 */
static int compare_binding_name(char *s1, char *s2)
{
	int i;
	for (i = 0; i < OZ_MAX_BINDING_LEN; i++) {
		if (*s1 != *s2)
			return 0;
		if (!*s1++)
			return 1;
		s2++;
	}
	return 1;
}
/*------------------------------------------------------------------------------
 * Context: process
 */
static void pd_stop_all_for_device(struct net_device *net_dev)
{
	struct list_head h;
	struct oz_pd *pd;
	struct oz_pd *n;
	INIT_LIST_HEAD(&h);
	spin_lock_bh(&g_polling_lock);
	list_for_each_entry_safe(pd, n, &g_pd_list, link) {
		if (pd->net_dev == net_dev) {
			list_move(&pd->link, &h);
			oz_pd_get(pd);
		}
	}
	spin_unlock_bh(&g_polling_lock);
	while (!list_empty(&h)) {
		pd = list_first_entry(&h, struct oz_pd, link);
		oz_pd_stop(pd);
		oz_pd_put(pd);
	}
}
/*------------------------------------------------------------------------------
 * Context: process
 */
void oz_binding_remove(char *net_dev)
{
	struct oz_binding *binding;
	struct oz_binding **link;
	oz_trace("Removing binding: %s\n", net_dev);
	spin_lock_bh(&g_binding_lock);
	binding = g_binding;
	link = &g_binding;
	while (binding) {
		if (compare_binding_name(binding->name, net_dev)) {
			oz_trace("Binding '%s' found\n", net_dev);
			*link = binding->next;
			break;
		} else {
			link = &binding;
			binding = binding->next;
		}
	}
	spin_unlock_bh(&g_binding_lock);
	if (binding) {
		dev_remove_pack(&binding->ptype);
		if (binding->ptype.dev) {
			dev_put(binding->ptype.dev);
			pd_stop_all_for_device(binding->ptype.dev);
		}
		kfree(binding);
	}
}
/*------------------------------------------------------------------------------
 * Context: process
 */
static char *oz_get_next_device_name(char *s, char *dname, int max_size)
{
	while (*s == ',')
		s++;
	while (*s && (*s != ',') && max_size > 1) {
		*dname++ = *s++;
		max_size--;
	}
	*dname = 0;
	return s;
}
/*------------------------------------------------------------------------------
 * Context: process
 */
int oz_protocol_init(char *devs)
{
	skb_queue_head_init(&g_rx_queue);
	if (devs && (devs[0] == '*')) {
		oz_binding_add(NULL);
	} else {
		char d[32];
		while (*devs) {
			devs = oz_get_next_device_name(devs, d, sizeof(d));
			if (d[0])
				oz_binding_add(d);
		}
	}
	init_timer(&g_timer);
	return 0;
}
/*------------------------------------------------------------------------------
 * Context: process
 */
int oz_get_pd_list(struct oz_mac_addr *addr, int max_count)
{
	struct oz_pd *pd;
	struct list_head *e;
	int count = 0;
	spin_lock_bh(&g_polling_lock);
	list_for_each(e, &g_pd_list) {
		if (count >= max_count)
			break;
		pd = container_of(e, struct oz_pd, link);
		memcpy(&addr[count++], pd->mac_addr, ETH_ALEN);
	}
	spin_unlock_bh(&g_polling_lock);
	return count;
}
/*------------------------------------------------------------------------------
*/
void oz_polling_lock_bh(void)
{
	spin_lock_bh(&g_polling_lock);
}
/*------------------------------------------------------------------------------
*/
void oz_polling_unlock_bh(void)
{
	spin_unlock_bh(&g_polling_lock);
}
