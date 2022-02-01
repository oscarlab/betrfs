/*
 * IBSS mode implementation
 * Copyright 2003-2008, Jouni Malinen <j@w1.fi>
 * Copyright 2004, Instant802 Networks, Inc.
 * Copyright 2005, Devicescape Software, Inc.
 * Copyright 2006-2007	Jiri Benc <jbenc@suse.cz>
 * Copyright 2007, Michael Wu <flamingice@sourmilk.net>
 * Copyright 2009, Johannes Berg <johannes@sipsolutions.net>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

#include <linux/delay.h>
#include <linux/slab.h>
#include <linux/if_ether.h>
#include <linux/skbuff.h>
#include <linux/if_arp.h>
#include <linux/etherdevice.h>
#include <linux/rtnetlink.h>
#include <net/mac80211.h>

#include "ieee80211_i.h"
#include "driver-ops.h"
#include "rate.h"

#define IEEE80211_SCAN_INTERVAL (2 * HZ)
#define IEEE80211_IBSS_JOIN_TIMEOUT (7 * HZ)

#define IEEE80211_IBSS_MERGE_INTERVAL (30 * HZ)
#define IEEE80211_IBSS_INACTIVITY_LIMIT (60 * HZ)

#define IEEE80211_IBSS_MAX_STA_ENTRIES 128


static void __ieee80211_sta_join_ibss(struct ieee80211_sub_if_data *sdata,
				      const u8 *bssid, const int beacon_int,
				      struct cfg80211_chan_def *req_chandef,
				      const u32 basic_rates,
				      const u16 capability, u64 tsf,
				      bool creator)
{
	struct ieee80211_if_ibss *ifibss = &sdata->u.ibss;
	struct ieee80211_local *local = sdata->local;
	int rates, i;
	struct ieee80211_mgmt *mgmt;
	u8 *pos;
	struct ieee80211_supported_band *sband;
	struct cfg80211_bss *bss;
	u32 bss_change;
	u8 supp_rates[IEEE80211_MAX_SUPP_RATES];
	struct cfg80211_chan_def chandef;
	struct ieee80211_channel *chan;
	struct beacon_data *presp;
	int frame_len;

	sdata_assert_lock(sdata);

	/* Reset own TSF to allow time synchronization work. */
	drv_reset_tsf(local, sdata);

	if (!ether_addr_equal(ifibss->bssid, bssid))
		sta_info_flush(sdata);

	/* if merging, indicate to driver that we leave the old IBSS */
	if (sdata->vif.bss_conf.ibss_joined) {
		sdata->vif.bss_conf.ibss_joined = false;
		sdata->vif.bss_conf.ibss_creator = false;
		sdata->vif.bss_conf.enable_beacon = false;
		netif_carrier_off(sdata->dev);
		ieee80211_bss_info_change_notify(sdata,
						 BSS_CHANGED_IBSS |
						 BSS_CHANGED_BEACON_ENABLED);
	}

	presp = rcu_dereference_protected(ifibss->presp,
					  lockdep_is_held(&sdata->wdev.mtx));
	rcu_assign_pointer(ifibss->presp, NULL);
	if (presp)
		kfree_rcu(presp, rcu_head);

	sdata->drop_unencrypted = capability & WLAN_CAPABILITY_PRIVACY ? 1 : 0;

	/* make a copy of the chandef, it could be modified below. */
	chandef = *req_chandef;
	chan = chandef.chan;
	if (!cfg80211_reg_can_beacon(local->hw.wiphy, &chandef)) {
		chandef.width = NL80211_CHAN_WIDTH_20;
		chandef.center_freq1 = chan->center_freq;
	}

	ieee80211_vif_release_channel(sdata);
	if (ieee80211_vif_use_channel(sdata, &chandef,
				      ifibss->fixed_channel ?
					IEEE80211_CHANCTX_SHARED :
					IEEE80211_CHANCTX_EXCLUSIVE)) {
		sdata_info(sdata, "Failed to join IBSS, no channel context\n");
		return;
	}

	memcpy(ifibss->bssid, bssid, ETH_ALEN);

	sband = local->hw.wiphy->bands[chan->band];

	/* Build IBSS probe response */
	frame_len = sizeof(struct ieee80211_hdr_3addr) +
		    12 /* struct ieee80211_mgmt.u.beacon */ +
		    2 + IEEE80211_MAX_SSID_LEN /* max SSID */ +
		    2 + 8 /* max Supported Rates */ +
		    3 /* max DS params */ +
		    4 /* IBSS params */ +
		    2 + (IEEE80211_MAX_SUPP_RATES - 8) +
		    2 + sizeof(struct ieee80211_ht_cap) +
		    2 + sizeof(struct ieee80211_ht_operation) +
		    ifibss->ie_len;
	presp = kzalloc(sizeof(*presp) + frame_len, GFP_KERNEL);
	if (!presp)
		return;

	presp->head = (void *)(presp + 1);

	mgmt = (void *) presp->head;
	mgmt->frame_control = cpu_to_le16(IEEE80211_FTYPE_MGMT |
					  IEEE80211_STYPE_PROBE_RESP);
	eth_broadcast_addr(mgmt->da);
	memcpy(mgmt->sa, sdata->vif.addr, ETH_ALEN);
	memcpy(mgmt->bssid, ifibss->bssid, ETH_ALEN);
	mgmt->u.beacon.beacon_int = cpu_to_le16(beacon_int);
	mgmt->u.beacon.timestamp = cpu_to_le64(tsf);
	mgmt->u.beacon.capab_info = cpu_to_le16(capability);

	pos = (u8 *)mgmt + offsetof(struct ieee80211_mgmt, u.beacon.variable);

	*pos++ = WLAN_EID_SSID;
	*pos++ = ifibss->ssid_len;
	memcpy(pos, ifibss->ssid, ifibss->ssid_len);
	pos += ifibss->ssid_len;

	rates = min_t(int, 8, sband->n_bitrates);
	*pos++ = WLAN_EID_SUPP_RATES;
	*pos++ = rates;
	for (i = 0; i < rates; i++) {
		int rate = sband->bitrates[i].bitrate;
		u8 basic = 0;
		if (basic_rates & BIT(i))
			basic = 0x80;
		*pos++ = basic | (u8) (rate / 5);
	}

	if (sband->band == IEEE80211_BAND_2GHZ) {
		*pos++ = WLAN_EID_DS_PARAMS;
		*pos++ = 1;
		*pos++ = ieee80211_frequency_to_channel(chan->center_freq);
	}

	*pos++ = WLAN_EID_IBSS_PARAMS;
	*pos++ = 2;
	/* FIX: set ATIM window based on scan results */
	*pos++ = 0;
	*pos++ = 0;

	if (sband->n_bitrates > 8) {
		*pos++ = WLAN_EID_EXT_SUPP_RATES;
		*pos++ = sband->n_bitrates - 8;
		for (i = 8; i < sband->n_bitrates; i++) {
			int rate = sband->bitrates[i].bitrate;
			u8 basic = 0;
			if (basic_rates & BIT(i))
				basic = 0x80;
			*pos++ = basic | (u8) (rate / 5);
		}
	}

	if (ifibss->ie_len) {
		memcpy(pos, ifibss->ie, ifibss->ie_len);
		pos += ifibss->ie_len;
	}

	/* add HT capability and information IEs */
	if (chandef.width != NL80211_CHAN_WIDTH_20_NOHT &&
	    chandef.width != NL80211_CHAN_WIDTH_5 &&
	    chandef.width != NL80211_CHAN_WIDTH_10 &&
	    sband->ht_cap.ht_supported) {
		pos = ieee80211_ie_build_ht_cap(pos, &sband->ht_cap,
						sband->ht_cap.cap);
		/*
		 * Note: According to 802.11n-2009 9.13.3.1, HT Protection
		 * field and RIFS Mode are reserved in IBSS mode, therefore
		 * keep them at 0
		 */
		pos = ieee80211_ie_build_ht_oper(pos, &sband->ht_cap,
						 &chandef, 0);
	}

	if (local->hw.queues >= IEEE80211_NUM_ACS) {
		*pos++ = WLAN_EID_VENDOR_SPECIFIC;
		*pos++ = 7; /* len */
		*pos++ = 0x00; /* Microsoft OUI 00:50:F2 */
		*pos++ = 0x50;
		*pos++ = 0xf2;
		*pos++ = 2; /* WME */
		*pos++ = 0; /* WME info */
		*pos++ = 1; /* WME ver */
		*pos++ = 0; /* U-APSD no in use */
	}

	presp->head_len = pos - presp->head;
	if (WARN_ON(presp->head_len > frame_len))
		return;

	rcu_assign_pointer(ifibss->presp, presp);

	sdata->vif.bss_conf.enable_beacon = true;
	sdata->vif.bss_conf.beacon_int = beacon_int;
	sdata->vif.bss_conf.basic_rates = basic_rates;
	sdata->vif.bss_conf.ssid_len = ifibss->ssid_len;
	memcpy(sdata->vif.bss_conf.ssid, ifibss->ssid, ifibss->ssid_len);
	bss_change = BSS_CHANGED_BEACON_INT;
	bss_change |= ieee80211_reset_erp_info(sdata);
	bss_change |= BSS_CHANGED_BSSID;
	bss_change |= BSS_CHANGED_BEACON;
	bss_change |= BSS_CHANGED_BEACON_ENABLED;
	bss_change |= BSS_CHANGED_BASIC_RATES;
	bss_change |= BSS_CHANGED_HT;
	bss_change |= BSS_CHANGED_IBSS;
	bss_change |= BSS_CHANGED_SSID;

	/*
	 * In 5 GHz/802.11a, we can always use short slot time.
	 * (IEEE 802.11-2012 18.3.8.7)
	 *
	 * In 2.4GHz, we must always use long slots in IBSS for compatibility
	 * reasons.
	 * (IEEE 802.11-2012 19.4.5)
	 *
	 * HT follows these specifications (IEEE 802.11-2012 20.3.18)
	 */
	sdata->vif.bss_conf.use_short_slot = chan->band == IEEE80211_BAND_5GHZ;
	bss_change |= BSS_CHANGED_ERP_SLOT;

	sdata->vif.bss_conf.ibss_joined = true;
	sdata->vif.bss_conf.ibss_creator = creator;
	ieee80211_bss_info_change_notify(sdata, bss_change);

	ieee80211_sta_def_wmm_params(sdata, sband->n_bitrates, supp_rates);

	ifibss->state = IEEE80211_IBSS_MLME_JOINED;
	mod_timer(&ifibss->timer,
		  round_jiffies(jiffies + IEEE80211_IBSS_MERGE_INTERVAL));

	bss = cfg80211_inform_bss_frame(local->hw.wiphy, chan,
					mgmt, presp->head_len, 0, GFP_KERNEL);
	cfg80211_put_bss(local->hw.wiphy, bss);
	netif_carrier_on(sdata->dev);
	cfg80211_ibss_joined(sdata->dev, ifibss->bssid, GFP_KERNEL);
}

static void ieee80211_sta_join_ibss(struct ieee80211_sub_if_data *sdata,
				    struct ieee80211_bss *bss)
{
	struct cfg80211_bss *cbss =
		container_of((void *)bss, struct cfg80211_bss, priv);
	struct ieee80211_supported_band *sband;
	struct cfg80211_chan_def chandef;
	u32 basic_rates;
	int i, j;
	u16 beacon_int = cbss->beacon_interval;
	const struct cfg80211_bss_ies *ies;
	enum nl80211_channel_type chan_type;
	u64 tsf;

	sdata_assert_lock(sdata);

	if (beacon_int < 10)
		beacon_int = 10;

	switch (sdata->u.ibss.chandef.width) {
	case NL80211_CHAN_WIDTH_20_NOHT:
	case NL80211_CHAN_WIDTH_20:
	case NL80211_CHAN_WIDTH_40:
		chan_type = cfg80211_get_chandef_type(&sdata->u.ibss.chandef);
		cfg80211_chandef_create(&chandef, cbss->channel, chan_type);
		break;
	case NL80211_CHAN_WIDTH_5:
	case NL80211_CHAN_WIDTH_10:
		cfg80211_chandef_create(&chandef, cbss->channel,
					NL80211_CHAN_WIDTH_20_NOHT);
		chandef.width = sdata->u.ibss.chandef.width;
		break;
	default:
		/* fall back to 20 MHz for unsupported modes */
		cfg80211_chandef_create(&chandef, cbss->channel,
					NL80211_CHAN_WIDTH_20_NOHT);
		break;
	}

	sband = sdata->local->hw.wiphy->bands[cbss->channel->band];

	basic_rates = 0;

	for (i = 0; i < bss->supp_rates_len; i++) {
		int rate = (bss->supp_rates[i] & 0x7f) * 5;
		bool is_basic = !!(bss->supp_rates[i] & 0x80);

		for (j = 0; j < sband->n_bitrates; j++) {
			if (sband->bitrates[j].bitrate == rate) {
				if (is_basic)
					basic_rates |= BIT(j);
				break;
			}
		}
	}

	rcu_read_lock();
	ies = rcu_dereference(cbss->ies);
	tsf = ies->tsf;
	rcu_read_unlock();

	__ieee80211_sta_join_ibss(sdata, cbss->bssid,
				  beacon_int,
				  &chandef,
				  basic_rates,
				  cbss->capability,
				  tsf, false);
}

static struct sta_info *ieee80211_ibss_finish_sta(struct sta_info *sta)
	__acquires(RCU)
{
	struct ieee80211_sub_if_data *sdata = sta->sdata;
	u8 addr[ETH_ALEN];

	memcpy(addr, sta->sta.addr, ETH_ALEN);

	ibss_dbg(sdata, "Adding new IBSS station %pM\n", addr);

	sta_info_pre_move_state(sta, IEEE80211_STA_AUTH);
	sta_info_pre_move_state(sta, IEEE80211_STA_ASSOC);
	/* authorize the station only if the network is not RSN protected. If
	 * not wait for the userspace to authorize it */
	if (!sta->sdata->u.ibss.control_port)
		sta_info_pre_move_state(sta, IEEE80211_STA_AUTHORIZED);

	rate_control_rate_init(sta);

	/* If it fails, maybe we raced another insertion? */
	if (sta_info_insert_rcu(sta))
		return sta_info_get(sdata, addr);
	return sta;
}

static struct sta_info *
ieee80211_ibss_add_sta(struct ieee80211_sub_if_data *sdata, const u8 *bssid,
		       const u8 *addr, u32 supp_rates)
	__acquires(RCU)
{
	struct ieee80211_if_ibss *ifibss = &sdata->u.ibss;
	struct ieee80211_local *local = sdata->local;
	struct sta_info *sta;
	struct ieee80211_chanctx_conf *chanctx_conf;
	struct ieee80211_supported_band *sband;
	int band;

	/*
	 * XXX: Consider removing the least recently used entry and
	 * 	allow new one to be added.
	 */
	if (local->num_sta >= IEEE80211_IBSS_MAX_STA_ENTRIES) {
		net_info_ratelimited("%s: No room for a new IBSS STA entry %pM\n",
				    sdata->name, addr);
		rcu_read_lock();
		return NULL;
	}

	if (ifibss->state == IEEE80211_IBSS_MLME_SEARCH) {
		rcu_read_lock();
		return NULL;
	}

	if (!ether_addr_equal(bssid, sdata->u.ibss.bssid)) {
		rcu_read_lock();
		return NULL;
	}

	rcu_read_lock();
	chanctx_conf = rcu_dereference(sdata->vif.chanctx_conf);
	if (WARN_ON_ONCE(!chanctx_conf))
		return NULL;
	band = chanctx_conf->def.chan->band;
	rcu_read_unlock();

	sta = sta_info_alloc(sdata, addr, GFP_KERNEL);
	if (!sta) {
		rcu_read_lock();
		return NULL;
	}

	sta->last_rx = jiffies;

	/* make sure mandatory rates are always added */
	sband = local->hw.wiphy->bands[band];
	sta->sta.supp_rates[band] = supp_rates |
			ieee80211_mandatory_rates(sband);

	return ieee80211_ibss_finish_sta(sta);
}

static void ieee80211_rx_mgmt_deauth_ibss(struct ieee80211_sub_if_data *sdata,
					  struct ieee80211_mgmt *mgmt,
					  size_t len)
{
	u16 reason = le16_to_cpu(mgmt->u.deauth.reason_code);

	if (len < IEEE80211_DEAUTH_FRAME_LEN)
		return;

	ibss_dbg(sdata, "RX DeAuth SA=%pM DA=%pM BSSID=%pM (reason: %d)\n",
		 mgmt->sa, mgmt->da, mgmt->bssid, reason);
	sta_info_destroy_addr(sdata, mgmt->sa);
}

static void ieee80211_rx_mgmt_auth_ibss(struct ieee80211_sub_if_data *sdata,
					struct ieee80211_mgmt *mgmt,
					size_t len)
{
	u16 auth_alg, auth_transaction;

	sdata_assert_lock(sdata);

	if (len < 24 + 6)
		return;

	auth_alg = le16_to_cpu(mgmt->u.auth.auth_alg);
	auth_transaction = le16_to_cpu(mgmt->u.auth.auth_transaction);

	ibss_dbg(sdata,
		 "RX Auth SA=%pM DA=%pM BSSID=%pM (auth_transaction=%d)\n",
		 mgmt->sa, mgmt->da, mgmt->bssid, auth_transaction);

	if (auth_alg != WLAN_AUTH_OPEN || auth_transaction != 1)
		return;

	/*
	 * IEEE 802.11 standard does not require authentication in IBSS
	 * networks and most implementations do not seem to use it.
	 * However, try to reply to authentication attempts if someone
	 * has actually implemented this.
	 */
	ieee80211_send_auth(sdata, 2, WLAN_AUTH_OPEN, 0, NULL, 0,
			    mgmt->sa, sdata->u.ibss.bssid, NULL, 0, 0, 0);
}

static void ieee80211_rx_bss_info(struct ieee80211_sub_if_data *sdata,
				  struct ieee80211_mgmt *mgmt, size_t len,
				  struct ieee80211_rx_status *rx_status,
				  struct ieee802_11_elems *elems)
{
	struct ieee80211_local *local = sdata->local;
	int freq;
	struct cfg80211_bss *cbss;
	struct ieee80211_bss *bss;
	struct sta_info *sta;
	struct ieee80211_channel *channel;
	u64 beacon_timestamp, rx_timestamp;
	u32 supp_rates = 0;
	enum ieee80211_band band = rx_status->band;
	struct ieee80211_supported_band *sband = local->hw.wiphy->bands[band];
	bool rates_updated = false;

	if (elems->ds_params)
		freq = ieee80211_channel_to_frequency(elems->ds_params[0],
						      band);
	else
		freq = rx_status->freq;

	channel = ieee80211_get_channel(local->hw.wiphy, freq);

	if (!channel || channel->flags & IEEE80211_CHAN_DISABLED)
		return;

	if (sdata->vif.type == NL80211_IFTYPE_ADHOC &&
	    ether_addr_equal(mgmt->bssid, sdata->u.ibss.bssid)) {

		rcu_read_lock();
		sta = sta_info_get(sdata, mgmt->sa);

		if (elems->supp_rates) {
			supp_rates = ieee80211_sta_get_rates(local, elems,
							     band, NULL);
			if (sta) {
				u32 prev_rates;

				prev_rates = sta->sta.supp_rates[band];
				/* make sure mandatory rates are always added */
				sta->sta.supp_rates[band] = supp_rates |
					ieee80211_mandatory_rates(sband);

				if (sta->sta.supp_rates[band] != prev_rates) {
					ibss_dbg(sdata,
						 "updated supp_rates set for %pM based on beacon/probe_resp (0x%x -> 0x%x)\n",
						 sta->sta.addr, prev_rates,
						 sta->sta.supp_rates[band]);
					rates_updated = true;
				}
			} else {
				rcu_read_unlock();
				sta = ieee80211_ibss_add_sta(sdata, mgmt->bssid,
						mgmt->sa, supp_rates);
			}
		}

		if (sta && elems->wmm_info)
			set_sta_flag(sta, WLAN_STA_WME);

		if (sta && elems->ht_operation && elems->ht_cap_elem &&
		    sdata->u.ibss.chandef.width != NL80211_CHAN_WIDTH_20_NOHT &&
		    sdata->u.ibss.chandef.width != NL80211_CHAN_WIDTH_5 &&
		    sdata->u.ibss.chandef.width != NL80211_CHAN_WIDTH_10) {
			/* we both use HT */
			struct ieee80211_ht_cap htcap_ie;
			struct cfg80211_chan_def chandef;

			ieee80211_ht_oper_to_chandef(channel,
						     elems->ht_operation,
						     &chandef);

			memcpy(&htcap_ie, elems->ht_cap_elem, sizeof(htcap_ie));

			/*
			 * fall back to HT20 if we don't use or use
			 * the other extension channel
			 */
			if (chandef.center_freq1 !=
			    sdata->u.ibss.chandef.center_freq1)
				htcap_ie.cap_info &=
					cpu_to_le16(~IEEE80211_HT_CAP_SUP_WIDTH_20_40);

			rates_updated |= ieee80211_ht_cap_ie_to_sta_ht_cap(
						sdata, sband, &htcap_ie, sta);
		}

		if (sta && rates_updated) {
			drv_sta_rc_update(local, sdata, &sta->sta,
					  IEEE80211_RC_SUPP_RATES_CHANGED);
			rate_control_rate_init(sta);
		}

		rcu_read_unlock();
	}

	bss = ieee80211_bss_info_update(local, rx_status, mgmt, len, elems,
					channel);
	if (!bss)
		return;

	cbss = container_of((void *)bss, struct cfg80211_bss, priv);

	/* same for beacon and probe response */
	beacon_timestamp = le64_to_cpu(mgmt->u.beacon.timestamp);

	/* check if we need to merge IBSS */

	/* we use a fixed BSSID */
	if (sdata->u.ibss.fixed_bssid)
		goto put_bss;

	/* not an IBSS */
	if (!(cbss->capability & WLAN_CAPABILITY_IBSS))
		goto put_bss;

	/* different channel */
	if (sdata->u.ibss.fixed_channel &&
	    sdata->u.ibss.chandef.chan != cbss->channel)
		goto put_bss;

	/* different SSID */
	if (elems->ssid_len != sdata->u.ibss.ssid_len ||
	    memcmp(elems->ssid, sdata->u.ibss.ssid,
				sdata->u.ibss.ssid_len))
		goto put_bss;

	/* same BSSID */
	if (ether_addr_equal(cbss->bssid, sdata->u.ibss.bssid))
		goto put_bss;

	if (ieee80211_have_rx_timestamp(rx_status)) {
		/* time when timestamp field was received */
		rx_timestamp =
			ieee80211_calculate_rx_timestamp(local, rx_status,
							 len + FCS_LEN, 24);
	} else {
		/*
		 * second best option: get current TSF
		 * (will return -1 if not supported)
		 */
		rx_timestamp = drv_get_tsf(local, sdata);
	}

	ibss_dbg(sdata,
		 "RX beacon SA=%pM BSSID=%pM TSF=0x%llx BCN=0x%llx diff=%lld @%lu\n",
		 mgmt->sa, mgmt->bssid,
		 (unsigned long long)rx_timestamp,
		 (unsigned long long)beacon_timestamp,
		 (unsigned long long)(rx_timestamp - beacon_timestamp),
		 jiffies);

	if (beacon_timestamp > rx_timestamp) {
		ibss_dbg(sdata,
			 "beacon TSF higher than local TSF - IBSS merge with BSSID %pM\n",
			 mgmt->bssid);
		ieee80211_sta_join_ibss(sdata, bss);
		supp_rates = ieee80211_sta_get_rates(local, elems, band, NULL);
		ieee80211_ibss_add_sta(sdata, mgmt->bssid, mgmt->sa,
				       supp_rates);
		rcu_read_unlock();
	}

 put_bss:
	ieee80211_rx_bss_put(local, bss);
}

void ieee80211_ibss_rx_no_sta(struct ieee80211_sub_if_data *sdata,
			      const u8 *bssid, const u8 *addr,
			      u32 supp_rates)
{
	struct ieee80211_if_ibss *ifibss = &sdata->u.ibss;
	struct ieee80211_local *local = sdata->local;
	struct sta_info *sta;
	struct ieee80211_chanctx_conf *chanctx_conf;
	struct ieee80211_supported_band *sband;
	int band;

	/*
	 * XXX: Consider removing the least recently used entry and
	 * 	allow new one to be added.
	 */
	if (local->num_sta >= IEEE80211_IBSS_MAX_STA_ENTRIES) {
		net_info_ratelimited("%s: No room for a new IBSS STA entry %pM\n",
				    sdata->name, addr);
		return;
	}

	if (ifibss->state == IEEE80211_IBSS_MLME_SEARCH)
		return;

	if (!ether_addr_equal(bssid, sdata->u.ibss.bssid))
		return;

	rcu_read_lock();
	chanctx_conf = rcu_dereference(sdata->vif.chanctx_conf);
	if (WARN_ON_ONCE(!chanctx_conf)) {
		rcu_read_unlock();
		return;
	}
	band = chanctx_conf->def.chan->band;
	rcu_read_unlock();

	sta = sta_info_alloc(sdata, addr, GFP_ATOMIC);
	if (!sta)
		return;

	sta->last_rx = jiffies;

	/* make sure mandatory rates are always added */
	sband = local->hw.wiphy->bands[band];
	sta->sta.supp_rates[band] = supp_rates |
			ieee80211_mandatory_rates(sband);

	spin_lock(&ifibss->incomplete_lock);
	list_add(&sta->list, &ifibss->incomplete_stations);
	spin_unlock(&ifibss->incomplete_lock);
	ieee80211_queue_work(&local->hw, &sdata->work);
}

static int ieee80211_sta_active_ibss(struct ieee80211_sub_if_data *sdata)
{
	struct ieee80211_local *local = sdata->local;
	int active = 0;
	struct sta_info *sta;

	sdata_assert_lock(sdata);

	rcu_read_lock();

	list_for_each_entry_rcu(sta, &local->sta_list, list) {
		if (sta->sdata == sdata &&
		    time_after(sta->last_rx + IEEE80211_IBSS_MERGE_INTERVAL,
			       jiffies)) {
			active++;
			break;
		}
	}

	rcu_read_unlock();

	return active;
}

/*
 * This function is called with state == IEEE80211_IBSS_MLME_JOINED
 */

static void ieee80211_sta_merge_ibss(struct ieee80211_sub_if_data *sdata)
{
	struct ieee80211_if_ibss *ifibss = &sdata->u.ibss;

	sdata_assert_lock(sdata);

	mod_timer(&ifibss->timer,
		  round_jiffies(jiffies + IEEE80211_IBSS_MERGE_INTERVAL));

	ieee80211_sta_expire(sdata, IEEE80211_IBSS_INACTIVITY_LIMIT);

	if (time_before(jiffies, ifibss->last_scan_completed +
		       IEEE80211_IBSS_MERGE_INTERVAL))
		return;

	if (ieee80211_sta_active_ibss(sdata))
		return;

	if (ifibss->fixed_channel)
		return;

	sdata_info(sdata,
		   "No active IBSS STAs - trying to scan for other IBSS networks with same SSID (merge)\n");

	ieee80211_request_ibss_scan(sdata, ifibss->ssid, ifibss->ssid_len,
				    NULL);
}

static void ieee80211_sta_create_ibss(struct ieee80211_sub_if_data *sdata)
{
	struct ieee80211_if_ibss *ifibss = &sdata->u.ibss;
	u8 bssid[ETH_ALEN];
	u16 capability;
	int i;

	sdata_assert_lock(sdata);

	if (ifibss->fixed_bssid) {
		memcpy(bssid, ifibss->bssid, ETH_ALEN);
	} else {
		/* Generate random, not broadcast, locally administered BSSID. Mix in
		 * own MAC address to make sure that devices that do not have proper
		 * random number generator get different BSSID. */
		get_random_bytes(bssid, ETH_ALEN);
		for (i = 0; i < ETH_ALEN; i++)
			bssid[i] ^= sdata->vif.addr[i];
		bssid[0] &= ~0x01;
		bssid[0] |= 0x02;
	}

	sdata_info(sdata, "Creating new IBSS network, BSSID %pM\n", bssid);

	capability = WLAN_CAPABILITY_IBSS;

	if (ifibss->privacy)
		capability |= WLAN_CAPABILITY_PRIVACY;
	else
		sdata->drop_unencrypted = 0;

	__ieee80211_sta_join_ibss(sdata, bssid, sdata->vif.bss_conf.beacon_int,
				  &ifibss->chandef, ifibss->basic_rates,
				  capability, 0, true);
}

/*
 * This function is called with state == IEEE80211_IBSS_MLME_SEARCH
 */

static void ieee80211_sta_find_ibss(struct ieee80211_sub_if_data *sdata)
{
	struct ieee80211_if_ibss *ifibss = &sdata->u.ibss;
	struct ieee80211_local *local = sdata->local;
	struct cfg80211_bss *cbss;
	struct ieee80211_channel *chan = NULL;
	const u8 *bssid = NULL;
	int active_ibss;
	u16 capability;

	sdata_assert_lock(sdata);

	active_ibss = ieee80211_sta_active_ibss(sdata);
	ibss_dbg(sdata, "sta_find_ibss (active_ibss=%d)\n", active_ibss);

	if (active_ibss)
		return;

	capability = WLAN_CAPABILITY_IBSS;
	if (ifibss->privacy)
		capability |= WLAN_CAPABILITY_PRIVACY;
	if (ifibss->fixed_bssid)
		bssid = ifibss->bssid;
	if (ifibss->fixed_channel)
		chan = ifibss->chandef.chan;
	if (!is_zero_ether_addr(ifibss->bssid))
		bssid = ifibss->bssid;
	cbss = cfg80211_get_bss(local->hw.wiphy, chan, bssid,
				ifibss->ssid, ifibss->ssid_len,
				WLAN_CAPABILITY_IBSS | WLAN_CAPABILITY_PRIVACY,
				capability);

	if (cbss) {
		struct ieee80211_bss *bss;

		bss = (void *)cbss->priv;
		ibss_dbg(sdata,
			 "sta_find_ibss: selected %pM current %pM\n",
			 cbss->bssid, ifibss->bssid);
		sdata_info(sdata,
			   "Selected IBSS BSSID %pM based on configured SSID\n",
			   cbss->bssid);

		ieee80211_sta_join_ibss(sdata, bss);
		ieee80211_rx_bss_put(local, bss);
		return;
	}

	ibss_dbg(sdata, "sta_find_ibss: did not try to join ibss\n");

	/* Selected IBSS not found in current scan results - try to scan */
	if (time_after(jiffies, ifibss->last_scan_completed +
					IEEE80211_SCAN_INTERVAL)) {
		sdata_info(sdata, "Trigger new scan to find an IBSS to join\n");

		ieee80211_request_ibss_scan(sdata, ifibss->ssid,
					    ifibss->ssid_len, chan);
	} else {
		int interval = IEEE80211_SCAN_INTERVAL;

		if (time_after(jiffies, ifibss->ibss_join_req +
			       IEEE80211_IBSS_JOIN_TIMEOUT))
			ieee80211_sta_create_ibss(sdata);

		mod_timer(&ifibss->timer,
			  round_jiffies(jiffies + interval));
	}
}

static void ieee80211_rx_mgmt_probe_req(struct ieee80211_sub_if_data *sdata,
					struct sk_buff *req)
{
	struct ieee80211_mgmt *mgmt = (void *)req->data;
	struct ieee80211_if_ibss *ifibss = &sdata->u.ibss;
	struct ieee80211_local *local = sdata->local;
	int tx_last_beacon, len = req->len;
	struct sk_buff *skb;
	struct beacon_data *presp;
	u8 *pos, *end;

	sdata_assert_lock(sdata);

	presp = rcu_dereference_protected(ifibss->presp,
					  lockdep_is_held(&sdata->wdev.mtx));

	if (ifibss->state != IEEE80211_IBSS_MLME_JOINED ||
	    len < 24 + 2 || !presp)
		return;

	tx_last_beacon = drv_tx_last_beacon(local);

	ibss_dbg(sdata,
		 "RX ProbeReq SA=%pM DA=%pM BSSID=%pM (tx_last_beacon=%d)\n",
		 mgmt->sa, mgmt->da, mgmt->bssid, tx_last_beacon);

	if (!tx_last_beacon && is_multicast_ether_addr(mgmt->da))
		return;

	if (!ether_addr_equal(mgmt->bssid, ifibss->bssid) &&
	    !is_broadcast_ether_addr(mgmt->bssid))
		return;

	end = ((u8 *) mgmt) + len;
	pos = mgmt->u.probe_req.variable;
	if (pos[0] != WLAN_EID_SSID ||
	    pos + 2 + pos[1] > end) {
		ibss_dbg(sdata, "Invalid SSID IE in ProbeReq from %pM\n",
			 mgmt->sa);
		return;
	}
	if (pos[1] != 0 &&
	    (pos[1] != ifibss->ssid_len ||
	     memcmp(pos + 2, ifibss->ssid, ifibss->ssid_len))) {
		/* Ignore ProbeReq for foreign SSID */
		return;
	}

	/* Reply with ProbeResp */
	skb = dev_alloc_skb(local->tx_headroom + presp->head_len);
	if (!skb)
		return;

	skb_reserve(skb, local->tx_headroom);
	memcpy(skb_put(skb, presp->head_len), presp->head, presp->head_len);

	memcpy(((struct ieee80211_mgmt *) skb->data)->da, mgmt->sa, ETH_ALEN);
	ibss_dbg(sdata, "Sending ProbeResp to %pM\n", mgmt->sa);
	IEEE80211_SKB_CB(skb)->flags |= IEEE80211_TX_INTFL_DONT_ENCRYPT;
	ieee80211_tx_skb(sdata, skb);
}

static
void ieee80211_rx_mgmt_probe_beacon(struct ieee80211_sub_if_data *sdata,
				    struct ieee80211_mgmt *mgmt, size_t len,
				    struct ieee80211_rx_status *rx_status)
{
	size_t baselen;
	struct ieee802_11_elems elems;

	BUILD_BUG_ON(offsetof(typeof(mgmt->u.probe_resp), variable) !=
		     offsetof(typeof(mgmt->u.beacon), variable));

	/*
	 * either beacon or probe_resp but the variable field is at the
	 * same offset
	 */
	baselen = (u8 *) mgmt->u.probe_resp.variable - (u8 *) mgmt;
	if (baselen > len)
		return;

	ieee802_11_parse_elems(mgmt->u.probe_resp.variable, len - baselen,
			       false, &elems);

	ieee80211_rx_bss_info(sdata, mgmt, len, rx_status, &elems);
}

void ieee80211_ibss_rx_queued_mgmt(struct ieee80211_sub_if_data *sdata,
				   struct sk_buff *skb)
{
	struct ieee80211_rx_status *rx_status;
	struct ieee80211_mgmt *mgmt;
	u16 fc;

	rx_status = IEEE80211_SKB_RXCB(skb);
	mgmt = (struct ieee80211_mgmt *) skb->data;
	fc = le16_to_cpu(mgmt->frame_control);

	sdata_lock(sdata);

	if (!sdata->u.ibss.ssid_len)
		goto mgmt_out; /* not ready to merge yet */

	switch (fc & IEEE80211_FCTL_STYPE) {
	case IEEE80211_STYPE_PROBE_REQ:
		ieee80211_rx_mgmt_probe_req(sdata, skb);
		break;
	case IEEE80211_STYPE_PROBE_RESP:
	case IEEE80211_STYPE_BEACON:
		ieee80211_rx_mgmt_probe_beacon(sdata, mgmt, skb->len,
					       rx_status);
		break;
	case IEEE80211_STYPE_AUTH:
		ieee80211_rx_mgmt_auth_ibss(sdata, mgmt, skb->len);
		break;
	case IEEE80211_STYPE_DEAUTH:
		ieee80211_rx_mgmt_deauth_ibss(sdata, mgmt, skb->len);
		break;
	}

 mgmt_out:
	sdata_unlock(sdata);
}

void ieee80211_ibss_work(struct ieee80211_sub_if_data *sdata)
{
	struct ieee80211_if_ibss *ifibss = &sdata->u.ibss;
	struct sta_info *sta;

	sdata_lock(sdata);

	/*
	 * Work could be scheduled after scan or similar
	 * when we aren't even joined (or trying) with a
	 * network.
	 */
	if (!ifibss->ssid_len)
		goto out;

	spin_lock_bh(&ifibss->incomplete_lock);
	while (!list_empty(&ifibss->incomplete_stations)) {
		sta = list_first_entry(&ifibss->incomplete_stations,
				       struct sta_info, list);
		list_del(&sta->list);
		spin_unlock_bh(&ifibss->incomplete_lock);

		ieee80211_ibss_finish_sta(sta);
		rcu_read_unlock();
		spin_lock_bh(&ifibss->incomplete_lock);
	}
	spin_unlock_bh(&ifibss->incomplete_lock);

	switch (ifibss->state) {
	case IEEE80211_IBSS_MLME_SEARCH:
		ieee80211_sta_find_ibss(sdata);
		break;
	case IEEE80211_IBSS_MLME_JOINED:
		ieee80211_sta_merge_ibss(sdata);
		break;
	default:
		WARN_ON(1);
		break;
	}

 out:
	sdata_unlock(sdata);
}

static void ieee80211_ibss_timer(unsigned long data)
{
	struct ieee80211_sub_if_data *sdata =
		(struct ieee80211_sub_if_data *) data;

	ieee80211_queue_work(&sdata->local->hw, &sdata->work);
}

void ieee80211_ibss_setup_sdata(struct ieee80211_sub_if_data *sdata)
{
	struct ieee80211_if_ibss *ifibss = &sdata->u.ibss;

	setup_timer(&ifibss->timer, ieee80211_ibss_timer,
		    (unsigned long) sdata);
	INIT_LIST_HEAD(&ifibss->incomplete_stations);
	spin_lock_init(&ifibss->incomplete_lock);
}

/* scan finished notification */
void ieee80211_ibss_notify_scan_completed(struct ieee80211_local *local)
{
	struct ieee80211_sub_if_data *sdata;

	mutex_lock(&local->iflist_mtx);
	list_for_each_entry(sdata, &local->interfaces, list) {
		if (!ieee80211_sdata_running(sdata))
			continue;
		if (sdata->vif.type != NL80211_IFTYPE_ADHOC)
			continue;
		sdata->u.ibss.last_scan_completed = jiffies;
		ieee80211_queue_work(&local->hw, &sdata->work);
	}
	mutex_unlock(&local->iflist_mtx);
}

int ieee80211_ibss_join(struct ieee80211_sub_if_data *sdata,
			struct cfg80211_ibss_params *params)
{
	u32 changed = 0;

	if (params->bssid) {
		memcpy(sdata->u.ibss.bssid, params->bssid, ETH_ALEN);
		sdata->u.ibss.fixed_bssid = true;
	} else
		sdata->u.ibss.fixed_bssid = false;

	sdata->u.ibss.privacy = params->privacy;
	sdata->u.ibss.control_port = params->control_port;
	sdata->u.ibss.basic_rates = params->basic_rates;
	memcpy(sdata->vif.bss_conf.mcast_rate, params->mcast_rate,
	       sizeof(params->mcast_rate));

	sdata->vif.bss_conf.beacon_int = params->beacon_interval;

	sdata->u.ibss.chandef = params->chandef;
	sdata->u.ibss.fixed_channel = params->channel_fixed;

	if (params->ie) {
		sdata->u.ibss.ie = kmemdup(params->ie, params->ie_len,
					   GFP_KERNEL);
		if (sdata->u.ibss.ie)
			sdata->u.ibss.ie_len = params->ie_len;
	}

	sdata->u.ibss.state = IEEE80211_IBSS_MLME_SEARCH;
	sdata->u.ibss.ibss_join_req = jiffies;

	memcpy(sdata->u.ibss.ssid, params->ssid, params->ssid_len);
	sdata->u.ibss.ssid_len = params->ssid_len;

	/*
	 * 802.11n-2009 9.13.3.1: In an IBSS, the HT Protection field is
	 * reserved, but an HT STA shall protect HT transmissions as though
	 * the HT Protection field were set to non-HT mixed mode.
	 *
	 * In an IBSS, the RIFS Mode field of the HT Operation element is
	 * also reserved, but an HT STA shall operate as though this field
	 * were set to 1.
	 */

	sdata->vif.bss_conf.ht_operation_mode |=
		  IEEE80211_HT_OP_MODE_PROTECTION_NONHT_MIXED
		| IEEE80211_HT_PARAM_RIFS_MODE;

	changed |= BSS_CHANGED_HT;
	ieee80211_bss_info_change_notify(sdata, changed);

	sdata->smps_mode = IEEE80211_SMPS_OFF;
	sdata->needed_rx_chains = sdata->local->rx_chains;

	ieee80211_queue_work(&sdata->local->hw, &sdata->work);

	return 0;
}

int ieee80211_ibss_leave(struct ieee80211_sub_if_data *sdata)
{
	struct ieee80211_if_ibss *ifibss = &sdata->u.ibss;
	struct ieee80211_local *local = sdata->local;
	struct cfg80211_bss *cbss;
	u16 capability;
	int active_ibss;
	struct sta_info *sta;
	struct beacon_data *presp;

	active_ibss = ieee80211_sta_active_ibss(sdata);

	if (!active_ibss && !is_zero_ether_addr(ifibss->bssid)) {
		capability = WLAN_CAPABILITY_IBSS;

		if (ifibss->privacy)
			capability |= WLAN_CAPABILITY_PRIVACY;

		cbss = cfg80211_get_bss(local->hw.wiphy, ifibss->chandef.chan,
					ifibss->bssid, ifibss->ssid,
					ifibss->ssid_len, WLAN_CAPABILITY_IBSS |
					WLAN_CAPABILITY_PRIVACY,
					capability);

		if (cbss) {
			cfg80211_unlink_bss(local->hw.wiphy, cbss);
			cfg80211_put_bss(local->hw.wiphy, cbss);
		}
	}

	ifibss->state = IEEE80211_IBSS_MLME_SEARCH;
	memset(ifibss->bssid, 0, ETH_ALEN);
	ifibss->ssid_len = 0;

	sta_info_flush(sdata);

	spin_lock_bh(&ifibss->incomplete_lock);
	while (!list_empty(&ifibss->incomplete_stations)) {
		sta = list_first_entry(&ifibss->incomplete_stations,
				       struct sta_info, list);
		list_del(&sta->list);
		spin_unlock_bh(&ifibss->incomplete_lock);

		sta_info_free(local, sta);
		spin_lock_bh(&ifibss->incomplete_lock);
	}
	spin_unlock_bh(&ifibss->incomplete_lock);

	netif_carrier_off(sdata->dev);

	/* remove beacon */
	kfree(sdata->u.ibss.ie);
	presp = rcu_dereference_protected(ifibss->presp,
					  lockdep_is_held(&sdata->wdev.mtx));
	RCU_INIT_POINTER(sdata->u.ibss.presp, NULL);
	sdata->vif.bss_conf.ibss_joined = false;
	sdata->vif.bss_conf.ibss_creator = false;
	sdata->vif.bss_conf.enable_beacon = false;
	sdata->vif.bss_conf.ssid_len = 0;
	clear_bit(SDATA_STATE_OFFCHANNEL_BEACON_STOPPED, &sdata->state);
	ieee80211_bss_info_change_notify(sdata, BSS_CHANGED_BEACON_ENABLED |
						BSS_CHANGED_IBSS);
	ieee80211_vif_release_channel(sdata);
	synchronize_rcu();
	kfree(presp);

	skb_queue_purge(&sdata->skb_queue);

	del_timer_sync(&sdata->u.ibss.timer);

	return 0;
}
