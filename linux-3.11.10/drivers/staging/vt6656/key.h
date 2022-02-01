/*
 * Copyright (c) 1996, 2003 VIA Networking Technologies, Inc.
 * All rights reserved.
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
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 *
 * File: key.h
 *
 * Purpose: Implement functions for 802.11i Key management
 *
 * Author: Jerry Chen
 *
 * Date: May 29, 2003
 *
 */

#ifndef __KEY_H__
#define __KEY_H__

#include "tether.h"
#include "80211mgr.h"

#define MAX_GROUP_KEY       4
#define MAX_KEY_TABLE       11
#define MAX_KEY_LEN         32
#define AES_KEY_LEN         16

#define AUTHENTICATOR_KEY   0x10000000
#define USE_KEYRSC          0x20000000
#define PAIRWISE_KEY        0x40000000
#define TRANSMIT_KEY        0x80000000

#define GROUP_KEY           0x00000000

#define KEY_CTL_WEP         0x00
#define KEY_CTL_NONE        0x01
#define KEY_CTL_TKIP        0x02
#define KEY_CTL_CCMP        0x03
#define KEY_CTL_INVALID     0xFF

typedef struct tagSKeyItem
{
    bool        bKeyValid;
	u32 uKeyLength;
    u8        abyKey[MAX_KEY_LEN];
	u64 KeyRSC;
    u32       dwTSC47_16;
    u16        wTSC15_0;
    u8        byCipherSuite;
    u8        byReserved0;
    u32       dwKeyIndex;
    void *pvKeyTable;
} SKeyItem, *PSKeyItem; //64

typedef struct tagSKeyTable
{
    u8        abyBSSID[ETH_ALEN];  /* 6 */
    u8        byReserved0[2];              //8
    SKeyItem    PairwiseKey;
    SKeyItem    GroupKey[MAX_GROUP_KEY]; //64*5 = 320, 320+8=328
    u32       dwGTKeyIndex;            // GroupTransmitKey Index
    bool        bInUse;
    u16        wKeyCtl;
    bool        bSoftWEP;
    u8        byReserved1[6];
} SKeyTable, *PSKeyTable; //352

typedef struct tagSKeyManagement
{
    SKeyTable   KeyTable[MAX_KEY_TABLE];
} SKeyManagement, *PSKeyManagement;

void KeyvInitTable(struct vnt_private *, PSKeyManagement pTable);

int KeybGetKey(PSKeyManagement pTable, u8 *pbyBSSID, u32 dwKeyIndex,
	PSKeyItem *pKey);

int KeybSetKey(struct vnt_private *, PSKeyManagement pTable, u8 *pbyBSSID,
	u32 dwKeyIndex, u32 uKeyLength, u64 *KeyRSC, u8 *pbyKey,
	u8 byKeyDecMode);

int KeybRemoveKey(struct vnt_private *, PSKeyManagement pTable,
	u8 *pbyBSSID, u32 dwKeyIndex);

int KeybRemoveAllKey(struct vnt_private *, PSKeyManagement pTable,
	u8 *pbyBSSID);

int KeybGetTransmitKey(PSKeyManagement pTable, u8 *pbyBSSID, u32 dwKeyType,
	PSKeyItem *pKey);

int KeybSetDefaultKey(struct vnt_private *, PSKeyManagement pTable,
	u32 dwKeyIndex, u32 uKeyLength, u64 *KeyRSC, u8 *pbyKey,
	u8 byKeyDecMode);

int KeybSetAllGroupKey(struct vnt_private *, PSKeyManagement pTable,
	u32 dwKeyIndex, u32 uKeyLength, u64 *KeyRSC, u8 *pbyKey,
	u8 byKeyDecMode);

#endif /* __KEY_H__ */
