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
 * File: srom.h
 *
 * Purpose: Implement functions to access eeprom
 *
 * Author: Jerry Chen
 *
 * Date: Jan 29, 2003
 *
 */

#ifndef __SROM_H__
#define __SROM_H__

#define EEP_MAX_CONTEXT_SIZE    256

#define CB_EEPROM_READBYTE_WAIT 900     //us

#define W_MAX_I2CRETRY          0x0fff

//
// Contents in the EEPROM
//
#define EEP_OFS_PAR         0x00        // physical address
#define EEP_OFS_ANTENNA     0x17
#define EEP_OFS_RADIOCTL    0x18
#define EEP_OFS_RFTYPE      0x1B        // for select RF
#define EEP_OFS_MINCHANNEL  0x1C        // Min Channel #
#define EEP_OFS_MAXCHANNEL  0x1D        // Max Channel #
#define EEP_OFS_SIGNATURE   0x1E        //
#define EEP_OFS_ZONETYPE    0x1F        //
#define EEP_OFS_RFTABLE     0x20        // RF POWER TABLE
#define EEP_OFS_PWR_CCK     0x20
#define EEP_OFS_SETPT_CCK   0x21
#define EEP_OFS_PWR_OFDMG   0x23

#define EEP_OFS_CALIB_TX_IQ 0x24
#define EEP_OFS_CALIB_TX_DC 0x25
#define EEP_OFS_CALIB_RX_IQ 0x26

#define EEP_OFS_MAJOR_VER 0x2E
#define EEP_OFS_MINOR_VER 0x2F

#define EEP_OFS_CCK_PWR_TBL     0x30
#define EEP_OFS_OFDM_PWR_TBL    0x40
#define EEP_OFS_OFDMA_PWR_TBL   0x50

//
// Bits in EEP_OFS_ANTENNA
//
#define EEP_ANTENNA_MAIN    0x01
#define EEP_ANTENNA_AUX     0x02
#define EEP_ANTINV          0x04

//
// Bits in EEP_OFS_RADIOCTL
//
#define EEP_RADIOCTL_ENABLE 0x80

// AT24C02 eeprom contents
//      2048 bits = 256 bytes = 128 words
//
typedef struct tagSSromReg {
    u8    abyPAR[6];                  // 0x00 (u16)

    u16    wSUB_VID;                   // 0x03 (u16)
    u16    wSUB_SID;

    u8    byBCFG0;                    // 0x05 (u16)
    u8    byBCFG1;

    u8    byFCR0;                     // 0x06 (u16)
    u8    byFCR1;
    u8    byPMC0;                     // 0x07 (u16)
    u8    byPMC1;
    u8    byMAXLAT;                   // 0x08 (u16)
    u8    byMINGNT;
    u8    byCFG0;                     // 0x09 (u16)
    u8    byCFG1;
    u16    wCISPTR;                    // 0x0A (u16)
    u16    wRsv0;                      // 0x0B (u16)
    u16    wRsv1;                      // 0x0C (u16)
    u8    byBBPAIR;                   // 0x0D (u16)
    u8    byRFTYPE;
    u8    byMinChannel;               // 0x0E (u16)
    u8    byMaxChannel;
    u8    bySignature;                // 0x0F (u16)
    u8    byCheckSum;

    u8    abyReserved0[96];           // 0x10 (u16)
    u8    abyCIS[128];                // 0x80 (u16)
} SSromReg, *PSSromReg;

#endif /* __EEPROM_H__ */
