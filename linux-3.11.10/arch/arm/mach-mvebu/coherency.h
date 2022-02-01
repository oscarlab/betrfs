/*
 * arch/arm/mach-mvebu/include/mach/coherency.h
 *
 *
 * Coherency fabric (Aurora) support for Armada 370 and XP platforms.
 *
 * Copyright (C) 2012 Marvell
 *
 * This file is licensed under the terms of the GNU General Public
 * License version 2.  This program is licensed "as is" without any
 * warranty of any kind, whether express or implied.
 */

#ifndef __MACH_370_XP_COHERENCY_H
#define __MACH_370_XP_COHERENCY_H

int set_cpu_coherent(int cpu_id, int smp_group_id);
int coherency_init(void);

#endif	/* __MACH_370_XP_COHERENCY_H */
