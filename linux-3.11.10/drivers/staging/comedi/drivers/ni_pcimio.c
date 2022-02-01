/*
    comedi/drivers/ni_pcimio.c
    Hardware driver for NI PCI-MIO E series cards

    COMEDI - Linux Control and Measurement Device Interface
    Copyright (C) 1997-8 David A. Schleef <ds@schleef.org>

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
*/
/*
Driver: ni_pcimio
Description: National Instruments PCI-MIO-E series and M series (all boards)
Author: ds, John Hallen, Frank Mori Hess, Rolf Mueller, Herbert Peremans,
  Herman Bruyninckx, Terry Barnaby
Status: works
Devices: [National Instruments] PCI-MIO-16XE-50 (ni_pcimio),
  PCI-MIO-16XE-10, PXI-6030E, PCI-MIO-16E-1, PCI-MIO-16E-4, PCI-6014, PCI-6040E,
  PXI-6040E, PCI-6030E, PCI-6031E, PCI-6032E, PCI-6033E, PCI-6071E, PCI-6023E,
  PCI-6024E, PCI-6025E, PXI-6025E, PCI-6034E, PCI-6035E, PCI-6052E,
  PCI-6110, PCI-6111, PCI-6220, PCI-6221, PCI-6224, PXI-6224,
  PCI-6225, PXI-6225, PCI-6229, PCI-6250, PCI-6251, PCIe-6251, PXIe-6251,
  PCI-6254, PCI-6259, PCIe-6259,
  PCI-6280, PCI-6281, PXI-6281, PCI-6284, PCI-6289,
  PCI-6711, PXI-6711, PCI-6713, PXI-6713,
  PXI-6071E, PCI-6070E, PXI-6070E,
  PXI-6052E, PCI-6036E, PCI-6731, PCI-6733, PXI-6733,
  PCI-6143, PXI-6143
Updated: Mon, 09 Jan 2012 14:52:48 +0000

These boards are almost identical to the AT-MIO E series, except that
they use the PCI bus instead of ISA (i.e., AT).  See the notes for
the ni_atmio.o driver for additional information about these boards.

Autocalibration is supported on many of the devices, using the
comedi_calibrate (or comedi_soft_calibrate for m-series) utility.
M-Series boards do analog input and analog output calibration entirely
in software. The software calibration corrects
the analog input for offset, gain and
nonlinearity.  The analog outputs are corrected for offset and gain.
See the comedilib documentation on comedi_get_softcal_converter() for
more information.

By default, the driver uses DMA to transfer analog input data to
memory.  When DMA is enabled, not all triggering features are
supported.

Digital I/O may not work on 673x.

Note that the PCI-6143 is a simultaineous sampling device with 8 convertors.
With this board all of the convertors perform one simultaineous sample during
a scan interval. The period for a scan is used for the convert time in a
Comedi cmd. The convert trigger source is normally set to TRIG_NOW by default.

The RTSI trigger bus is supported on these cards on
subdevice 10. See the comedilib documentation for details.

Information (number of channels, bits, etc.) for some devices may be
incorrect.  Please check this and submit a bug if there are problems
for your device.

SCXI is probably broken for m-series boards.

Bugs:
 - When DMA is enabled, COMEDI_EV_CONVERT does
   not work correctly.

*/
/*
	The PCI-MIO E series driver was originally written by
	Tomasz Motylewski <...>, and ported to comedi by ds.

	References:

	   341079b.pdf  PCI E Series Register-Level Programmer Manual
	   340934b.pdf  DAQ-STC reference manual

	   322080b.pdf  6711/6713/6715 User Manual

	   320945c.pdf  PCI E Series User Manual
	   322138a.pdf  PCI-6052E and DAQPad-6052E User Manual

	ISSUES:

	need to deal with external reference for DAC, and other DAC
	properties in board properties

	deal with at-mio-16de-10 revision D to N changes, etc.

	need to add other CALDAC type

	need to slow down DAC loading.  I don't trust NI's claim that
	two writes to the PCI bus slows IO enough.  I would prefer to
	use udelay().  Timing specs: (clock)
		AD8522		30ns
		DAC8043		120ns
		DAC8800		60ns
		MB88341		?

*/

#include <linux/delay.h>

#include "../comedidev.h"

#include <asm/byteorder.h>

#include "ni_stc.h"
#include "mite.h"

/* #define PCI_DEBUG */

#define PCIDMA

#define PCIMIO 1
#undef ATMIO

#define MAX_N_CALDACS (16+16+2)

#define DRV_NAME "ni_pcimio"

/* These are not all the possible ao ranges for 628x boards.
 They can do OFFSET +- REFERENCE where OFFSET can be
 0V, 5V, APFI<0,1>, or AO<0...3> and RANGE can
 be 10V, 5V, 2V, 1V, APFI<0,1>, AO<0...3>.  That's
 63 different possibilities.  An AO channel
 can not act as it's own OFFSET or REFERENCE.
*/
static const struct comedi_lrange range_ni_M_628x_ao = { 8, {
							     RANGE(-10, 10),
							     RANGE(-5, 5),
							     RANGE(-2, 2),
							     RANGE(-1, 1),
							     RANGE(-5, 15),
							     RANGE(0, 10),
							     RANGE(3, 7),
							     RANGE(4, 6),
							     RANGE_ext(-1, 1)
							     }
};

static const struct comedi_lrange range_ni_M_625x_ao = { 3, {
							     RANGE(-10, 10),
							     RANGE(-5, 5),
							     RANGE_ext(-1, 1)
							     }
};

enum ni_pcimio_boardid {
	BOARD_PCIMIO_16XE_50,
	BOARD_PCIMIO_16XE_10,
	BOARD_PCI6014,
	BOARD_PXI6030E,
	BOARD_PCIMIO_16E_1,
	BOARD_PCIMIO_16E_4,
	BOARD_PXI6040E,
	BOARD_PCI6031E,
	BOARD_PCI6032E,
	BOARD_PCI6033E,
	BOARD_PCI6071E,
	BOARD_PCI6023E,
	BOARD_PCI6024E,
	BOARD_PCI6025E,
	BOARD_PXI6025E,
	BOARD_PCI6034E,
	BOARD_PCI6035E,
	BOARD_PCI6052E,
	BOARD_PCI6110,
	BOARD_PCI6111,
	/* BOARD_PCI6115, */
	/* BOARD_PXI6115, */
	BOARD_PCI6711,
	BOARD_PXI6711,
	BOARD_PCI6713,
	BOARD_PXI6713,
	BOARD_PCI6731,
	/* BOARD_PXI6731, */
	BOARD_PCI6733,
	BOARD_PXI6733,
	BOARD_PXI6071E,
	BOARD_PXI6070E,
	BOARD_PXI6052E,
	BOARD_PXI6031E,
	BOARD_PCI6036E,
	BOARD_PCI6220,
	BOARD_PCI6221,
	BOARD_PCI6221_37PIN,
	BOARD_PCI6224,
	BOARD_PXI6224,
	BOARD_PCI6225,
	BOARD_PXI6225,
	BOARD_PCI6229,
	BOARD_PCI6250,
	BOARD_PCI6251,
	BOARD_PCIE6251,
	BOARD_PXIE6251,
	BOARD_PCI6254,
	BOARD_PCI6259,
	BOARD_PCIE6259,
	BOARD_PCI6280,
	BOARD_PCI6281,
	BOARD_PXI6281,
	BOARD_PCI6284,
	BOARD_PCI6289,
	BOARD_PCI6143,
	BOARD_PXI6143,
};

static const struct ni_board_struct ni_boards[] = {
	[BOARD_PCIMIO_16XE_50] = {
		.name		= "pci-mio-16xe-50",
		.n_adchan	= 16,
		.adbits		= 16,
		.ai_fifo_depth	= 2048,
		.alwaysdither	= 1,
		.gainlkup	= ai_gain_8,
		.ai_speed	= 50000,
		.n_aochan	= 2,
		.aobits		= 12,
		.ao_range_table	= &range_bipolar10,
		.ao_speed	= 50000,
		.num_p0_dio_channels = 8,
		.caldac		= { dac8800, dac8043 },
	},
	[BOARD_PCIMIO_16XE_10] = {
		.name		= "pci-mio-16xe-10",	/*  aka pci-6030E */
		.n_adchan	= 16,
		.adbits		= 16,
		.ai_fifo_depth	= 512,
		.alwaysdither	= 1,
		.gainlkup	= ai_gain_14,
		.ai_speed	= 10000,
		.n_aochan	= 2,
		.aobits		= 16,
		.ao_fifo_depth	= 2048,
		.ao_range_table	= &range_ni_E_ao_ext,
		.ao_unipolar	= 1,
		.ao_speed	= 10000,
		.num_p0_dio_channels = 8,
		.caldac		= { dac8800, dac8043, ad8522 },
	},
	[BOARD_PCI6014] = {
		.name		= "pci-6014",
		.n_adchan	= 16,
		.adbits		= 16,
		.ai_fifo_depth	= 512,
		.alwaysdither	= 1,
		.gainlkup	= ai_gain_4,
		.ai_speed	= 5000,
		.n_aochan	= 2,
		.aobits		= 16,
		.ao_range_table	= &range_bipolar10,
		.ao_speed	= 100000,
		.num_p0_dio_channels = 8,
		.caldac		= { ad8804_debug },
	},
	[BOARD_PXI6030E] = {
		.name		= "pxi-6030e",
		.n_adchan	= 16,
		.adbits		= 16,
		.ai_fifo_depth	= 512,
		.alwaysdither	= 1,
		.gainlkup	= ai_gain_14,
		.ai_speed	= 10000,
		.n_aochan	= 2,
		.aobits		= 16,
		.ao_fifo_depth	= 2048,
		.ao_range_table	= &range_ni_E_ao_ext,
		.ao_unipolar	= 1,
		.ao_speed	= 10000,
		.num_p0_dio_channels = 8,
		.caldac		= { dac8800, dac8043, ad8522 },
	},
	[BOARD_PCIMIO_16E_1] = {
		.name		= "pci-mio-16e-1",	/* aka pci-6070e */
		.n_adchan	= 16,
		.adbits		= 12,
		.ai_fifo_depth	= 512,
		.gainlkup	= ai_gain_16,
		.ai_speed	= 800,
		.n_aochan	= 2,
		.aobits		= 12,
		.ao_fifo_depth	= 2048,
		.ao_range_table	= &range_ni_E_ao_ext,
		.ao_unipolar	= 1,
		.ao_speed	= 1000,
		.num_p0_dio_channels = 8,
		.caldac		= { mb88341 },
	},
	[BOARD_PCIMIO_16E_4] = {
		.name		= "pci-mio-16e-4",	/* aka pci-6040e */
		.n_adchan	= 16,
		.adbits		= 12,
		.ai_fifo_depth	= 512,
		.gainlkup	= ai_gain_16,
		/*
		 * there have been reported problems with
		 * full speed on this board
		 */
		.ai_speed	= 2000,
		.n_aochan	= 2,
		.aobits		= 12,
		.ao_fifo_depth	= 512,
		.ao_range_table	= &range_ni_E_ao_ext,
		.ao_unipolar	= 1,
		.ao_speed	= 1000,
		.num_p0_dio_channels = 8,
		.caldac		= { ad8804_debug },	/* doc says mb88341 */
	},
	[BOARD_PXI6040E] = {
		.name		= "pxi-6040e",
		.n_adchan	= 16,
		.adbits		= 12,
		.ai_fifo_depth	= 512,
		.gainlkup	= ai_gain_16,
		.ai_speed	= 2000,
		.n_aochan	= 2,
		.aobits		= 12,
		.ao_fifo_depth	= 512,
		.ao_range_table	= &range_ni_E_ao_ext,
		.ao_unipolar	= 1,
		.ao_speed	= 1000,
		.num_p0_dio_channels = 8,
		.caldac		= { mb88341 },
	},
	[BOARD_PCI6031E] = {
		.name		= "pci-6031e",
		.n_adchan	= 64,
		.adbits		= 16,
		.ai_fifo_depth	= 512,
		.alwaysdither	= 1,
		.gainlkup	= ai_gain_14,
		.ai_speed	= 10000,
		.n_aochan	= 2,
		.aobits		= 16,
		.ao_fifo_depth	= 2048,
		.ao_range_table	= &range_ni_E_ao_ext,
		.ao_unipolar	= 1,
		.ao_speed	= 10000,
		.num_p0_dio_channels = 8,
		.caldac		= { dac8800, dac8043, ad8522 },
	},
	[BOARD_PCI6032E] = {
		.name		= "pci-6032e",
		.n_adchan	= 16,
		.adbits		= 16,
		.ai_fifo_depth	= 512,
		.alwaysdither	= 1,
		.gainlkup	= ai_gain_14,
		.ai_speed	= 10000,
		.num_p0_dio_channels = 8,
		.caldac		= { dac8800, dac8043, ad8522 },
	},
	[BOARD_PCI6033E] = {
		.name		= "pci-6033e",
		.n_adchan	= 64,
		.adbits		= 16,
		.ai_fifo_depth	= 512,
		.alwaysdither	= 1,
		.gainlkup	= ai_gain_14,
		.ai_speed	= 10000,
		.num_p0_dio_channels = 8,
		.caldac		= { dac8800, dac8043, ad8522 },
	},
	[BOARD_PCI6071E] = {
		.name		= "pci-6071e",
		.n_adchan	= 64,
		.adbits		= 12,
		.ai_fifo_depth	= 512,
		.alwaysdither	= 1,
		.gainlkup	= ai_gain_16,
		.ai_speed	= 800,
		.n_aochan	= 2,
		.aobits		= 12,
		.ao_fifo_depth	= 2048,
		.ao_range_table	= &range_ni_E_ao_ext,
		.ao_unipolar	= 1,
		.ao_speed	= 1000,
		.num_p0_dio_channels = 8,
		.caldac		= { ad8804_debug },
	},
	[BOARD_PCI6023E] = {
		.name		= "pci-6023e",
		.n_adchan	= 16,
		.adbits		= 12,
		.ai_fifo_depth	= 512,
		.gainlkup	= ai_gain_4,
		.ai_speed	= 5000,
		.num_p0_dio_channels = 8,
		.caldac		= { ad8804_debug },	/* manual is wrong */
	},
	[BOARD_PCI6024E] = {
		.name		= "pci-6024e",
		.n_adchan	= 16,
		.adbits		= 12,
		.ai_fifo_depth	= 512,
		.gainlkup	= ai_gain_4,
		.ai_speed	= 5000,
		.n_aochan	= 2,
		.aobits		= 12,
		.ao_range_table	= &range_bipolar10,
		.ao_speed	= 100000,
		.num_p0_dio_channels = 8,
		.caldac		= { ad8804_debug },	/* manual is wrong */
	},
	[BOARD_PCI6025E] = {
		.name		= "pci-6025e",
		.n_adchan	= 16,
		.adbits		= 12,
		.ai_fifo_depth	= 512,
		.gainlkup	= ai_gain_4,
		.ai_speed	= 5000,
		.n_aochan	= 2,
		.aobits		= 12,
		.ao_range_table	= &range_bipolar10,
		.ao_speed	= 100000,
		.num_p0_dio_channels = 8,
		.caldac		= { ad8804_debug },	/* manual is wrong */
		.has_8255	= 1,
	},
	[BOARD_PXI6025E] = {
		.name		= "pxi-6025e",
		.n_adchan	= 16,
		.adbits		= 12,
		.ai_fifo_depth	= 512,
		.gainlkup	= ai_gain_4,
		.ai_speed	= 5000,
		.n_aochan	= 2,
		.aobits		= 12,
		.ao_range_table	= &range_ni_E_ao_ext,
		.ao_unipolar	= 1,
		.ao_speed	= 100000,
		.num_p0_dio_channels = 8,
		.caldac		= { ad8804_debug },	/* manual is wrong */
		.has_8255	= 1,
	},
	[BOARD_PCI6034E] = {
		.name		= "pci-6034e",
		.n_adchan	= 16,
		.adbits		= 16,
		.ai_fifo_depth	= 512,
		.alwaysdither	= 1,
		.gainlkup	= ai_gain_4,
		.ai_speed	= 5000,
		.num_p0_dio_channels = 8,
		.caldac		= { ad8804_debug },
	},
	[BOARD_PCI6035E] = {
		.name		= "pci-6035e",
		.n_adchan	= 16,
		.adbits		= 16,
		.ai_fifo_depth	= 512,
		.alwaysdither	= 1,
		.gainlkup	= ai_gain_4,
		.ai_speed	= 5000,
		.n_aochan	= 2,
		.aobits		= 12,
		.ao_range_table	= &range_bipolar10,
		.ao_speed	= 100000,
		.num_p0_dio_channels = 8,
		.caldac		= { ad8804_debug },
	},
	[BOARD_PCI6052E] = {
		.name		= "pci-6052e",
		.n_adchan	= 16,
		.adbits		= 16,
		.ai_fifo_depth	= 512,
		.alwaysdither	= 1,
		.gainlkup	= ai_gain_16,
		.ai_speed	= 3000,
		.n_aochan	= 2,
		.aobits		= 16,
		.ao_unipolar	= 1,
		.ao_fifo_depth	= 2048,
		.ao_range_table	= &range_ni_E_ao_ext,
		.ao_speed	= 3000,
		.num_p0_dio_channels = 8,
		/* manual is wrong */
		.caldac		= { ad8804_debug, ad8804_debug, ad8522 },
	},
	[BOARD_PCI6110] = {
		.name		= "pci-6110",
		.n_adchan	= 4,
		.adbits		= 12,
		.ai_fifo_depth	= 8192,
		.alwaysdither	= 0,
		.gainlkup	= ai_gain_611x,
		.ai_speed	= 200,
		.n_aochan	= 2,
		.aobits		= 16,
		.reg_type	= ni_reg_611x,
		.ao_range_table	= &range_bipolar10,
		.ao_fifo_depth	= 2048,
		.ao_speed	= 250,
		.num_p0_dio_channels = 8,
		.caldac		= { ad8804, ad8804 },
	},
	[BOARD_PCI6111] = {
		.name		= "pci-6111",
		.n_adchan	= 2,
		.adbits		= 12,
		.ai_fifo_depth	= 8192,
		.gainlkup	= ai_gain_611x,
		.ai_speed	= 200,
		.n_aochan	= 2,
		.aobits		= 16,
		.reg_type	= ni_reg_611x,
		.ao_range_table	= &range_bipolar10,
		.ao_fifo_depth	= 2048,
		.ao_speed	= 250,
		.num_p0_dio_channels = 8,
		.caldac		= { ad8804, ad8804 },
	},
#if 0
	/* The 6115 boards probably need their own driver */
	[BOARD_PCI6115] = {	/* .device_id = 0x2ed0, */
		.name		= "pci-6115",
		.n_adchan	= 4,
		.adbits		= 12,
		.ai_fifo_depth	= 8192,
		.gainlkup	= ai_gain_611x,
		.ai_speed	= 100,
		.n_aochan	= 2,
		.aobits		= 16,
		.ao_671x	= 1,
		.ao_fifo_depth	= 2048,
		.ao_speed	= 250,
		.num_p0_dio_channels = 8,
		.reg_611x	= 1,
		/* XXX */
		.caldac		= { ad8804_debug, ad8804_debug, ad8804_debug },
	},
#endif
#if 0
	[BOARD_PXI6115] = {	/* .device_id = ????, */
		.name		= "pxi-6115",
		.n_adchan	= 4,
		.adbits		= 12,
		.ai_fifo_depth	= 8192,
		.gainlkup	= ai_gain_611x,
		.ai_speed	= 100,
		.n_aochan	= 2,
		.aobits		= 16,
		.ao_671x	= 1,
		.ao_fifo_depth	= 2048,
		.ao_speed	= 250,
		.reg_611x	= 1,
		.num_p0_dio_channels = 8,
		/* XXX */
		.caldac		= { ad8804_debug, ad8804_debug, ad8804_debug },
	},
#endif
	[BOARD_PCI6711] = {
		.name = "pci-6711",
		.n_aochan	= 4,
		.aobits		= 12,
		/* data sheet says 8192, but fifo really holds 16384 samples */
		.ao_fifo_depth	= 16384,
		.ao_range_table	= &range_bipolar10,
		.ao_speed	= 1000,
		.num_p0_dio_channels = 8,
		.reg_type	= ni_reg_6711,
		.caldac		= { ad8804_debug },
	},
	[BOARD_PXI6711] = {
		.name		= "pxi-6711",
		.n_aochan	= 4,
		.aobits		= 12,
		.ao_fifo_depth	= 16384,
		.ao_range_table	= &range_bipolar10,
		.ao_speed	= 1000,
		.num_p0_dio_channels = 8,
		.reg_type	= ni_reg_6711,
		.caldac		= { ad8804_debug },
	},
	[BOARD_PCI6713] = {
		.name		= "pci-6713",
		.n_aochan	= 8,
		.aobits		= 12,
		.ao_fifo_depth	= 16384,
		.ao_range_table	= &range_bipolar10,
		.ao_speed	= 1000,
		.num_p0_dio_channels = 8,
		.reg_type	= ni_reg_6713,
		.caldac		= { ad8804_debug, ad8804_debug },
	},
	[BOARD_PXI6713] = {
		.name		= "pxi-6713",
		.n_aochan	= 8,
		.aobits		= 12,
		.ao_fifo_depth	= 16384,
		.ao_range_table	= &range_bipolar10,
		.ao_speed	= 1000,
		.num_p0_dio_channels = 8,
		.reg_type	= ni_reg_6713,
		.caldac		= { ad8804_debug, ad8804_debug },
	},
	[BOARD_PCI6731] = {
		.name		= "pci-6731",
		.n_aochan	= 4,
		.aobits		= 16,
		.ao_fifo_depth	= 8192,
		.ao_range_table	= &range_bipolar10,
		.ao_speed	= 1000,
		.num_p0_dio_channels = 8,
		.reg_type	= ni_reg_6711,
		.caldac		= { ad8804_debug },
	},
#if 0
	[BOARD_PXI6731] = {	/* .device_id = ????, */
		.name		= "pxi-6731",
		.n_aochan	= 4,
		.aobits		= 16,
		.ao_fifo_depth	= 8192,
		.ao_range_table	= &range_bipolar10,
		.num_p0_dio_channels = 8,
		.reg_type	= ni_reg_6711,
		.caldac		= { ad8804_debug },
	},
#endif
	[BOARD_PCI6733] = {
		.name		= "pci-6733",
		.n_aochan	= 8,
		.aobits		= 16,
		.ao_fifo_depth	= 16384,
		.ao_range_table	= &range_bipolar10,
		.ao_speed	= 1000,
		.num_p0_dio_channels = 8,
		.reg_type	= ni_reg_6713,
		.caldac		= { ad8804_debug, ad8804_debug },
	},
	[BOARD_PXI6733] = {
		.name		= "pxi-6733",
		.n_aochan	= 8,
		.aobits		= 16,
		.ao_fifo_depth	= 16384,
		.ao_range_table	= &range_bipolar10,
		.ao_speed	= 1000,
		.num_p0_dio_channels = 8,
		.reg_type	= ni_reg_6713,
		.caldac		= { ad8804_debug, ad8804_debug },
	},
	[BOARD_PXI6071E] = {
		.name		= "pxi-6071e",
		.n_adchan	= 64,
		.adbits		= 12,
		.ai_fifo_depth	= 512,
		.alwaysdither	= 1,
		.gainlkup	= ai_gain_16,
		.ai_speed	= 800,
		.n_aochan	= 2,
		.aobits		= 12,
		.ao_fifo_depth	= 2048,
		.ao_range_table	= &range_ni_E_ao_ext,
		.ao_unipolar	= 1,
		.ao_speed	= 1000,
		.num_p0_dio_channels = 8,
		.caldac		= { ad8804_debug },
	},
	[BOARD_PXI6070E] = {
		.name		= "pxi-6070e",
		.n_adchan	= 16,
		.adbits		= 12,
		.ai_fifo_depth	= 512,
		.alwaysdither	= 1,
		.gainlkup	= ai_gain_16,
		.ai_speed	= 800,
		.n_aochan	= 2,
		.aobits		= 12,
		.ao_fifo_depth	= 2048,
		.ao_range_table	= &range_ni_E_ao_ext,
		.ao_unipolar	= 1,
		.ao_speed	= 1000,
		.num_p0_dio_channels = 8,
		.caldac		= { ad8804_debug },
	},
	[BOARD_PXI6052E] = {
		.name		= "pxi-6052e",
		.n_adchan	= 16,
		.adbits		= 16,
		.ai_fifo_depth	= 512,
		.alwaysdither	= 1,
		.gainlkup	= ai_gain_16,
		.ai_speed	= 3000,
		.n_aochan	= 2,
		.aobits		= 16,
		.ao_unipolar	= 1,
		.ao_fifo_depth	= 2048,
		.ao_range_table	= &range_ni_E_ao_ext,
		.ao_speed	= 3000,
		.num_p0_dio_channels = 8,
		.caldac		= { mb88341, mb88341, ad8522 },
	},
	[BOARD_PXI6031E] = {
		.name		= "pxi-6031e",
		.n_adchan	= 64,
		.adbits		= 16,
		.ai_fifo_depth	= 512,
		.alwaysdither	= 1,
		.gainlkup	= ai_gain_14,
		.ai_speed	= 10000,
		.n_aochan	= 2,
		.aobits		= 16,
		.ao_fifo_depth	= 2048,
		.ao_range_table	= &range_ni_E_ao_ext,
		.ao_unipolar	= 1,
		.ao_speed	= 10000,
		.num_p0_dio_channels = 8,
		.caldac		= { dac8800, dac8043, ad8522 },
	},
	[BOARD_PCI6036E] = {
		.name = "pci-6036e",
		.n_adchan	= 16,
		.adbits		= 16,
		.ai_fifo_depth	= 512,
		.alwaysdither	= 1,
		.gainlkup	= ai_gain_4,
		.ai_speed	= 5000,
		.n_aochan	= 2,
		.aobits		= 16,
		.ao_range_table	= &range_bipolar10,
		.ao_speed	= 100000,
		.num_p0_dio_channels = 8,
		.caldac		= { ad8804_debug },
	},
	[BOARD_PCI6220] = {
		.name		= "pci-6220",
		.n_adchan	= 16,
		.adbits		= 16,
		.ai_fifo_depth	= 512,		/* FIXME: guess */
		.gainlkup	= ai_gain_622x,
		.ai_speed	= 4000,
		.num_p0_dio_channels = 8,
		.reg_type	= ni_reg_622x,
		.caldac		= { caldac_none },
	},
	[BOARD_PCI6221] = {
		.name		= "pci-6221",
		.n_adchan	= 16,
		.adbits		= 16,
		.ai_fifo_depth	= 4095,
		.gainlkup	= ai_gain_622x,
		.ai_speed	= 4000,
		.n_aochan	= 2,
		.aobits		= 16,
		.ao_fifo_depth	= 8191,
		.ao_range_table	= &range_bipolar10,
		.reg_type	= ni_reg_622x,
		.ao_speed	= 1200,
		.num_p0_dio_channels = 8,
		.caldac		= { caldac_none },
	},
	[BOARD_PCI6221_37PIN] = {
		.name		= "pci-6221_37pin",
		.n_adchan	= 16,
		.adbits		= 16,
		.ai_fifo_depth	= 4095,
		.gainlkup	= ai_gain_622x,
		.ai_speed	= 4000,
		.n_aochan	= 2,
		.aobits		= 16,
		.ao_fifo_depth	= 8191,
		.ao_range_table	= &range_bipolar10,
		.reg_type	= ni_reg_622x,
		.ao_speed	= 1200,
		.num_p0_dio_channels = 8,
		.caldac		= { caldac_none },
	},
	[BOARD_PCI6224] = {
		.name		= "pci-6224",
		.n_adchan	= 32,
		.adbits		= 16,
		.ai_fifo_depth	= 4095,
		.gainlkup	= ai_gain_622x,
		.ai_speed	= 4000,
		.reg_type	= ni_reg_622x,
		.num_p0_dio_channels = 32,
		.caldac		= { caldac_none },
	},
	[BOARD_PXI6224] = {
		.name		= "pxi-6224",
		.n_adchan	= 32,
		.adbits		= 16,
		.ai_fifo_depth	= 4095,
		.gainlkup	= ai_gain_622x,
		.ai_speed	= 4000,
		.reg_type	= ni_reg_622x,
		.num_p0_dio_channels = 32,
		.caldac		= { caldac_none },
	},
	[BOARD_PCI6225] = {
		.name		= "pci-6225",
		.n_adchan	= 80,
		.adbits		= 16,
		.ai_fifo_depth	= 4095,
		.gainlkup	= ai_gain_622x,
		.ai_speed	= 4000,
		.n_aochan	= 2,
		.aobits		= 16,
		.ao_fifo_depth	= 8191,
		.ao_range_table	= &range_bipolar10,
		.reg_type	= ni_reg_622x,
		.ao_speed	= 1200,
		.num_p0_dio_channels = 32,
		.caldac		= { caldac_none },
	},
	[BOARD_PXI6225] = {
		.name		= "pxi-6225",
		.n_adchan	= 80,
		.adbits		= 16,
		.ai_fifo_depth	= 4095,
		.gainlkup	= ai_gain_622x,
		.ai_speed	= 4000,
		.n_aochan	= 2,
		.aobits		= 16,
		.ao_fifo_depth	= 8191,
		.ao_range_table	= &range_bipolar10,
		.reg_type	= ni_reg_622x,
		.ao_speed	= 1200,
		.num_p0_dio_channels = 32,
		.caldac		= { caldac_none },
	},
	[BOARD_PCI6229] = {
		.name		= "pci-6229",
		.n_adchan	= 32,
		.adbits		= 16,
		.ai_fifo_depth	= 4095,
		.gainlkup	= ai_gain_622x,
		.ai_speed	= 4000,
		.n_aochan	= 4,
		.aobits		= 16,
		.ao_fifo_depth	= 8191,
		.ao_range_table	= &range_bipolar10,
		.reg_type	= ni_reg_622x,
		.ao_speed	= 1200,
		.num_p0_dio_channels = 32,
		.caldac		= { caldac_none },
	},
	[BOARD_PCI6250] = {
		.name		= "pci-6250",
		.n_adchan	= 16,
		.adbits		= 16,
		.ai_fifo_depth	= 4095,
		.gainlkup	= ai_gain_628x,
		.ai_speed	= 800,
		.reg_type	= ni_reg_625x,
		.num_p0_dio_channels = 8,
		.caldac		= { caldac_none },
	},
	[BOARD_PCI6251] = {
		.name		= "pci-6251",
		.n_adchan	= 16,
		.adbits		= 16,
		.ai_fifo_depth	= 4095,
		.gainlkup	= ai_gain_628x,
		.ai_speed	= 800,
		.n_aochan	= 2,
		.aobits		= 16,
		.ao_fifo_depth	= 8191,
		.ao_range_table	= &range_ni_M_625x_ao,
		.reg_type	= ni_reg_625x,
		.ao_speed	= 350,
		.num_p0_dio_channels = 8,
		.caldac		= { caldac_none },
	},
	[BOARD_PCIE6251] = {
		.name		= "pcie-6251",
		.n_adchan	= 16,
		.adbits		= 16,
		.ai_fifo_depth	= 4095,
		.gainlkup	= ai_gain_628x,
		.ai_speed	= 800,
		.n_aochan	= 2,
		.aobits		= 16,
		.ao_fifo_depth	= 8191,
		.ao_range_table	= &range_ni_M_625x_ao,
		.reg_type	= ni_reg_625x,
		.ao_speed	= 350,
		.num_p0_dio_channels = 8,
		.caldac		= { caldac_none },
	},
	[BOARD_PXIE6251] = {
		.name		= "pxie-6251",
		.n_adchan	= 16,
		.adbits		= 16,
		.ai_fifo_depth	= 4095,
		.gainlkup	= ai_gain_628x,
		.ai_speed	= 800,
		.n_aochan	= 2,
		.aobits		= 16,
		.ao_fifo_depth	= 8191,
		.ao_range_table	= &range_ni_M_625x_ao,
		.reg_type	= ni_reg_625x,
		.ao_speed	= 350,
		.num_p0_dio_channels = 8,
		.caldac		= { caldac_none },
	},
	[BOARD_PCI6254] = {
		.name		= "pci-6254",
		.n_adchan	= 32,
		.adbits		= 16,
		.ai_fifo_depth	= 4095,
		.gainlkup	= ai_gain_628x,
		.ai_speed	= 800,
		.reg_type	= ni_reg_625x,
		.num_p0_dio_channels = 32,
		.caldac		= { caldac_none },
	},
	[BOARD_PCI6259] = {
		.name		= "pci-6259",
		.n_adchan	= 32,
		.adbits		= 16,
		.ai_fifo_depth	= 4095,
		.gainlkup	= ai_gain_628x,
		.ai_speed	= 800,
		.n_aochan	= 4,
		.aobits		= 16,
		.ao_fifo_depth	= 8191,
		.ao_range_table	= &range_ni_M_625x_ao,
		.reg_type	= ni_reg_625x,
		.ao_speed	= 350,
		.num_p0_dio_channels = 32,
		.caldac		= { caldac_none },
	},
	[BOARD_PCIE6259] = {
		.name		= "pcie-6259",
		.n_adchan	= 32,
		.adbits		= 16,
		.ai_fifo_depth	= 4095,
		.gainlkup	= ai_gain_628x,
		.ai_speed	= 800,
		.n_aochan	= 4,
		.aobits		= 16,
		.ao_fifo_depth	= 8191,
		.ao_range_table	= &range_ni_M_625x_ao,
		.reg_type	= ni_reg_625x,
		.ao_speed	= 350,
		.num_p0_dio_channels = 32,
		.caldac		= { caldac_none },
	},
	[BOARD_PCI6280] = {
		.name		= "pci-6280",
		.n_adchan	= 16,
		.adbits		= 18,
		.ai_fifo_depth	= 2047,
		.gainlkup	= ai_gain_628x,
		.ai_speed	= 1600,
		.ao_fifo_depth	= 8191,
		.reg_type	= ni_reg_628x,
		.num_p0_dio_channels = 8,
		.caldac		= { caldac_none },
	},
	[BOARD_PCI6281] = {
		.name		= "pci-6281",
		.n_adchan	= 16,
		.adbits		= 18,
		.ai_fifo_depth	= 2047,
		.gainlkup	= ai_gain_628x,
		.ai_speed	= 1600,
		.n_aochan	= 2,
		.aobits		= 16,
		.ao_fifo_depth	= 8191,
		.ao_range_table = &range_ni_M_628x_ao,
		.reg_type	= ni_reg_628x,
		.ao_unipolar	= 1,
		.ao_speed	= 350,
		.num_p0_dio_channels = 8,
		.caldac		= { caldac_none },
	},
	[BOARD_PXI6281] = {
		.name		= "pxi-6281",
		.n_adchan	= 16,
		.adbits		= 18,
		.ai_fifo_depth	= 2047,
		.gainlkup	= ai_gain_628x,
		.ai_speed	= 1600,
		.n_aochan	= 2,
		.aobits		= 16,
		.ao_fifo_depth	= 8191,
		.ao_range_table	= &range_ni_M_628x_ao,
		.reg_type	= ni_reg_628x,
		.ao_unipolar	= 1,
		.ao_speed	= 350,
		.num_p0_dio_channels = 8,
		.caldac		= { caldac_none },
	},
	[BOARD_PCI6284] = {
		.name		= "pci-6284",
		.n_adchan	= 32,
		.adbits		= 18,
		.ai_fifo_depth	= 2047,
		.gainlkup	= ai_gain_628x,
		.ai_speed	= 1600,
		.reg_type	= ni_reg_628x,
		.num_p0_dio_channels = 32,
		.caldac		= { caldac_none },
	},
	[BOARD_PCI6289] = {
		.name		= "pci-6289",
		.n_adchan	= 32,
		.adbits		= 18,
		.ai_fifo_depth	= 2047,
		.gainlkup	= ai_gain_628x,
		.ai_speed	= 1600,
		.n_aochan	= 4,
		.aobits		= 16,
		.ao_fifo_depth	= 8191,
		.ao_range_table	= &range_ni_M_628x_ao,
		.reg_type	= ni_reg_628x,
		.ao_unipolar	= 1,
		.ao_speed	= 350,
		.num_p0_dio_channels = 32,
		.caldac		= { caldac_none },
	},
	[BOARD_PCI6143] = {
		.name		= "pci-6143",
		.n_adchan	= 8,
		.adbits		= 16,
		.ai_fifo_depth	= 1024,
		.gainlkup	= ai_gain_6143,
		.ai_speed	= 4000,
		.reg_type	= ni_reg_6143,
		.num_p0_dio_channels = 8,
		.caldac		= { ad8804_debug, ad8804_debug },
	},
	[BOARD_PXI6143] = {
		.name		= "pxi-6143",
		.n_adchan	= 8,
		.adbits		= 16,
		.ai_fifo_depth	= 1024,
		.gainlkup	= ai_gain_6143,
		.ai_speed	= 4000,
		.reg_type	= ni_reg_6143,
		.num_p0_dio_channels = 8,
		.caldac		= { ad8804_debug, ad8804_debug },
	},
};

struct ni_private {
NI_PRIVATE_COMMON};

/* How we access registers */

#define ni_writel(a, b)	(writel((a), devpriv->mite->daq_io_addr + (b)))
#define ni_readl(a)	(readl(devpriv->mite->daq_io_addr + (a)))
#define ni_writew(a, b)	(writew((a), devpriv->mite->daq_io_addr + (b)))
#define ni_readw(a)	(readw(devpriv->mite->daq_io_addr + (a)))
#define ni_writeb(a, b)	(writeb((a), devpriv->mite->daq_io_addr + (b)))
#define ni_readb(a)	(readb(devpriv->mite->daq_io_addr + (a)))

/* How we access STC registers */

/* We automatically take advantage of STC registers that can be
 * read/written directly in the I/O space of the board.  Most
 * PCIMIO devices map the low 8 STC registers to iobase+addr*2.
 * The 611x devices map the write registers to iobase+addr*2, and
 * the read registers to iobase+(addr-1)*2. */
/* However, the 611x boards still aren't working, so I'm disabling
 * non-windowed STC access temporarily */

static void e_series_win_out(struct comedi_device *dev, uint16_t data, int reg)
{
	struct ni_private *devpriv = dev->private;
	unsigned long flags;

	spin_lock_irqsave(&devpriv->window_lock, flags);
	ni_writew(reg, Window_Address);
	ni_writew(data, Window_Data);
	spin_unlock_irqrestore(&devpriv->window_lock, flags);
}

static uint16_t e_series_win_in(struct comedi_device *dev, int reg)
{
	struct ni_private *devpriv = dev->private;
	unsigned long flags;
	uint16_t ret;

	spin_lock_irqsave(&devpriv->window_lock, flags);
	ni_writew(reg, Window_Address);
	ret = ni_readw(Window_Data);
	spin_unlock_irqrestore(&devpriv->window_lock, flags);

	return ret;
}

static void m_series_stc_writew(struct comedi_device *dev, uint16_t data,
				int reg)
{
	struct ni_private *devpriv = dev->private;
	unsigned offset;

	switch (reg) {
	case ADC_FIFO_Clear:
		offset = M_Offset_AI_FIFO_Clear;
		break;
	case AI_Command_1_Register:
		offset = M_Offset_AI_Command_1;
		break;
	case AI_Command_2_Register:
		offset = M_Offset_AI_Command_2;
		break;
	case AI_Mode_1_Register:
		offset = M_Offset_AI_Mode_1;
		break;
	case AI_Mode_2_Register:
		offset = M_Offset_AI_Mode_2;
		break;
	case AI_Mode_3_Register:
		offset = M_Offset_AI_Mode_3;
		break;
	case AI_Output_Control_Register:
		offset = M_Offset_AI_Output_Control;
		break;
	case AI_Personal_Register:
		offset = M_Offset_AI_Personal;
		break;
	case AI_SI2_Load_A_Register:
		/*  this is actually a 32 bit register on m series boards */
		ni_writel(data, M_Offset_AI_SI2_Load_A);
		return;
		break;
	case AI_SI2_Load_B_Register:
		/*  this is actually a 32 bit register on m series boards */
		ni_writel(data, M_Offset_AI_SI2_Load_B);
		return;
		break;
	case AI_START_STOP_Select_Register:
		offset = M_Offset_AI_START_STOP_Select;
		break;
	case AI_Trigger_Select_Register:
		offset = M_Offset_AI_Trigger_Select;
		break;
	case Analog_Trigger_Etc_Register:
		offset = M_Offset_Analog_Trigger_Etc;
		break;
	case AO_Command_1_Register:
		offset = M_Offset_AO_Command_1;
		break;
	case AO_Command_2_Register:
		offset = M_Offset_AO_Command_2;
		break;
	case AO_Mode_1_Register:
		offset = M_Offset_AO_Mode_1;
		break;
	case AO_Mode_2_Register:
		offset = M_Offset_AO_Mode_2;
		break;
	case AO_Mode_3_Register:
		offset = M_Offset_AO_Mode_3;
		break;
	case AO_Output_Control_Register:
		offset = M_Offset_AO_Output_Control;
		break;
	case AO_Personal_Register:
		offset = M_Offset_AO_Personal;
		break;
	case AO_Start_Select_Register:
		offset = M_Offset_AO_Start_Select;
		break;
	case AO_Trigger_Select_Register:
		offset = M_Offset_AO_Trigger_Select;
		break;
	case Clock_and_FOUT_Register:
		offset = M_Offset_Clock_and_FOUT;
		break;
	case Configuration_Memory_Clear:
		offset = M_Offset_Configuration_Memory_Clear;
		break;
	case DAC_FIFO_Clear:
		offset = M_Offset_AO_FIFO_Clear;
		break;
	case DIO_Control_Register:
		printk
		    ("%s: FIXME: register 0x%x does not map cleanly on to m-series boards.\n",
		     __func__, reg);
		return;
		break;
	case G_Autoincrement_Register(0):
		offset = M_Offset_G0_Autoincrement;
		break;
	case G_Autoincrement_Register(1):
		offset = M_Offset_G1_Autoincrement;
		break;
	case G_Command_Register(0):
		offset = M_Offset_G0_Command;
		break;
	case G_Command_Register(1):
		offset = M_Offset_G1_Command;
		break;
	case G_Input_Select_Register(0):
		offset = M_Offset_G0_Input_Select;
		break;
	case G_Input_Select_Register(1):
		offset = M_Offset_G1_Input_Select;
		break;
	case G_Mode_Register(0):
		offset = M_Offset_G0_Mode;
		break;
	case G_Mode_Register(1):
		offset = M_Offset_G1_Mode;
		break;
	case Interrupt_A_Ack_Register:
		offset = M_Offset_Interrupt_A_Ack;
		break;
	case Interrupt_A_Enable_Register:
		offset = M_Offset_Interrupt_A_Enable;
		break;
	case Interrupt_B_Ack_Register:
		offset = M_Offset_Interrupt_B_Ack;
		break;
	case Interrupt_B_Enable_Register:
		offset = M_Offset_Interrupt_B_Enable;
		break;
	case Interrupt_Control_Register:
		offset = M_Offset_Interrupt_Control;
		break;
	case IO_Bidirection_Pin_Register:
		offset = M_Offset_IO_Bidirection_Pin;
		break;
	case Joint_Reset_Register:
		offset = M_Offset_Joint_Reset;
		break;
	case RTSI_Trig_A_Output_Register:
		offset = M_Offset_RTSI_Trig_A_Output;
		break;
	case RTSI_Trig_B_Output_Register:
		offset = M_Offset_RTSI_Trig_B_Output;
		break;
	case RTSI_Trig_Direction_Register:
		offset = M_Offset_RTSI_Trig_Direction;
		break;
		/* FIXME: DIO_Output_Register (16 bit reg) is replaced by M_Offset_Static_Digital_Output (32 bit)
		   and M_Offset_SCXI_Serial_Data_Out (8 bit) */
	default:
		dev_warn(dev->class_dev,
			 "%s: bug! unhandled register=0x%x in switch.\n",
			 __func__, reg);
		BUG();
		return;
		break;
	}
	ni_writew(data, offset);
}

static uint16_t m_series_stc_readw(struct comedi_device *dev, int reg)
{
	struct ni_private *devpriv = dev->private;
	unsigned offset;

	switch (reg) {
	case AI_Status_1_Register:
		offset = M_Offset_AI_Status_1;
		break;
	case AO_Status_1_Register:
		offset = M_Offset_AO_Status_1;
		break;
	case AO_Status_2_Register:
		offset = M_Offset_AO_Status_2;
		break;
	case DIO_Serial_Input_Register:
		return ni_readb(M_Offset_SCXI_Serial_Data_In);
		break;
	case Joint_Status_1_Register:
		offset = M_Offset_Joint_Status_1;
		break;
	case Joint_Status_2_Register:
		offset = M_Offset_Joint_Status_2;
		break;
	case G_Status_Register:
		offset = M_Offset_G01_Status;
		break;
	default:
		dev_warn(dev->class_dev,
			 "%s: bug! unhandled register=0x%x in switch.\n",
			 __func__, reg);
		BUG();
		return 0;
		break;
	}
	return ni_readw(offset);
}

static void m_series_stc_writel(struct comedi_device *dev, uint32_t data,
				int reg)
{
	struct ni_private *devpriv = dev->private;
	unsigned offset;

	switch (reg) {
	case AI_SC_Load_A_Registers:
		offset = M_Offset_AI_SC_Load_A;
		break;
	case AI_SI_Load_A_Registers:
		offset = M_Offset_AI_SI_Load_A;
		break;
	case AO_BC_Load_A_Register:
		offset = M_Offset_AO_BC_Load_A;
		break;
	case AO_UC_Load_A_Register:
		offset = M_Offset_AO_UC_Load_A;
		break;
	case AO_UI_Load_A_Register:
		offset = M_Offset_AO_UI_Load_A;
		break;
	case G_Load_A_Register(0):
		offset = M_Offset_G0_Load_A;
		break;
	case G_Load_A_Register(1):
		offset = M_Offset_G1_Load_A;
		break;
	case G_Load_B_Register(0):
		offset = M_Offset_G0_Load_B;
		break;
	case G_Load_B_Register(1):
		offset = M_Offset_G1_Load_B;
		break;
	default:
		dev_warn(dev->class_dev,
			 "%s: bug! unhandled register=0x%x in switch.\n",
			 __func__, reg);
		BUG();
		return;
		break;
	}
	ni_writel(data, offset);
}

static uint32_t m_series_stc_readl(struct comedi_device *dev, int reg)
{
	struct ni_private *devpriv = dev->private;
	unsigned offset;

	switch (reg) {
	case G_HW_Save_Register(0):
		offset = M_Offset_G0_HW_Save;
		break;
	case G_HW_Save_Register(1):
		offset = M_Offset_G1_HW_Save;
		break;
	case G_Save_Register(0):
		offset = M_Offset_G0_Save;
		break;
	case G_Save_Register(1):
		offset = M_Offset_G1_Save;
		break;
	default:
		dev_warn(dev->class_dev,
			 "%s: bug! unhandled register=0x%x in switch.\n",
			 __func__, reg);
		BUG();
		return 0;
		break;
	}
	return ni_readl(offset);
}

#define interrupt_pin(a)	0
#define IRQ_POLARITY 1

#define NI_E_IRQ_FLAGS		IRQF_SHARED

#include "ni_mio_common.c"

static int pcimio_ai_change(struct comedi_device *dev,
			    struct comedi_subdevice *s, unsigned long new_size);
static int pcimio_ao_change(struct comedi_device *dev,
			    struct comedi_subdevice *s, unsigned long new_size);
static int pcimio_gpct0_change(struct comedi_device *dev,
			       struct comedi_subdevice *s,
			       unsigned long new_size);
static int pcimio_gpct1_change(struct comedi_device *dev,
			       struct comedi_subdevice *s,
			       unsigned long new_size);
static int pcimio_dio_change(struct comedi_device *dev,
			     struct comedi_subdevice *s,
			     unsigned long new_size);

static void m_series_init_eeprom_buffer(struct comedi_device *dev)
{
	struct ni_private *devpriv = dev->private;
	static const int Start_Cal_EEPROM = 0x400;
	static const unsigned window_size = 10;
	static const int serial_number_eeprom_offset = 0x4;
	static const int serial_number_eeprom_length = 0x4;
	unsigned old_iodwbsr_bits;
	unsigned old_iodwbsr1_bits;
	unsigned old_iodwcr1_bits;
	int i;

	old_iodwbsr_bits = readl(devpriv->mite->mite_io_addr + MITE_IODWBSR);
	old_iodwbsr1_bits = readl(devpriv->mite->mite_io_addr + MITE_IODWBSR_1);
	old_iodwcr1_bits = readl(devpriv->mite->mite_io_addr + MITE_IODWCR_1);
	writel(0x0, devpriv->mite->mite_io_addr + MITE_IODWBSR);
	writel(((0x80 | window_size) | devpriv->mite->daq_phys_addr),
	       devpriv->mite->mite_io_addr + MITE_IODWBSR_1);
	writel(0x1 | old_iodwcr1_bits,
	       devpriv->mite->mite_io_addr + MITE_IODWCR_1);
	writel(0xf, devpriv->mite->mite_io_addr + 0x30);

	BUG_ON(serial_number_eeprom_length > sizeof(devpriv->serial_number));
	for (i = 0; i < serial_number_eeprom_length; ++i) {
		char *byte_ptr = (char *)&devpriv->serial_number + i;
		*byte_ptr = ni_readb(serial_number_eeprom_offset + i);
	}
	devpriv->serial_number = be32_to_cpu(devpriv->serial_number);

	for (i = 0; i < M_SERIES_EEPROM_SIZE; ++i)
		devpriv->eeprom_buffer[i] = ni_readb(Start_Cal_EEPROM + i);

	writel(old_iodwbsr1_bits, devpriv->mite->mite_io_addr + MITE_IODWBSR_1);
	writel(old_iodwbsr_bits, devpriv->mite->mite_io_addr + MITE_IODWBSR);
	writel(old_iodwcr1_bits, devpriv->mite->mite_io_addr + MITE_IODWCR_1);
	writel(0x0, devpriv->mite->mite_io_addr + 0x30);
}

static void init_6143(struct comedi_device *dev)
{
	const struct ni_board_struct *board = comedi_board(dev);
	struct ni_private *devpriv = dev->private;

	/*  Disable interrupts */
	devpriv->stc_writew(dev, 0, Interrupt_Control_Register);

	/*  Initialise 6143 AI specific bits */
	ni_writeb(0x00, Magic_6143);	/*  Set G0,G1 DMA mode to E series version */
	ni_writeb(0x80, PipelineDelay_6143);	/*  Set EOCMode, ADCMode and pipelinedelay */
	ni_writeb(0x00, EOC_Set_6143);	/*  Set EOC Delay */

	/* Set the FIFO half full level */
	ni_writel(board->ai_fifo_depth / 2, AIFIFO_Flag_6143);

	/*  Strobe Relay disable bit */
	devpriv->ai_calib_source_enabled = 0;
	ni_writew(devpriv->ai_calib_source | Calibration_Channel_6143_RelayOff,
		  Calibration_Channel_6143);
	ni_writew(devpriv->ai_calib_source, Calibration_Channel_6143);
}

static void pcimio_detach(struct comedi_device *dev)
{
	struct ni_private *devpriv = dev->private;

	mio_common_detach(dev);
	if (dev->irq)
		free_irq(dev->irq, dev);
	if (devpriv) {
		mite_free_ring(devpriv->ai_mite_ring);
		mite_free_ring(devpriv->ao_mite_ring);
		mite_free_ring(devpriv->cdo_mite_ring);
		mite_free_ring(devpriv->gpct_mite_ring[0]);
		mite_free_ring(devpriv->gpct_mite_ring[1]);
		if (devpriv->mite) {
			mite_unsetup(devpriv->mite);
			mite_free(devpriv->mite);
		}
	}
	comedi_pci_disable(dev);
}

static int pcimio_auto_attach(struct comedi_device *dev,
			      unsigned long context)
{
	struct pci_dev *pcidev = comedi_to_pci_dev(dev);
	const struct ni_board_struct *board = NULL;
	struct ni_private *devpriv;
	int ret;

	if (context < ARRAY_SIZE(ni_boards))
		board = &ni_boards[context];
	if (!board)
		return -ENODEV;
	dev->board_ptr = board;
	dev->board_name = board->name;

	ret = comedi_pci_enable(dev);
	if (ret)
		return ret;

	ret = ni_alloc_private(dev);
	if (ret)
		return ret;
	devpriv = dev->private;

	devpriv->mite = mite_alloc(pcidev);
	if (!devpriv->mite)
		return -ENOMEM;

	if (board->reg_type & ni_reg_m_series_mask) {
		devpriv->stc_writew = &m_series_stc_writew;
		devpriv->stc_readw = &m_series_stc_readw;
		devpriv->stc_writel = &m_series_stc_writel;
		devpriv->stc_readl = &m_series_stc_readl;
	} else {
		devpriv->stc_writew = &e_series_win_out;
		devpriv->stc_readw = &e_series_win_in;
		devpriv->stc_writel = &win_out2;
		devpriv->stc_readl = &win_in2;
	}

	ret = mite_setup(devpriv->mite);
	if (ret < 0) {
		pr_warn("error setting up mite\n");
		return ret;
	}

	devpriv->ai_mite_ring = mite_alloc_ring(devpriv->mite);
	if (devpriv->ai_mite_ring == NULL)
		return -ENOMEM;
	devpriv->ao_mite_ring = mite_alloc_ring(devpriv->mite);
	if (devpriv->ao_mite_ring == NULL)
		return -ENOMEM;
	devpriv->cdo_mite_ring = mite_alloc_ring(devpriv->mite);
	if (devpriv->cdo_mite_ring == NULL)
		return -ENOMEM;
	devpriv->gpct_mite_ring[0] = mite_alloc_ring(devpriv->mite);
	if (devpriv->gpct_mite_ring[0] == NULL)
		return -ENOMEM;
	devpriv->gpct_mite_ring[1] = mite_alloc_ring(devpriv->mite);
	if (devpriv->gpct_mite_ring[1] == NULL)
		return -ENOMEM;

	if (board->reg_type & ni_reg_m_series_mask)
		m_series_init_eeprom_buffer(dev);
	if (board->reg_type == ni_reg_6143)
		init_6143(dev);

	dev->irq = mite_irq(devpriv->mite);

	if (dev->irq == 0) {
		pr_warn("unknown irq (bad)\n");
	} else {
		pr_debug("( irq = %u )\n", dev->irq);
		ret = request_irq(dev->irq, ni_E_interrupt, NI_E_IRQ_FLAGS,
				  DRV_NAME, dev);
		if (ret < 0) {
			pr_warn("irq not available\n");
			dev->irq = 0;
		}
	}

	ret = ni_E_init(dev);
	if (ret < 0)
		return ret;

	dev->subdevices[NI_AI_SUBDEV].buf_change = &pcimio_ai_change;
	dev->subdevices[NI_AO_SUBDEV].buf_change = &pcimio_ao_change;
	dev->subdevices[NI_GPCT_SUBDEV(0)].buf_change = &pcimio_gpct0_change;
	dev->subdevices[NI_GPCT_SUBDEV(1)].buf_change = &pcimio_gpct1_change;
	dev->subdevices[NI_DIO_SUBDEV].buf_change = &pcimio_dio_change;

	return ret;
}

static int pcimio_ai_change(struct comedi_device *dev,
			    struct comedi_subdevice *s, unsigned long new_size)
{
	struct ni_private *devpriv = dev->private;
	int ret;

	ret = mite_buf_change(devpriv->ai_mite_ring, s->async);
	if (ret < 0)
		return ret;

	return 0;
}

static int pcimio_ao_change(struct comedi_device *dev,
			    struct comedi_subdevice *s, unsigned long new_size)
{
	struct ni_private *devpriv = dev->private;
	int ret;

	ret = mite_buf_change(devpriv->ao_mite_ring, s->async);
	if (ret < 0)
		return ret;

	return 0;
}

static int pcimio_gpct0_change(struct comedi_device *dev,
			       struct comedi_subdevice *s,
			       unsigned long new_size)
{
	struct ni_private *devpriv = dev->private;
	int ret;

	ret = mite_buf_change(devpriv->gpct_mite_ring[0], s->async);
	if (ret < 0)
		return ret;

	return 0;
}

static int pcimio_gpct1_change(struct comedi_device *dev,
			       struct comedi_subdevice *s,
			       unsigned long new_size)
{
	struct ni_private *devpriv = dev->private;
	int ret;

	ret = mite_buf_change(devpriv->gpct_mite_ring[1], s->async);
	if (ret < 0)
		return ret;

	return 0;
}

static int pcimio_dio_change(struct comedi_device *dev,
			     struct comedi_subdevice *s, unsigned long new_size)
{
	struct ni_private *devpriv = dev->private;
	int ret;

	ret = mite_buf_change(devpriv->cdo_mite_ring, s->async);
	if (ret < 0)
		return ret;

	return 0;
}

static struct comedi_driver ni_pcimio_driver = {
	.driver_name	= "ni_pcimio",
	.module		= THIS_MODULE,
	.auto_attach	= pcimio_auto_attach,
	.detach		= pcimio_detach,
};

static int ni_pcimio_pci_probe(struct pci_dev *dev,
			       const struct pci_device_id *id)
{
	return comedi_pci_auto_config(dev, &ni_pcimio_driver, id->driver_data);
}

static DEFINE_PCI_DEVICE_TABLE(ni_pcimio_pci_table) = {
	{ PCI_VDEVICE(NI, 0x0162), BOARD_PCIMIO_16XE_50 },	/* 0x1620? */
	{ PCI_VDEVICE(NI, 0x1170), BOARD_PCIMIO_16XE_10 },
	{ PCI_VDEVICE(NI, 0x1180), BOARD_PCIMIO_16E_1 },
	{ PCI_VDEVICE(NI, 0x1190), BOARD_PCIMIO_16E_4 },
	{ PCI_VDEVICE(NI, 0x11b0), BOARD_PXI6070E },
	{ PCI_VDEVICE(NI, 0x11c0), BOARD_PXI6040E },
	{ PCI_VDEVICE(NI, 0x11d0), BOARD_PXI6030E },
	{ PCI_VDEVICE(NI, 0x1270), BOARD_PCI6032E },
	{ PCI_VDEVICE(NI, 0x1330), BOARD_PCI6031E },
	{ PCI_VDEVICE(NI, 0x1340), BOARD_PCI6033E },
	{ PCI_VDEVICE(NI, 0x1350), BOARD_PCI6071E },
	{ PCI_VDEVICE(NI, 0x14e0), BOARD_PCI6110 },
	{ PCI_VDEVICE(NI, 0x14f0), BOARD_PCI6111 },
	{ PCI_VDEVICE(NI, 0x1580), BOARD_PXI6031E },
	{ PCI_VDEVICE(NI, 0x15b0), BOARD_PXI6071E },
	{ PCI_VDEVICE(NI, 0x1880), BOARD_PCI6711 },
	{ PCI_VDEVICE(NI, 0x1870), BOARD_PCI6713 },
	{ PCI_VDEVICE(NI, 0x18b0), BOARD_PCI6052E },
	{ PCI_VDEVICE(NI, 0x18c0), BOARD_PXI6052E },
	{ PCI_VDEVICE(NI, 0x2410), BOARD_PCI6733 },
	{ PCI_VDEVICE(NI, 0x2420), BOARD_PXI6733 },
	{ PCI_VDEVICE(NI, 0x2430), BOARD_PCI6731 },
	{ PCI_VDEVICE(NI, 0x2890), BOARD_PCI6036E },
	{ PCI_VDEVICE(NI, 0x28c0), BOARD_PCI6014 },
	{ PCI_VDEVICE(NI, 0x2a60), BOARD_PCI6023E },
	{ PCI_VDEVICE(NI, 0x2a70), BOARD_PCI6024E },
	{ PCI_VDEVICE(NI, 0x2a80), BOARD_PCI6025E },
	{ PCI_VDEVICE(NI, 0x2ab0), BOARD_PXI6025E },
	{ PCI_VDEVICE(NI, 0x2b80), BOARD_PXI6713 },
	{ PCI_VDEVICE(NI, 0x2b90), BOARD_PXI6711 },
	{ PCI_VDEVICE(NI, 0x2c80), BOARD_PCI6035E },
	{ PCI_VDEVICE(NI, 0x2ca0), BOARD_PCI6034E },
	{ PCI_VDEVICE(NI, 0x70aa), BOARD_PCI6229 },
	{ PCI_VDEVICE(NI, 0x70ab), BOARD_PCI6259 },
	{ PCI_VDEVICE(NI, 0x70ac), BOARD_PCI6289 },
	{ PCI_VDEVICE(NI, 0x70af), BOARD_PCI6221 },
	{ PCI_VDEVICE(NI, 0x70b0), BOARD_PCI6220 },
	{ PCI_VDEVICE(NI, 0x70b4), BOARD_PCI6250 },
	{ PCI_VDEVICE(NI, 0x70b6), BOARD_PCI6280 },
	{ PCI_VDEVICE(NI, 0x70b7), BOARD_PCI6254 },
	{ PCI_VDEVICE(NI, 0x70b8), BOARD_PCI6251 },
	{ PCI_VDEVICE(NI, 0x70bc), BOARD_PCI6284 },
	{ PCI_VDEVICE(NI, 0x70bd), BOARD_PCI6281 },
	{ PCI_VDEVICE(NI, 0x70bf), BOARD_PXI6281 },
	{ PCI_VDEVICE(NI, 0x70c0), BOARD_PCI6143 },
	{ PCI_VDEVICE(NI, 0x70f2), BOARD_PCI6224 },
	{ PCI_VDEVICE(NI, 0x70f3), BOARD_PXI6224 },
	{ PCI_VDEVICE(NI, 0x710d), BOARD_PXI6143 },
	{ PCI_VDEVICE(NI, 0x716c), BOARD_PCI6225 },
	{ PCI_VDEVICE(NI, 0x716d), BOARD_PXI6225 },
	{ PCI_VDEVICE(NI, 0x717f), BOARD_PCIE6259 },
	{ PCI_VDEVICE(NI, 0x71bc), BOARD_PCI6221_37PIN },
	{ PCI_VDEVICE(NI, 0x717d), BOARD_PCIE6251 },
	{ PCI_VDEVICE(NI, 0x72e8), BOARD_PXIE6251 },
	{ 0 }
};
MODULE_DEVICE_TABLE(pci, ni_pcimio_pci_table);

static struct pci_driver ni_pcimio_pci_driver = {
	.name		= "ni_pcimio",
	.id_table	= ni_pcimio_pci_table,
	.probe		= ni_pcimio_pci_probe,
	.remove		= comedi_pci_auto_unconfig,
};
module_comedi_pci_driver(ni_pcimio_driver, ni_pcimio_pci_driver);

MODULE_AUTHOR("Comedi http://www.comedi.org");
MODULE_DESCRIPTION("Comedi low-level driver");
MODULE_LICENSE("GPL");
