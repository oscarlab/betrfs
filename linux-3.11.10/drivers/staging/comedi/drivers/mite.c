/*
    comedi/drivers/mite.c
    Hardware driver for NI Mite PCI interface chip

    COMEDI - Linux Control and Measurement Device Interface
    Copyright (C) 1997-2002 David A. Schleef <ds@schleef.org>

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
	The PCI-MIO E series driver was originally written by
	Tomasz Motylewski <...>, and ported to comedi by ds.

	References for specifications:

	   321747b.pdf  Register Level Programmer Manual (obsolete)
	   321747c.pdf  Register Level Programmer Manual (new)
	   DAQ-STC reference manual

	Other possibly relevant info:

	   320517c.pdf  User manual (obsolete)
	   320517f.pdf  User manual (new)
	   320889a.pdf  delete
	   320906c.pdf  maximum signal ratings
	   321066a.pdf  about 16x
	   321791a.pdf  discontinuation of at-mio-16e-10 rev. c
	   321808a.pdf  about at-mio-16e-10 rev P
	   321837a.pdf  discontinuation of at-mio-16de-10 rev d
	   321838a.pdf  about at-mio-16de-10 rev N

	ISSUES:

*/

/* #define USE_KMALLOC */

#define pr_fmt(fmt) KBUILD_MODNAME ": " fmt

#include <linux/pci.h>

#include "../comedidev.h"

#include "comedi_fc.h"
#include "mite.h"

#define TOP_OF_PAGE(x) ((x)|(~(PAGE_MASK)))

struct mite_struct *mite_alloc(struct pci_dev *pcidev)
{
	struct mite_struct *mite;
	unsigned int i;

	mite = kzalloc(sizeof(*mite), GFP_KERNEL);
	if (mite) {
		spin_lock_init(&mite->lock);
		mite->pcidev = pcidev;
		for (i = 0; i < MAX_MITE_DMA_CHANNELS; ++i) {
			mite->channels[i].mite = mite;
			mite->channels[i].channel = i;
			mite->channels[i].done = 1;
		}
	}
	return mite;
}
EXPORT_SYMBOL_GPL(mite_alloc);

static void dump_chip_signature(u32 csigr_bits)
{
	pr_info("version = %i, type = %i, mite mode = %i, interface mode = %i\n",
		mite_csigr_version(csigr_bits), mite_csigr_type(csigr_bits),
		mite_csigr_mmode(csigr_bits), mite_csigr_imode(csigr_bits));
	pr_info("num channels = %i, write post fifo depth = %i, wins = %i, iowins = %i\n",
		mite_csigr_dmac(csigr_bits), mite_csigr_wpdep(csigr_bits),
		mite_csigr_wins(csigr_bits), mite_csigr_iowins(csigr_bits));
}

static unsigned mite_fifo_size(struct mite_struct *mite, unsigned channel)
{
	unsigned fcr_bits = readl(mite->mite_io_addr + MITE_FCR(channel));
	unsigned empty_count = (fcr_bits >> 16) & 0xff;
	unsigned full_count = fcr_bits & 0xff;
	return empty_count + full_count;
}

int mite_setup2(struct mite_struct *mite, unsigned use_iodwbsr_1)
{
	unsigned long length;
	int i;
	u32 csigr_bits;
	unsigned unknown_dma_burst_bits;

	pci_set_master(mite->pcidev);

	mite->mite_io_addr = pci_ioremap_bar(mite->pcidev, 0);
	if (!mite->mite_io_addr) {
		dev_err(&mite->pcidev->dev,
			"Failed to remap mite io memory address\n");
		return -ENOMEM;
	}
	mite->mite_phys_addr = pci_resource_start(mite->pcidev, 0);

	mite->daq_io_addr = pci_ioremap_bar(mite->pcidev, 1);
	if (!mite->daq_io_addr) {
		dev_err(&mite->pcidev->dev,
			"Failed to remap daq io memory address\n");
		return -ENOMEM;
	}
	mite->daq_phys_addr = pci_resource_start(mite->pcidev, 1);
	length = pci_resource_len(mite->pcidev, 1);

	if (use_iodwbsr_1) {
		writel(0, mite->mite_io_addr + MITE_IODWBSR);
		dev_info(&mite->pcidev->dev,
			 "using I/O Window Base Size register 1\n");
		writel(mite->daq_phys_addr | WENAB |
		       MITE_IODWBSR_1_WSIZE_bits(length),
		       mite->mite_io_addr + MITE_IODWBSR_1);
		writel(0, mite->mite_io_addr + MITE_IODWCR_1);
	} else {
		writel(mite->daq_phys_addr | WENAB,
		       mite->mite_io_addr + MITE_IODWBSR);
	}
	/*
	 * make sure dma bursts work. I got this from running a bus analyzer
	 * on a pxi-6281 and a pxi-6713. 6713 powered up with register value
	 * of 0x61f and bursts worked. 6281 powered up with register value of
	 * 0x1f and bursts didn't work. The NI windows driver reads the
	 * register, then does a bitwise-or of 0x600 with it and writes it back.
	 */
	unknown_dma_burst_bits =
	    readl(mite->mite_io_addr + MITE_UNKNOWN_DMA_BURST_REG);
	unknown_dma_burst_bits |= UNKNOWN_DMA_BURST_ENABLE_BITS;
	writel(unknown_dma_burst_bits,
	       mite->mite_io_addr + MITE_UNKNOWN_DMA_BURST_REG);

	csigr_bits = readl(mite->mite_io_addr + MITE_CSIGR);
	mite->num_channels = mite_csigr_dmac(csigr_bits);
	if (mite->num_channels > MAX_MITE_DMA_CHANNELS) {
		dev_warn(&mite->pcidev->dev,
			 "mite: bug? chip claims to have %i dma channels. Setting to %i.\n",
			 mite->num_channels, MAX_MITE_DMA_CHANNELS);
		mite->num_channels = MAX_MITE_DMA_CHANNELS;
	}
	dump_chip_signature(csigr_bits);
	for (i = 0; i < mite->num_channels; i++) {
		writel(CHOR_DMARESET, mite->mite_io_addr + MITE_CHOR(i));
		/* disable interrupts */
		writel(CHCR_CLR_DMA_IE | CHCR_CLR_LINKP_IE | CHCR_CLR_SAR_IE |
		       CHCR_CLR_DONE_IE | CHCR_CLR_MRDY_IE | CHCR_CLR_DRDY_IE |
		       CHCR_CLR_LC_IE | CHCR_CLR_CONT_RB_IE,
		       mite->mite_io_addr + MITE_CHCR(i));
	}
	mite->fifo_size = mite_fifo_size(mite, 0);
	dev_info(&mite->pcidev->dev, "fifo size is %i.\n", mite->fifo_size);
	return 0;
}
EXPORT_SYMBOL_GPL(mite_setup2);

int mite_setup(struct mite_struct *mite)
{
	return mite_setup2(mite, 0);
}
EXPORT_SYMBOL_GPL(mite_setup);

void mite_unsetup(struct mite_struct *mite)
{
	/* unsigned long offset, start, length; */

	if (!mite)
		return;

	if (mite->mite_io_addr) {
		iounmap(mite->mite_io_addr);
		mite->mite_io_addr = NULL;
	}
	if (mite->daq_io_addr) {
		iounmap(mite->daq_io_addr);
		mite->daq_io_addr = NULL;
	}
	if (mite->mite_phys_addr)
		mite->mite_phys_addr = 0;
}
EXPORT_SYMBOL_GPL(mite_unsetup);

struct mite_dma_descriptor_ring *mite_alloc_ring(struct mite_struct *mite)
{
	struct mite_dma_descriptor_ring *ring =
	    kmalloc(sizeof(struct mite_dma_descriptor_ring), GFP_KERNEL);

	if (ring == NULL)
		return ring;
	ring->hw_dev = get_device(&mite->pcidev->dev);
	if (ring->hw_dev == NULL) {
		kfree(ring);
		return NULL;
	}
	ring->n_links = 0;
	ring->descriptors = NULL;
	ring->descriptors_dma_addr = 0;
	return ring;
};
EXPORT_SYMBOL_GPL(mite_alloc_ring);

void mite_free_ring(struct mite_dma_descriptor_ring *ring)
{
	if (ring) {
		if (ring->descriptors) {
			dma_free_coherent(ring->hw_dev,
					  ring->n_links *
					  sizeof(struct mite_dma_descriptor),
					  ring->descriptors,
					  ring->descriptors_dma_addr);
		}
		put_device(ring->hw_dev);
		kfree(ring);
	}
};
EXPORT_SYMBOL_GPL(mite_free_ring);

struct mite_channel *mite_request_channel_in_range(struct mite_struct *mite,
						   struct
						   mite_dma_descriptor_ring
						   *ring, unsigned min_channel,
						   unsigned max_channel)
{
	int i;
	unsigned long flags;
	struct mite_channel *channel = NULL;

	/* spin lock so mite_release_channel can be called safely
	 * from interrupts
	 */
	spin_lock_irqsave(&mite->lock, flags);
	for (i = min_channel; i <= max_channel; ++i) {
		if (mite->channel_allocated[i] == 0) {
			mite->channel_allocated[i] = 1;
			channel = &mite->channels[i];
			channel->ring = ring;
			break;
		}
	}
	spin_unlock_irqrestore(&mite->lock, flags);
	return channel;
}
EXPORT_SYMBOL_GPL(mite_request_channel_in_range);

void mite_release_channel(struct mite_channel *mite_chan)
{
	struct mite_struct *mite = mite_chan->mite;
	unsigned long flags;

	/*  spin lock to prevent races with mite_request_channel */
	spin_lock_irqsave(&mite->lock, flags);
	if (mite->channel_allocated[mite_chan->channel]) {
		mite_dma_disarm(mite_chan);
		mite_dma_reset(mite_chan);
	/*
	 * disable all channel's interrupts (do it after disarm/reset so
	 * MITE_CHCR reg isn't changed while dma is still active!)
	 */
		writel(CHCR_CLR_DMA_IE | CHCR_CLR_LINKP_IE |
		       CHCR_CLR_SAR_IE | CHCR_CLR_DONE_IE |
		       CHCR_CLR_MRDY_IE | CHCR_CLR_DRDY_IE |
		       CHCR_CLR_LC_IE | CHCR_CLR_CONT_RB_IE,
		       mite->mite_io_addr + MITE_CHCR(mite_chan->channel));
		mite->channel_allocated[mite_chan->channel] = 0;
		mite_chan->ring = NULL;
		mmiowb();
	}
	spin_unlock_irqrestore(&mite->lock, flags);
}
EXPORT_SYMBOL_GPL(mite_release_channel);

void mite_dma_arm(struct mite_channel *mite_chan)
{
	struct mite_struct *mite = mite_chan->mite;
	int chor;
	unsigned long flags;

	MDPRINTK("mite_dma_arm ch%i\n", mite_chan->channel);
	/*
	 * memory barrier is intended to insure any twiddling with the buffer
	 * is done before writing to the mite to arm dma transfer
	 */
	smp_mb();
	/* arm */
	chor = CHOR_START;
	spin_lock_irqsave(&mite->lock, flags);
	mite_chan->done = 0;
	writel(chor, mite->mite_io_addr + MITE_CHOR(mite_chan->channel));
	mmiowb();
	spin_unlock_irqrestore(&mite->lock, flags);
/*       mite_dma_tcr(mite, channel); */
}
EXPORT_SYMBOL_GPL(mite_dma_arm);

/**************************************/

int mite_buf_change(struct mite_dma_descriptor_ring *ring,
		    struct comedi_async *async)
{
	unsigned int n_links;
	int i;

	if (ring->descriptors) {
		dma_free_coherent(ring->hw_dev,
				  ring->n_links *
				  sizeof(struct mite_dma_descriptor),
				  ring->descriptors,
				  ring->descriptors_dma_addr);
	}
	ring->descriptors = NULL;
	ring->descriptors_dma_addr = 0;
	ring->n_links = 0;

	if (async->prealloc_bufsz == 0)
		return 0;

	n_links = async->prealloc_bufsz >> PAGE_SHIFT;

	MDPRINTK("ring->hw_dev=%p, n_links=0x%04x\n", ring->hw_dev, n_links);

	ring->descriptors =
	    dma_alloc_coherent(ring->hw_dev,
			       n_links * sizeof(struct mite_dma_descriptor),
			       &ring->descriptors_dma_addr, GFP_KERNEL);
	if (!ring->descriptors) {
		dev_err(async->subdevice->device->class_dev,
			"mite: ring buffer allocation failed\n");
		return -ENOMEM;
	}
	ring->n_links = n_links;

	for (i = 0; i < n_links; i++) {
		ring->descriptors[i].count = cpu_to_le32(PAGE_SIZE);
		ring->descriptors[i].addr =
		    cpu_to_le32(async->buf_page_list[i].dma_addr);
		ring->descriptors[i].next =
		    cpu_to_le32(ring->descriptors_dma_addr + (i +
							      1) *
				sizeof(struct mite_dma_descriptor));
	}
	ring->descriptors[n_links - 1].next =
	    cpu_to_le32(ring->descriptors_dma_addr);
	/*
	 * barrier is meant to insure that all the writes to the dma descriptors
	 * have completed before the dma controller is commanded to read them
	 */
	smp_wmb();
	return 0;
}
EXPORT_SYMBOL_GPL(mite_buf_change);

void mite_prep_dma(struct mite_channel *mite_chan,
		   unsigned int num_device_bits, unsigned int num_memory_bits)
{
	unsigned int chor, chcr, mcr, dcr, lkcr;
	struct mite_struct *mite = mite_chan->mite;

	MDPRINTK("mite_prep_dma ch%i\n", mite_chan->channel);

	/* reset DMA and FIFO */
	chor = CHOR_DMARESET | CHOR_FRESET;
	writel(chor, mite->mite_io_addr + MITE_CHOR(mite_chan->channel));

	/* short link chaining mode */
	chcr = CHCR_SET_DMA_IE | CHCR_LINKSHORT | CHCR_SET_DONE_IE |
	    CHCR_BURSTEN;
	/*
	 * Link Complete Interrupt: interrupt every time a link
	 * in MITE_RING is completed. This can generate a lot of
	 * extra interrupts, but right now we update the values
	 * of buf_int_ptr and buf_int_count at each interrupt. A
	 * better method is to poll the MITE before each user
	 * "read()" to calculate the number of bytes available.
	 */
	chcr |= CHCR_SET_LC_IE;
	if (num_memory_bits == 32 && num_device_bits == 16) {
		/*
		 * Doing a combined 32 and 16 bit byteswap gets the 16 bit
		 * samples into the fifo in the right order. Tested doing 32 bit
		 * memory to 16 bit device transfers to the analog out of a
		 * pxi-6281, which has mite version = 1, type = 4. This also
		 * works for dma reads from the counters on e-series boards.
		 */
		chcr |= CHCR_BYTE_SWAP_DEVICE | CHCR_BYTE_SWAP_MEMORY;
	}
	if (mite_chan->dir == COMEDI_INPUT)
		chcr |= CHCR_DEV_TO_MEM;

	writel(chcr, mite->mite_io_addr + MITE_CHCR(mite_chan->channel));

	/* to/from memory */
	mcr = CR_RL(64) | CR_ASEQUP;
	switch (num_memory_bits) {
	case 8:
		mcr |= CR_PSIZE8;
		break;
	case 16:
		mcr |= CR_PSIZE16;
		break;
	case 32:
		mcr |= CR_PSIZE32;
		break;
	default:
		pr_warn("bug! invalid mem bit width for dma transfer\n");
		break;
	}
	writel(mcr, mite->mite_io_addr + MITE_MCR(mite_chan->channel));

	/* from/to device */
	dcr = CR_RL(64) | CR_ASEQUP;
	dcr |= CR_PORTIO | CR_AMDEVICE | CR_REQSDRQ(mite_chan->channel);
	switch (num_device_bits) {
	case 8:
		dcr |= CR_PSIZE8;
		break;
	case 16:
		dcr |= CR_PSIZE16;
		break;
	case 32:
		dcr |= CR_PSIZE32;
		break;
	default:
		pr_warn("bug! invalid dev bit width for dma transfer\n");
		break;
	}
	writel(dcr, mite->mite_io_addr + MITE_DCR(mite_chan->channel));

	/* reset the DAR */
	writel(0, mite->mite_io_addr + MITE_DAR(mite_chan->channel));

	/* the link is 32bits */
	lkcr = CR_RL(64) | CR_ASEQUP | CR_PSIZE32;
	writel(lkcr, mite->mite_io_addr + MITE_LKCR(mite_chan->channel));

	/* starting address for link chaining */
	writel(mite_chan->ring->descriptors_dma_addr,
	       mite->mite_io_addr + MITE_LKAR(mite_chan->channel));

	MDPRINTK("exit mite_prep_dma\n");
}
EXPORT_SYMBOL_GPL(mite_prep_dma);

static u32 mite_device_bytes_transferred(struct mite_channel *mite_chan)
{
	struct mite_struct *mite = mite_chan->mite;
	return readl(mite->mite_io_addr + MITE_DAR(mite_chan->channel));
}

u32 mite_bytes_in_transit(struct mite_channel *mite_chan)
{
	struct mite_struct *mite = mite_chan->mite;
	return readl(mite->mite_io_addr +
		     MITE_FCR(mite_chan->channel)) & 0x000000FF;
}
EXPORT_SYMBOL_GPL(mite_bytes_in_transit);

/* returns lower bound for number of bytes transferred from device to memory */
u32 mite_bytes_written_to_memory_lb(struct mite_channel *mite_chan)
{
	u32 device_byte_count;

	device_byte_count = mite_device_bytes_transferred(mite_chan);
	return device_byte_count - mite_bytes_in_transit(mite_chan);
}
EXPORT_SYMBOL_GPL(mite_bytes_written_to_memory_lb);

/* returns upper bound for number of bytes transferred from device to memory */
u32 mite_bytes_written_to_memory_ub(struct mite_channel *mite_chan)
{
	u32 in_transit_count;

	in_transit_count = mite_bytes_in_transit(mite_chan);
	return mite_device_bytes_transferred(mite_chan) - in_transit_count;
}
EXPORT_SYMBOL_GPL(mite_bytes_written_to_memory_ub);

/* returns lower bound for number of bytes read from memory to device */
u32 mite_bytes_read_from_memory_lb(struct mite_channel *mite_chan)
{
	u32 device_byte_count;

	device_byte_count = mite_device_bytes_transferred(mite_chan);
	return device_byte_count + mite_bytes_in_transit(mite_chan);
}
EXPORT_SYMBOL_GPL(mite_bytes_read_from_memory_lb);

/* returns upper bound for number of bytes read from memory to device */
u32 mite_bytes_read_from_memory_ub(struct mite_channel *mite_chan)
{
	u32 in_transit_count;

	in_transit_count = mite_bytes_in_transit(mite_chan);
	return mite_device_bytes_transferred(mite_chan) + in_transit_count;
}
EXPORT_SYMBOL_GPL(mite_bytes_read_from_memory_ub);

unsigned mite_dma_tcr(struct mite_channel *mite_chan)
{
	struct mite_struct *mite = mite_chan->mite;
	int tcr;
	int lkar;

	lkar = readl(mite->mite_io_addr + MITE_LKAR(mite_chan->channel));
	tcr = readl(mite->mite_io_addr + MITE_TCR(mite_chan->channel));
	MDPRINTK("mite_dma_tcr ch%i, lkar=0x%08x tcr=%d\n", mite_chan->channel,
		 lkar, tcr);

	return tcr;
}
EXPORT_SYMBOL_GPL(mite_dma_tcr);

void mite_dma_disarm(struct mite_channel *mite_chan)
{
	struct mite_struct *mite = mite_chan->mite;
	unsigned chor;

	/* disarm */
	chor = CHOR_ABORT;
	writel(chor, mite->mite_io_addr + MITE_CHOR(mite_chan->channel));
}
EXPORT_SYMBOL_GPL(mite_dma_disarm);

int mite_sync_input_dma(struct mite_channel *mite_chan,
			struct comedi_async *async)
{
	int count;
	unsigned int nbytes, old_alloc_count;
	const unsigned bytes_per_scan = cfc_bytes_per_scan(async->subdevice);

	old_alloc_count = async->buf_write_alloc_count;
	/* write alloc as much as we can */
	comedi_buf_write_alloc(async, async->prealloc_bufsz);

	nbytes = mite_bytes_written_to_memory_lb(mite_chan);
	if ((int)(mite_bytes_written_to_memory_ub(mite_chan) -
		  old_alloc_count) > 0) {
		dev_warn(async->subdevice->device->class_dev,
			 "mite: DMA overwrite of free area\n");
		async->events |= COMEDI_CB_OVERFLOW;
		return -1;
	}

	count = nbytes - async->buf_write_count;
	/* it's possible count will be negative due to
	 * conservative value returned by mite_bytes_written_to_memory_lb */
	if (count <= 0)
		return 0;

	comedi_buf_write_free(async, count);

	async->scan_progress += count;
	if (async->scan_progress >= bytes_per_scan) {
		async->scan_progress %= bytes_per_scan;
		async->events |= COMEDI_CB_EOS;
	}
	async->events |= COMEDI_CB_BLOCK;
	return 0;
}
EXPORT_SYMBOL_GPL(mite_sync_input_dma);

int mite_sync_output_dma(struct mite_channel *mite_chan,
			 struct comedi_async *async)
{
	int count;
	u32 nbytes_ub, nbytes_lb;
	unsigned int old_alloc_count;
	u32 stop_count =
	    async->cmd.stop_arg * cfc_bytes_per_scan(async->subdevice);

	old_alloc_count = async->buf_read_alloc_count;
	/*  read alloc as much as we can */
	comedi_buf_read_alloc(async, async->prealloc_bufsz);
	nbytes_lb = mite_bytes_read_from_memory_lb(mite_chan);
	if (async->cmd.stop_src == TRIG_COUNT &&
	    (int)(nbytes_lb - stop_count) > 0)
		nbytes_lb = stop_count;
	nbytes_ub = mite_bytes_read_from_memory_ub(mite_chan);
	if (async->cmd.stop_src == TRIG_COUNT &&
	    (int)(nbytes_ub - stop_count) > 0)
		nbytes_ub = stop_count;
	if ((int)(nbytes_ub - old_alloc_count) > 0) {
		dev_warn(async->subdevice->device->class_dev,
			 "mite: DMA underrun\n");
		async->events |= COMEDI_CB_OVERFLOW;
		return -1;
	}
	count = nbytes_lb - async->buf_read_count;
	if (count <= 0)
		return 0;

	if (count) {
		comedi_buf_read_free(async, count);
		async->events |= COMEDI_CB_BLOCK;
	}
	return 0;
}
EXPORT_SYMBOL_GPL(mite_sync_output_dma);

unsigned mite_get_status(struct mite_channel *mite_chan)
{
	struct mite_struct *mite = mite_chan->mite;
	unsigned status;
	unsigned long flags;

	spin_lock_irqsave(&mite->lock, flags);
	status = readl(mite->mite_io_addr + MITE_CHSR(mite_chan->channel));
	if (status & CHSR_DONE) {
		mite_chan->done = 1;
		writel(CHOR_CLRDONE,
		       mite->mite_io_addr + MITE_CHOR(mite_chan->channel));
	}
	mmiowb();
	spin_unlock_irqrestore(&mite->lock, flags);
	return status;
}
EXPORT_SYMBOL_GPL(mite_get_status);

int mite_done(struct mite_channel *mite_chan)
{
	struct mite_struct *mite = mite_chan->mite;
	unsigned long flags;
	int done;

	mite_get_status(mite_chan);
	spin_lock_irqsave(&mite->lock, flags);
	done = mite_chan->done;
	spin_unlock_irqrestore(&mite->lock, flags);
	return done;
}
EXPORT_SYMBOL_GPL(mite_done);

#ifdef DEBUG_MITE

/* names of bits in mite registers */

static const char *const mite_CHOR_strings[] = {
	"start", "cont", "stop", "abort",
	"freset", "clrlc", "clrrb", "clrdone",
	"clr_lpause", "set_lpause", "clr_send_tc",
	"set_send_tc", "12", "13", "14",
	"15", "16", "17", "18",
	"19", "20", "21", "22",
	"23", "24", "25", "26",
	"27", "28", "29", "30",
	"dmareset",
};

static const char *const mite_CHCR_strings[] = {
	"continue", "ringbuff", "2", "3",
	"4", "5", "6", "7",
	"8", "9", "10", "11",
	"12", "13", "bursten", "fifodis",
	"clr_cont_rb_ie", "set_cont_rb_ie", "clr_lc_ie", "set_lc_ie",
	"clr_drdy_ie", "set_drdy_ie", "clr_mrdy_ie", "set_mrdy_ie",
	"clr_done_ie", "set_done_ie", "clr_sar_ie", "set_sar_ie",
	"clr_linkp_ie", "set_linkp_ie", "clr_dma_ie", "set_dma_ie",
};

static const char *const mite_MCR_strings[] = {
	"amdevice", "1", "2", "3",
	"4", "5", "portio", "portvxi",
	"psizebyte", "psizehalf (byte & half = word)", "aseqxp1", "11",
	"12", "13", "blocken", "berhand",
	"reqsintlim/reqs0", "reqs1", "reqs2", "rd32",
	"rd512", "rl1", "rl2", "rl8",
	"24", "25", "26", "27",
	"28", "29", "30", "stopen",
};

static const char *const mite_DCR_strings[] = {
	"amdevice", "1", "2", "3",
	"4", "5", "portio", "portvxi",
	"psizebyte", "psizehalf (byte & half = word)", "aseqxp1", "aseqxp2",
	"aseqxp8", "13", "blocken", "berhand",
	"reqsintlim", "reqs1", "reqs2", "rd32",
	"rd512", "rl1", "rl2", "rl8",
	"23", "24", "25", "27",
	"28", "wsdevc", "wsdevs", "rwdevpack",
};

static const char *const mite_LKCR_strings[] = {
	"amdevice", "1", "2", "3",
	"4", "5", "portio", "portvxi",
	"psizebyte", "psizehalf (byte & half = word)", "asequp", "aseqdown",
	"12", "13", "14", "berhand",
	"16", "17", "18", "rd32",
	"rd512", "rl1", "rl2", "rl8",
	"24", "25", "26", "27",
	"28", "29", "30", "chngend",
};

static const char *const mite_CHSR_strings[] = {
	"d.err0", "d.err1", "m.err0", "m.err1",
	"l.err0", "l.err1", "drq0", "drq1",
	"end", "xferr", "operr0", "operr1",
	"stops", "habort", "sabort", "error",
	"16", "conts_rb", "18", "linkc",
	"20", "drdy", "22", "mrdy",
	"24", "done", "26", "sars",
	"28", "lpauses", "30", "int",
};

static void mite_decode(const char *const *bit_str, unsigned int bits)
{
	int i;

	for (i = 31; i >= 0; i--) {
		if (bits & (1 << i))
			pr_debug(" %s\n", bit_str[i]);
	}
}

void mite_dump_regs(struct mite_channel *mite_chan)
{
	void __iomem *mite_io_addr = mite_chan->mite->mite_io_addr;
	unsigned int offset;
	unsigned int value;
	int channel = mite_chan->channel;

	pr_debug("mite_dump_regs ch%i\n", channel);
	pr_debug("mite address is  =%p\n", mite_io_addr);

	offset = MITE_CHOR(channel);
	value = readl(mite_io_addr + offset);
	pr_debug("mite status[CHOR] at 0x%08x =0x%08x\n", offset, value);
	mite_decode(mite_CHOR_strings, value);
	offset = MITE_CHCR(channel);
	value = readl(mite_io_addr + offset);
	pr_debug("mite status[CHCR] at 0x%08x =0x%08x\n", offset, value);
	mite_decode(mite_CHCR_strings, value);
	offset = MITE_TCR(channel);
	value = readl(mite_io_addr + offset);
	pr_debug("mite status[TCR] at 0x%08x =0x%08x\n", offset, value);
	offset = MITE_MCR(channel);
	value = readl(mite_io_addr + offset);
	pr_debug("mite status[MCR] at 0x%08x =0x%08x\n", offset, value);
	mite_decode(mite_MCR_strings, value);
	offset = MITE_MAR(channel);
	value = readl(mite_io_addr + offset);
	pr_debug("mite status[MAR] at 0x%08x =0x%08x\n", offset, value);
	offset = MITE_DCR(channel);
	value = readl(mite_io_addr + offset);
	pr_debug("mite status[DCR] at 0x%08x =0x%08x\n", offset, value);
	mite_decode(mite_DCR_strings, value);
	offset = MITE_DAR(channel);
	value = readl(mite_io_addr + offset);
	pr_debug("mite status[DAR] at 0x%08x =0x%08x\n", offset, value);
	offset = MITE_LKCR(channel);
	value = readl(mite_io_addr + offset);
	pr_debug("mite status[LKCR] at 0x%08x =0x%08x\n", offset, value);
	mite_decode(mite_LKCR_strings, value);
	offset = MITE_LKAR(channel);
	value = readl(mite_io_addr + offset);
	pr_debug("mite status[LKAR] at 0x%08x =0x%08x\n", offset, value);
	offset = MITE_CHSR(channel);
	value = readl(mite_io_addr + offset);
	pr_debug("mite status[CHSR] at 0x%08x =0x%08x\n", offset, value);
	mite_decode(mite_CHSR_strings, value);
	offset = MITE_FCR(channel);
	value = readl(mite_io_addr + offset);
	pr_debug("mite status[FCR] at 0x%08x =0x%08x\n", offset, value);
}
EXPORT_SYMBOL_GPL(mite_dump_regs);
#endif

static int __init mite_module_init(void)
{
	return 0;
}

static void __exit mite_module_exit(void)
{
}

module_init(mite_module_init);
module_exit(mite_module_exit);

MODULE_AUTHOR("Comedi http://www.comedi.org");
MODULE_DESCRIPTION("Comedi low-level driver");
MODULE_LICENSE("GPL");
