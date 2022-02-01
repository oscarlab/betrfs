/*
 * rcar_du_drv.h  --  R-Car Display Unit DRM driver
 *
 * Copyright (C) 2013 Renesas Corporation
 *
 * Contact: Laurent Pinchart (laurent.pinchart@ideasonboard.com)
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 */

#ifndef __RCAR_DU_DRV_H__
#define __RCAR_DU_DRV_H__

#include <linux/kernel.h>
#include <linux/mutex.h>
#include <linux/platform_data/rcar-du.h>

#include "rcar_du_crtc.h"
#include "rcar_du_plane.h"

struct clk;
struct device;
struct drm_device;

struct rcar_du_device {
	struct device *dev;
	const struct rcar_du_platform_data *pdata;

	void __iomem *mmio;
	struct clk *clock;
	unsigned int use_count;

	struct drm_device *ddev;

	struct rcar_du_crtc crtcs[2];
	unsigned int used_crtcs;
	unsigned int num_crtcs;

	struct {
		struct rcar_du_plane planes[RCAR_DU_NUM_SW_PLANES];
		unsigned int free;
		struct mutex lock;

		struct drm_property *alpha;
		struct drm_property *colorkey;
		struct drm_property *zpos;
	} planes;
};

int rcar_du_get(struct rcar_du_device *rcdu);
void rcar_du_put(struct rcar_du_device *rcdu);

static inline u32 rcar_du_read(struct rcar_du_device *rcdu, u32 reg)
{
	return ioread32(rcdu->mmio + reg);
}

static inline void rcar_du_write(struct rcar_du_device *rcdu, u32 reg, u32 data)
{
	iowrite32(data, rcdu->mmio + reg);
}

#endif /* __RCAR_DU_DRV_H__ */
