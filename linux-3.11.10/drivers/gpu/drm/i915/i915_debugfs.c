/*
 * Copyright © 2008 Intel Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice (including the next
 * paragraph) shall be included in all copies or substantial portions of the
 * Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *
 * Authors:
 *    Eric Anholt <eric@anholt.net>
 *    Keith Packard <keithp@keithp.com>
 *
 */

#include <linux/seq_file.h>
#include <linux/debugfs.h>
#include <linux/slab.h>
#include <linux/export.h>
#include <generated/utsrelease.h>
#include <drm/drmP.h>
#include "intel_drv.h"
#include "intel_ringbuffer.h"
#include <drm/i915_drm.h>
#include "i915_drv.h"

#define DRM_I915_RING_DEBUG 1


#if defined(CONFIG_DEBUG_FS)

enum {
	ACTIVE_LIST,
	INACTIVE_LIST,
	PINNED_LIST,
};

static const char *yesno(int v)
{
	return v ? "yes" : "no";
}

static int i915_capabilities(struct seq_file *m, void *data)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	const struct intel_device_info *info = INTEL_INFO(dev);

	seq_printf(m, "gen: %d\n", info->gen);
	seq_printf(m, "pch: %d\n", INTEL_PCH_TYPE(dev));
#define PRINT_FLAG(x)  seq_printf(m, #x ": %s\n", yesno(info->x))
#define SEP_SEMICOLON ;
	DEV_INFO_FOR_EACH_FLAG(PRINT_FLAG, SEP_SEMICOLON);
#undef PRINT_FLAG
#undef SEP_SEMICOLON

	return 0;
}

static const char *get_pin_flag(struct drm_i915_gem_object *obj)
{
	if (obj->user_pin_count > 0)
		return "P";
	else if (obj->pin_count > 0)
		return "p";
	else
		return " ";
}

static const char *get_tiling_flag(struct drm_i915_gem_object *obj)
{
	switch (obj->tiling_mode) {
	default:
	case I915_TILING_NONE: return " ";
	case I915_TILING_X: return "X";
	case I915_TILING_Y: return "Y";
	}
}

static const char *cache_level_str(int type)
{
	switch (type) {
	case I915_CACHE_NONE: return " uncached";
	case I915_CACHE_LLC: return " snooped (LLC)";
	case I915_CACHE_LLC_MLC: return " snooped (LLC+MLC)";
	default: return "";
	}
}

static void
describe_obj(struct seq_file *m, struct drm_i915_gem_object *obj)
{
	seq_printf(m, "%pK: %s%s %8zdKiB %02x %02x %d %d %d%s%s%s",
		   &obj->base,
		   get_pin_flag(obj),
		   get_tiling_flag(obj),
		   obj->base.size / 1024,
		   obj->base.read_domains,
		   obj->base.write_domain,
		   obj->last_read_seqno,
		   obj->last_write_seqno,
		   obj->last_fenced_seqno,
		   cache_level_str(obj->cache_level),
		   obj->dirty ? " dirty" : "",
		   obj->madv == I915_MADV_DONTNEED ? " purgeable" : "");
	if (obj->base.name)
		seq_printf(m, " (name: %d)", obj->base.name);
	if (obj->pin_count)
		seq_printf(m, " (pinned x %d)", obj->pin_count);
	if (obj->fence_reg != I915_FENCE_REG_NONE)
		seq_printf(m, " (fence: %d)", obj->fence_reg);
	if (obj->gtt_space != NULL)
		seq_printf(m, " (gtt offset: %08x, size: %08x)",
			   obj->gtt_offset, (unsigned int)obj->gtt_space->size);
	if (obj->stolen)
		seq_printf(m, " (stolen: %08lx)", obj->stolen->start);
	if (obj->pin_mappable || obj->fault_mappable) {
		char s[3], *t = s;
		if (obj->pin_mappable)
			*t++ = 'p';
		if (obj->fault_mappable)
			*t++ = 'f';
		*t = '\0';
		seq_printf(m, " (%s mappable)", s);
	}
	if (obj->ring != NULL)
		seq_printf(m, " (%s)", obj->ring->name);
}

static int i915_gem_object_list_info(struct seq_file *m, void *data)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	uintptr_t list = (uintptr_t) node->info_ent->data;
	struct list_head *head;
	struct drm_device *dev = node->minor->dev;
	drm_i915_private_t *dev_priv = dev->dev_private;
	struct drm_i915_gem_object *obj;
	size_t total_obj_size, total_gtt_size;
	int count, ret;

	ret = mutex_lock_interruptible(&dev->struct_mutex);
	if (ret)
		return ret;

	switch (list) {
	case ACTIVE_LIST:
		seq_printf(m, "Active:\n");
		head = &dev_priv->mm.active_list;
		break;
	case INACTIVE_LIST:
		seq_printf(m, "Inactive:\n");
		head = &dev_priv->mm.inactive_list;
		break;
	default:
		mutex_unlock(&dev->struct_mutex);
		return -EINVAL;
	}

	total_obj_size = total_gtt_size = count = 0;
	list_for_each_entry(obj, head, mm_list) {
		seq_printf(m, "   ");
		describe_obj(m, obj);
		seq_printf(m, "\n");
		total_obj_size += obj->base.size;
		total_gtt_size += obj->gtt_space->size;
		count++;
	}
	mutex_unlock(&dev->struct_mutex);

	seq_printf(m, "Total %d objects, %zu bytes, %zu GTT size\n",
		   count, total_obj_size, total_gtt_size);
	return 0;
}

#define count_objects(list, member) do { \
	list_for_each_entry(obj, list, member) { \
		size += obj->gtt_space->size; \
		++count; \
		if (obj->map_and_fenceable) { \
			mappable_size += obj->gtt_space->size; \
			++mappable_count; \
		} \
	} \
} while (0)

struct file_stats {
	int count;
	size_t total, active, inactive, unbound;
};

static int per_file_stats(int id, void *ptr, void *data)
{
	struct drm_i915_gem_object *obj = ptr;
	struct file_stats *stats = data;

	stats->count++;
	stats->total += obj->base.size;

	if (obj->gtt_space) {
		if (!list_empty(&obj->ring_list))
			stats->active += obj->base.size;
		else
			stats->inactive += obj->base.size;
	} else {
		if (!list_empty(&obj->global_list))
			stats->unbound += obj->base.size;
	}

	return 0;
}

static int i915_gem_object_info(struct seq_file *m, void* data)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	struct drm_i915_private *dev_priv = dev->dev_private;
	u32 count, mappable_count, purgeable_count;
	size_t size, mappable_size, purgeable_size;
	struct drm_i915_gem_object *obj;
	struct drm_file *file;
	int ret;

	ret = mutex_lock_interruptible(&dev->struct_mutex);
	if (ret)
		return ret;

	seq_printf(m, "%u objects, %zu bytes\n",
		   dev_priv->mm.object_count,
		   dev_priv->mm.object_memory);

	size = count = mappable_size = mappable_count = 0;
	count_objects(&dev_priv->mm.bound_list, global_list);
	seq_printf(m, "%u [%u] objects, %zu [%zu] bytes in gtt\n",
		   count, mappable_count, size, mappable_size);

	size = count = mappable_size = mappable_count = 0;
	count_objects(&dev_priv->mm.active_list, mm_list);
	seq_printf(m, "  %u [%u] active objects, %zu [%zu] bytes\n",
		   count, mappable_count, size, mappable_size);

	size = count = mappable_size = mappable_count = 0;
	count_objects(&dev_priv->mm.inactive_list, mm_list);
	seq_printf(m, "  %u [%u] inactive objects, %zu [%zu] bytes\n",
		   count, mappable_count, size, mappable_size);

	size = count = purgeable_size = purgeable_count = 0;
	list_for_each_entry(obj, &dev_priv->mm.unbound_list, global_list) {
		size += obj->base.size, ++count;
		if (obj->madv == I915_MADV_DONTNEED)
			purgeable_size += obj->base.size, ++purgeable_count;
	}
	seq_printf(m, "%u unbound objects, %zu bytes\n", count, size);

	size = count = mappable_size = mappable_count = 0;
	list_for_each_entry(obj, &dev_priv->mm.bound_list, global_list) {
		if (obj->fault_mappable) {
			size += obj->gtt_space->size;
			++count;
		}
		if (obj->pin_mappable) {
			mappable_size += obj->gtt_space->size;
			++mappable_count;
		}
		if (obj->madv == I915_MADV_DONTNEED) {
			purgeable_size += obj->base.size;
			++purgeable_count;
		}
	}
	seq_printf(m, "%u purgeable objects, %zu bytes\n",
		   purgeable_count, purgeable_size);
	seq_printf(m, "%u pinned mappable objects, %zu bytes\n",
		   mappable_count, mappable_size);
	seq_printf(m, "%u fault mappable objects, %zu bytes\n",
		   count, size);

	seq_printf(m, "%zu [%lu] gtt total\n",
		   dev_priv->gtt.total,
		   dev_priv->gtt.mappable_end - dev_priv->gtt.start);

	seq_printf(m, "\n");
	list_for_each_entry_reverse(file, &dev->filelist, lhead) {
		struct file_stats stats;

		memset(&stats, 0, sizeof(stats));
		idr_for_each(&file->object_idr, per_file_stats, &stats);
		seq_printf(m, "%s: %u objects, %zu bytes (%zu active, %zu inactive, %zu unbound)\n",
			   get_pid_task(file->pid, PIDTYPE_PID)->comm,
			   stats.count,
			   stats.total,
			   stats.active,
			   stats.inactive,
			   stats.unbound);
	}

	mutex_unlock(&dev->struct_mutex);

	return 0;
}

static int i915_gem_gtt_info(struct seq_file *m, void* data)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	uintptr_t list = (uintptr_t) node->info_ent->data;
	struct drm_i915_private *dev_priv = dev->dev_private;
	struct drm_i915_gem_object *obj;
	size_t total_obj_size, total_gtt_size;
	int count, ret;

	ret = mutex_lock_interruptible(&dev->struct_mutex);
	if (ret)
		return ret;

	total_obj_size = total_gtt_size = count = 0;
	list_for_each_entry(obj, &dev_priv->mm.bound_list, global_list) {
		if (list == PINNED_LIST && obj->pin_count == 0)
			continue;

		seq_printf(m, "   ");
		describe_obj(m, obj);
		seq_printf(m, "\n");
		total_obj_size += obj->base.size;
		total_gtt_size += obj->gtt_space->size;
		count++;
	}

	mutex_unlock(&dev->struct_mutex);

	seq_printf(m, "Total %d objects, %zu bytes, %zu GTT size\n",
		   count, total_obj_size, total_gtt_size);

	return 0;
}

static int i915_gem_pageflip_info(struct seq_file *m, void *data)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	unsigned long flags;
	struct intel_crtc *crtc;

	list_for_each_entry(crtc, &dev->mode_config.crtc_list, base.head) {
		const char pipe = pipe_name(crtc->pipe);
		const char plane = plane_name(crtc->plane);
		struct intel_unpin_work *work;

		spin_lock_irqsave(&dev->event_lock, flags);
		work = crtc->unpin_work;
		if (work == NULL) {
			seq_printf(m, "No flip due on pipe %c (plane %c)\n",
				   pipe, plane);
		} else {
			if (atomic_read(&work->pending) < INTEL_FLIP_COMPLETE) {
				seq_printf(m, "Flip queued on pipe %c (plane %c)\n",
					   pipe, plane);
			} else {
				seq_printf(m, "Flip pending (waiting for vsync) on pipe %c (plane %c)\n",
					   pipe, plane);
			}
			if (work->enable_stall_check)
				seq_printf(m, "Stall check enabled, ");
			else
				seq_printf(m, "Stall check waiting for page flip ioctl, ");
			seq_printf(m, "%d prepares\n", atomic_read(&work->pending));

			if (work->old_fb_obj) {
				struct drm_i915_gem_object *obj = work->old_fb_obj;
				if (obj)
					seq_printf(m, "Old framebuffer gtt_offset 0x%08x\n", obj->gtt_offset);
			}
			if (work->pending_flip_obj) {
				struct drm_i915_gem_object *obj = work->pending_flip_obj;
				if (obj)
					seq_printf(m, "New framebuffer gtt_offset 0x%08x\n", obj->gtt_offset);
			}
		}
		spin_unlock_irqrestore(&dev->event_lock, flags);
	}

	return 0;
}

static int i915_gem_request_info(struct seq_file *m, void *data)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	drm_i915_private_t *dev_priv = dev->dev_private;
	struct intel_ring_buffer *ring;
	struct drm_i915_gem_request *gem_request;
	int ret, count, i;

	ret = mutex_lock_interruptible(&dev->struct_mutex);
	if (ret)
		return ret;

	count = 0;
	for_each_ring(ring, dev_priv, i) {
		if (list_empty(&ring->request_list))
			continue;

		seq_printf(m, "%s requests:\n", ring->name);
		list_for_each_entry(gem_request,
				    &ring->request_list,
				    list) {
			seq_printf(m, "    %d @ %d\n",
				   gem_request->seqno,
				   (int) (jiffies - gem_request->emitted_jiffies));
		}
		count++;
	}
	mutex_unlock(&dev->struct_mutex);

	if (count == 0)
		seq_printf(m, "No requests\n");

	return 0;
}

static void i915_ring_seqno_info(struct seq_file *m,
				 struct intel_ring_buffer *ring)
{
	if (ring->get_seqno) {
		seq_printf(m, "Current sequence (%s): %u\n",
			   ring->name, ring->get_seqno(ring, false));
	}
}

static int i915_gem_seqno_info(struct seq_file *m, void *data)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	drm_i915_private_t *dev_priv = dev->dev_private;
	struct intel_ring_buffer *ring;
	int ret, i;

	ret = mutex_lock_interruptible(&dev->struct_mutex);
	if (ret)
		return ret;

	for_each_ring(ring, dev_priv, i)
		i915_ring_seqno_info(m, ring);

	mutex_unlock(&dev->struct_mutex);

	return 0;
}


static int i915_interrupt_info(struct seq_file *m, void *data)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	drm_i915_private_t *dev_priv = dev->dev_private;
	struct intel_ring_buffer *ring;
	int ret, i, pipe;

	ret = mutex_lock_interruptible(&dev->struct_mutex);
	if (ret)
		return ret;

	if (IS_VALLEYVIEW(dev)) {
		seq_printf(m, "Display IER:\t%08x\n",
			   I915_READ(VLV_IER));
		seq_printf(m, "Display IIR:\t%08x\n",
			   I915_READ(VLV_IIR));
		seq_printf(m, "Display IIR_RW:\t%08x\n",
			   I915_READ(VLV_IIR_RW));
		seq_printf(m, "Display IMR:\t%08x\n",
			   I915_READ(VLV_IMR));
		for_each_pipe(pipe)
			seq_printf(m, "Pipe %c stat:\t%08x\n",
				   pipe_name(pipe),
				   I915_READ(PIPESTAT(pipe)));

		seq_printf(m, "Master IER:\t%08x\n",
			   I915_READ(VLV_MASTER_IER));

		seq_printf(m, "Render IER:\t%08x\n",
			   I915_READ(GTIER));
		seq_printf(m, "Render IIR:\t%08x\n",
			   I915_READ(GTIIR));
		seq_printf(m, "Render IMR:\t%08x\n",
			   I915_READ(GTIMR));

		seq_printf(m, "PM IER:\t\t%08x\n",
			   I915_READ(GEN6_PMIER));
		seq_printf(m, "PM IIR:\t\t%08x\n",
			   I915_READ(GEN6_PMIIR));
		seq_printf(m, "PM IMR:\t\t%08x\n",
			   I915_READ(GEN6_PMIMR));

		seq_printf(m, "Port hotplug:\t%08x\n",
			   I915_READ(PORT_HOTPLUG_EN));
		seq_printf(m, "DPFLIPSTAT:\t%08x\n",
			   I915_READ(VLV_DPFLIPSTAT));
		seq_printf(m, "DPINVGTT:\t%08x\n",
			   I915_READ(DPINVGTT));

	} else if (!HAS_PCH_SPLIT(dev)) {
		seq_printf(m, "Interrupt enable:    %08x\n",
			   I915_READ(IER));
		seq_printf(m, "Interrupt identity:  %08x\n",
			   I915_READ(IIR));
		seq_printf(m, "Interrupt mask:      %08x\n",
			   I915_READ(IMR));
		for_each_pipe(pipe)
			seq_printf(m, "Pipe %c stat:         %08x\n",
				   pipe_name(pipe),
				   I915_READ(PIPESTAT(pipe)));
	} else {
		seq_printf(m, "North Display Interrupt enable:		%08x\n",
			   I915_READ(DEIER));
		seq_printf(m, "North Display Interrupt identity:	%08x\n",
			   I915_READ(DEIIR));
		seq_printf(m, "North Display Interrupt mask:		%08x\n",
			   I915_READ(DEIMR));
		seq_printf(m, "South Display Interrupt enable:		%08x\n",
			   I915_READ(SDEIER));
		seq_printf(m, "South Display Interrupt identity:	%08x\n",
			   I915_READ(SDEIIR));
		seq_printf(m, "South Display Interrupt mask:		%08x\n",
			   I915_READ(SDEIMR));
		seq_printf(m, "Graphics Interrupt enable:		%08x\n",
			   I915_READ(GTIER));
		seq_printf(m, "Graphics Interrupt identity:		%08x\n",
			   I915_READ(GTIIR));
		seq_printf(m, "Graphics Interrupt mask:		%08x\n",
			   I915_READ(GTIMR));
	}
	seq_printf(m, "Interrupts received: %d\n",
		   atomic_read(&dev_priv->irq_received));
	for_each_ring(ring, dev_priv, i) {
		if (IS_GEN6(dev) || IS_GEN7(dev)) {
			seq_printf(m,
				   "Graphics Interrupt mask (%s):	%08x\n",
				   ring->name, I915_READ_IMR(ring));
		}
		i915_ring_seqno_info(m, ring);
	}
	mutex_unlock(&dev->struct_mutex);

	return 0;
}

static int i915_gem_fence_regs_info(struct seq_file *m, void *data)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	drm_i915_private_t *dev_priv = dev->dev_private;
	int i, ret;

	ret = mutex_lock_interruptible(&dev->struct_mutex);
	if (ret)
		return ret;

	seq_printf(m, "Reserved fences = %d\n", dev_priv->fence_reg_start);
	seq_printf(m, "Total fences = %d\n", dev_priv->num_fence_regs);
	for (i = 0; i < dev_priv->num_fence_regs; i++) {
		struct drm_i915_gem_object *obj = dev_priv->fence_regs[i].obj;

		seq_printf(m, "Fence %d, pin count = %d, object = ",
			   i, dev_priv->fence_regs[i].pin_count);
		if (obj == NULL)
			seq_printf(m, "unused");
		else
			describe_obj(m, obj);
		seq_printf(m, "\n");
	}

	mutex_unlock(&dev->struct_mutex);
	return 0;
}

static int i915_hws_info(struct seq_file *m, void *data)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	drm_i915_private_t *dev_priv = dev->dev_private;
	struct intel_ring_buffer *ring;
	const u32 *hws;
	int i;

	ring = &dev_priv->ring[(uintptr_t)node->info_ent->data];
	hws = ring->status_page.page_addr;
	if (hws == NULL)
		return 0;

	for (i = 0; i < 4096 / sizeof(u32) / 4; i += 4) {
		seq_printf(m, "0x%08x: 0x%08x 0x%08x 0x%08x 0x%08x\n",
			   i * 4,
			   hws[i], hws[i + 1], hws[i + 2], hws[i + 3]);
	}
	return 0;
}

static const char *ring_str(int ring)
{
	switch (ring) {
	case RCS: return "render";
	case VCS: return "bsd";
	case BCS: return "blt";
	case VECS: return "vebox";
	default: return "";
	}
}

static const char *pin_flag(int pinned)
{
	if (pinned > 0)
		return " P";
	else if (pinned < 0)
		return " p";
	else
		return "";
}

static const char *tiling_flag(int tiling)
{
	switch (tiling) {
	default:
	case I915_TILING_NONE: return "";
	case I915_TILING_X: return " X";
	case I915_TILING_Y: return " Y";
	}
}

static const char *dirty_flag(int dirty)
{
	return dirty ? " dirty" : "";
}

static const char *purgeable_flag(int purgeable)
{
	return purgeable ? " purgeable" : "";
}

static bool __i915_error_ok(struct drm_i915_error_state_buf *e)
{

	if (!e->err && WARN(e->bytes > (e->size - 1), "overflow")) {
		e->err = -ENOSPC;
		return false;
	}

	if (e->bytes == e->size - 1 || e->err)
		return false;

	return true;
}

static bool __i915_error_seek(struct drm_i915_error_state_buf *e,
			      unsigned len)
{
	if (e->pos + len <= e->start) {
		e->pos += len;
		return false;
	}

	/* First vsnprintf needs to fit in its entirety for memmove */
	if (len >= e->size) {
		e->err = -EIO;
		return false;
	}

	return true;
}

static void __i915_error_advance(struct drm_i915_error_state_buf *e,
				 unsigned len)
{
	/* If this is first printf in this window, adjust it so that
	 * start position matches start of the buffer
	 */

	if (e->pos < e->start) {
		const size_t off = e->start - e->pos;

		/* Should not happen but be paranoid */
		if (off > len || e->bytes) {
			e->err = -EIO;
			return;
		}

		memmove(e->buf, e->buf + off, len - off);
		e->bytes = len - off;
		e->pos = e->start;
		return;
	}

	e->bytes += len;
	e->pos += len;
}

static void i915_error_vprintf(struct drm_i915_error_state_buf *e,
			       const char *f, va_list args)
{
	unsigned len;

	if (!__i915_error_ok(e))
		return;

	/* Seek the first printf which is hits start position */
	if (e->pos < e->start) {
		len = vsnprintf(NULL, 0, f, args);
		if (!__i915_error_seek(e, len))
			return;
	}

	len = vsnprintf(e->buf + e->bytes, e->size - e->bytes, f, args);
	if (len >= e->size - e->bytes)
		len = e->size - e->bytes - 1;

	__i915_error_advance(e, len);
}

static void i915_error_puts(struct drm_i915_error_state_buf *e,
			    const char *str)
{
	unsigned len;

	if (!__i915_error_ok(e))
		return;

	len = strlen(str);

	/* Seek the first printf which is hits start position */
	if (e->pos < e->start) {
		if (!__i915_error_seek(e, len))
			return;
	}

	if (len >= e->size - e->bytes)
		len = e->size - e->bytes - 1;
	memcpy(e->buf + e->bytes, str, len);

	__i915_error_advance(e, len);
}

void i915_error_printf(struct drm_i915_error_state_buf *e, const char *f, ...)
{
	va_list args;

	va_start(args, f);
	i915_error_vprintf(e, f, args);
	va_end(args);
}

#define err_printf(e, ...) i915_error_printf(e, __VA_ARGS__)
#define err_puts(e, s) i915_error_puts(e, s)

static void print_error_buffers(struct drm_i915_error_state_buf *m,
				const char *name,
				struct drm_i915_error_buffer *err,
				int count)
{
	err_printf(m, "%s [%d]:\n", name, count);

	while (count--) {
		err_printf(m, "  %08x %8u %02x %02x %x %x",
			   err->gtt_offset,
			   err->size,
			   err->read_domains,
			   err->write_domain,
			   err->rseqno, err->wseqno);
		err_puts(m, pin_flag(err->pinned));
		err_puts(m, tiling_flag(err->tiling));
		err_puts(m, dirty_flag(err->dirty));
		err_puts(m, purgeable_flag(err->purgeable));
		err_puts(m, err->ring != -1 ? " " : "");
		err_puts(m, ring_str(err->ring));
		err_puts(m, cache_level_str(err->cache_level));

		if (err->name)
			err_printf(m, " (name: %d)", err->name);
		if (err->fence_reg != I915_FENCE_REG_NONE)
			err_printf(m, " (fence: %d)", err->fence_reg);

		err_puts(m, "\n");
		err++;
	}
}

static void i915_ring_error_state(struct drm_i915_error_state_buf *m,
				  struct drm_device *dev,
				  struct drm_i915_error_state *error,
				  unsigned ring)
{
	BUG_ON(ring >= I915_NUM_RINGS); /* shut up confused gcc */
	err_printf(m, "%s command stream:\n", ring_str(ring));
	err_printf(m, "  HEAD: 0x%08x\n", error->head[ring]);
	err_printf(m, "  TAIL: 0x%08x\n", error->tail[ring]);
	err_printf(m, "  CTL: 0x%08x\n", error->ctl[ring]);
	err_printf(m, "  ACTHD: 0x%08x\n", error->acthd[ring]);
	err_printf(m, "  IPEIR: 0x%08x\n", error->ipeir[ring]);
	err_printf(m, "  IPEHR: 0x%08x\n", error->ipehr[ring]);
	err_printf(m, "  INSTDONE: 0x%08x\n", error->instdone[ring]);
	if (ring == RCS && INTEL_INFO(dev)->gen >= 4)
		err_printf(m, "  BBADDR: 0x%08llx\n", error->bbaddr);

	if (INTEL_INFO(dev)->gen >= 4)
		err_printf(m, "  INSTPS: 0x%08x\n", error->instps[ring]);
	err_printf(m, "  INSTPM: 0x%08x\n", error->instpm[ring]);
	err_printf(m, "  FADDR: 0x%08x\n", error->faddr[ring]);
	if (INTEL_INFO(dev)->gen >= 6) {
		err_printf(m, "  RC PSMI: 0x%08x\n", error->rc_psmi[ring]);
		err_printf(m, "  FAULT_REG: 0x%08x\n", error->fault_reg[ring]);
		err_printf(m, "  SYNC_0: 0x%08x [last synced 0x%08x]\n",
			   error->semaphore_mboxes[ring][0],
			   error->semaphore_seqno[ring][0]);
		err_printf(m, "  SYNC_1: 0x%08x [last synced 0x%08x]\n",
			   error->semaphore_mboxes[ring][1],
			   error->semaphore_seqno[ring][1]);
	}
	err_printf(m, "  seqno: 0x%08x\n", error->seqno[ring]);
	err_printf(m, "  waiting: %s\n", yesno(error->waiting[ring]));
	err_printf(m, "  ring->head: 0x%08x\n", error->cpu_ring_head[ring]);
	err_printf(m, "  ring->tail: 0x%08x\n", error->cpu_ring_tail[ring]);
}

struct i915_error_state_file_priv {
	struct drm_device *dev;
	struct drm_i915_error_state *error;
};


static int i915_error_state(struct i915_error_state_file_priv *error_priv,
			    struct drm_i915_error_state_buf *m)

{
	struct drm_device *dev = error_priv->dev;
	drm_i915_private_t *dev_priv = dev->dev_private;
	struct drm_i915_error_state *error = error_priv->error;
	struct intel_ring_buffer *ring;
	int i, j, page, offset, elt;

	if (!error) {
		err_printf(m, "no error state collected\n");
		return 0;
	}

	err_printf(m, "Time: %ld s %ld us\n", error->time.tv_sec,
		   error->time.tv_usec);
	err_printf(m, "Kernel: " UTS_RELEASE "\n");
	err_printf(m, "PCI ID: 0x%04x\n", dev->pci_device);
	err_printf(m, "EIR: 0x%08x\n", error->eir);
	err_printf(m, "IER: 0x%08x\n", error->ier);
	err_printf(m, "PGTBL_ER: 0x%08x\n", error->pgtbl_er);
	err_printf(m, "FORCEWAKE: 0x%08x\n", error->forcewake);
	err_printf(m, "DERRMR: 0x%08x\n", error->derrmr);
	err_printf(m, "CCID: 0x%08x\n", error->ccid);

	for (i = 0; i < dev_priv->num_fence_regs; i++)
		err_printf(m, "  fence[%d] = %08llx\n", i, error->fence[i]);

	for (i = 0; i < ARRAY_SIZE(error->extra_instdone); i++)
		err_printf(m, "  INSTDONE_%d: 0x%08x\n", i,
			   error->extra_instdone[i]);

	if (INTEL_INFO(dev)->gen >= 6) {
		err_printf(m, "ERROR: 0x%08x\n", error->error);
		err_printf(m, "DONE_REG: 0x%08x\n", error->done_reg);
	}

	if (INTEL_INFO(dev)->gen == 7)
		err_printf(m, "ERR_INT: 0x%08x\n", error->err_int);

	for_each_ring(ring, dev_priv, i)
		i915_ring_error_state(m, dev, error, i);

	if (error->active_bo)
		print_error_buffers(m, "Active",
				    error->active_bo,
				    error->active_bo_count);

	if (error->pinned_bo)
		print_error_buffers(m, "Pinned",
				    error->pinned_bo,
				    error->pinned_bo_count);

	for (i = 0; i < ARRAY_SIZE(error->ring); i++) {
		struct drm_i915_error_object *obj;

		if ((obj = error->ring[i].batchbuffer)) {
			err_printf(m, "%s --- gtt_offset = 0x%08x\n",
				   dev_priv->ring[i].name,
				   obj->gtt_offset);
			offset = 0;
			for (page = 0; page < obj->page_count; page++) {
				for (elt = 0; elt < PAGE_SIZE/4; elt++) {
					err_printf(m, "%08x :  %08x\n", offset,
						   obj->pages[page][elt]);
					offset += 4;
				}
			}
		}

		if (error->ring[i].num_requests) {
			err_printf(m, "%s --- %d requests\n",
				   dev_priv->ring[i].name,
				   error->ring[i].num_requests);
			for (j = 0; j < error->ring[i].num_requests; j++) {
				err_printf(m, "  seqno 0x%08x, emitted %ld, tail 0x%08x\n",
					   error->ring[i].requests[j].seqno,
					   error->ring[i].requests[j].jiffies,
					   error->ring[i].requests[j].tail);
			}
		}

		if ((obj = error->ring[i].ringbuffer)) {
			err_printf(m, "%s --- ringbuffer = 0x%08x\n",
				   dev_priv->ring[i].name,
				   obj->gtt_offset);
			offset = 0;
			for (page = 0; page < obj->page_count; page++) {
				for (elt = 0; elt < PAGE_SIZE/4; elt++) {
					err_printf(m, "%08x :  %08x\n",
						   offset,
						   obj->pages[page][elt]);
					offset += 4;
				}
			}
		}

		obj = error->ring[i].ctx;
		if (obj) {
			err_printf(m, "%s --- HW Context = 0x%08x\n",
				   dev_priv->ring[i].name,
				   obj->gtt_offset);
			offset = 0;
			for (elt = 0; elt < PAGE_SIZE/16; elt += 4) {
				err_printf(m, "[%04x] %08x %08x %08x %08x\n",
					   offset,
					   obj->pages[0][elt],
					   obj->pages[0][elt+1],
					   obj->pages[0][elt+2],
					   obj->pages[0][elt+3]);
					offset += 16;
			}
		}
	}

	if (error->overlay)
		intel_overlay_print_error_state(m, error->overlay);

	if (error->display)
		intel_display_print_error_state(m, dev, error->display);

	return 0;
}

static ssize_t
i915_error_state_write(struct file *filp,
		       const char __user *ubuf,
		       size_t cnt,
		       loff_t *ppos)
{
	struct i915_error_state_file_priv *error_priv = filp->private_data;
	struct drm_device *dev = error_priv->dev;
	int ret;

	DRM_DEBUG_DRIVER("Resetting error state\n");

	ret = mutex_lock_interruptible(&dev->struct_mutex);
	if (ret)
		return ret;

	i915_destroy_error_state(dev);
	mutex_unlock(&dev->struct_mutex);

	return cnt;
}

static int i915_error_state_open(struct inode *inode, struct file *file)
{
	struct drm_device *dev = inode->i_private;
	drm_i915_private_t *dev_priv = dev->dev_private;
	struct i915_error_state_file_priv *error_priv;
	unsigned long flags;

	error_priv = kzalloc(sizeof(*error_priv), GFP_KERNEL);
	if (!error_priv)
		return -ENOMEM;

	error_priv->dev = dev;

	spin_lock_irqsave(&dev_priv->gpu_error.lock, flags);
	error_priv->error = dev_priv->gpu_error.first_error;
	if (error_priv->error)
		kref_get(&error_priv->error->ref);
	spin_unlock_irqrestore(&dev_priv->gpu_error.lock, flags);

	file->private_data = error_priv;

	return 0;
}

static int i915_error_state_release(struct inode *inode, struct file *file)
{
	struct i915_error_state_file_priv *error_priv = file->private_data;

	if (error_priv->error)
		kref_put(&error_priv->error->ref, i915_error_state_free);
	kfree(error_priv);

	return 0;
}

static ssize_t i915_error_state_read(struct file *file, char __user *userbuf,
				     size_t count, loff_t *pos)
{
	struct i915_error_state_file_priv *error_priv = file->private_data;
	struct drm_i915_error_state_buf error_str;
	loff_t tmp_pos = 0;
	ssize_t ret_count = 0;
	int ret = 0;

	memset(&error_str, 0, sizeof(error_str));

	/* We need to have enough room to store any i915_error_state printf
	 * so that we can move it to start position.
	 */
	error_str.size = count + 1 > PAGE_SIZE ? count + 1 : PAGE_SIZE;
	error_str.buf = kmalloc(error_str.size,
				GFP_TEMPORARY | __GFP_NORETRY | __GFP_NOWARN);

	if (error_str.buf == NULL) {
		error_str.size = PAGE_SIZE;
		error_str.buf = kmalloc(error_str.size, GFP_TEMPORARY);
	}

	if (error_str.buf == NULL) {
		error_str.size = 128;
		error_str.buf = kmalloc(error_str.size, GFP_TEMPORARY);
	}

	if (error_str.buf == NULL)
		return -ENOMEM;

	error_str.start = *pos;

	ret = i915_error_state(error_priv, &error_str);
	if (ret)
		goto out;

	if (error_str.bytes == 0 && error_str.err) {
		ret = error_str.err;
		goto out;
	}

	ret_count = simple_read_from_buffer(userbuf, count, &tmp_pos,
					    error_str.buf,
					    error_str.bytes);

	if (ret_count < 0)
		ret = ret_count;
	else
		*pos = error_str.start + ret_count;
out:
	kfree(error_str.buf);
	return ret ?: ret_count;
}

static const struct file_operations i915_error_state_fops = {
	.owner = THIS_MODULE,
	.open = i915_error_state_open,
	.read = i915_error_state_read,
	.write = i915_error_state_write,
	.llseek = default_llseek,
	.release = i915_error_state_release,
};

static int
i915_next_seqno_get(void *data, u64 *val)
{
	struct drm_device *dev = data;
	drm_i915_private_t *dev_priv = dev->dev_private;
	int ret;

	ret = mutex_lock_interruptible(&dev->struct_mutex);
	if (ret)
		return ret;

	*val = dev_priv->next_seqno;
	mutex_unlock(&dev->struct_mutex);

	return 0;
}

static int
i915_next_seqno_set(void *data, u64 val)
{
	struct drm_device *dev = data;
	int ret;

	ret = mutex_lock_interruptible(&dev->struct_mutex);
	if (ret)
		return ret;

	ret = i915_gem_set_seqno(dev, val);
	mutex_unlock(&dev->struct_mutex);

	return ret;
}

DEFINE_SIMPLE_ATTRIBUTE(i915_next_seqno_fops,
			i915_next_seqno_get, i915_next_seqno_set,
			"0x%llx\n");

static int i915_rstdby_delays(struct seq_file *m, void *unused)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	drm_i915_private_t *dev_priv = dev->dev_private;
	u16 crstanddelay;
	int ret;

	ret = mutex_lock_interruptible(&dev->struct_mutex);
	if (ret)
		return ret;

	crstanddelay = I915_READ16(CRSTANDVID);

	mutex_unlock(&dev->struct_mutex);

	seq_printf(m, "w/ctx: %d, w/o ctx: %d\n", (crstanddelay >> 8) & 0x3f, (crstanddelay & 0x3f));

	return 0;
}

static int i915_cur_delayinfo(struct seq_file *m, void *unused)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	drm_i915_private_t *dev_priv = dev->dev_private;
	int ret;

	if (IS_GEN5(dev)) {
		u16 rgvswctl = I915_READ16(MEMSWCTL);
		u16 rgvstat = I915_READ16(MEMSTAT_ILK);

		seq_printf(m, "Requested P-state: %d\n", (rgvswctl >> 8) & 0xf);
		seq_printf(m, "Requested VID: %d\n", rgvswctl & 0x3f);
		seq_printf(m, "Current VID: %d\n", (rgvstat & MEMSTAT_VID_MASK) >>
			   MEMSTAT_VID_SHIFT);
		seq_printf(m, "Current P-state: %d\n",
			   (rgvstat & MEMSTAT_PSTATE_MASK) >> MEMSTAT_PSTATE_SHIFT);
	} else if ((IS_GEN6(dev) || IS_GEN7(dev)) && !IS_VALLEYVIEW(dev)) {
		u32 gt_perf_status = I915_READ(GEN6_GT_PERF_STATUS);
		u32 rp_state_limits = I915_READ(GEN6_RP_STATE_LIMITS);
		u32 rp_state_cap = I915_READ(GEN6_RP_STATE_CAP);
		u32 rpstat, cagf;
		u32 rpupei, rpcurup, rpprevup;
		u32 rpdownei, rpcurdown, rpprevdown;
		int max_freq;

		/* RPSTAT1 is in the GT power well */
		ret = mutex_lock_interruptible(&dev->struct_mutex);
		if (ret)
			return ret;

		gen6_gt_force_wake_get(dev_priv);

		rpstat = I915_READ(GEN6_RPSTAT1);
		rpupei = I915_READ(GEN6_RP_CUR_UP_EI);
		rpcurup = I915_READ(GEN6_RP_CUR_UP);
		rpprevup = I915_READ(GEN6_RP_PREV_UP);
		rpdownei = I915_READ(GEN6_RP_CUR_DOWN_EI);
		rpcurdown = I915_READ(GEN6_RP_CUR_DOWN);
		rpprevdown = I915_READ(GEN6_RP_PREV_DOWN);
		if (IS_HASWELL(dev))
			cagf = (rpstat & HSW_CAGF_MASK) >> HSW_CAGF_SHIFT;
		else
			cagf = (rpstat & GEN6_CAGF_MASK) >> GEN6_CAGF_SHIFT;
		cagf *= GT_FREQUENCY_MULTIPLIER;

		gen6_gt_force_wake_put(dev_priv);
		mutex_unlock(&dev->struct_mutex);

		seq_printf(m, "GT_PERF_STATUS: 0x%08x\n", gt_perf_status);
		seq_printf(m, "RPSTAT1: 0x%08x\n", rpstat);
		seq_printf(m, "Render p-state ratio: %d\n",
			   (gt_perf_status & 0xff00) >> 8);
		seq_printf(m, "Render p-state VID: %d\n",
			   gt_perf_status & 0xff);
		seq_printf(m, "Render p-state limit: %d\n",
			   rp_state_limits & 0xff);
		seq_printf(m, "CAGF: %dMHz\n", cagf);
		seq_printf(m, "RP CUR UP EI: %dus\n", rpupei &
			   GEN6_CURICONT_MASK);
		seq_printf(m, "RP CUR UP: %dus\n", rpcurup &
			   GEN6_CURBSYTAVG_MASK);
		seq_printf(m, "RP PREV UP: %dus\n", rpprevup &
			   GEN6_CURBSYTAVG_MASK);
		seq_printf(m, "RP CUR DOWN EI: %dus\n", rpdownei &
			   GEN6_CURIAVG_MASK);
		seq_printf(m, "RP CUR DOWN: %dus\n", rpcurdown &
			   GEN6_CURBSYTAVG_MASK);
		seq_printf(m, "RP PREV DOWN: %dus\n", rpprevdown &
			   GEN6_CURBSYTAVG_MASK);

		max_freq = (rp_state_cap & 0xff0000) >> 16;
		seq_printf(m, "Lowest (RPN) frequency: %dMHz\n",
			   max_freq * GT_FREQUENCY_MULTIPLIER);

		max_freq = (rp_state_cap & 0xff00) >> 8;
		seq_printf(m, "Nominal (RP1) frequency: %dMHz\n",
			   max_freq * GT_FREQUENCY_MULTIPLIER);

		max_freq = rp_state_cap & 0xff;
		seq_printf(m, "Max non-overclocked (RP0) frequency: %dMHz\n",
			   max_freq * GT_FREQUENCY_MULTIPLIER);

		seq_printf(m, "Max overclocked frequency: %dMHz\n",
			   dev_priv->rps.hw_max * GT_FREQUENCY_MULTIPLIER);
	} else if (IS_VALLEYVIEW(dev)) {
		u32 freq_sts, val;

		mutex_lock(&dev_priv->rps.hw_lock);
		freq_sts = vlv_punit_read(dev_priv, PUNIT_REG_GPU_FREQ_STS);
		seq_printf(m, "PUNIT_REG_GPU_FREQ_STS: 0x%08x\n", freq_sts);
		seq_printf(m, "DDR freq: %d MHz\n", dev_priv->mem_freq);

		val = vlv_punit_read(dev_priv, PUNIT_FUSE_BUS1);
		seq_printf(m, "max GPU freq: %d MHz\n",
			   vlv_gpu_freq(dev_priv->mem_freq, val));

		val = vlv_punit_read(dev_priv, PUNIT_REG_GPU_LFM);
		seq_printf(m, "min GPU freq: %d MHz\n",
			   vlv_gpu_freq(dev_priv->mem_freq, val));

		seq_printf(m, "current GPU freq: %d MHz\n",
			   vlv_gpu_freq(dev_priv->mem_freq,
					(freq_sts >> 8) & 0xff));
		mutex_unlock(&dev_priv->rps.hw_lock);
	} else {
		seq_printf(m, "no P-state info available\n");
	}

	return 0;
}

static int i915_delayfreq_table(struct seq_file *m, void *unused)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	drm_i915_private_t *dev_priv = dev->dev_private;
	u32 delayfreq;
	int ret, i;

	ret = mutex_lock_interruptible(&dev->struct_mutex);
	if (ret)
		return ret;

	for (i = 0; i < 16; i++) {
		delayfreq = I915_READ(PXVFREQ_BASE + i * 4);
		seq_printf(m, "P%02dVIDFREQ: 0x%08x (VID: %d)\n", i, delayfreq,
			   (delayfreq & PXVFREQ_PX_MASK) >> PXVFREQ_PX_SHIFT);
	}

	mutex_unlock(&dev->struct_mutex);

	return 0;
}

static inline int MAP_TO_MV(int map)
{
	return 1250 - (map * 25);
}

static int i915_inttoext_table(struct seq_file *m, void *unused)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	drm_i915_private_t *dev_priv = dev->dev_private;
	u32 inttoext;
	int ret, i;

	ret = mutex_lock_interruptible(&dev->struct_mutex);
	if (ret)
		return ret;

	for (i = 1; i <= 32; i++) {
		inttoext = I915_READ(INTTOEXT_BASE_ILK + i * 4);
		seq_printf(m, "INTTOEXT%02d: 0x%08x\n", i, inttoext);
	}

	mutex_unlock(&dev->struct_mutex);

	return 0;
}

static int ironlake_drpc_info(struct seq_file *m)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	drm_i915_private_t *dev_priv = dev->dev_private;
	u32 rgvmodectl, rstdbyctl;
	u16 crstandvid;
	int ret;

	ret = mutex_lock_interruptible(&dev->struct_mutex);
	if (ret)
		return ret;

	rgvmodectl = I915_READ(MEMMODECTL);
	rstdbyctl = I915_READ(RSTDBYCTL);
	crstandvid = I915_READ16(CRSTANDVID);

	mutex_unlock(&dev->struct_mutex);

	seq_printf(m, "HD boost: %s\n", (rgvmodectl & MEMMODE_BOOST_EN) ?
		   "yes" : "no");
	seq_printf(m, "Boost freq: %d\n",
		   (rgvmodectl & MEMMODE_BOOST_FREQ_MASK) >>
		   MEMMODE_BOOST_FREQ_SHIFT);
	seq_printf(m, "HW control enabled: %s\n",
		   rgvmodectl & MEMMODE_HWIDLE_EN ? "yes" : "no");
	seq_printf(m, "SW control enabled: %s\n",
		   rgvmodectl & MEMMODE_SWMODE_EN ? "yes" : "no");
	seq_printf(m, "Gated voltage change: %s\n",
		   rgvmodectl & MEMMODE_RCLK_GATE ? "yes" : "no");
	seq_printf(m, "Starting frequency: P%d\n",
		   (rgvmodectl & MEMMODE_FSTART_MASK) >> MEMMODE_FSTART_SHIFT);
	seq_printf(m, "Max P-state: P%d\n",
		   (rgvmodectl & MEMMODE_FMAX_MASK) >> MEMMODE_FMAX_SHIFT);
	seq_printf(m, "Min P-state: P%d\n", (rgvmodectl & MEMMODE_FMIN_MASK));
	seq_printf(m, "RS1 VID: %d\n", (crstandvid & 0x3f));
	seq_printf(m, "RS2 VID: %d\n", ((crstandvid >> 8) & 0x3f));
	seq_printf(m, "Render standby enabled: %s\n",
		   (rstdbyctl & RCX_SW_EXIT) ? "no" : "yes");
	seq_printf(m, "Current RS state: ");
	switch (rstdbyctl & RSX_STATUS_MASK) {
	case RSX_STATUS_ON:
		seq_printf(m, "on\n");
		break;
	case RSX_STATUS_RC1:
		seq_printf(m, "RC1\n");
		break;
	case RSX_STATUS_RC1E:
		seq_printf(m, "RC1E\n");
		break;
	case RSX_STATUS_RS1:
		seq_printf(m, "RS1\n");
		break;
	case RSX_STATUS_RS2:
		seq_printf(m, "RS2 (RC6)\n");
		break;
	case RSX_STATUS_RS3:
		seq_printf(m, "RC3 (RC6+)\n");
		break;
	default:
		seq_printf(m, "unknown\n");
		break;
	}

	return 0;
}

static int gen6_drpc_info(struct seq_file *m)
{

	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	struct drm_i915_private *dev_priv = dev->dev_private;
	u32 rpmodectl1, gt_core_status, rcctl1, rc6vids = 0;
	unsigned forcewake_count;
	int count=0, ret;


	ret = mutex_lock_interruptible(&dev->struct_mutex);
	if (ret)
		return ret;

	spin_lock_irq(&dev_priv->gt_lock);
	forcewake_count = dev_priv->forcewake_count;
	spin_unlock_irq(&dev_priv->gt_lock);

	if (forcewake_count) {
		seq_printf(m, "RC information inaccurate because somebody "
			      "holds a forcewake reference \n");
	} else {
		/* NB: we cannot use forcewake, else we read the wrong values */
		while (count++ < 50 && (I915_READ_NOTRACE(FORCEWAKE_ACK) & 1))
			udelay(10);
		seq_printf(m, "RC information accurate: %s\n", yesno(count < 51));
	}

	gt_core_status = readl(dev_priv->regs + GEN6_GT_CORE_STATUS);
	trace_i915_reg_rw(false, GEN6_GT_CORE_STATUS, gt_core_status, 4);

	rpmodectl1 = I915_READ(GEN6_RP_CONTROL);
	rcctl1 = I915_READ(GEN6_RC_CONTROL);
	mutex_unlock(&dev->struct_mutex);
	mutex_lock(&dev_priv->rps.hw_lock);
	sandybridge_pcode_read(dev_priv, GEN6_PCODE_READ_RC6VIDS, &rc6vids);
	mutex_unlock(&dev_priv->rps.hw_lock);

	seq_printf(m, "Video Turbo Mode: %s\n",
		   yesno(rpmodectl1 & GEN6_RP_MEDIA_TURBO));
	seq_printf(m, "HW control enabled: %s\n",
		   yesno(rpmodectl1 & GEN6_RP_ENABLE));
	seq_printf(m, "SW control enabled: %s\n",
		   yesno((rpmodectl1 & GEN6_RP_MEDIA_MODE_MASK) ==
			  GEN6_RP_MEDIA_SW_MODE));
	seq_printf(m, "RC1e Enabled: %s\n",
		   yesno(rcctl1 & GEN6_RC_CTL_RC1e_ENABLE));
	seq_printf(m, "RC6 Enabled: %s\n",
		   yesno(rcctl1 & GEN6_RC_CTL_RC6_ENABLE));
	seq_printf(m, "Deep RC6 Enabled: %s\n",
		   yesno(rcctl1 & GEN6_RC_CTL_RC6p_ENABLE));
	seq_printf(m, "Deepest RC6 Enabled: %s\n",
		   yesno(rcctl1 & GEN6_RC_CTL_RC6pp_ENABLE));
	seq_printf(m, "Current RC state: ");
	switch (gt_core_status & GEN6_RCn_MASK) {
	case GEN6_RC0:
		if (gt_core_status & GEN6_CORE_CPD_STATE_MASK)
			seq_printf(m, "Core Power Down\n");
		else
			seq_printf(m, "on\n");
		break;
	case GEN6_RC3:
		seq_printf(m, "RC3\n");
		break;
	case GEN6_RC6:
		seq_printf(m, "RC6\n");
		break;
	case GEN6_RC7:
		seq_printf(m, "RC7\n");
		break;
	default:
		seq_printf(m, "Unknown\n");
		break;
	}

	seq_printf(m, "Core Power Down: %s\n",
		   yesno(gt_core_status & GEN6_CORE_CPD_STATE_MASK));

	/* Not exactly sure what this is */
	seq_printf(m, "RC6 \"Locked to RPn\" residency since boot: %u\n",
		   I915_READ(GEN6_GT_GFX_RC6_LOCKED));
	seq_printf(m, "RC6 residency since boot: %u\n",
		   I915_READ(GEN6_GT_GFX_RC6));
	seq_printf(m, "RC6+ residency since boot: %u\n",
		   I915_READ(GEN6_GT_GFX_RC6p));
	seq_printf(m, "RC6++ residency since boot: %u\n",
		   I915_READ(GEN6_GT_GFX_RC6pp));

	seq_printf(m, "RC6   voltage: %dmV\n",
		   GEN6_DECODE_RC6_VID(((rc6vids >> 0) & 0xff)));
	seq_printf(m, "RC6+  voltage: %dmV\n",
		   GEN6_DECODE_RC6_VID(((rc6vids >> 8) & 0xff)));
	seq_printf(m, "RC6++ voltage: %dmV\n",
		   GEN6_DECODE_RC6_VID(((rc6vids >> 16) & 0xff)));
	return 0;
}

static int i915_drpc_info(struct seq_file *m, void *unused)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;

	if (IS_GEN6(dev) || IS_GEN7(dev))
		return gen6_drpc_info(m);
	else
		return ironlake_drpc_info(m);
}

static int i915_fbc_status(struct seq_file *m, void *unused)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	drm_i915_private_t *dev_priv = dev->dev_private;

	if (!I915_HAS_FBC(dev)) {
		seq_printf(m, "FBC unsupported on this chipset\n");
		return 0;
	}

	if (intel_fbc_enabled(dev)) {
		seq_printf(m, "FBC enabled\n");
	} else {
		seq_printf(m, "FBC disabled: ");
		switch (dev_priv->no_fbc_reason) {
		case FBC_NO_OUTPUT:
			seq_printf(m, "no outputs");
			break;
		case FBC_STOLEN_TOO_SMALL:
			seq_printf(m, "not enough stolen memory");
			break;
		case FBC_UNSUPPORTED_MODE:
			seq_printf(m, "mode not supported");
			break;
		case FBC_MODE_TOO_LARGE:
			seq_printf(m, "mode too large");
			break;
		case FBC_BAD_PLANE:
			seq_printf(m, "FBC unsupported on plane");
			break;
		case FBC_NOT_TILED:
			seq_printf(m, "scanout buffer not tiled");
			break;
		case FBC_MULTIPLE_PIPES:
			seq_printf(m, "multiple pipes are enabled");
			break;
		case FBC_MODULE_PARAM:
			seq_printf(m, "disabled per module param (default off)");
			break;
		default:
			seq_printf(m, "unknown reason");
		}
		seq_printf(m, "\n");
	}
	return 0;
}

static int i915_ips_status(struct seq_file *m, void *unused)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	struct drm_i915_private *dev_priv = dev->dev_private;

	if (!HAS_IPS(dev)) {
		seq_puts(m, "not supported\n");
		return 0;
	}

	if (I915_READ(IPS_CTL) & IPS_ENABLE)
		seq_puts(m, "enabled\n");
	else
		seq_puts(m, "disabled\n");

	return 0;
}

static int i915_sr_status(struct seq_file *m, void *unused)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	drm_i915_private_t *dev_priv = dev->dev_private;
	bool sr_enabled = false;

	if (HAS_PCH_SPLIT(dev))
		sr_enabled = I915_READ(WM1_LP_ILK) & WM1_LP_SR_EN;
	else if (IS_CRESTLINE(dev) || IS_I945G(dev) || IS_I945GM(dev))
		sr_enabled = I915_READ(FW_BLC_SELF) & FW_BLC_SELF_EN;
	else if (IS_I915GM(dev))
		sr_enabled = I915_READ(INSTPM) & INSTPM_SELF_EN;
	else if (IS_PINEVIEW(dev))
		sr_enabled = I915_READ(DSPFW3) & PINEVIEW_SELF_REFRESH_EN;

	seq_printf(m, "self-refresh: %s\n",
		   sr_enabled ? "enabled" : "disabled");

	return 0;
}

static int i915_emon_status(struct seq_file *m, void *unused)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	drm_i915_private_t *dev_priv = dev->dev_private;
	unsigned long temp, chipset, gfx;
	int ret;

	if (!IS_GEN5(dev))
		return -ENODEV;

	ret = mutex_lock_interruptible(&dev->struct_mutex);
	if (ret)
		return ret;

	temp = i915_mch_val(dev_priv);
	chipset = i915_chipset_val(dev_priv);
	gfx = i915_gfx_val(dev_priv);
	mutex_unlock(&dev->struct_mutex);

	seq_printf(m, "GMCH temp: %ld\n", temp);
	seq_printf(m, "Chipset power: %ld\n", chipset);
	seq_printf(m, "GFX power: %ld\n", gfx);
	seq_printf(m, "Total power: %ld\n", chipset + gfx);

	return 0;
}

static int i915_ring_freq_table(struct seq_file *m, void *unused)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	drm_i915_private_t *dev_priv = dev->dev_private;
	int ret;
	int gpu_freq, ia_freq;

	if (!(IS_GEN6(dev) || IS_GEN7(dev))) {
		seq_printf(m, "unsupported on this chipset\n");
		return 0;
	}

	ret = mutex_lock_interruptible(&dev_priv->rps.hw_lock);
	if (ret)
		return ret;

	seq_printf(m, "GPU freq (MHz)\tEffective CPU freq (MHz)\tEffective Ring freq (MHz)\n");

	for (gpu_freq = dev_priv->rps.min_delay;
	     gpu_freq <= dev_priv->rps.max_delay;
	     gpu_freq++) {
		ia_freq = gpu_freq;
		sandybridge_pcode_read(dev_priv,
				       GEN6_PCODE_READ_MIN_FREQ_TABLE,
				       &ia_freq);
		seq_printf(m, "%d\t\t%d\t\t\t\t%d\n",
			   gpu_freq * GT_FREQUENCY_MULTIPLIER,
			   ((ia_freq >> 0) & 0xff) * 100,
			   ((ia_freq >> 8) & 0xff) * 100);
	}

	mutex_unlock(&dev_priv->rps.hw_lock);

	return 0;
}

static int i915_gfxec(struct seq_file *m, void *unused)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	drm_i915_private_t *dev_priv = dev->dev_private;
	int ret;

	ret = mutex_lock_interruptible(&dev->struct_mutex);
	if (ret)
		return ret;

	seq_printf(m, "GFXEC: %ld\n", (unsigned long)I915_READ(0x112f4));

	mutex_unlock(&dev->struct_mutex);

	return 0;
}

static int i915_opregion(struct seq_file *m, void *unused)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	drm_i915_private_t *dev_priv = dev->dev_private;
	struct intel_opregion *opregion = &dev_priv->opregion;
	void *data = kmalloc(OPREGION_SIZE, GFP_KERNEL);
	int ret;

	if (data == NULL)
		return -ENOMEM;

	ret = mutex_lock_interruptible(&dev->struct_mutex);
	if (ret)
		goto out;

	if (opregion->header) {
		memcpy_fromio(data, opregion->header, OPREGION_SIZE);
		seq_write(m, data, OPREGION_SIZE);
	}

	mutex_unlock(&dev->struct_mutex);

out:
	kfree(data);
	return 0;
}

static int i915_gem_framebuffer_info(struct seq_file *m, void *data)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	drm_i915_private_t *dev_priv = dev->dev_private;
	struct intel_fbdev *ifbdev;
	struct intel_framebuffer *fb;
	int ret;

	ret = mutex_lock_interruptible(&dev->mode_config.mutex);
	if (ret)
		return ret;

	ifbdev = dev_priv->fbdev;
	fb = to_intel_framebuffer(ifbdev->helper.fb);

	seq_printf(m, "fbcon size: %d x %d, depth %d, %d bpp, refcount %d, obj ",
		   fb->base.width,
		   fb->base.height,
		   fb->base.depth,
		   fb->base.bits_per_pixel,
		   atomic_read(&fb->base.refcount.refcount));
	describe_obj(m, fb->obj);
	seq_printf(m, "\n");
	mutex_unlock(&dev->mode_config.mutex);

	mutex_lock(&dev->mode_config.fb_lock);
	list_for_each_entry(fb, &dev->mode_config.fb_list, base.head) {
		if (&fb->base == ifbdev->helper.fb)
			continue;

		seq_printf(m, "user size: %d x %d, depth %d, %d bpp, refcount %d, obj ",
			   fb->base.width,
			   fb->base.height,
			   fb->base.depth,
			   fb->base.bits_per_pixel,
			   atomic_read(&fb->base.refcount.refcount));
		describe_obj(m, fb->obj);
		seq_printf(m, "\n");
	}
	mutex_unlock(&dev->mode_config.fb_lock);

	return 0;
}

static int i915_context_status(struct seq_file *m, void *unused)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	drm_i915_private_t *dev_priv = dev->dev_private;
	struct intel_ring_buffer *ring;
	int ret, i;

	ret = mutex_lock_interruptible(&dev->mode_config.mutex);
	if (ret)
		return ret;

	if (dev_priv->ips.pwrctx) {
		seq_printf(m, "power context ");
		describe_obj(m, dev_priv->ips.pwrctx);
		seq_printf(m, "\n");
	}

	if (dev_priv->ips.renderctx) {
		seq_printf(m, "render context ");
		describe_obj(m, dev_priv->ips.renderctx);
		seq_printf(m, "\n");
	}

	for_each_ring(ring, dev_priv, i) {
		if (ring->default_context) {
			seq_printf(m, "HW default context %s ring ", ring->name);
			describe_obj(m, ring->default_context->obj);
			seq_printf(m, "\n");
		}
	}

	mutex_unlock(&dev->mode_config.mutex);

	return 0;
}

static int i915_gen6_forcewake_count_info(struct seq_file *m, void *data)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	struct drm_i915_private *dev_priv = dev->dev_private;
	unsigned forcewake_count;

	spin_lock_irq(&dev_priv->gt_lock);
	forcewake_count = dev_priv->forcewake_count;
	spin_unlock_irq(&dev_priv->gt_lock);

	seq_printf(m, "forcewake count = %u\n", forcewake_count);

	return 0;
}

static const char *swizzle_string(unsigned swizzle)
{
	switch(swizzle) {
	case I915_BIT_6_SWIZZLE_NONE:
		return "none";
	case I915_BIT_6_SWIZZLE_9:
		return "bit9";
	case I915_BIT_6_SWIZZLE_9_10:
		return "bit9/bit10";
	case I915_BIT_6_SWIZZLE_9_11:
		return "bit9/bit11";
	case I915_BIT_6_SWIZZLE_9_10_11:
		return "bit9/bit10/bit11";
	case I915_BIT_6_SWIZZLE_9_17:
		return "bit9/bit17";
	case I915_BIT_6_SWIZZLE_9_10_17:
		return "bit9/bit10/bit17";
	case I915_BIT_6_SWIZZLE_UNKNOWN:
		return "unknown";
	}

	return "bug";
}

static int i915_swizzle_info(struct seq_file *m, void *data)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	struct drm_i915_private *dev_priv = dev->dev_private;
	int ret;

	ret = mutex_lock_interruptible(&dev->struct_mutex);
	if (ret)
		return ret;

	seq_printf(m, "bit6 swizzle for X-tiling = %s\n",
		   swizzle_string(dev_priv->mm.bit_6_swizzle_x));
	seq_printf(m, "bit6 swizzle for Y-tiling = %s\n",
		   swizzle_string(dev_priv->mm.bit_6_swizzle_y));

	if (IS_GEN3(dev) || IS_GEN4(dev)) {
		seq_printf(m, "DDC = 0x%08x\n",
			   I915_READ(DCC));
		seq_printf(m, "C0DRB3 = 0x%04x\n",
			   I915_READ16(C0DRB3));
		seq_printf(m, "C1DRB3 = 0x%04x\n",
			   I915_READ16(C1DRB3));
	} else if (IS_GEN6(dev) || IS_GEN7(dev)) {
		seq_printf(m, "MAD_DIMM_C0 = 0x%08x\n",
			   I915_READ(MAD_DIMM_C0));
		seq_printf(m, "MAD_DIMM_C1 = 0x%08x\n",
			   I915_READ(MAD_DIMM_C1));
		seq_printf(m, "MAD_DIMM_C2 = 0x%08x\n",
			   I915_READ(MAD_DIMM_C2));
		seq_printf(m, "TILECTL = 0x%08x\n",
			   I915_READ(TILECTL));
		seq_printf(m, "ARB_MODE = 0x%08x\n",
			   I915_READ(ARB_MODE));
		seq_printf(m, "DISP_ARB_CTL = 0x%08x\n",
			   I915_READ(DISP_ARB_CTL));
	}
	mutex_unlock(&dev->struct_mutex);

	return 0;
}

static int i915_ppgtt_info(struct seq_file *m, void *data)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	struct drm_i915_private *dev_priv = dev->dev_private;
	struct intel_ring_buffer *ring;
	int i, ret;


	ret = mutex_lock_interruptible(&dev->struct_mutex);
	if (ret)
		return ret;
	if (INTEL_INFO(dev)->gen == 6)
		seq_printf(m, "GFX_MODE: 0x%08x\n", I915_READ(GFX_MODE));

	for_each_ring(ring, dev_priv, i) {
		seq_printf(m, "%s\n", ring->name);
		if (INTEL_INFO(dev)->gen == 7)
			seq_printf(m, "GFX_MODE: 0x%08x\n", I915_READ(RING_MODE_GEN7(ring)));
		seq_printf(m, "PP_DIR_BASE: 0x%08x\n", I915_READ(RING_PP_DIR_BASE(ring)));
		seq_printf(m, "PP_DIR_BASE_READ: 0x%08x\n", I915_READ(RING_PP_DIR_BASE_READ(ring)));
		seq_printf(m, "PP_DIR_DCLV: 0x%08x\n", I915_READ(RING_PP_DIR_DCLV(ring)));
	}
	if (dev_priv->mm.aliasing_ppgtt) {
		struct i915_hw_ppgtt *ppgtt = dev_priv->mm.aliasing_ppgtt;

		seq_printf(m, "aliasing PPGTT:\n");
		seq_printf(m, "pd gtt offset: 0x%08x\n", ppgtt->pd_offset);
	}
	seq_printf(m, "ECOCHK: 0x%08x\n", I915_READ(GAM_ECOCHK));
	mutex_unlock(&dev->struct_mutex);

	return 0;
}

static int i915_dpio_info(struct seq_file *m, void *data)
{
	struct drm_info_node *node = (struct drm_info_node *) m->private;
	struct drm_device *dev = node->minor->dev;
	struct drm_i915_private *dev_priv = dev->dev_private;
	int ret;


	if (!IS_VALLEYVIEW(dev)) {
		seq_printf(m, "unsupported\n");
		return 0;
	}

	ret = mutex_lock_interruptible(&dev_priv->dpio_lock);
	if (ret)
		return ret;

	seq_printf(m, "DPIO_CTL: 0x%08x\n", I915_READ(DPIO_CTL));

	seq_printf(m, "DPIO_DIV_A: 0x%08x\n",
		   vlv_dpio_read(dev_priv, _DPIO_DIV_A));
	seq_printf(m, "DPIO_DIV_B: 0x%08x\n",
		   vlv_dpio_read(dev_priv, _DPIO_DIV_B));

	seq_printf(m, "DPIO_REFSFR_A: 0x%08x\n",
		   vlv_dpio_read(dev_priv, _DPIO_REFSFR_A));
	seq_printf(m, "DPIO_REFSFR_B: 0x%08x\n",
		   vlv_dpio_read(dev_priv, _DPIO_REFSFR_B));

	seq_printf(m, "DPIO_CORE_CLK_A: 0x%08x\n",
		   vlv_dpio_read(dev_priv, _DPIO_CORE_CLK_A));
	seq_printf(m, "DPIO_CORE_CLK_B: 0x%08x\n",
		   vlv_dpio_read(dev_priv, _DPIO_CORE_CLK_B));

	seq_printf(m, "DPIO_LPF_COEFF_A: 0x%08x\n",
		   vlv_dpio_read(dev_priv, _DPIO_LPF_COEFF_A));
	seq_printf(m, "DPIO_LPF_COEFF_B: 0x%08x\n",
		   vlv_dpio_read(dev_priv, _DPIO_LPF_COEFF_B));

	seq_printf(m, "DPIO_FASTCLK_DISABLE: 0x%08x\n",
		   vlv_dpio_read(dev_priv, DPIO_FASTCLK_DISABLE));

	mutex_unlock(&dev_priv->dpio_lock);

	return 0;
}

static int
i915_wedged_get(void *data, u64 *val)
{
	struct drm_device *dev = data;
	drm_i915_private_t *dev_priv = dev->dev_private;

	*val = atomic_read(&dev_priv->gpu_error.reset_counter);

	return 0;
}

static int
i915_wedged_set(void *data, u64 val)
{
	struct drm_device *dev = data;

	DRM_INFO("Manually setting wedged to %llu\n", val);
	i915_handle_error(dev, val);

	return 0;
}

DEFINE_SIMPLE_ATTRIBUTE(i915_wedged_fops,
			i915_wedged_get, i915_wedged_set,
			"%llu\n");

static int
i915_ring_stop_get(void *data, u64 *val)
{
	struct drm_device *dev = data;
	drm_i915_private_t *dev_priv = dev->dev_private;

	*val = dev_priv->gpu_error.stop_rings;

	return 0;
}

static int
i915_ring_stop_set(void *data, u64 val)
{
	struct drm_device *dev = data;
	struct drm_i915_private *dev_priv = dev->dev_private;
	int ret;

	DRM_DEBUG_DRIVER("Stopping rings 0x%08llx\n", val);

	ret = mutex_lock_interruptible(&dev->struct_mutex);
	if (ret)
		return ret;

	dev_priv->gpu_error.stop_rings = val;
	mutex_unlock(&dev->struct_mutex);

	return 0;
}

DEFINE_SIMPLE_ATTRIBUTE(i915_ring_stop_fops,
			i915_ring_stop_get, i915_ring_stop_set,
			"0x%08llx\n");

#define DROP_UNBOUND 0x1
#define DROP_BOUND 0x2
#define DROP_RETIRE 0x4
#define DROP_ACTIVE 0x8
#define DROP_ALL (DROP_UNBOUND | \
		  DROP_BOUND | \
		  DROP_RETIRE | \
		  DROP_ACTIVE)
static int
i915_drop_caches_get(void *data, u64 *val)
{
	*val = DROP_ALL;

	return 0;
}

static int
i915_drop_caches_set(void *data, u64 val)
{
	struct drm_device *dev = data;
	struct drm_i915_private *dev_priv = dev->dev_private;
	struct drm_i915_gem_object *obj, *next;
	int ret;

	DRM_DEBUG_DRIVER("Dropping caches: 0x%08llx\n", val);

	/* No need to check and wait for gpu resets, only libdrm auto-restarts
	 * on ioctls on -EAGAIN. */
	ret = mutex_lock_interruptible(&dev->struct_mutex);
	if (ret)
		return ret;

	if (val & DROP_ACTIVE) {
		ret = i915_gpu_idle(dev);
		if (ret)
			goto unlock;
	}

	if (val & (DROP_RETIRE | DROP_ACTIVE))
		i915_gem_retire_requests(dev);

	if (val & DROP_BOUND) {
		list_for_each_entry_safe(obj, next, &dev_priv->mm.inactive_list, mm_list)
			if (obj->pin_count == 0) {
				ret = i915_gem_object_unbind(obj);
				if (ret)
					goto unlock;
			}
	}

	if (val & DROP_UNBOUND) {
		list_for_each_entry_safe(obj, next, &dev_priv->mm.unbound_list,
					 global_list)
			if (obj->pages_pin_count == 0) {
				ret = i915_gem_object_put_pages(obj);
				if (ret)
					goto unlock;
			}
	}

unlock:
	mutex_unlock(&dev->struct_mutex);

	return ret;
}

DEFINE_SIMPLE_ATTRIBUTE(i915_drop_caches_fops,
			i915_drop_caches_get, i915_drop_caches_set,
			"0x%08llx\n");

static int
i915_max_freq_get(void *data, u64 *val)
{
	struct drm_device *dev = data;
	drm_i915_private_t *dev_priv = dev->dev_private;
	int ret;

	if (!(IS_GEN6(dev) || IS_GEN7(dev)))
		return -ENODEV;

	ret = mutex_lock_interruptible(&dev_priv->rps.hw_lock);
	if (ret)
		return ret;

	if (IS_VALLEYVIEW(dev))
		*val = vlv_gpu_freq(dev_priv->mem_freq,
				    dev_priv->rps.max_delay);
	else
		*val = dev_priv->rps.max_delay * GT_FREQUENCY_MULTIPLIER;
	mutex_unlock(&dev_priv->rps.hw_lock);

	return 0;
}

static int
i915_max_freq_set(void *data, u64 val)
{
	struct drm_device *dev = data;
	struct drm_i915_private *dev_priv = dev->dev_private;
	int ret;

	if (!(IS_GEN6(dev) || IS_GEN7(dev)))
		return -ENODEV;

	DRM_DEBUG_DRIVER("Manually setting max freq to %llu\n", val);

	ret = mutex_lock_interruptible(&dev_priv->rps.hw_lock);
	if (ret)
		return ret;

	/*
	 * Turbo will still be enabled, but won't go above the set value.
	 */
	if (IS_VALLEYVIEW(dev)) {
		val = vlv_freq_opcode(dev_priv->mem_freq, val);
		dev_priv->rps.max_delay = val;
		gen6_set_rps(dev, val);
	} else {
		do_div(val, GT_FREQUENCY_MULTIPLIER);
		dev_priv->rps.max_delay = val;
		gen6_set_rps(dev, val);
	}

	mutex_unlock(&dev_priv->rps.hw_lock);

	return 0;
}

DEFINE_SIMPLE_ATTRIBUTE(i915_max_freq_fops,
			i915_max_freq_get, i915_max_freq_set,
			"%llu\n");

static int
i915_min_freq_get(void *data, u64 *val)
{
	struct drm_device *dev = data;
	drm_i915_private_t *dev_priv = dev->dev_private;
	int ret;

	if (!(IS_GEN6(dev) || IS_GEN7(dev)))
		return -ENODEV;

	ret = mutex_lock_interruptible(&dev_priv->rps.hw_lock);
	if (ret)
		return ret;

	if (IS_VALLEYVIEW(dev))
		*val = vlv_gpu_freq(dev_priv->mem_freq,
				    dev_priv->rps.min_delay);
	else
		*val = dev_priv->rps.min_delay * GT_FREQUENCY_MULTIPLIER;
	mutex_unlock(&dev_priv->rps.hw_lock);

	return 0;
}

static int
i915_min_freq_set(void *data, u64 val)
{
	struct drm_device *dev = data;
	struct drm_i915_private *dev_priv = dev->dev_private;
	int ret;

	if (!(IS_GEN6(dev) || IS_GEN7(dev)))
		return -ENODEV;

	DRM_DEBUG_DRIVER("Manually setting min freq to %llu\n", val);

	ret = mutex_lock_interruptible(&dev_priv->rps.hw_lock);
	if (ret)
		return ret;

	/*
	 * Turbo will still be enabled, but won't go below the set value.
	 */
	if (IS_VALLEYVIEW(dev)) {
		val = vlv_freq_opcode(dev_priv->mem_freq, val);
		dev_priv->rps.min_delay = val;
		valleyview_set_rps(dev, val);
	} else {
		do_div(val, GT_FREQUENCY_MULTIPLIER);
		dev_priv->rps.min_delay = val;
		gen6_set_rps(dev, val);
	}
	mutex_unlock(&dev_priv->rps.hw_lock);

	return 0;
}

DEFINE_SIMPLE_ATTRIBUTE(i915_min_freq_fops,
			i915_min_freq_get, i915_min_freq_set,
			"%llu\n");

static int
i915_cache_sharing_get(void *data, u64 *val)
{
	struct drm_device *dev = data;
	drm_i915_private_t *dev_priv = dev->dev_private;
	u32 snpcr;
	int ret;

	if (!(IS_GEN6(dev) || IS_GEN7(dev)))
		return -ENODEV;

	ret = mutex_lock_interruptible(&dev->struct_mutex);
	if (ret)
		return ret;

	snpcr = I915_READ(GEN6_MBCUNIT_SNPCR);
	mutex_unlock(&dev_priv->dev->struct_mutex);

	*val = (snpcr & GEN6_MBC_SNPCR_MASK) >> GEN6_MBC_SNPCR_SHIFT;

	return 0;
}

static int
i915_cache_sharing_set(void *data, u64 val)
{
	struct drm_device *dev = data;
	struct drm_i915_private *dev_priv = dev->dev_private;
	u32 snpcr;

	if (!(IS_GEN6(dev) || IS_GEN7(dev)))
		return -ENODEV;

	if (val > 3)
		return -EINVAL;

	DRM_DEBUG_DRIVER("Manually setting uncore sharing to %llu\n", val);

	/* Update the cache sharing policy here as well */
	snpcr = I915_READ(GEN6_MBCUNIT_SNPCR);
	snpcr &= ~GEN6_MBC_SNPCR_MASK;
	snpcr |= (val << GEN6_MBC_SNPCR_SHIFT);
	I915_WRITE(GEN6_MBCUNIT_SNPCR, snpcr);

	return 0;
}

DEFINE_SIMPLE_ATTRIBUTE(i915_cache_sharing_fops,
			i915_cache_sharing_get, i915_cache_sharing_set,
			"%llu\n");

/* As the drm_debugfs_init() routines are called before dev->dev_private is
 * allocated we need to hook into the minor for release. */
static int
drm_add_fake_info_node(struct drm_minor *minor,
		       struct dentry *ent,
		       const void *key)
{
	struct drm_info_node *node;

	node = kmalloc(sizeof(struct drm_info_node), GFP_KERNEL);
	if (node == NULL) {
		debugfs_remove(ent);
		return -ENOMEM;
	}

	node->minor = minor;
	node->dent = ent;
	node->info_ent = (void *) key;

	mutex_lock(&minor->debugfs_lock);
	list_add(&node->list, &minor->debugfs_list);
	mutex_unlock(&minor->debugfs_lock);

	return 0;
}

static int i915_forcewake_open(struct inode *inode, struct file *file)
{
	struct drm_device *dev = inode->i_private;
	struct drm_i915_private *dev_priv = dev->dev_private;

	if (INTEL_INFO(dev)->gen < 6)
		return 0;

	gen6_gt_force_wake_get(dev_priv);

	return 0;
}

static int i915_forcewake_release(struct inode *inode, struct file *file)
{
	struct drm_device *dev = inode->i_private;
	struct drm_i915_private *dev_priv = dev->dev_private;

	if (INTEL_INFO(dev)->gen < 6)
		return 0;

	gen6_gt_force_wake_put(dev_priv);

	return 0;
}

static const struct file_operations i915_forcewake_fops = {
	.owner = THIS_MODULE,
	.open = i915_forcewake_open,
	.release = i915_forcewake_release,
};

static int i915_forcewake_create(struct dentry *root, struct drm_minor *minor)
{
	struct drm_device *dev = minor->dev;
	struct dentry *ent;

	ent = debugfs_create_file("i915_forcewake_user",
				  S_IRUSR,
				  root, dev,
				  &i915_forcewake_fops);
	if (IS_ERR(ent))
		return PTR_ERR(ent);

	return drm_add_fake_info_node(minor, ent, &i915_forcewake_fops);
}

static int i915_debugfs_create(struct dentry *root,
			       struct drm_minor *minor,
			       const char *name,
			       const struct file_operations *fops)
{
	struct drm_device *dev = minor->dev;
	struct dentry *ent;

	ent = debugfs_create_file(name,
				  S_IRUGO | S_IWUSR,
				  root, dev,
				  fops);
	if (IS_ERR(ent))
		return PTR_ERR(ent);

	return drm_add_fake_info_node(minor, ent, fops);
}

static struct drm_info_list i915_debugfs_list[] = {
	{"i915_capabilities", i915_capabilities, 0},
	{"i915_gem_objects", i915_gem_object_info, 0},
	{"i915_gem_gtt", i915_gem_gtt_info, 0},
	{"i915_gem_pinned", i915_gem_gtt_info, 0, (void *) PINNED_LIST},
	{"i915_gem_active", i915_gem_object_list_info, 0, (void *) ACTIVE_LIST},
	{"i915_gem_inactive", i915_gem_object_list_info, 0, (void *) INACTIVE_LIST},
	{"i915_gem_pageflip", i915_gem_pageflip_info, 0},
	{"i915_gem_request", i915_gem_request_info, 0},
	{"i915_gem_seqno", i915_gem_seqno_info, 0},
	{"i915_gem_fence_regs", i915_gem_fence_regs_info, 0},
	{"i915_gem_interrupt", i915_interrupt_info, 0},
	{"i915_gem_hws", i915_hws_info, 0, (void *)RCS},
	{"i915_gem_hws_blt", i915_hws_info, 0, (void *)BCS},
	{"i915_gem_hws_bsd", i915_hws_info, 0, (void *)VCS},
	{"i915_gem_hws_vebox", i915_hws_info, 0, (void *)VECS},
	{"i915_rstdby_delays", i915_rstdby_delays, 0},
	{"i915_cur_delayinfo", i915_cur_delayinfo, 0},
	{"i915_delayfreq_table", i915_delayfreq_table, 0},
	{"i915_inttoext_table", i915_inttoext_table, 0},
	{"i915_drpc_info", i915_drpc_info, 0},
	{"i915_emon_status", i915_emon_status, 0},
	{"i915_ring_freq_table", i915_ring_freq_table, 0},
	{"i915_gfxec", i915_gfxec, 0},
	{"i915_fbc_status", i915_fbc_status, 0},
	{"i915_ips_status", i915_ips_status, 0},
	{"i915_sr_status", i915_sr_status, 0},
	{"i915_opregion", i915_opregion, 0},
	{"i915_gem_framebuffer", i915_gem_framebuffer_info, 0},
	{"i915_context_status", i915_context_status, 0},
	{"i915_gen6_forcewake_count", i915_gen6_forcewake_count_info, 0},
	{"i915_swizzle_info", i915_swizzle_info, 0},
	{"i915_ppgtt_info", i915_ppgtt_info, 0},
	{"i915_dpio", i915_dpio_info, 0},
};
#define I915_DEBUGFS_ENTRIES ARRAY_SIZE(i915_debugfs_list)

int i915_debugfs_init(struct drm_minor *minor)
{
	int ret;

	ret = i915_debugfs_create(minor->debugfs_root, minor,
				  "i915_wedged",
				  &i915_wedged_fops);
	if (ret)
		return ret;

	ret = i915_forcewake_create(minor->debugfs_root, minor);
	if (ret)
		return ret;

	ret = i915_debugfs_create(minor->debugfs_root, minor,
				  "i915_max_freq",
				  &i915_max_freq_fops);
	if (ret)
		return ret;

	ret = i915_debugfs_create(minor->debugfs_root, minor,
				  "i915_min_freq",
				  &i915_min_freq_fops);
	if (ret)
		return ret;

	ret = i915_debugfs_create(minor->debugfs_root, minor,
				  "i915_cache_sharing",
				  &i915_cache_sharing_fops);
	if (ret)
		return ret;

	ret = i915_debugfs_create(minor->debugfs_root, minor,
				  "i915_ring_stop",
				  &i915_ring_stop_fops);
	if (ret)
		return ret;

	ret = i915_debugfs_create(minor->debugfs_root, minor,
				  "i915_gem_drop_caches",
				  &i915_drop_caches_fops);
	if (ret)
		return ret;

	ret = i915_debugfs_create(minor->debugfs_root, minor,
				  "i915_error_state",
				  &i915_error_state_fops);
	if (ret)
		return ret;

	ret = i915_debugfs_create(minor->debugfs_root, minor,
				 "i915_next_seqno",
				 &i915_next_seqno_fops);
	if (ret)
		return ret;

	return drm_debugfs_create_files(i915_debugfs_list,
					I915_DEBUGFS_ENTRIES,
					minor->debugfs_root, minor);
}

void i915_debugfs_cleanup(struct drm_minor *minor)
{
	drm_debugfs_remove_files(i915_debugfs_list,
				 I915_DEBUGFS_ENTRIES, minor);
	drm_debugfs_remove_files((struct drm_info_list *) &i915_forcewake_fops,
				 1, minor);
	drm_debugfs_remove_files((struct drm_info_list *) &i915_wedged_fops,
				 1, minor);
	drm_debugfs_remove_files((struct drm_info_list *) &i915_max_freq_fops,
				 1, minor);
	drm_debugfs_remove_files((struct drm_info_list *) &i915_min_freq_fops,
				 1, minor);
	drm_debugfs_remove_files((struct drm_info_list *) &i915_cache_sharing_fops,
				 1, minor);
	drm_debugfs_remove_files((struct drm_info_list *) &i915_drop_caches_fops,
				 1, minor);
	drm_debugfs_remove_files((struct drm_info_list *) &i915_ring_stop_fops,
				 1, minor);
	drm_debugfs_remove_files((struct drm_info_list *) &i915_error_state_fops,
				 1, minor);
	drm_debugfs_remove_files((struct drm_info_list *) &i915_next_seqno_fops,
				 1, minor);
}

#endif /* CONFIG_DEBUG_FS */
