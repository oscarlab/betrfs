/*
 * f_hid.c -- USB HID function driver
 *
 * Copyright (C) 2010 Fabien Chouteau <fabien.chouteau@barco.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 */

#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/hid.h>
#include <linux/cdev.h>
#include <linux/mutex.h>
#include <linux/poll.h>
#include <linux/uaccess.h>
#include <linux/wait.h>
#include <linux/sched.h>
#include <linux/usb/g_hid.h>

static int major, minors;
static struct class *hidg_class;

/*-------------------------------------------------------------------------*/
/*                            HID gadget struct                            */

struct f_hidg_req_list {
	struct usb_request	*req;
	unsigned int		pos;
	struct list_head 	list;
};

struct f_hidg {
	/* configuration */
	unsigned char			bInterfaceSubClass;
	unsigned char			bInterfaceProtocol;
	unsigned short			report_desc_length;
	char				*report_desc;
	unsigned short			report_length;

	/* recv report */
	struct list_head		completed_out_req;
	spinlock_t			spinlock;
	wait_queue_head_t		read_queue;
	unsigned int			qlen;

	/* send report */
	struct mutex			lock;
	bool				write_pending;
	wait_queue_head_t		write_queue;
	struct usb_request		*req;

	int				minor;
	struct cdev			cdev;
	struct usb_function		func;

	struct usb_ep			*in_ep;
	struct usb_ep			*out_ep;
};

static inline struct f_hidg *func_to_hidg(struct usb_function *f)
{
	return container_of(f, struct f_hidg, func);
}

/*-------------------------------------------------------------------------*/
/*                           Static descriptors                            */

static struct usb_interface_descriptor hidg_interface_desc = {
	.bLength		= sizeof hidg_interface_desc,
	.bDescriptorType	= USB_DT_INTERFACE,
	/* .bInterfaceNumber	= DYNAMIC */
	.bAlternateSetting	= 0,
	.bNumEndpoints		= 2,
	.bInterfaceClass	= USB_CLASS_HID,
	/* .bInterfaceSubClass	= DYNAMIC */
	/* .bInterfaceProtocol	= DYNAMIC */
	/* .iInterface		= DYNAMIC */
};

static struct hid_descriptor hidg_desc = {
	.bLength			= sizeof hidg_desc,
	.bDescriptorType		= HID_DT_HID,
	.bcdHID				= 0x0101,
	.bCountryCode			= 0x00,
	.bNumDescriptors		= 0x1,
	/*.desc[0].bDescriptorType	= DYNAMIC */
	/*.desc[0].wDescriptorLenght	= DYNAMIC */
};

/* High-Speed Support */

static struct usb_endpoint_descriptor hidg_hs_in_ep_desc = {
	.bLength		= USB_DT_ENDPOINT_SIZE,
	.bDescriptorType	= USB_DT_ENDPOINT,
	.bEndpointAddress	= USB_DIR_IN,
	.bmAttributes		= USB_ENDPOINT_XFER_INT,
	/*.wMaxPacketSize	= DYNAMIC */
	.bInterval		= 4, /* FIXME: Add this field in the
				      * HID gadget configuration?
				      * (struct hidg_func_descriptor)
				      */
};

static struct usb_endpoint_descriptor hidg_hs_out_ep_desc = {
	.bLength		= USB_DT_ENDPOINT_SIZE,
	.bDescriptorType	= USB_DT_ENDPOINT,
	.bEndpointAddress	= USB_DIR_OUT,
	.bmAttributes		= USB_ENDPOINT_XFER_INT,
	/*.wMaxPacketSize	= DYNAMIC */
	.bInterval		= 4, /* FIXME: Add this field in the
				      * HID gadget configuration?
				      * (struct hidg_func_descriptor)
				      */
};

static struct usb_descriptor_header *hidg_hs_descriptors[] = {
	(struct usb_descriptor_header *)&hidg_interface_desc,
	(struct usb_descriptor_header *)&hidg_desc,
	(struct usb_descriptor_header *)&hidg_hs_in_ep_desc,
	(struct usb_descriptor_header *)&hidg_hs_out_ep_desc,
	NULL,
};

/* Full-Speed Support */

static struct usb_endpoint_descriptor hidg_fs_in_ep_desc = {
	.bLength		= USB_DT_ENDPOINT_SIZE,
	.bDescriptorType	= USB_DT_ENDPOINT,
	.bEndpointAddress	= USB_DIR_IN,
	.bmAttributes		= USB_ENDPOINT_XFER_INT,
	/*.wMaxPacketSize	= DYNAMIC */
	.bInterval		= 10, /* FIXME: Add this field in the
				       * HID gadget configuration?
				       * (struct hidg_func_descriptor)
				       */
};

static struct usb_endpoint_descriptor hidg_fs_out_ep_desc = {
	.bLength		= USB_DT_ENDPOINT_SIZE,
	.bDescriptorType	= USB_DT_ENDPOINT,
	.bEndpointAddress	= USB_DIR_OUT,
	.bmAttributes		= USB_ENDPOINT_XFER_INT,
	/*.wMaxPacketSize	= DYNAMIC */
	.bInterval		= 10, /* FIXME: Add this field in the
				       * HID gadget configuration?
				       * (struct hidg_func_descriptor)
				       */
};

static struct usb_descriptor_header *hidg_fs_descriptors[] = {
	(struct usb_descriptor_header *)&hidg_interface_desc,
	(struct usb_descriptor_header *)&hidg_desc,
	(struct usb_descriptor_header *)&hidg_fs_in_ep_desc,
	(struct usb_descriptor_header *)&hidg_fs_out_ep_desc,
	NULL,
};

/*-------------------------------------------------------------------------*/
/*                              Char Device                                */

static ssize_t f_hidg_read(struct file *file, char __user *buffer,
			size_t count, loff_t *ptr)
{
	struct f_hidg *hidg = file->private_data;
	struct f_hidg_req_list *list;
	struct usb_request *req;
	unsigned long flags;
	int ret;

	if (!count)
		return 0;

	if (!access_ok(VERIFY_WRITE, buffer, count))
		return -EFAULT;

	spin_lock_irqsave(&hidg->spinlock, flags);

#define READ_COND (!list_empty(&hidg->completed_out_req))

	/* wait for at least one buffer to complete */
	while (!READ_COND) {
		spin_unlock_irqrestore(&hidg->spinlock, flags);
		if (file->f_flags & O_NONBLOCK)
			return -EAGAIN;

		if (wait_event_interruptible(hidg->read_queue, READ_COND))
			return -ERESTARTSYS;

		spin_lock_irqsave(&hidg->spinlock, flags);
	}

	/* pick the first one */
	list = list_first_entry(&hidg->completed_out_req,
				struct f_hidg_req_list, list);
	req = list->req;
	count = min_t(unsigned int, count, req->actual - list->pos);
	spin_unlock_irqrestore(&hidg->spinlock, flags);

	/* copy to user outside spinlock */
	count -= copy_to_user(buffer, req->buf + list->pos, count);
	list->pos += count;

	/*
	 * if this request is completely handled and transfered to
	 * userspace, remove its entry from the list and requeue it
	 * again. Otherwise, we will revisit it again upon the next
	 * call, taking into account its current read position.
	 */
	if (list->pos == req->actual) {
		spin_lock_irqsave(&hidg->spinlock, flags);
		list_del(&list->list);
		kfree(list);
		spin_unlock_irqrestore(&hidg->spinlock, flags);

		req->length = hidg->report_length;
		ret = usb_ep_queue(hidg->out_ep, req, GFP_KERNEL);
		if (ret < 0)
			return ret;
	}

	return count;
}

static void f_hidg_req_complete(struct usb_ep *ep, struct usb_request *req)
{
	struct f_hidg *hidg = (struct f_hidg *)ep->driver_data;

	if (req->status != 0) {
		ERROR(hidg->func.config->cdev,
			"End Point Request ERROR: %d\n", req->status);
	}

	hidg->write_pending = 0;
	wake_up(&hidg->write_queue);
}

static ssize_t f_hidg_write(struct file *file, const char __user *buffer,
			    size_t count, loff_t *offp)
{
	struct f_hidg *hidg  = file->private_data;
	ssize_t status = -ENOMEM;

	if (!access_ok(VERIFY_READ, buffer, count))
		return -EFAULT;

	mutex_lock(&hidg->lock);

#define WRITE_COND (!hidg->write_pending)

	/* write queue */
	while (!WRITE_COND) {
		mutex_unlock(&hidg->lock);
		if (file->f_flags & O_NONBLOCK)
			return -EAGAIN;

		if (wait_event_interruptible_exclusive(
				hidg->write_queue, WRITE_COND))
			return -ERESTARTSYS;

		mutex_lock(&hidg->lock);
	}

	count  = min_t(unsigned, count, hidg->report_length);
	status = copy_from_user(hidg->req->buf, buffer, count);

	if (status != 0) {
		ERROR(hidg->func.config->cdev,
			"copy_from_user error\n");
		mutex_unlock(&hidg->lock);
		return -EINVAL;
	}

	hidg->req->status   = 0;
	hidg->req->zero     = 0;
	hidg->req->length   = count;
	hidg->req->complete = f_hidg_req_complete;
	hidg->req->context  = hidg;
	hidg->write_pending = 1;

	status = usb_ep_queue(hidg->in_ep, hidg->req, GFP_ATOMIC);
	if (status < 0) {
		ERROR(hidg->func.config->cdev,
			"usb_ep_queue error on int endpoint %zd\n", status);
		hidg->write_pending = 0;
		wake_up(&hidg->write_queue);
	} else {
		status = count;
	}

	mutex_unlock(&hidg->lock);

	return status;
}

static unsigned int f_hidg_poll(struct file *file, poll_table *wait)
{
	struct f_hidg	*hidg  = file->private_data;
	unsigned int	ret = 0;

	poll_wait(file, &hidg->read_queue, wait);
	poll_wait(file, &hidg->write_queue, wait);

	if (WRITE_COND)
		ret |= POLLOUT | POLLWRNORM;

	if (READ_COND)
		ret |= POLLIN | POLLRDNORM;

	return ret;
}

#undef WRITE_COND
#undef READ_COND

static int f_hidg_release(struct inode *inode, struct file *fd)
{
	fd->private_data = NULL;
	return 0;
}

static int f_hidg_open(struct inode *inode, struct file *fd)
{
	struct f_hidg *hidg =
		container_of(inode->i_cdev, struct f_hidg, cdev);

	fd->private_data = hidg;

	return 0;
}

/*-------------------------------------------------------------------------*/
/*                                usb_function                             */

static struct usb_request *hidg_alloc_ep_req(struct usb_ep *ep, unsigned length)
{
	struct usb_request *req;

	req = usb_ep_alloc_request(ep, GFP_ATOMIC);
	if (req) {
		req->length = length;
		req->buf = kmalloc(length, GFP_ATOMIC);
		if (!req->buf) {
			usb_ep_free_request(ep, req);
			req = NULL;
		}
	}
	return req;
}

static void hidg_set_report_complete(struct usb_ep *ep, struct usb_request *req)
{
	struct f_hidg *hidg = (struct f_hidg *) req->context;
	struct f_hidg_req_list *req_list;
	unsigned long flags;

	req_list = kzalloc(sizeof(*req_list), GFP_ATOMIC);
	if (!req_list)
		return;

	req_list->req = req;

	spin_lock_irqsave(&hidg->spinlock, flags);
	list_add_tail(&req_list->list, &hidg->completed_out_req);
	spin_unlock_irqrestore(&hidg->spinlock, flags);

	wake_up(&hidg->read_queue);
}

static int hidg_setup(struct usb_function *f,
		const struct usb_ctrlrequest *ctrl)
{
	struct f_hidg			*hidg = func_to_hidg(f);
	struct usb_composite_dev	*cdev = f->config->cdev;
	struct usb_request		*req  = cdev->req;
	int status = 0;
	__u16 value, length;

	value	= __le16_to_cpu(ctrl->wValue);
	length	= __le16_to_cpu(ctrl->wLength);

	VDBG(cdev, "hid_setup crtl_request : bRequestType:0x%x bRequest:0x%x "
		"Value:0x%x\n", ctrl->bRequestType, ctrl->bRequest, value);

	switch ((ctrl->bRequestType << 8) | ctrl->bRequest) {
	case ((USB_DIR_IN | USB_TYPE_CLASS | USB_RECIP_INTERFACE) << 8
		  | HID_REQ_GET_REPORT):
		VDBG(cdev, "get_report\n");

		/* send an empty report */
		length = min_t(unsigned, length, hidg->report_length);
		memset(req->buf, 0x0, length);

		goto respond;
		break;

	case ((USB_DIR_IN | USB_TYPE_CLASS | USB_RECIP_INTERFACE) << 8
		  | HID_REQ_GET_PROTOCOL):
		VDBG(cdev, "get_protocol\n");
		goto stall;
		break;

	case ((USB_DIR_OUT | USB_TYPE_CLASS | USB_RECIP_INTERFACE) << 8
		  | HID_REQ_SET_REPORT):
		VDBG(cdev, "set_report | wLenght=%d\n", ctrl->wLength);
		goto stall;
		break;

	case ((USB_DIR_OUT | USB_TYPE_CLASS | USB_RECIP_INTERFACE) << 8
		  | HID_REQ_SET_PROTOCOL):
		VDBG(cdev, "set_protocol\n");
		goto stall;
		break;

	case ((USB_DIR_IN | USB_TYPE_STANDARD | USB_RECIP_INTERFACE) << 8
		  | USB_REQ_GET_DESCRIPTOR):
		switch (value >> 8) {
		case HID_DT_HID:
			VDBG(cdev, "USB_REQ_GET_DESCRIPTOR: HID\n");
			length = min_t(unsigned short, length,
						   hidg_desc.bLength);
			memcpy(req->buf, &hidg_desc, length);
			goto respond;
			break;
		case HID_DT_REPORT:
			VDBG(cdev, "USB_REQ_GET_DESCRIPTOR: REPORT\n");
			length = min_t(unsigned short, length,
						   hidg->report_desc_length);
			memcpy(req->buf, hidg->report_desc, length);
			goto respond;
			break;

		default:
			VDBG(cdev, "Unknown descriptor request 0x%x\n",
				 value >> 8);
			goto stall;
			break;
		}
		break;

	default:
		VDBG(cdev, "Unknown request 0x%x\n",
			 ctrl->bRequest);
		goto stall;
		break;
	}

stall:
	return -EOPNOTSUPP;

respond:
	req->zero = 0;
	req->length = length;
	status = usb_ep_queue(cdev->gadget->ep0, req, GFP_ATOMIC);
	if (status < 0)
		ERROR(cdev, "usb_ep_queue error on ep0 %d\n", value);
	return status;
}

static void hidg_disable(struct usb_function *f)
{
	struct f_hidg *hidg = func_to_hidg(f);
	struct f_hidg_req_list *list, *next;

	usb_ep_disable(hidg->in_ep);
	hidg->in_ep->driver_data = NULL;

	usb_ep_disable(hidg->out_ep);
	hidg->out_ep->driver_data = NULL;

	list_for_each_entry_safe(list, next, &hidg->completed_out_req, list) {
		list_del(&list->list);
		kfree(list);
	}
}

static int hidg_set_alt(struct usb_function *f, unsigned intf, unsigned alt)
{
	struct usb_composite_dev		*cdev = f->config->cdev;
	struct f_hidg				*hidg = func_to_hidg(f);
	int i, status = 0;

	VDBG(cdev, "hidg_set_alt intf:%d alt:%d\n", intf, alt);

	if (hidg->in_ep != NULL) {
		/* restart endpoint */
		if (hidg->in_ep->driver_data != NULL)
			usb_ep_disable(hidg->in_ep);

		status = config_ep_by_speed(f->config->cdev->gadget, f,
					    hidg->in_ep);
		if (status) {
			ERROR(cdev, "config_ep_by_speed FAILED!\n");
			goto fail;
		}
		status = usb_ep_enable(hidg->in_ep);
		if (status < 0) {
			ERROR(cdev, "Enable IN endpoint FAILED!\n");
			goto fail;
		}
		hidg->in_ep->driver_data = hidg;
	}


	if (hidg->out_ep != NULL) {
		/* restart endpoint */
		if (hidg->out_ep->driver_data != NULL)
			usb_ep_disable(hidg->out_ep);

		status = config_ep_by_speed(f->config->cdev->gadget, f,
					    hidg->out_ep);
		if (status) {
			ERROR(cdev, "config_ep_by_speed FAILED!\n");
			goto fail;
		}
		status = usb_ep_enable(hidg->out_ep);
		if (status < 0) {
			ERROR(cdev, "Enable IN endpoint FAILED!\n");
			goto fail;
		}
		hidg->out_ep->driver_data = hidg;

		/*
		 * allocate a bunch of read buffers and queue them all at once.
		 */
		for (i = 0; i < hidg->qlen && status == 0; i++) {
			struct usb_request *req =
					hidg_alloc_ep_req(hidg->out_ep,
							  hidg->report_length);
			if (req) {
				req->complete = hidg_set_report_complete;
				req->context  = hidg;
				status = usb_ep_queue(hidg->out_ep, req,
						      GFP_ATOMIC);
				if (status)
					ERROR(cdev, "%s queue req --> %d\n",
						hidg->out_ep->name, status);
			} else {
				usb_ep_disable(hidg->out_ep);
				hidg->out_ep->driver_data = NULL;
				status = -ENOMEM;
				goto fail;
			}
		}
	}

fail:
	return status;
}

const struct file_operations f_hidg_fops = {
	.owner		= THIS_MODULE,
	.open		= f_hidg_open,
	.release	= f_hidg_release,
	.write		= f_hidg_write,
	.read		= f_hidg_read,
	.poll		= f_hidg_poll,
	.llseek		= noop_llseek,
};

static int __init hidg_bind(struct usb_configuration *c, struct usb_function *f)
{
	struct usb_ep		*ep;
	struct f_hidg		*hidg = func_to_hidg(f);
	int			status;
	dev_t			dev;

	/* allocate instance-specific interface IDs, and patch descriptors */
	status = usb_interface_id(c, f);
	if (status < 0)
		goto fail;
	hidg_interface_desc.bInterfaceNumber = status;

	/* allocate instance-specific endpoints */
	status = -ENODEV;
	ep = usb_ep_autoconfig(c->cdev->gadget, &hidg_fs_in_ep_desc);
	if (!ep)
		goto fail;
	ep->driver_data = c->cdev;	/* claim */
	hidg->in_ep = ep;

	ep = usb_ep_autoconfig(c->cdev->gadget, &hidg_fs_out_ep_desc);
	if (!ep)
		goto fail;
	ep->driver_data = c->cdev;	/* claim */
	hidg->out_ep = ep;

	/* preallocate request and buffer */
	status = -ENOMEM;
	hidg->req = usb_ep_alloc_request(hidg->in_ep, GFP_KERNEL);
	if (!hidg->req)
		goto fail;

	hidg->req->buf = kmalloc(hidg->report_length, GFP_KERNEL);
	if (!hidg->req->buf)
		goto fail;

	/* set descriptor dynamic values */
	hidg_interface_desc.bInterfaceSubClass = hidg->bInterfaceSubClass;
	hidg_interface_desc.bInterfaceProtocol = hidg->bInterfaceProtocol;
	hidg_hs_in_ep_desc.wMaxPacketSize = cpu_to_le16(hidg->report_length);
	hidg_fs_in_ep_desc.wMaxPacketSize = cpu_to_le16(hidg->report_length);
	hidg_hs_out_ep_desc.wMaxPacketSize = cpu_to_le16(hidg->report_length);
	hidg_fs_out_ep_desc.wMaxPacketSize = cpu_to_le16(hidg->report_length);
	hidg_desc.desc[0].bDescriptorType = HID_DT_REPORT;
	hidg_desc.desc[0].wDescriptorLength =
		cpu_to_le16(hidg->report_desc_length);

	hidg_hs_in_ep_desc.bEndpointAddress =
		hidg_fs_in_ep_desc.bEndpointAddress;
	hidg_hs_out_ep_desc.bEndpointAddress =
		hidg_fs_out_ep_desc.bEndpointAddress;

	status = usb_assign_descriptors(f, hidg_fs_descriptors,
			hidg_hs_descriptors, NULL);
	if (status)
		goto fail;

	mutex_init(&hidg->lock);
	spin_lock_init(&hidg->spinlock);
	init_waitqueue_head(&hidg->write_queue);
	init_waitqueue_head(&hidg->read_queue);
	INIT_LIST_HEAD(&hidg->completed_out_req);

	/* create char device */
	cdev_init(&hidg->cdev, &f_hidg_fops);
	dev = MKDEV(major, hidg->minor);
	status = cdev_add(&hidg->cdev, dev, 1);
	if (status)
		goto fail;

	device_create(hidg_class, NULL, dev, NULL, "%s%d", "hidg", hidg->minor);

	return 0;

fail:
	ERROR(f->config->cdev, "hidg_bind FAILED\n");
	if (hidg->req != NULL) {
		kfree(hidg->req->buf);
		if (hidg->in_ep != NULL)
			usb_ep_free_request(hidg->in_ep, hidg->req);
	}

	usb_free_all_descriptors(f);
	return status;
}

static void hidg_unbind(struct usb_configuration *c, struct usb_function *f)
{
	struct f_hidg *hidg = func_to_hidg(f);

	device_destroy(hidg_class, MKDEV(major, hidg->minor));
	cdev_del(&hidg->cdev);

	/* disable/free request and end point */
	usb_ep_disable(hidg->in_ep);
	usb_ep_dequeue(hidg->in_ep, hidg->req);
	kfree(hidg->req->buf);
	usb_ep_free_request(hidg->in_ep, hidg->req);

	usb_free_all_descriptors(f);

	kfree(hidg->report_desc);
	kfree(hidg);
}

/*-------------------------------------------------------------------------*/
/*                                 Strings                                 */

#define CT_FUNC_HID_IDX	0

static struct usb_string ct_func_string_defs[] = {
	[CT_FUNC_HID_IDX].s	= "HID Interface",
	{},			/* end of list */
};

static struct usb_gadget_strings ct_func_string_table = {
	.language	= 0x0409,	/* en-US */
	.strings	= ct_func_string_defs,
};

static struct usb_gadget_strings *ct_func_strings[] = {
	&ct_func_string_table,
	NULL,
};

/*-------------------------------------------------------------------------*/
/*                             usb_configuration                           */

int __init hidg_bind_config(struct usb_configuration *c,
			    struct hidg_func_descriptor *fdesc, int index)
{
	struct f_hidg *hidg;
	int status;

	if (index >= minors)
		return -ENOENT;

	/* maybe allocate device-global string IDs, and patch descriptors */
	if (ct_func_string_defs[CT_FUNC_HID_IDX].id == 0) {
		status = usb_string_id(c->cdev);
		if (status < 0)
			return status;
		ct_func_string_defs[CT_FUNC_HID_IDX].id = status;
		hidg_interface_desc.iInterface = status;
	}

	/* allocate and initialize one new instance */
	hidg = kzalloc(sizeof *hidg, GFP_KERNEL);
	if (!hidg)
		return -ENOMEM;

	hidg->minor = index;
	hidg->bInterfaceSubClass = fdesc->subclass;
	hidg->bInterfaceProtocol = fdesc->protocol;
	hidg->report_length = fdesc->report_length;
	hidg->report_desc_length = fdesc->report_desc_length;
	hidg->report_desc = kmemdup(fdesc->report_desc,
				    fdesc->report_desc_length,
				    GFP_KERNEL);
	if (!hidg->report_desc) {
		kfree(hidg);
		return -ENOMEM;
	}

	hidg->func.name    = "hid";
	hidg->func.strings = ct_func_strings;
	hidg->func.bind    = hidg_bind;
	hidg->func.unbind  = hidg_unbind;
	hidg->func.set_alt = hidg_set_alt;
	hidg->func.disable = hidg_disable;
	hidg->func.setup   = hidg_setup;

	/* this could me made configurable at some point */
	hidg->qlen	   = 4;

	status = usb_add_function(c, &hidg->func);
	if (status)
		kfree(hidg);

	return status;
}

int __init ghid_setup(struct usb_gadget *g, int count)
{
	int status;
	dev_t dev;

	hidg_class = class_create(THIS_MODULE, "hidg");

	status = alloc_chrdev_region(&dev, 0, count, "hidg");
	if (!status) {
		major = MAJOR(dev);
		minors = count;
	}

	return status;
}

void ghid_cleanup(void)
{
	if (major) {
		unregister_chrdev_region(MKDEV(major, 0), minors);
		major = minors = 0;
	}

	class_destroy(hidg_class);
	hidg_class = NULL;
}
