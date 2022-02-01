/*
 * mgr.c
 *
 * DSP-BIOS Bridge driver support functions for TI OMAP processors.
 *
 * Implementation of Manager interface to the device object at the
 * driver level. This queries the NDB data base and retrieves the
 * data about Node and Processor.
 *
 * Copyright (C) 2005-2006 Texas Instruments, Inc.
 *
 * This package is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * THIS PACKAGE IS PROVIDED ``AS IS'' AND WITHOUT ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED
 * WARRANTIES OF MERCHANTIBILITY AND FITNESS FOR A PARTICULAR PURPOSE.
 */

#include <linux/types.h>

/*  ----------------------------------- Host OS */
#include <dspbridge/host_os.h>

/*  ----------------------------------- DSP/BIOS Bridge */
#include <dspbridge/dbdefs.h>

/*  ----------------------------------- OS Adaptation Layer */
#include <dspbridge/sync.h>

/*  ----------------------------------- Others */
#include <dspbridge/dbdcd.h>
#include <dspbridge/drv.h>
#include <dspbridge/dev.h>

/*  ----------------------------------- This */
#include <dspbridge/mgr.h>

/*  ----------------------------------- Defines, Data Structures, Typedefs */
#define ZLDLLNAME               ""

struct mgr_object {
	struct dcd_manager *dcd_mgr;	/* Proc/Node data manager */
};

/*  ----------------------------------- Globals */
static u32 refs;

/*
 *  ========= mgr_create =========
 *  Purpose:
 *      MGR Object gets created only once during driver Loading.
 */
int mgr_create(struct mgr_object **mgr_obj,
		      struct cfg_devnode *dev_node_obj)
{
	int status = 0;
	struct mgr_object *pmgr_obj = NULL;
	struct drv_data *drv_datap = dev_get_drvdata(bridge);

	pmgr_obj = kzalloc(sizeof(struct mgr_object), GFP_KERNEL);
	if (pmgr_obj) {
		status = dcd_create_manager(ZLDLLNAME, &pmgr_obj->dcd_mgr);
		if (!status) {
			/* If succeeded store the handle in the MGR Object */
			if (drv_datap) {
				drv_datap->mgr_object = (void *)pmgr_obj;
			} else {
				status = -EPERM;
				pr_err("%s: Failed to store MGR object\n",
								__func__);
			}

			if (!status) {
				*mgr_obj = pmgr_obj;
			} else {
				dcd_destroy_manager(pmgr_obj->dcd_mgr);
				kfree(pmgr_obj);
			}
		} else {
			/* failed to Create DCD Manager */
			kfree(pmgr_obj);
		}
	} else {
		status = -ENOMEM;
	}

	return status;
}

/*
 *  ========= mgr_destroy =========
 *     This function is invoked during bridge driver unloading.Frees MGR object.
 */
int mgr_destroy(struct mgr_object *hmgr_obj)
{
	int status = 0;
	struct mgr_object *pmgr_obj = (struct mgr_object *)hmgr_obj;
	struct drv_data *drv_datap = dev_get_drvdata(bridge);

	/* Free resources */
	if (hmgr_obj->dcd_mgr)
		dcd_destroy_manager(hmgr_obj->dcd_mgr);

	kfree(pmgr_obj);
	/* Update the driver data with NULL for MGR Object */
	if (drv_datap) {
		drv_datap->mgr_object = NULL;
	} else {
		status = -EPERM;
		pr_err("%s: Failed to store MGR object\n", __func__);
	}

	return status;
}

/*
 *  ======== mgr_enum_node_info ========
 *      Enumerate and get configuration information about nodes configured
 *      in the node database.
 */
int mgr_enum_node_info(u32 node_id, struct dsp_ndbprops *pndb_props,
			      u32 undb_props_size, u32 *pu_num_nodes)
{
	int status = 0;
	struct dsp_uuid node_uuid;
	u32 node_index = 0;
	struct dcd_genericobj gen_obj;
	struct mgr_object *pmgr_obj = NULL;
	struct drv_data *drv_datap = dev_get_drvdata(bridge);

	*pu_num_nodes = 0;
	/* Get the Manager Object from the driver data */
	if (!drv_datap || !drv_datap->mgr_object) {
		pr_err("%s: Failed to retrieve the object handle\n", __func__);
		return -ENODATA;
	}
	pmgr_obj = drv_datap->mgr_object;

	/* Forever loop till we hit failed or no more items in the
	 * Enumeration. We will exit the loop other than 0; */
	while (!status) {
		status = dcd_enumerate_object(node_index++, DSP_DCDNODETYPE,
				&node_uuid);
		if (status)
			break;
		*pu_num_nodes = node_index;
		if (node_id == (node_index - 1)) {
			status = dcd_get_object_def(pmgr_obj->dcd_mgr,
					&node_uuid, DSP_DCDNODETYPE, &gen_obj);
			if (status)
				break;
			/* Get the Obj def */
			*pndb_props = gen_obj.obj_data.node_obj.ndb_props;
		}
	}

	/* the last status is not 0, but neither an error */
	if (status > 0)
		status = 0;

	return status;
}

/*
 *  ======== mgr_enum_processor_info ========
 *      Enumerate and get configuration information about available
 *      DSP processors.
 */
int mgr_enum_processor_info(u32 processor_id,
				   struct dsp_processorinfo *
				   processor_info, u32 processor_info_size,
				   u8 *pu_num_procs)
{
	int status = 0;
	int status1 = 0;
	int status2 = 0;
	struct dsp_uuid temp_uuid;
	u32 temp_index = 0;
	u32 proc_index = 0;
	struct dcd_genericobj gen_obj;
	struct mgr_object *pmgr_obj = NULL;
	struct mgr_processorextinfo *ext_info;
	struct dev_object *hdev_obj;
	struct drv_object *hdrv_obj;
	u8 dev_type;
	struct cfg_devnode *dev_node;
	struct drv_data *drv_datap = dev_get_drvdata(bridge);
	bool proc_detect = false;

	*pu_num_procs = 0;

	/* Retrieve the Object handle from the driver data */
	if (!drv_datap || !drv_datap->drv_object) {
		status = -ENODATA;
		pr_err("%s: Failed to retrieve the object handle\n", __func__);
	} else {
		hdrv_obj = drv_datap->drv_object;
	}

	if (!status) {
		status = drv_get_dev_object(processor_id, hdrv_obj, &hdev_obj);
		if (!status) {
			status = dev_get_dev_type(hdev_obj, (u8 *) &dev_type);
			status = dev_get_dev_node(hdev_obj, &dev_node);
			if (dev_type != DSP_UNIT)
				status = -EPERM;

			if (!status)
				processor_info->processor_type = DSPTYPE64;
		}
	}
	if (status)
		goto func_end;

	/* Get The Manager Object from the driver data */
	if (drv_datap && drv_datap->mgr_object) {
		pmgr_obj = drv_datap->mgr_object;
	} else {
		dev_dbg(bridge, "%s: Failed to get MGR Object\n", __func__);
		goto func_end;
	}
	/* Forever loop till we hit no more items in the
	 * Enumeration. We will exit the loop other than 0; */
	while (status1 == 0) {
		status1 = dcd_enumerate_object(temp_index++,
					       DSP_DCDPROCESSORTYPE,
					       &temp_uuid);
		if (status1 != 0)
			break;

		proc_index++;
		/* Get the Object properties to find the Device/Processor
		 * Type */
		if (proc_detect != false)
			continue;

		status2 = dcd_get_object_def(pmgr_obj->dcd_mgr,
					     (struct dsp_uuid *)&temp_uuid,
					     DSP_DCDPROCESSORTYPE, &gen_obj);
		if (!status2) {
			/* Get the Obj def */
			if (processor_info_size <
			    sizeof(struct mgr_processorextinfo)) {
				*processor_info = gen_obj.obj_data.proc_info;
			} else {
				/* extended info */
				ext_info = (struct mgr_processorextinfo *)
				    processor_info;
				*ext_info = gen_obj.obj_data.ext_proc_obj;
			}
			dev_dbg(bridge, "%s: Got proctype  from DCD %x\n",
				__func__, processor_info->processor_type);
			/* See if we got the needed processor */
			if (dev_type == DSP_UNIT) {
				if (processor_info->processor_type ==
				    DSPPROCTYPE_C64)
					proc_detect = true;
			} else if (dev_type == IVA_UNIT) {
				if (processor_info->processor_type ==
				    IVAPROCTYPE_ARM7)
					proc_detect = true;
			}
			/* User applications only check for chip type, so
			 * this is a clumsy overwrite */
			processor_info->processor_type = DSPTYPE64;
		} else {
			dev_dbg(bridge, "%s: Failed to get DCD processor info "
				"%x\n", __func__, status2);
			status = -EPERM;
		}
	}
	*pu_num_procs = proc_index;
	if (proc_detect == false) {
		dev_dbg(bridge, "%s: Failed to get proc info from DCD, so use "
			"CFG registry\n", __func__);
		processor_info->processor_type = DSPTYPE64;
	}
func_end:
	return status;
}

/*
 *  ======== mgr_exit ========
 *      Decrement reference count, and free resources when reference count is
 *      0.
 */
void mgr_exit(void)
{
	refs--;
	if (refs == 0)
		dcd_exit();
}

/*
 *  ======== mgr_get_dcd_handle ========
 *      Retrieves the MGR handle. Accessor Function.
 */
int mgr_get_dcd_handle(struct mgr_object *mgr_handle,
			      u32 *dcd_handle)
{
	int status = -EPERM;
	struct mgr_object *pmgr_obj = (struct mgr_object *)mgr_handle;

	*dcd_handle = (u32) NULL;
	if (pmgr_obj) {
		*dcd_handle = (u32) pmgr_obj->dcd_mgr;
		status = 0;
	}

	return status;
}

/*
 *  ======== mgr_init ========
 *      Initialize MGR's private state, keeping a reference count on each call.
 */
bool mgr_init(void)
{
	bool ret = true;

	if (refs == 0)
		ret = dcd_init();	/*  DCD Module */

	if (ret)
		refs++;

	return ret;
}

/*
 *  ======== mgr_wait_for_bridge_events ========
 *      Block on any Bridge event(s)
 */
int mgr_wait_for_bridge_events(struct dsp_notification **anotifications,
				      u32 count, u32 *pu_index,
				      u32 utimeout)
{
	int status;
	struct sync_object *sync_events[MAX_EVENTS];
	u32 i;

	for (i = 0; i < count; i++)
		sync_events[i] = anotifications[i]->handle;

	status = sync_wait_on_multiple_events(sync_events, count, utimeout,
					      pu_index);

	return status;

}
