/*
 * Copyright (c) 2005-2011 Atheros Communications Inc.
 * Copyright (c) 2011-2013 Qualcomm Atheros, Inc.
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <linux/module.h>
#include <linux/firmware.h>

#include "core.h"
#include "mac.h"
#include "htc.h"
#include "hif.h"
#include "wmi.h"
#include "bmi.h"
#include "debug.h"
#include "htt.h"

unsigned int ath10k_debug_mask;
static bool uart_print;
static unsigned int ath10k_p2p;
module_param_named(debug_mask, ath10k_debug_mask, uint, 0644);
module_param(uart_print, bool, 0644);
module_param_named(p2p, ath10k_p2p, uint, 0644);
MODULE_PARM_DESC(debug_mask, "Debugging mask");
MODULE_PARM_DESC(uart_print, "Uart target debugging");
MODULE_PARM_DESC(p2p, "Enable ath10k P2P support");

static const struct ath10k_hw_params ath10k_hw_params_list[] = {
	{
		.id = QCA988X_HW_1_0_VERSION,
		.name = "qca988x hw1.0",
		.patch_load_addr = QCA988X_HW_1_0_PATCH_LOAD_ADDR,
		.fw = {
			.dir = QCA988X_HW_1_0_FW_DIR,
			.fw = QCA988X_HW_1_0_FW_FILE,
			.otp = QCA988X_HW_1_0_OTP_FILE,
			.board = QCA988X_HW_1_0_BOARD_DATA_FILE,
		},
	},
	{
		.id = QCA988X_HW_2_0_VERSION,
		.name = "qca988x hw2.0",
		.patch_load_addr = QCA988X_HW_2_0_PATCH_LOAD_ADDR,
		.fw = {
			.dir = QCA988X_HW_2_0_FW_DIR,
			.fw = QCA988X_HW_2_0_FW_FILE,
			.otp = QCA988X_HW_2_0_OTP_FILE,
			.board = QCA988X_HW_2_0_BOARD_DATA_FILE,
		},
	},
};

static void ath10k_send_suspend_complete(struct ath10k *ar)
{
	ath10k_dbg(ATH10K_DBG_CORE, "%s\n", __func__);

	ar->is_target_paused = true;
	wake_up(&ar->event_queue);
}

static int ath10k_check_fw_version(struct ath10k *ar)
{
	char version[32];

	if (ar->fw_version_major >= SUPPORTED_FW_MAJOR &&
	    ar->fw_version_minor >= SUPPORTED_FW_MINOR &&
	    ar->fw_version_release >= SUPPORTED_FW_RELEASE &&
	    ar->fw_version_build >= SUPPORTED_FW_BUILD)
		return 0;

	snprintf(version, sizeof(version), "%u.%u.%u.%u",
		 SUPPORTED_FW_MAJOR, SUPPORTED_FW_MINOR,
		 SUPPORTED_FW_RELEASE, SUPPORTED_FW_BUILD);

	ath10k_warn("WARNING: Firmware version %s is not officially supported.\n",
		    ar->hw->wiphy->fw_version);
	ath10k_warn("Please upgrade to version %s (or newer)\n", version);

	return 0;
}

static int ath10k_init_connect_htc(struct ath10k *ar)
{
	int status;

	status = ath10k_wmi_connect_htc_service(ar);
	if (status)
		goto conn_fail;

	/* Start HTC */
	status = ath10k_htc_start(ar->htc);
	if (status)
		goto conn_fail;

	/* Wait for WMI event to be ready */
	status = ath10k_wmi_wait_for_service_ready(ar);
	if (status <= 0) {
		ath10k_warn("wmi service ready event not received");
		status = -ETIMEDOUT;
		goto timeout;
	}

	ath10k_dbg(ATH10K_DBG_CORE, "core wmi ready\n");
	return 0;

timeout:
	ath10k_htc_stop(ar->htc);
conn_fail:
	return status;
}

static int ath10k_init_configure_target(struct ath10k *ar)
{
	u32 param_host;
	int ret;

	/* tell target which HTC version it is used*/
	ret = ath10k_bmi_write32(ar, hi_app_host_interest,
				 HTC_PROTOCOL_VERSION);
	if (ret) {
		ath10k_err("settings HTC version failed\n");
		return ret;
	}

	/* set the firmware mode to STA/IBSS/AP */
	ret = ath10k_bmi_read32(ar, hi_option_flag, &param_host);
	if (ret) {
		ath10k_err("setting firmware mode (1/2) failed\n");
		return ret;
	}

	/* TODO following parameters need to be re-visited. */
	/* num_device */
	param_host |= (1 << HI_OPTION_NUM_DEV_SHIFT);
	/* Firmware mode */
	/* FIXME: Why FW_MODE_AP ??.*/
	param_host |= (HI_OPTION_FW_MODE_AP << HI_OPTION_FW_MODE_SHIFT);
	/* mac_addr_method */
	param_host |= (1 << HI_OPTION_MAC_ADDR_METHOD_SHIFT);
	/* firmware_bridge */
	param_host |= (0 << HI_OPTION_FW_BRIDGE_SHIFT);
	/* fwsubmode */
	param_host |= (0 << HI_OPTION_FW_SUBMODE_SHIFT);

	ret = ath10k_bmi_write32(ar, hi_option_flag, param_host);
	if (ret) {
		ath10k_err("setting firmware mode (2/2) failed\n");
		return ret;
	}

	/* We do all byte-swapping on the host */
	ret = ath10k_bmi_write32(ar, hi_be, 0);
	if (ret) {
		ath10k_err("setting host CPU BE mode failed\n");
		return ret;
	}

	/* FW descriptor/Data swap flags */
	ret = ath10k_bmi_write32(ar, hi_fw_swap, 0);

	if (ret) {
		ath10k_err("setting FW data/desc swap flags failed\n");
		return ret;
	}

	return 0;
}

static const struct firmware *ath10k_fetch_fw_file(struct ath10k *ar,
						   const char *dir,
						   const char *file)
{
	char filename[100];
	const struct firmware *fw;
	int ret;

	if (file == NULL)
		return ERR_PTR(-ENOENT);

	if (dir == NULL)
		dir = ".";

	snprintf(filename, sizeof(filename), "%s/%s", dir, file);
	ret = request_firmware(&fw, filename, ar->dev);
	if (ret)
		return ERR_PTR(ret);

	return fw;
}

static int ath10k_push_board_ext_data(struct ath10k *ar,
				      const struct firmware *fw)
{
	u32 board_data_size = QCA988X_BOARD_DATA_SZ;
	u32 board_ext_data_size = QCA988X_BOARD_EXT_DATA_SZ;
	u32 board_ext_data_addr;
	int ret;

	ret = ath10k_bmi_read32(ar, hi_board_ext_data, &board_ext_data_addr);
	if (ret) {
		ath10k_err("could not read board ext data addr (%d)\n", ret);
		return ret;
	}

	ath10k_dbg(ATH10K_DBG_CORE,
		   "ath10k: Board extended Data download addr: 0x%x\n",
		   board_ext_data_addr);

	if (board_ext_data_addr == 0)
		return 0;

	if (fw->size != (board_data_size + board_ext_data_size)) {
		ath10k_err("invalid board (ext) data sizes %zu != %d+%d\n",
			   fw->size, board_data_size, board_ext_data_size);
		return -EINVAL;
	}

	ret = ath10k_bmi_write_memory(ar, board_ext_data_addr,
				      fw->data + board_data_size,
				      board_ext_data_size);
	if (ret) {
		ath10k_err("could not write board ext data (%d)\n", ret);
		return ret;
	}

	ret = ath10k_bmi_write32(ar, hi_board_ext_data_config,
				 (board_ext_data_size << 16) | 1);
	if (ret) {
		ath10k_err("could not write board ext data bit (%d)\n", ret);
		return ret;
	}

	return 0;
}

static int ath10k_download_board_data(struct ath10k *ar)
{
	u32 board_data_size = QCA988X_BOARD_DATA_SZ;
	u32 address;
	const struct firmware *fw;
	int ret;

	fw = ath10k_fetch_fw_file(ar, ar->hw_params.fw.dir,
				  ar->hw_params.fw.board);
	if (IS_ERR(fw)) {
		ath10k_err("could not fetch board data fw file (%ld)\n",
			   PTR_ERR(fw));
		return PTR_ERR(fw);
	}

	ret = ath10k_push_board_ext_data(ar, fw);
	if (ret) {
		ath10k_err("could not push board ext data (%d)\n", ret);
		goto exit;
	}

	ret = ath10k_bmi_read32(ar, hi_board_data, &address);
	if (ret) {
		ath10k_err("could not read board data addr (%d)\n", ret);
		goto exit;
	}

	ret = ath10k_bmi_write_memory(ar, address, fw->data,
				      min_t(u32, board_data_size, fw->size));
	if (ret) {
		ath10k_err("could not write board data (%d)\n", ret);
		goto exit;
	}

	ret = ath10k_bmi_write32(ar, hi_board_data_initialized, 1);
	if (ret) {
		ath10k_err("could not write board data bit (%d)\n", ret);
		goto exit;
	}

exit:
	release_firmware(fw);
	return ret;
}

static int ath10k_download_and_run_otp(struct ath10k *ar)
{
	const struct firmware *fw;
	u32 address;
	u32 exec_param;
	int ret;

	/* OTP is optional */

	if (ar->hw_params.fw.otp == NULL) {
		ath10k_info("otp file not defined\n");
		return 0;
	}

	address = ar->hw_params.patch_load_addr;

	fw = ath10k_fetch_fw_file(ar, ar->hw_params.fw.dir,
				  ar->hw_params.fw.otp);
	if (IS_ERR(fw)) {
		ath10k_warn("could not fetch otp (%ld)\n", PTR_ERR(fw));
		return 0;
	}

	ret = ath10k_bmi_fast_download(ar, address, fw->data, fw->size);
	if (ret) {
		ath10k_err("could not write otp (%d)\n", ret);
		goto exit;
	}

	exec_param = 0;
	ret = ath10k_bmi_execute(ar, address, &exec_param);
	if (ret) {
		ath10k_err("could not execute otp (%d)\n", ret);
		goto exit;
	}

exit:
	release_firmware(fw);
	return ret;
}

static int ath10k_download_fw(struct ath10k *ar)
{
	const struct firmware *fw;
	u32 address;
	int ret;

	if (ar->hw_params.fw.fw == NULL)
		return -EINVAL;

	address = ar->hw_params.patch_load_addr;

	fw = ath10k_fetch_fw_file(ar, ar->hw_params.fw.dir,
				  ar->hw_params.fw.fw);
	if (IS_ERR(fw)) {
		ath10k_err("could not fetch fw (%ld)\n", PTR_ERR(fw));
		return PTR_ERR(fw);
	}

	ret = ath10k_bmi_fast_download(ar, address, fw->data, fw->size);
	if (ret) {
		ath10k_err("could not write fw (%d)\n", ret);
		goto exit;
	}

exit:
	release_firmware(fw);
	return ret;
}

static int ath10k_init_download_firmware(struct ath10k *ar)
{
	int ret;

	ret = ath10k_download_board_data(ar);
	if (ret)
		return ret;

	ret = ath10k_download_and_run_otp(ar);
	if (ret)
		return ret;

	ret = ath10k_download_fw(ar);
	if (ret)
		return ret;

	return ret;
}

static int ath10k_init_uart(struct ath10k *ar)
{
	int ret;

	/*
	 * Explicitly setting UART prints to zero as target turns it on
	 * based on scratch registers.
	 */
	ret = ath10k_bmi_write32(ar, hi_serial_enable, 0);
	if (ret) {
		ath10k_warn("could not disable UART prints (%d)\n", ret);
		return ret;
	}

	if (!uart_print) {
		ath10k_info("UART prints disabled\n");
		return 0;
	}

	ret = ath10k_bmi_write32(ar, hi_dbg_uart_txpin, 7);
	if (ret) {
		ath10k_warn("could not enable UART prints (%d)\n", ret);
		return ret;
	}

	ret = ath10k_bmi_write32(ar, hi_serial_enable, 1);
	if (ret) {
		ath10k_warn("could not enable UART prints (%d)\n", ret);
		return ret;
	}

	ath10k_info("UART prints enabled\n");
	return 0;
}

static int ath10k_init_hw_params(struct ath10k *ar)
{
	const struct ath10k_hw_params *uninitialized_var(hw_params);
	int i;

	for (i = 0; i < ARRAY_SIZE(ath10k_hw_params_list); i++) {
		hw_params = &ath10k_hw_params_list[i];

		if (hw_params->id == ar->target_version)
			break;
	}

	if (i == ARRAY_SIZE(ath10k_hw_params_list)) {
		ath10k_err("Unsupported hardware version: 0x%x\n",
			   ar->target_version);
		return -EINVAL;
	}

	ar->hw_params = *hw_params;

	ath10k_info("Hardware name %s version 0x%x\n",
		    ar->hw_params.name, ar->target_version);

	return 0;
}

struct ath10k *ath10k_core_create(void *hif_priv, struct device *dev,
				  enum ath10k_bus bus,
				  const struct ath10k_hif_ops *hif_ops)
{
	struct ath10k *ar;

	ar = ath10k_mac_create();
	if (!ar)
		return NULL;

	ar->ath_common.priv = ar;
	ar->ath_common.hw = ar->hw;

	ar->p2p = !!ath10k_p2p;
	ar->dev = dev;

	ar->hif.priv = hif_priv;
	ar->hif.ops = hif_ops;
	ar->hif.bus = bus;

	ar->free_vdev_map = 0xFF; /* 8 vdevs */

	init_completion(&ar->scan.started);
	init_completion(&ar->scan.completed);
	init_completion(&ar->scan.on_channel);

	init_completion(&ar->install_key_done);
	init_completion(&ar->vdev_setup_done);

	setup_timer(&ar->scan.timeout, ath10k_reset_scan, (unsigned long)ar);

	ar->workqueue = create_singlethread_workqueue("ath10k_wq");
	if (!ar->workqueue)
		goto err_wq;

	mutex_init(&ar->conf_mutex);
	spin_lock_init(&ar->data_lock);

	INIT_LIST_HEAD(&ar->peers);
	init_waitqueue_head(&ar->peer_mapping_wq);

	init_completion(&ar->offchan_tx_completed);
	INIT_WORK(&ar->offchan_tx_work, ath10k_offchan_tx_work);
	skb_queue_head_init(&ar->offchan_tx_queue);

	init_waitqueue_head(&ar->event_queue);

	return ar;

err_wq:
	ath10k_mac_destroy(ar);
	return NULL;
}
EXPORT_SYMBOL(ath10k_core_create);

void ath10k_core_destroy(struct ath10k *ar)
{
	flush_workqueue(ar->workqueue);
	destroy_workqueue(ar->workqueue);

	ath10k_mac_destroy(ar);
}
EXPORT_SYMBOL(ath10k_core_destroy);


int ath10k_core_register(struct ath10k *ar)
{
	struct ath10k_htc_ops htc_ops;
	struct bmi_target_info target_info;
	int status;

	memset(&target_info, 0, sizeof(target_info));
	status = ath10k_bmi_get_target_info(ar, &target_info);
	if (status)
		goto err;

	ar->target_version = target_info.version;
	ar->hw->wiphy->hw_version = target_info.version;

	status = ath10k_init_hw_params(ar);
	if (status)
		goto err;

	if (ath10k_init_configure_target(ar)) {
		status = -EINVAL;
		goto err;
	}

	status = ath10k_init_download_firmware(ar);
	if (status)
		goto err;

	status = ath10k_init_uart(ar);
	if (status)
		goto err;

	htc_ops.target_send_suspend_complete = ath10k_send_suspend_complete;

	ar->htc = ath10k_htc_create(ar, &htc_ops);
	if (IS_ERR(ar->htc)) {
		status = PTR_ERR(ar->htc);
		ath10k_err("could not create HTC (%d)\n", status);
		goto err;
	}

	status = ath10k_bmi_done(ar);
	if (status)
		goto err_htc_destroy;

	status = ath10k_wmi_attach(ar);
	if (status) {
		ath10k_err("WMI attach failed: %d\n", status);
		goto err_htc_destroy;
	}

	status = ath10k_htc_wait_target(ar->htc);
	if (status)
		goto err_wmi_detach;

	ar->htt = ath10k_htt_attach(ar);
	if (!ar->htt) {
		status = -ENOMEM;
		goto err_wmi_detach;
	}

	status = ath10k_init_connect_htc(ar);
	if (status)
		goto err_htt_detach;

	ath10k_info("firmware %s booted\n", ar->hw->wiphy->fw_version);

	status = ath10k_check_fw_version(ar);
	if (status)
		goto err_disconnect_htc;

	status = ath10k_wmi_cmd_init(ar);
	if (status) {
		ath10k_err("could not send WMI init command (%d)\n", status);
		goto err_disconnect_htc;
	}

	status = ath10k_wmi_wait_for_unified_ready(ar);
	if (status <= 0) {
		ath10k_err("wmi unified ready event not received\n");
		status = -ETIMEDOUT;
		goto err_disconnect_htc;
	}

	status = ath10k_htt_attach_target(ar->htt);
	if (status)
		goto err_disconnect_htc;

	status = ath10k_mac_register(ar);
	if (status)
		goto err_disconnect_htc;

	status = ath10k_debug_create(ar);
	if (status) {
		ath10k_err("unable to initialize debugfs\n");
		goto err_unregister_mac;
	}

	return 0;

err_unregister_mac:
	ath10k_mac_unregister(ar);
err_disconnect_htc:
	ath10k_htc_stop(ar->htc);
err_htt_detach:
	ath10k_htt_detach(ar->htt);
err_wmi_detach:
	ath10k_wmi_detach(ar);
err_htc_destroy:
	ath10k_htc_destroy(ar->htc);
err:
	return status;
}
EXPORT_SYMBOL(ath10k_core_register);

void ath10k_core_unregister(struct ath10k *ar)
{
	/* We must unregister from mac80211 before we stop HTC and HIF.
	 * Otherwise we will fail to submit commands to FW and mac80211 will be
	 * unhappy about callback failures. */
	ath10k_mac_unregister(ar);
	ath10k_htc_stop(ar->htc);
	ath10k_htt_detach(ar->htt);
	ath10k_wmi_detach(ar);
	ath10k_htc_destroy(ar->htc);
}
EXPORT_SYMBOL(ath10k_core_unregister);

int ath10k_core_target_suspend(struct ath10k *ar)
{
	int ret;

	ath10k_dbg(ATH10K_DBG_CORE, "%s: called", __func__);

	ret = ath10k_wmi_pdev_suspend_target(ar);
	if (ret)
		ath10k_warn("could not suspend target (%d)\n", ret);

	return ret;
}
EXPORT_SYMBOL(ath10k_core_target_suspend);

int ath10k_core_target_resume(struct ath10k *ar)
{
	int ret;

	ath10k_dbg(ATH10K_DBG_CORE, "%s: called", __func__);

	ret = ath10k_wmi_pdev_resume_target(ar);
	if (ret)
		ath10k_warn("could not resume target (%d)\n", ret);

	return ret;
}
EXPORT_SYMBOL(ath10k_core_target_resume);

MODULE_AUTHOR("Qualcomm Atheros");
MODULE_DESCRIPTION("Core module for QCA988X PCIe devices.");
MODULE_LICENSE("Dual BSD/GPL");
