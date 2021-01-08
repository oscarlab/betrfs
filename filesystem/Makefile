
include $(PWD)/.mkinclude

obj-m += ftfs.o
ccflags-y += -fno-builtin

ftfs-objs := \
	ftfs_module.o \
	sb_linkage.o \
	sb_partitioned_counter.o \
	sb_random.o \
	sb_error.o \
	sb_rwsem.o \
	sb_pthread.o \
	sb_files.o \
	sb_malloc.o \
	ftfs_southbound.o \
	sb_stat.o \
	sb_assert.o \
	sb_stdio.o \
	sb_misc.o \
	sb_strerror.o \
	nb_super.o \
	nb_bstore.o \
	nb_proc_toku_engine_status.o \
	nb_proc_toku_checkpoint.o \
	nb_proc_toku_flusher.o \
	nb_proc_toku_dump_node.o \
	nb_proc_toku_memleak_detect.o \
	sb_zlib.o \
	libftindex.o \
	sb_flock.o \
	sb_sfs_ops.o

#KBUILD_CFLAGS += -DUSE_SFS_DIO
#KBUILD_CFLAGS += -DFTFS_CIRCLE
KBUILD_CFLAGS += -DFTFS_PFSPLIT
KBUILD_CFLAGS += -DFTFS_NO_PRINT
#KBUILD_CFLAGS += -DFTFS_DEBUG
#KBUILD_CFLAGS += -DFTFS_MEM_DEBUG
#KBUILD_CFLAGS += -DTOKU_MEMLEAK_DETECT
#KBUILD_CFLAGS += -DTOKU_DEBUG_PTRS
#KBUILD_CFLAGS += -DSOUTHBOUND_DEBUG
KBUILD_CFLAGS += -DRMDIR_RANGE_DELETE
#KBUILD_CFLAGS += -DFTFS_EMPTY_DIR_VERIFY

#include $(PWD)/.turnoff-opts.mkinclude

LIBSRC-OBJS = \
libsrc/ydb.cc.o \
libsrc/ydb_cursor.cc.o \
libsrc/ydb_db.cc.o \
libsrc/ydb_env_func.cc.o \
libsrc/ydb_row_lock.cc.o \
libsrc/ydb_txn.cc.o \
libsrc/ydb_write.cc.o \
libsrc/errors.cc.o \
libsrc/indexer.cc.o \
libsrc/indexer-undo-do.cc.o
#libsrc/indexer-undo-do.cc.o \
#libsrc/toku_patent.cc.o \


LIBFT-OBJS = \
libft/background_job_manager.cc.o \
libft/block_allocator.cc.o \
libft/block_table.cc.o \
libft/bndata.cc.o \
libft/cachetable.cc.o \
libft/checkpoint.cc.o \
libft/compress.cc.o \
libft/fifo.cc.o \
libft/queue.cc.o \
libft/ft.cc.o \
libft/ft-cachetable-wrappers.cc.o \
libft/ft-flusher.cc.o \
libft/ft-hot-flusher.cc.o \
libft/ft_msg.cc.o \
libft/ft_node-serialize.cc.o \
libft/ft-node-deserialize.cc.o \
libft/ft-ops.cc.o \
libft/ft-serialize.cc.o \
libft/ft-test-helpers.cc.o \
libft/ft-verify.cc.o \
libft/key.cc.o \
libft/leafentry.cc.o \
libft/le-cursor.cc.o \
libft/logcursor.cc.o \
libft/logfilemgr.cc.o \
libft/logger.cc.o \
libft/log_upgrade.cc.o \
libft/memarena.cc.o \
libft/minicron.cc.o \
libft/omt.cc.o \
libft/quicklz.cc.o \
libft/recover.cc.o \
libft/rollback.cc.o \
libft/rollback-apply.cc.o \
libft/rollback-ct-callbacks.cc.o \
libft/rollback_log_node_cache.cc.o \
libft/roll.cc.o \
libft/ftloader-callback.cc.o \
libft/sub_block.cc.o \
libft/txn.cc.o \
libft/pqueue.cc.o \
libft/txn_child_manager.cc.o \
libft/txn_manager.cc.o \
libft/ule.cc.o \
libft/x1764.cc.o \
libft/xids.cc.o \
libft/ybt.cc.o \
libft/log_code.cc.o \
libft/log_print.cc.o


# Not needed in kernel, according to Leif
#libft/ftloader.cc.o \
#libft/ftloader-callback.cc.o \
# libft/dbufio.cc.o \
# libft/pqueue.cc.o \
# libft/queue.cc.o \

LIBUTIL-OBJS = \
libutil/frwlock.cc.o \
libutil/kibbutz.cc.o \
libutil/mempool.cc.o \
libutil/threadpool.cc.o
# Thread-local storage
#libutil/partitioned_counter.cc.o

LIBPORTABILITY-OBJS = \
libportability/huge_page_detection.cc.o \
libportability/file.cc.o \
libportability/portability.cc.o \
libportability/toku_path.cc.o \
libportability/toku_pthread.cc.o \
libportability/toku_time.cc.o
#libportability/memory.cc.o \
#libportability/toku_crash.cc.o \
# libportability/toku_assert.cc.o
# Thread-local storage
#libportability/toku_fair_rwlock.cc.o

LIBLOCKTREE-OBJS = \
liblocktree/locktree.cc.o \
liblocktree/manager.cc.o \
liblocktree/lock_request.cc.o \
liblocktree/txnid_set.cc.o \
liblocktree/range_buffer.cc.o \
liblocktree/keyrange.cc.o \
liblocktree/wfg.cc.o

LIBLZMA-OBJS = \
	liblzma/liblzma_la-common.o \
	liblzma/liblzma_la-easy_buffer_encoder.o \
	liblzma/liblzma_la-easy_decoder_memusage.o \
	liblzma/liblzma_la-stream_buffer_encoder.o \
	liblzma/liblzma_la-stream_buffer_decoder.o \
	liblzma/liblzma_la-stream_flags_encoder.o \
	liblzma/liblzma_la-stream_flags_decoder.o \
	liblzma/liblzma_la-stream_flags_common.o \
	liblzma/liblzma_la-stream_decoder.o \
	liblzma/liblzma_la-block_buffer_encoder.o \
	liblzma/liblzma_la-block_buffer_decoder.o \
	liblzma/liblzma_la-easy_preset.o \
	liblzma/liblzma_la-block_header_encoder.o \
	liblzma/liblzma_la-block_header_decoder.o \
	liblzma/liblzma_la-block_decoder.o \
	liblzma/liblzma_la-check.o \
	liblzma/liblzma_la-index.o \
	liblzma/liblzma_la-vli_size.o \
	liblzma/liblzma_la-crc32_fast.o \
	liblzma/liblzma_la-crc32_table.o \
	liblzma/liblzma_la-crc64_fast.o \
	liblzma/liblzma_la-crc64_table.o \
	liblzma/liblzma_la-filter_flags_encoder.o \
	liblzma/liblzma_la-filter_flags_decoder.o \
	liblzma/liblzma_la-filter_encoder.o \
	liblzma/liblzma_la-filter_decoder.o \
	liblzma/liblzma_la-arm.o \
	liblzma/liblzma_la-armthumb.o \
	liblzma/liblzma_la-lzma_encoder.o \
	liblzma/liblzma_la-lzma_decoder.o \
	liblzma/liblzma_la-lzma_encoder_presets.o \
	liblzma/liblzma_la-lzma2_encoder.o \
	liblzma/liblzma_la-lzma2_decoder.o \
	liblzma/liblzma_la-x86.o \
	liblzma/liblzma_la-filter_common.o \
	liblzma/liblzma_la-fastpos_table.o \
	liblzma/liblzma_la-delta_encoder.o \
	liblzma/liblzma_la-delta_decoder.o \
	liblzma/liblzma_la-delta_common.o \
	liblzma/liblzma_la-simple_coder.o \
	liblzma/liblzma_la-simple_encoder.o \
	liblzma/liblzma_la-simple_decoder.o \
	liblzma/liblzma_la-lzma_encoder_optimum_fast.o \
	liblzma/liblzma_la-lzma_encoder_optimum_normal.o \
	liblzma/liblzma_la-powerpc.o \
	liblzma/liblzma_la-price_table.o \
	liblzma/liblzma_la-sha256.o \
	liblzma/liblzma_la-sparc.o \
	liblzma/liblzma_la-vli_encoder.o \
	liblzma/liblzma_la-vli_decoder.o \
	liblzma/liblzma_la-index_encoder.o \
	liblzma/liblzma_la-index_decoder.o \
	liblzma/liblzma_la-index_hash.o \
	liblzma/liblzma_la-ia64.o \
	liblzma/liblzma_la-lz_encoder.o \
	liblzma/liblzma_la-lz_decoder.o \
	liblzma/liblzma_la-lz_encoder_mf.o \
	liblzma/liblzma_la-block_util.o \

all: libftindex.o
	make -C $(MOD_KERN_SOURCE) M=$(PWD) modules

libftindex.o: $(LIBPORTABILITY-OBJS) $(LIBFT-OBJS) $(LIBUTIL-OBJS) $(LIBLOCKTREE-OBJS) $(LIBSRC-OBJS) $(LIBLZMA-OBJS)
	ld -r $^ -o $@

libsrc/%.o: ../build/src/libtokufractaltree_static.a
	(cd libsrc; ar x ../$< $*.o)

libft/%.o: ../build/ft/libft_static.a
	(cd libft; ar x ../$< $*.o)

libutil/%.o: ../build/util/libutil_static.a
	(cd libutil; ar x ../$< $*.o)

libportability/%.o: ../build/portability/libtokuportability_static_conv.a
	(cd libportability; ar x ../$< $*.o)

liblocktree/%.o: ../build/locktree/liblocktree_static.a
	(cd liblocktree; ar x ../$< $*.o)

liblzma/%.o: ../build/xz/lib/liblzma.a
	(cd liblzma; ar x ../$< $*.o)

clean:
	make -C /lib/modules/$(KDIR)/build M=$(PWD) clean
