ifdef M
	include $(M)/.mkinclude
else
	include $(PWD)/.mkinclude
endif

obj-m += ftfs.o
ccflags-y += -fno-builtin -Wall -Werror

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
	nb_proc_pfn.o \
	sb_zlib.o \
	libftindex.o \
	sb_flock.o \
	sb_sfs_ops.o \
	nb_readahead.o \
	sb_pfn.o

#KBUILD_CFLAGS += -DFTFS_CIRCLE
#KBUILD_CFLAGS += -DFTFS_SMART_PIVOT
KBUILD_CFLAGS += -DFTFS_PFSPLIT
KBUILD_CFLAGS += -DFTFS_NO_PRINT
KBUILD_CFLAGS += -DRMDIR_RANGE_DELETE
KBUILD_CFLAGS += -DFT_INDIRECT
#KBUILD_CFLAGS += -DREAD_DIR_OPT

# TODO: couldn't figure out how to set KBUILD_CFLAGS only for debug target. So,
# just create a debug flags variables, and use it directly in the debug target.
# There has to be a better way, but we're on a deadline.
FTFS_DEBUG_CFLAGS := -ggdb -g3 -Og

# This flag enables extremely verbose output and only enables only one extra
# check. If enabled by for all debug builds, we'd need to increase Jenkins jobs
# timeouts simply due to the verbosity.
# FTFS_DEBUG_CFLAGS += -DFTFS_DEBUG

# We don't understand why, but xattrs are causing performance regressions.
# Enable them only for the debug build, since one xfstest uses xattrs.
FTFS_DEBUG_CFLAGS += -DFTFS_XATTR
FTFS_DEBUG_CFLAGS += -DFT_PAGE_DEBUG
# FTFS_DEBUG_CFLAGS += -DFTFS_MEM_DEBUG
# FTFS_DEBUG_CFLAGS += -DSOUTHBOUND_DEBUG
# FTFS_DEBUG_CFLAGS += -DTOKU_MEMLEAK_DETECT
# FTFS_DEBUG_CFLAGS += -DFTFS_EMPTY_DIR_VERIFY

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
libft/ft-indirect.cc.o \
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
libft/ft-slice.cc.o \
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
libportability/file.cc.o \
libportability/portability.cc.o \
libportability/toku_path.cc.o \
libportability/toku_pthread.cc.o \
libportability/toku_time.cc.o
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

all: libftindex.o
	$(MAKE) -C $(MOD_KERN_SOURCE) M=$(PWD) modules

bench: libftindex.o
	$(MAKE) -C $(MOD_KERN_SOURCE) M=$(PWD) modules

debug: libftindex.o
	$(MAKE) -C $(MOD_KERN_SOURCE) M=$(PWD) ccflags-y="$(ccflags-y) $(FTFS_DEBUG_CFLAGS)" modules

libftindex.o: $(LIBPORTABILITY-OBJS) $(LIBFT-OBJS) $(LIBUTIL-OBJS) $(LIBLOCKTREE-OBJS) $(LIBSRC-OBJS)
	ld -r $^ -o $@

libsrc/%.o: ../build/src/libtokudb_static_conv.a
	(cd libsrc; ar x ../$< $*.o)

libft/%.o: ../build/ft/libft_static.a
	(cd libft; ar x ../$< $*.o)

libutil/%.o: ../build/util/libutil_static.a
	(cd libutil; ar x ../$< $*.o)

libportability/%.o: ../build/portability/libtokuportability_static_conv.a
	(cd libportability; ar x ../$< $*.o)

liblocktree/%.o: ../build/locktree/liblocktree_static.a
	(cd liblocktree; ar x ../$< $*.o)

clean:
	$(MAKE) -C $(MOD_KERN_SOURCE) M=$(PWD) clean
