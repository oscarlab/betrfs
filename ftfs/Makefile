ifdef M
	include $(M)/.mkinclude
else
	include $(PWD)/.mkinclude
endif

obj-m += ftfs.o
ccflags-y += -fno-builtin

ftfs-objs := \
	ftfs_module.o \
	toku_linkage.o \
	ftfs_partitioned_counter.o \
	ftfs_malloc.o \
	ftfs_random.o \
	ftfs_error.o \
	ftfs_pthread.o \
	ftfs_files.o \
	ftfs_southbound.o \
	ftfs_dir.o \
	ftfs_stat.o \
	ftfs_assert.o \
	toku_memory.o \
	ftfs_stdio.o \
	toku_run_test.o \
	toku_test_mempool.o \
	toku_test_southbound.o \
	toku_misc.o \
	strerror.o \
	toku_zlib.o \
	libftindex.o

KBUILD_CFLAGS += -DSOUTHBOUND_TESTING
#KBUILD_CFLAGS += -DFTFS_DEBUG

# CFLAGS_REMOVE_ftfs_module.o = -O2
# CFLAGS_ftfs_module.o = -g -O0
# CFLAGS_REMOVE_toku_linkage.o = -O2
# CFLAGS_toku_linkage.o = -g -O0
# CFLAGS_REMOVE_ftfs_malloc.o = -O2
# CFLAGS_ftfs_malloc.o = -g -O0
# CFLAGS_REMOVE_ftfs_random.o = -O2
# CFLAGS_ftfs_random.o = -g -O0
# CFLAGS_REMOVE_ftfs_error.o = -O2
# CFLAGS_ftfs_error.o = -g -O0
# CFLAGS_REMOVE_ftfs_pthread.o = -O2
# CFLAGS_ftfs_pthread.o = -g -O0
 CFLAGS_REMOVE_ftfs_files.o = -O2
 CFLAGS_ftfs_files.o = -g -O0
# CFLAGS_REMOVE_ftfs_southbound.o = -O2
# CFLAGS_ftfs_southbound.o = -g -O0
# CFLAGS_REMOVE_ftfs_dir.o = -O2
# CFLAGS_ftfs_dir.o = -g -O0
# CFLAGS_REMOVE_ftfs_stat.o = -O2
# CFLAGS_ftfs_stat.o = -g -O0
# CFLAGS_REMOVE_ftfs_compress.o = -O2
# CFLAGS_ftfs_compress.o = -g -O0
# CFLAGS_REMOVE_ftfs_assert.o = -O2
# CFLAGS_ftfs_assert.o = -g -O0
# CFLAGS_REMOVE_toku_memory.o = -O2
# CFLAGS_toku_memory.o = -g -O0
# CFLAGS_REMOVE_ftfs_stdio.o = -O2
# CFLAGS_ftfs_stdio.o = -g -O0
# CFLAGS_REMOVE_toku_run_test.o = -O2
# CFLAGS_toku_run_test.o = -g -O0
 CFLAGS_REMOVE_toku_test_southbound.o = -O2
 CFLAGS_toku_test_southbound.o = -g -O0
# CFLAGS_REMOVE_toku_misc.o = -O2
# CFLAGS_toku_misc.o = -g -O0
# CFLAGS_REMOVE_ftfs.o = -O2
# CFLAGS_ftfs.o = -g -O0

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
libsrc/indexer-undo-do.cc.o \
libsrc/test/seq-io.cc.o \
libsrc/test/get_key_after_bytes_unit.cc.o \
libsrc/test/test_db_dbt_mem_behavior.cc.o \
libsrc/test/test_update_broadcast_stress.cc.o \
libsrc/test/test_db_txn_locks_nonheaviside.cc.o \
libsrc/test/test_log6.cc.o \
libsrc/test/test_log4.cc.o \
libsrc/test/test_log10.cc.o \
libsrc/test/test_log9.cc.o \
libsrc/test/test_log8.cc.o \
libsrc/test/update.cc.o \
libsrc/test/test_db_env_set_lg_dir.cc.o \
libsrc/test/test_db_env_set_tmp_dir.cc.o \
libsrc/test/test-prepare.cc.o \
libsrc/test/test-prepare2.cc.o \
libsrc/test/test-prepare3.cc.o \
libsrc/test/test_kv_limits.cc.o \
libsrc/test/test_restrict.cc.o \
libsrc/test/test_db_change_xxx.cc.o \
libsrc/test/test_cursor_DB_NEXT_no_dup.cc.o \
libsrc/test/test_log4_abort.cc.o \
libsrc/test/test_log6_abort.cc.o \
libsrc/test/test_log5.cc.o \
libsrc/test/test_log5_abort.cc.o \
libsrc/test/test_log6a_abort.cc.o \
libsrc/test/test_reverse_compare_fun.cc.o \
libsrc/test/test_read_txn_invalid_ops.cc.o \
libsrc/test/test_update_txn_snapshot_works_concurrently.cc.o \
libsrc/test/test_db_descriptor.cc.o \
libsrc/test/test_3645.cc.o \
libsrc/test/test_update_txn_snapshot_works_correctly_with_deletes.cc.o \
libsrc/test/test_iterate_pending_lock_requests.cc.o \
libsrc/test/test_cursor_nonleaf_expand.cc.o \
libsrc/test/test_mostly_seq.cc.o \
libsrc/test/test_db_named_delete_last.cc.o \
libsrc/test/test_db_already_exists.cc.o \
libsrc/test/test_4015.cc.o \
libsrc/test/test_cursor_db_current.cc.o \
libsrc/test/test_db_set_flags.cc.o \
libsrc/test/test_db_current_clobbers_db.cc.o \
libsrc/test/test_insert_cursor_delete_insert.cc.o \
libsrc/test/test_db_version.cc.o \
libsrc/test/test_large_update_broadcast_small_cachetable.cc.o \
libsrc/test/test_cursor_with_read_txn.cc.o \
libsrc/test/test_db_close_no_open.cc.o \
libsrc/test/test_789.cc.o \
libsrc/test/test-rollinclude.cc.o \
libsrc/test/test_db_change_pagesize.cc.o \
libsrc/test/test_3755.cc.o \
libsrc/test/test-5138.cc.o \
libsrc/test/test_3645.cc.o \
libsrc/test/test_5469.cc.o \
libsrc/test/test_update_with_empty_table.cc.o \
libsrc/test/test_updates_single_key.cc.o \
libsrc/test/test_cachesize.cc.o \
libsrc/test/checkpoint1.cc.o \
libsrc/test/update-multiple-data-diagonal.cc.o \
libsrc/test/test_935.cc.o \
libsrc/test/progress.cc.o \
libsrc/test/test_weakxaction.cc.o \
libsrc/test/transactional_fileops.cc.o \
libsrc/test/test_db_txn_locks_read_uncommitted.cc.o \
libsrc/test/test_simple_read_txn.cc.o \
libsrc/test/test_4657.cc.o \
libsrc/test/test_4368.cc.o \
libsrc/test/test938b.cc.o \
libsrc/test/test4573-logtrim.cc.o \
libsrc/test/test5092.cc.o \
libsrc/test/zombie_db.cc.o \
libsrc/test/test_cursor_null.cc.o \
libsrc/test/test_db_remove_subdb.cc.o \
libsrc/test/test_groupcommit_perf.cc.o \
libsrc/test/test_db_subdb_different_flags.cc.o \
libsrc/test/test_txn_cursor_last.cc.o \
libsrc/test/test_insert_memleak.cc.o \
libsrc/test/inflate.cc.o \
libsrc/test/test-xa-prepare.cc.o \
libsrc/test/test_cursor_2.cc.o \
libsrc/test/test_db_get_put_flags.cc.o \
libsrc/test/isolation-read-committed.cc.o \
libsrc/test/isolation.cc.o \
libsrc/test/test_txn_commit8.cc.o \
libsrc/test/test-xopen-eclose.cc.o \
libsrc/test/inflate.cc.o \
libsrc/test/test_db_delete.cc.o \
libsrc/test/test938.cc.o \
libsrc/test/test_hsoc.cc.o \
libsrc/test/test_locktree_close.cc.o \
libsrc/test/inflate2.cc.o \
libsrc/test/filesize.cc.o \
libsrc/test/test_txn_nested1.cc.o \
libsrc/test/test_txn_nested2.cc.o \
libsrc/test/test_txn_nested3.cc.o \
libsrc/test/test_txn_nested4.cc.o \
libsrc/test/test_txn_nested5.cc.o \
libsrc/test/rowsize.cc.o \
libsrc/test/test_db_subdb.cc.o \
libsrc/test/test_txn_begin_commit.cc.o \
libsrc/test/test_get_max_row_size.cc.o \
libsrc/test/test_update_broadcast_changes_values.cc.o \
libsrc/test/test_update_broadcast_indexer.cc.o \
libsrc/test/update-multiple-nochange.cc.o \
libsrc/test/test_5015.cc.o \
libsrc/test/update-multiple-key0.cc.o \
libsrc/test/update-multiple-with-indexer.cc.o \
libsrc/test/update-multiple-with-indexer-array.cc.o \
libsrc/test/test_log0.cc.o \
libsrc/test/test_log1.cc.o \
libsrc/test/test_log2.cc.o \
libsrc/test/test_log3.cc.o \
libsrc/test/test_log2_abort.cc.o \
libsrc/test/test_log3_abort.cc.o \
libsrc/test/blackhole.cc.o \
libsrc/test/cursor-isolation.cc.o \
libsrc/test/put-del-multiple-array-indexing.cc.o \
libsrc/test/test_cursor_flags.cc.o \
libsrc/test/bug1381.cc.o \
libsrc/test/last-verify-time.cc.o \
libsrc/test/cursor-step-over-delete.cc.o \
libsrc/test/seqinsert.cc.o \
libsrc/test/blocking-first.cc.o \
libsrc/test/blocking-first-empty.cc.o \
libsrc/test/blocking-next-prev.cc.o \
libsrc/test/blocking-next-prev-deadlock.cc.o \
libsrc/test/blocking-put.cc.o \
libsrc/test/blocking-put-wakeup.cc.o \
libsrc/test/blocking-put-timeout.cc.o \
libsrc/test/blocking-prelock-range.cc.o \
libsrc/test/manyfiles.cc.o \
libsrc/test/stat64.cc.o \
libsrc/test/stat64-create-modify-times.cc.o \
libsrc/test/stat64-null-txn.cc.o \
libsrc/test/stat64-root-changes.cc.o \
libsrc/test/create-datadir.cc.o \
libsrc/test/redirect.cc.o \
libsrc/test/del-multiple.cc.o \
libsrc/test/del-multiple-huge-primary-row.cc.o \
libsrc/test/del-simple.cc.o \
libsrc/test/del-multiple-srcdb.cc.o \
libsrc/test/test_abort1.cc.o \
libsrc/test/test_abort2.cc.o \
libsrc/test/test_abort3.cc.o \
libsrc/test/test_abort4.cc.o \
libsrc/test/test_abort5.cc.o \
libsrc/test/test_abort_delete_first.cc.o \
libsrc/test/db-put-simple-deadlock.cc.o \
libsrc/test/db-put-update-deadlock.cc.o \
libsrc/test/db-put-simple-deadlock-threads.cc.o \
libsrc/test/big-nested-abort-abort.cc.o \
libsrc/test/big-nested-commit-commit.cc.o \
libsrc/test/big-nested-commit-abort.cc.o \
libsrc/test/big-nested-abort-commit.cc.o \
libsrc/test/db-put-simple-lockwait.cc.o \
libsrc/test/keyrange-merge.cc.o \
libsrc/test/bigtxn27.cc.o\
libsrc/test/blocking-last.cc.o \
libsrc/test/test_db_env_open_close.cc.o \
libsrc/test/blocking-set.cc.o \
libsrc/test/blocking-set-range-0.cc.o \
libsrc/test/blocking-set-range-n.cc.o \
libsrc/test/blocking-set-range-reverse-0.cc.o \
libsrc/test/env_loader_memory.cc.o \
libsrc/test/directory_lock.cc.o\
libsrc/test/cursor-set-range-rmw.cc.o \
libsrc/test/cursor-set-del-rmw.cc.o \
libsrc/test/checkpoint_fairness.cc.o \
libsrc/test/checkpoint_stress.cc.o \
libsrc/test/cachetable-race.cc.o \
libsrc/test/blocking-table-lock.cc.o \
libsrc/test/test_txn_nested_abort.cc.o \
libsrc/test/preload-db.cc.o \
libsrc/test/simple.cc.o \
libsrc/test/multiprocess.cc.o \
libsrc/test/test_archive0.cc.o \
libsrc/test/test_archive1.cc.o \
libsrc/test/test_archive2.cc.o \
libsrc/test/preload-db-nested.cc.o\
libsrc/test/test_txn_nested_abort.cc.o\
libsrc/test/test_txn_nested_abort2.cc.o\
libsrc/test/test_txn_nested_abort3.cc.o\
libsrc/test/test_txn_nested_abort4.cc.o \
libsrc/test/recover-test1.cc.o \
libsrc/test/recover-test2.cc.o \
libsrc/test/recover-test3.cc.o \
libsrc/test/recover-2483.cc.o \
libsrc/test/shutdown-3344.cc.o \
libsrc/test/test_txn_nested_abort4.cc.o\
libsrc/test/test_txn_recover3.cc.o\
libsrc/test/test_unused_memory_crash.cc.o\
libsrc/test/test_update_abort_works.cc.o\
libsrc/test/test_update_broadcast_abort_works.cc.o\
libsrc/test/test_update_broadcast_calls_back.cc.o\
libsrc/test/test_update_broadcast_can_delete_elements.cc.o\
libsrc/test/test_update_broadcast_nested_updates.cc.o\
libsrc/test/prelock-read-read.cc.o \
libsrc/test/prelock-read-write.cc.o \
libsrc/test/prelock-write-write.cc.o \
libsrc/test/prelock-write-read.cc.o \
libsrc/test/insert-dup-prelock.cc.o \
libsrc/test/env_startup.cc.o \
libsrc/test/openlimit17.cc.o \
libsrc/test/openlimit17-metafiles.cc.o \
libsrc/test/openlimit17-locktree.cc.o \
libsrc/test/test_update_broadcast_previously_deleted.cc.o\
libsrc/test/test_update_broadcast_update_fun_has_choices.cc.o\
libsrc/test/test_update_broadcast_with_empty_table.cc.o \
libsrc/test/cursor-more-than-a-leaf-provdel.cc.o \
libsrc/test/dump-env.cc.o \
libsrc/test/medium-nested-commit-commit.cc.o \
libsrc/test/test_stress0.cc.o \
libsrc/test/test_stress1.cc.o \
libsrc/test/test_stress2.cc.o \
libsrc/test/test_stress3.cc.o \
libsrc/test/test_stress4.cc.o \
libsrc/test/test_stress6.cc.o \
libsrc/test/test_stress7.cc.o \
libsrc/test/test_stress_openclose.cc.o \
libsrc/test/test_stress_with_verify.cc.o \
libsrc/test/env-put-multiple.cc.o \
libsrc/test/test_transactional_descriptor.cc.o \
libsrc/test/mvcc-create-table.cc.o \
libsrc/test/mvcc-many-committed.cc.o \
libsrc/test/perf_checkpoint_var.cc.o \
libsrc/test/perf_child_txn.cc.o \
libsrc/test/perf_rangequery.cc.o \
libsrc/test/perf_read_txn.cc.o \
libsrc/test/perf_read_write.cc.o \
libsrc/test/perf_read_txn_single_thread.cc.o \
libsrc/test/perf_ptquery.cc.o \
libsrc/test/perf_txn_single_thread.cc.o \
libsrc/test/perf_nop.cc.o \
libsrc/test/perf_cursor_nop.cc.o \
libsrc/test/perf_insert.cc.o \
libsrc/test/perf_iibench.cc.o \
libsrc/test/perf_malloc_free.cc.o \
libsrc/test/perf_ptquery2.cc.o \
libsrc/test/stress-test.cc.o \
libsrc/test/stress-gc.cc.o \
libsrc/test/stress-gc2.cc.o \
libsrc/test/mvcc-read-committed.cc.o \
libsrc/test/test_update_calls_back.cc.o \
libsrc/test/test1753.cc.o \
libsrc/test/test1572.cc.o \
libsrc/test/test1842.cc.o \
libsrc/test/test3039.cc.o \
libsrc/test/test3219.cc.o \
libsrc/test/test3522.cc.o \
libsrc/test/test3522b.cc.o \
libsrc/test/test_3529_insert_2.cc.o \
libsrc/test/test_3529_table_lock.cc.o \
libsrc/test/test_thread_flags.cc.o \
libsrc/test/test_thread_insert.cc.o \
libsrc/test/test_txn_abort5.cc.o \
libsrc/test/test_txn_abort5a.cc.o \
libsrc/test/test_txn_abort6.cc.o \
libsrc/test/test_txn_abort7.cc.o \
libsrc/test/test_txn_abort8.cc.o \
libsrc/test/test_txn_abort9.cc.o \
libsrc/test/test_txn_close_before_commit.cc.o \
libsrc/test/test_txn_close_before_prepare_commit.cc.o \
libsrc/test/test_txn_close_open_commit.cc.o \
libsrc/test/test_env_close_flags.cc.o \
libsrc/test/test_env_open_flags.cc.o \
libsrc/test/test_env_create_db_create.cc.o \
libsrc/test/test_error.cc.o \
libsrc/test/test_forkjoin.cc.o \
libsrc/test/test_get_zeroed_dbt.cc.o\
libsrc/test/test_groupcommit_count.cc.o\
libsrc/test/test_locktree_close.cc.o\
libsrc/test/test_lock_timeout_callback.cc.o\
libsrc/test/test_locking_with_read_txn.cc.o\
libsrc/test/test_trans_desc_during_chkpt.cc.o \
libsrc/test/test_trans_desc_during_chkpt2.cc.o \
libsrc/test/test_trans_desc_during_chkpt4.cc.o \
libsrc/test/test_trans_desc_during_chkpt3.cc.o \
libsrc/test/test_update_can_delete_elements.cc.o \
libsrc/test/test_update_changes_values.cc.o \
libsrc/test/test_update_nested_updates.cc.o \
libsrc/test/test_update_nonexistent_keys.cc.o \
libsrc/test/test_update_previously_deleted.cc.o \
libsrc/test/test_blobs_leaf_split.cc.o \
libsrc/test/test_bulk_fetch.cc.o \
libsrc/test/test_cmp_descriptor.cc.o \
libsrc/test/test_compression_methods.cc.o \
libsrc/test/test_cursor_3.cc.o \
libsrc/test/test_log1_abort.cc.o \
libsrc/test/test_logflush.cc.o \
libsrc/test/test_logmax.cc.o \
libsrc/test/test_cursor_stickyness.cc.o \
libsrc/test/test_cursor_delete2.cc.o \
libsrc/test/replace-into-write-lock.cc.o \
libsrc/test/test_query.cc.o \
libsrc/test/test_nested.cc.o \
libsrc/test/test_nodup_set.cc.o \
libsrc/test/test_rand_insert.cc.o \
libsrc/test/test_zero_length_keys.cc.o \
libsrc/test/test_db_env_open_nocreate.cc.o \
libsrc/test/test_db_env_open_open_close.cc.o \
libsrc/test/test_db_env_set_errpfx.cc.o \
libsrc/test/test_db_open_notexist_reopen.cc.o \
libsrc/test/test_db_remove.cc.o \
libsrc/test/test_iterate_live_transactions.cc.o \
libsrc/test/root_fifo_1.cc.o \
libsrc/test/root_fifo_31.cc.o \
libsrc/test/root_fifo_32.cc.o \
libsrc/test/root_fifo_41.cc.o \
libsrc/test/root_fifo_2.cc.o \
libsrc/test/test_multiple_checkpoints_block_commit.cc.o \
libsrc/test/test_set_func_malloc.cc.o \
libsrc/test/test-xopen-eclose.cc.o \
libsrc/test/test-nested-xopen-eclose.cc.o \
libsrc/test/test_log7.cc.o \
libsrc/test/print_engine_status.cc.o \
libsrc/test/queries_with_deletes.cc.o \
libsrc/test/test_restrict.cc.o \
libsrc/test/test_update_stress.cc.o \
libsrc/test/test_db_env_strdup_null.cc.o \
libsrc/test/seqwrites-no-txn.cc.o \
libsrc/test/bench.cc.o \
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
libft/log_print.cc.o \
libft/test/keytest.cc.o \
libft/test/omt-test.cc.o \
libft/test/msnfilter.cc.o \
libft/test/orthopush-flush.cc.o \
libft/test/verify-bad-msn.cc.o \
libft/test/verify-bad-pivots.cc.o \
libft/test/verify-misrouted-msgs.cc.o \
libft/test/verify-dup-pivots.cc.o \
libft/test/verify-unsorted-pivots.cc.o \
libft/test/x1764-test.cc.o \
libft/test/xid_lsn_independent.cc.o \
libft/test/verify-dup-in-leaf.cc.o \
libft/test/pqueue-test.cc.o \
libft/test/keyrange.cc.o \
libft/test/comparator-test.cc.o \
libft/test/log-test.cc.o \
libft/test/is_empty.cc.o \
libft/test/verify-unsorted-leaf.cc.o \
libft/test/make-tree.cc.o \
libft/test/queue-test.cc.o \
libft/test/fifo-test.cc.o \
libft/test/logcursor-bw.cc.o \
libft/test/logcursor-fw.cc.o \
libft/test/test_logcursor.cc.o \
libft/test/logcursor-print.cc.o\
libft/test/list-test.cc.o \
libft/test/logfilemgr-print.cc.o \
libft/test/logfilemgr-create-destroy.cc.o \
libft/test/logcursor-print.cc.o \
libft/test/log-test3.cc.o \
libft/test/log-test4.cc.o \
libft/test/log-test2.cc.o \
libft/test/log-test6.cc.o \
libft/test/log-test7.cc.o \
libft/test/logcursor-timestamp.cc.o \
libft/test/logcursor-bad-checksum.cc.o \
libft/test/logcursor-empty-logfile.cc.o \
libft/test/logcursor-empty-logfile-2.cc.o \
libft/test/shortcut.cc.o \
libft/test/logcursor-empty-logfile-3.cc.o \
libft/test/logcursor-empty-logdir.cc.o \
libft/test/list-test.cc.o \
libft/test/minicron-test.cc.o \
libft/test/cachetable-all-write.cc.o \
libft/test/cachetable-cleaner-checkpoint2.cc.o \
libft/test/cachetable-prefetch-maybegetandpin-test.cc.o \
libft/test/cachetable-prefetch-getandpin-test.cc.o \
libft/test/cachetable-simple-maybe-get-pin.cc.o \
libft/test/cachetable-simple-pin-dep-nodes.cc.o \
libft/test/cachetable-simple-put-dep-nodes.cc.o \
libft/test/cachetable-simple-read-pin-nonblocking.cc.o \
libft/test/cachetable-simple-unpin-remove-checkpoint.cc.o \
libft/test/cachetable-clock-eviction.cc.o \
libft/test/cachetable-cleaner-thread-simple.cc.o \
libft/test/cachetable-clock-eviction2.cc.o \
libft/test/cachetable-clock-eviction3.cc.o \
libft/test/cachetable-clock-eviction4.cc.o \
libft/test/cachetable-eviction-close-test.cc.o \
libft/test/cachetable-clone-unpin-remove.cc.o \
libft/test/cachetable-eviction-close-test2.cc.o \
libft/test/cachetable-flush-during-cleaner.cc.o \
libft/test/cachetable-eviction-getandpin-test.cc.o \
libft/test/cachetable-eviction-getandpin-test2.cc.o \
libft/test/test3856.cc.o \
libft/test/test3681.cc.o \
libft/test/test-checkpoint-during-split.cc.o \
libft/test/le-cursor-provdel.cc.o \
libft/test/cachetable-cleaner-thread-attrs-accumulate.cc.o \
libft/test/cachetable-simple-read-pin.cc.o \
libft/test/cachetable-prefetch-close-test.cc.o \
libft/test/cachetable-cleaner-thread-empty-cachetable.cc.o \
libft/test/cachetable-simple-verify.cc.o \
libft/test/cachetable-simple-pin-nonblocking-cheap.cc.o \
libft/test/cachetable-simple-pin-nonblocking.cc.o \
libft/test/cachetable-fd-test.cc.o \
libft/test/cachetable-4357.cc.o \
libft/test/cachetable-4365.cc.o \
libft/test/cachetable-put-test.cc.o \
libft/test/cachetable-simple-pin.cc.o \
libft/test/cachetable-debug-test.cc.o \
libft/test/cachetable-simple-close.cc.o \
libft/test/cachetable-simple-pin-cheap.cc.o \
libft/test/cachetable-getandpin-test.cc.o \
libft/test/recovery-datadir-is-file.cc.o \
libft/test/test-txn-child-manager.cc.o \
libft/test/cachetable-simple-clone.cc.o \
libft/test/cachetable-simple-clone2.cc.o \
libft/test/cachetable-writer-thread-limit.cc.o \
libft/test/cachetable-evictor-class.cc.o \
libft/test/cachetable-checkpoint-test.cc.o \
libft/test/cachetable-pin-nonblocking-checkpoint-clean.cc.o \
libft/test/cachetable-checkpointer-class.cc.o \
libft/test/cachetable-rwlock-test.cc.o \
libft/test/cachetable-prefetch-flowcontrol-test.cc.o \
libft/test/cachetable-5097.cc.o \
libft/test/cachetable-5978.cc.o \
libft/test/cachetable-5978-2.cc.o \
libft/test/cachetable-put-checkpoint.cc.o \
libft/test/cachetable-clone-checkpoint.cc.o \
libft/test/compress-test.cc.o \
libft/test/ft-test0.cc.o \
libft/test/benchmark-test.cc.o \
libft/test/log-test-maybe-trim.cc.o \
libft/test/log-test5.cc.o \
libft/test/cachetable-test.cc.o \
libft/test/cachetable-checkpoint-pending.cc.o \
libft/test/cachetable-count-pinned-test.cc.o \
libft/test/cachetable-unpin-rm-and-checkpoint.cc.o \
libft/test/cachetable-clone-pin-nonblocking.cc.o \
libft/test/cachetable-clone-partial-fetch.cc.o \
libft/test/cachetable-flush-test.cc.o \
libft/test/cachetable-partial-fetch.cc.o \
libft/test/ft-test1.cc.o \
libft/test/ft-test2.cc.o \
libft/test/ft-test3.cc.o \
libft/test/ft-test4.cc.o \
libft/test/ft-test5.cc.o \
libft/test/ft-test.cc.o \
libft/test/block_allocator_test.cc.o \
libft/test/cachetable-prefetch-checkpoint-test.cc.o \
libft/test/cachetable-cleaner-thread-same-fullhash.cc.o \
libft/test/ft-clock-test.cc.o \
libft/test/cachetable-unpin-and-remove-test.cc.o \
libft/test/cachetable-unpin-test.cc.o \
libft/test/cachetable-cleaner-thread-nothing-needs-flushing.cc.o \
libft/test/test-bjm.cc.o \
libft/test/test-assert.cc.o \
libft/test/bnc-insert-benchmark.cc.o \
libft/test/ft-serialize-sub-block-test.cc.o \
libft/test/ft-serialize-benchmark.cc.o \
libft/test/ft-serialize-test.cc.o \
libft/test/ft-test-cursor.cc.o \
libft/test/ft-test-cursor-2.cc.o \
libft/test/ft-test-header.cc.o \
libft/test/ybt-test.cc.o \
libft/test/subblock-test-compression.cc.o \
libft/test/test-dump-ft.cc.o \
libft/test/test4244.cc.o \
libft/test/test4115.cc.o \
libft/test/test3884.cc.o \
libft/test/test1308a.cc.o \
libft/test/subblock-test-checksum.cc.o \
libft/test/subblock-test-index.cc.o \
libft/test/subblock-test-size.cc.o \
libft/test/ft-bfe-query.cc.o \
libft/test/test-dirty-flushes-on-cleaner.cc.o \
libft/test/test-flushes-on-cleaner.cc.o \
libft/test/test-checkpoint-during-merge.cc.o \
libft/test/test-checkpoint-during-flush.cc.o \
libft/test/test-checkpoint-during-rebalance.cc.o \
libft/test/test-merges-on-cleaner.cc.o \
libft/test/test_toku_malloc_plain_free.cc.o \
libft/test/test-del-inorder.cc.o \
libft/test/test_block_allocator_merge.cc.o \
libft/test/recovery-no-logdir.cc.o \
libft/test/test-hot-with-bounds.cc.o \
libft/test/cachetable-cleaner-checkpoint.cc.o \
libft/test/cachetable-cleaner-thread-everything-pinned.cc.o \
libft/test/cachetable-kibbutz_and_flush_cachefile.cc.o \
libft/test/cachetable-prefetch-close-leak-test.cc.o \
libft/test/test_oexcl.cc.o \
libft/test/cachetable-pin-checkpoint.cc.o \
libft/test/test-oldest-referenced-xid-flush.cc.o \
libft/test/recovery-cbegin.cc.o \
libft/test/recovery-no-datadir.cc.o \
libft/test/recovery-cbegin-cend-hello.cc.o \
libft/test/recovery-cbegin-cend.cc.o \
libft/test/recovery-cend-cbegin.cc.o \
libft/test/recovery-no-log.cc.o \
libft/test/recovery-hello.cc.o \
libft/test/recovery-lsn-error-during-forward-scan.cc.o \
libft/test/test-inc-split.cc.o \
libft/test/recovery-empty.cc.o \
libft/test/cachetable-fetch-inducing-evictor.cc.o \
libft/test/cachetable-clock-all-pinned.cc.o \
libft/test/cachetable-prefetch2-test.cc.o \
libft/test/cachetable-checkpoint-prefetched-nodes.cc.o \
libft/test/cachetable-checkpoint-pinned-nodes.cc.o \
libft/test/recovery-test5123.cc.o \
libft/test/le-cursor-walk.cc.o \
libft/test/test-leafentry-nested.cc.o \
libft/test/test-leafentry-child-txn.cc.o \
libft/test/recovery-fopen-missing-file.cc.o \
libft/test/recovery-bad-last-entry.cc.o \
libft/test/le-cursor-right.cc.o \
libft/test/cachetable-clone-partial-fetch-pinned-node.cc.o \
libft/test/quicklz-test.cc.o \
libft/test/test-pick-child-to-flush.cc.o \
libft/test/test-ft-overflow.cc.o \
libft/test/test-range-del.cc.o \
libft/test/test-range-del2.cc.o \
libft/test/recovery-range-delete.cc.o \
libft/test/test-ubi-root-chkpt.cc.o
#libft/test/test_unbound_insert_lists.cc.o

#libft/compress.cc.o \
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
libutil/threadpool.cc.o \
libutil/test/marked-omt-test.cc.o \
libutil/test/omt-tmpl-test.cc.o \
libutil/test/test-frwlock-fair-writers.cc.o \
libutil/test/test-rwlock-unfair-writers.cc.o \
libutil/test/test-kibbutz.cc.o \
libutil/test/test-rwlock-cheapness.cc.o \
libutil/test/test-rwlock.cc.o \
libutil/test/sort-tmpl-test.cc.o \
libutil/test/threadpool-testrunf.cc.o \
libutil/test/threadpool-test.cc.o \
libutil/test/test_doubly_linked_list.cc.o \
libutil/test/test_partitioned_counter_5833.cc.o \
libutil/test/test_partitioned_counter.cc.o
# Thread-local storage
#libutil/partitioned_counter.cc.o

LIBPORTABILITY-OBJS = \
libportability/huge_page_detection.cc.o \
libportability/file.cc.o \
libportability/os_malloc.cc.o \
libportability/portability.cc.o \
libportability/toku_path.cc.o \
libportability/toku_pthread.cc.o \
libportability/toku_time.cc.o    \
libportability/test/test-gettime.cc.o \
libportability/test/test-gettimeofday.cc.o \
libportability/test/test-filesystem-sizes.cc.o \
libportability/test/test-stat.cc.o \
libportability/test/test-active-cpus.cc.o \
libportability/test/test-cpu-freq.cc.o \
libportability/test/test-cpu-freq-openlimit17.cc.o \
libportability/test/test-fair-rwlock.cc.o \
libportability/test/test-pthread-rwlock-rwr.cc.o \
libportability/test/test-pthread-rwlock-rdlock.cc.o \
libportability/test/test-flock.cc.o \
libportability/test/test-snprintf.cc.o \
libportability/test/test-hugepage.cc.o \
libportability/test/test-fsync.cc.o \
libportability/test/test-fsync-directory.cc.o \
libportability/test/test-toku-malloc.cc.o \
libportability/test/test-memory-status.cc.o \
libportability/test/test-pwrite4g.cc.o
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
liblocktree/test/range_buffer_test.cc.o \
liblocktree/wfg.cc.o \
liblocktree/test/wfg_test.cc.o \
liblocktree/test/manager_create_destroy.cc.o \
liblocktree/test/manager_params.cc.o \
liblocktree/test/locktree_single_txnid_optimization.cc.o \
liblocktree/test/concurrent_tree_lkr_insert_remove.cc.o \
liblocktree/test/concurrent_tree_create_destroy.cc.o \
liblocktree/test/concurrent_tree_lkr_remove_all.cc.o \
liblocktree/test/locktree_create_destroy.cc.o \
liblocktree/test/locktree_simple_lock.cc.o \
liblocktree/test/locktree_conflicts.cc.o \
liblocktree/test/locktree_misc.cc.o \
liblocktree/test/lock_request_create_set.cc.o \
liblocktree/test/lock_request_get_set_keys.cc.o \
liblocktree/test/concurrent_tree_lkr_acquire_release.cc.o \
liblocktree/test/manager_reference_release_lt.cc.o \
liblocktree/test/txnid_set_test.cc.o \
liblocktree/test/concurrent_tree_lkr_insert_serial_large.cc.o \
liblocktree/test/locktree_overlapping_relock.cc.o \
liblocktree/test/lock_request_start_pending.cc.o \
liblocktree/test/lock_request_start_deadlock.cc.o \
liblocktree/test/locktree_escalation_stalls.cc.o \
liblocktree/test/locktree_infinity.cc.o \
liblocktree/test/lock_request_wait_time_callback.cc.o \
liblocktree/test/manager_status.cc.o \
liblocktree/test/manager_locktree_map.cc.o

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
	make -C  /lib/modules/$(KDIR)/build M=$(PWD) modules

libftindex.o: $(LIBPORTABILITY-OBJS) $(LIBFT-OBJS) $(LIBUTIL-OBJS) $(LIBLOCKTREE-OBJS) $(LIBSRC-OBJS) $(LIBLZMA-OBJS)
	ld -r $^ -o $@


libsrc/test/%.o: ../build/src/kernel_tests/libtokusrc_kernel_static.a
	(mkdir -p libsrc/test; cd libsrc/test; ar x ../../$< $*.o)

libsrc/%.o: ../build/src/libtokufractaltree_static.a
	(mkdir -p libsrc; cd libsrc; ar x ../$< $*.o)

libft/test/%.o: ../build/ft/kernel_tests/libtokuft_kernel_static.a
	(mkdir -p libft/test; cd libft/test; ar x ../../$< $*.o)

libft/%.o: ../build/ft/libft_static.a
	(mkdir -p libft; cd libft; ar x ../$< $*.o)


libutil/test/%.o: ../build/util/kernel_tests/libutil_kernel_static.a
	(mkdir -p libutil/test; cd libutil/test; ar x ../../$< $*.o)

libportability/test/%.o: ../build/portability/kernel_tests/libtokuportability_kernel_static.a
	(mkdir -p libportability/test; cd libportability/test; ar x ../../$< $*.o)

libutil/%.o: ../build/util/libutil_static.a
	(mkdir -p libutil; cd libutil; ar x ../$< $*.o)

libportability/%.o: ../build/portability/libtokuportability_static_conv.a
	(mkdir -p libportability; cd libportability; ar x ../$< $*.o)

liblocktree/test/%.o: ../build/locktree/kernel_tests/liblocktree_kernel_static.a
	(mkdir -p liblocktree/test; cd liblocktree/test; ar x ../../$< $*.o)

liblocktree/%.o: ../build/locktree/liblocktree_static.a
	(mkdir -p liblocktree; cd liblocktree; ar x ../$< $*.o)

insmod:
	git diff-index --exit-code --quiet HEAD || git commit -a -m "doing insmod test"
	git push imola
	sudo insmod ftfs.ko

rmmod:
	git diff-index --exit-code --quiet HEAD || git commit -a -m "doing rmmod"
	git push imola
	sudo rmmod ftfs

liblzma/%.o: ../build/xz/lib/liblzma.a
	(cd liblzma; ar x ../$< $*.o)

clean:
	make -C /lib/modules/$(KDIR)/build M=$(PWD) clean
