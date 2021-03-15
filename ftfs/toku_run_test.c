/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:
//
#include <linux/string.h>
#include <linux/kernel.h>
#include <linux/slab.h>
#include "sb_pthread.h"

extern int atoi(char *);

/* unbound_insert tests */
extern int test_ubi_root_chkpt(void);

/* southbound test starts */
extern int perf_test_sequential_reads(void);
extern int perf_test_sequential_writes(void);
extern int test_test_cachesize(void);
extern int test_slab(void);
extern int test_assert(void);
extern int test_directio(void);
extern int list_open_southbound_files(void);
extern int test_ftfs_realloc(void);
extern int test_remove(void);
extern int test_preadwrite(void);
extern int test_readwrite(void);
extern int test_pwrite(void);
extern int test_write(void);
extern int test_trunc(void);
extern int test_ftrunc(void);
extern int test_mkdir(void);
extern int test_mkrmdir(void);
extern int test_verify_unsorted_leaf(void);
extern int test_unlink(void);
extern int test_openclose(void);
extern int test_stat_ftfs(void);
extern int test_statfs(void);
extern int filesize_test(void);
extern int test_fsync(void);
extern int test_readlink(void);
extern int test_f_all(void);
extern int test_shortcut(void);
extern int test_bug1381(void);
extern int test_cursor_step_over_delete(void);
extern int test_x1764 (void);
extern int test_isolation(void);
extern int test_cursor_isolation(void);
extern int test_redirect(void);
//extern int test_create_datadir(void);
extern int test_openclose_dir(void);
extern int test_recursive_deletion(void);
extern int test_fgetc(void);
extern int test_cachetable_all_write(void);
//extern int test_list_mounts(void);
/* southbound test ends */
extern int test_mempool(void);
extern int test_marked_omt(void);
extern int test_omt_tmpl(void);
extern int test_frwlock_fair_writers(void);
extern int test_rwlock_unfair_writers(void);
extern int test_kibbutz (void);
extern int test_sort_tmpl(void);
extern int test_rwlock_cheapness(void);
extern int test_rwlock(void);
extern int test_threadpool_runf(void);
extern int test_threadpool(void);
extern int test_doubly_list (void);
extern int test_partitioned_counter_5833(void);
extern int test_partitioned_counter (void);
extern int test_gettime (void);
extern int test_gettimeofday (void);
extern int test_filesystem_sizes (void);
extern int test_stat (void);
extern int test_active_cpus(void);
extern int test_pqueue (void);
extern int test_pthread_rwlock_rwr(void);
extern int test_pthread_rwlock_rdlock(void);
extern int test_fair_rwlock(void);
extern int test_msnfilter(void);
extern int test_cpu_freq(void);
extern int test_cpu_freq_openlimit17(void); // just a placeholder
extern int test_hugepage(void);
extern int test_range_buffer(void);
extern int test_toku_malloc(void);
extern int test_snprintf(void);
extern int test_keyrange (void);
extern int test_progress (void);
extern int test_checkpoint1 (void);
extern int test_memory_status(void);
extern int test_fsync_directory(void);
extern int test_fsync_files(void);
extern int test_flock(void);
extern int test_fcopy(void);
extern int test_fcopy_dio(void);
extern int test_sfs_dio_read_write(void);
extern int test_manager_status(void);
extern int test_omt(void);
extern int test_manager_create_destroy(void);
extern int test_isolation_read_committed(void);
extern int test_recovery_datadir_is_file(void);
extern int test_recovery_cbegin(void);
extern int test_recovery_no_log(void);
extern int test_recovery_no_datadir(void);
extern int test_recovery_cbegin_cend_hello(void);
extern int test_verify_bad_pivots(void);
extern int test_recovery_cbegin_cend(void);
extern int test_recovery_cend_cbegin(void);
extern int test_recovery_empty(void);
extern int test_verify_unsorted_pivots(void);
extern int test_recovery_hello(void);
extern int test_verify_bad_msn(void);
extern int test_txn_child_manager(void);
extern int test_oldest_referenced_xid_flush(void);
extern int test_cachetable_checkpoint_prefetched_nodes(void);
extern int test_cachetable_checkpoint_pinned_nodes(void);
extern int test_recovery_lsn_error_during_forward_scan(void);
extern int test_bnc_insert_benchmark(void);
extern int test_is_empty(void);
extern int test_make_tree(void);
extern int test_blackhole(void);

extern int test_wfg(void);
extern int test_fifo(void);
extern int test_txnid_set(void);
extern int test_manager_params(void);
extern int test_verify_dup_in_leaf (void);
extern int test_locktree_single_txnid_optimization(void);
extern int test_concurrent_tree_lkr_insert_remove(void);
extern int test_concurrent_tree_create_destroy(void);
extern int test_concurrent_tree_lkr_remove_all(void);
extern int test_locktree_create_destroy(void);
extern int test_locktree_conflicts(void);
extern int test_locktree_simple_lock(void);
extern int test_locktree_misc(void);
extern int test_concurrent_tree_lkr_acquire_release(void);
extern int test_manager_reference_release_lt(void);
extern int test_locktree_infinity(void);
extern int test_concurrent_tree_lkr_insert_serial_large(void);
extern int test_lock_request_create_set(void);
extern int test_lock_request_get_set_keys(void);
extern int test_lock_request_wait_time_callback(void);
extern int test_lock_request_start_deadlock(void);
extern int test_manager_lm(void);
extern int test_locktree_overlapping(void);
extern int test_lockrequest_pending(void);
extern int test_locktree_escalation_stalls(void);
extern int test_key(void);
extern int test_log(void);
extern int test_queue(void);
extern int test_pwrite4g(void);
extern int test_logcursor_bw(void);
extern int test_logcursor_fw(void);
extern int test_logcursor_print(void);
extern int test_logcursor_timestamp(void);
extern int test_logcursor_bad_checksum(void);
extern int test_logcursor_empty_logdir(void);
extern int test_logcursor_empty_logfile(void);
extern int test_logcursor_empty_logfile_2(void);
extern int test_logcursor_empty_logfile_3(void);
extern int test_logfilemgr_print(void);
extern int test_log_3(void);
extern int test_comparator(void);
extern int test_log2(void);
extern int test_verify_dup_pivots(void);
extern int log_test4(void);
extern int test_log5(void);
extern int test_log6(void);
extern int test_checkpoint_during_split(void);
extern int test_log7(void);
extern int test_list_test(void);
extern int test_minicron(void);
extern int test_benchmark_test(void);
extern int test_maybe_trim(void);
extern int test_logfilemgr_create_destroy(void);
extern int test_cachetable_test(void);
extern int test_cachetable_4365(void);
extern int test_cachetable_4357(void);
extern int cachetable_5978(void);
extern int test_cachetable_pin_checkpoint(void);
extern int test_cachetable_pin_nonblocking_checkpoint_clean(void);
extern int cachetable_5978_2(void);
extern int test_cachetable_simple_pin_nonblocking_cheap(void);
extern int test_cachetable_simple_pin_nonblocking(void);
extern int test_cachetable_fd(void);
extern int test_cachetable_debug(void);
extern int test_cachetable_put(void);
extern int test_cachetable_put_checkpoint(void);
extern int test_cachetable_getandpin(void);
extern int test_cachetable_cleaner_thread_simple(void);
extern int test_cachetable_flush_during_cleaner(void);
extern int test_cachetable_simple_close(void);
extern int test_cachetable_simple_pin(void);
extern int test_cachetable_simple_pin_cheap(void);
extern int test_cachetable_simple_clone(void);
extern int test_cachetable_writer_thread_limit(void);
extern int test_cachetable_simple_clone2(void);
extern int test_cachetable_evictor_class(void);
extern int test_cachetable_checkpoint(void);
extern int test_cachetable_checkpointer_class(void);
extern int test_cachetable_prefetch_flowcontrol(void);
extern int test_cachetable_rwlock(void);
extern int test_cachetable_5097(void);
extern int test_cachetable_clone_checkpoint(void);
extern int test_toku_compress(void);
extern int test_ft_test0(void);
extern int test_logcursor(void);
extern int test_cachetable_create(void);
extern int test_cachetable(void);
extern int test_cachetable_checkpoint_pending(void);
extern int test_cachetable_count_pinned(void);
extern int test_cachetable_clone_unpin_remove(void);
extern int test_cachetable_unpin_remove_and_checkpoint(void);
extern int test_cachetable_clone_pin_nonblocking(void);
extern int test_cachetable_clone_partial_fetch(void);
extern int test_cachetable_flush(void);
extern int test_cachetable_threadempty(void);
extern int test_cachetable_simplereadpin(void);
extern int test_cachetable_prefetch_close(void);
extern int test_cachetable_clockeviction(void);
extern int test_cachetable_clock_eviction2(void);
extern int test_cachetable_clock_eviction3(void);
extern int test_cachetable_clock_eviction4(void);
extern int test_cachetable_eviction_close(void);
extern int test_cachetable_eviction_close2(void);
extern int test_cachetable_eviction_getandpin(void);
extern int test_cachetable_eviction_getandpin2(void);
extern int test_3856(void);
extern int test_3681(void);
extern int test_le_cursor_provdel(void);
extern int test_cachetable_cleanerthread_attrs_accumulate(void);
extern int test_cachetable_partial_fetch(void);
extern int test_cachetable_simplepin_depnodes(void);
extern int test_cachetablesimple_put_depnodes(void);
extern int test_cachetable_prefetch_maybegetandpin(void);
extern int test_cachetable_prefetch_getandpin(void);
extern int test_cachetable_simple_readpin_nonblocking(void);
extern int test_cachetable_simple_maybegetpin(void);
extern int test_cachetable_cleaner_checkpoint2(void);
extern int test_ft_test1(void);
extern int test_ft_test2(void);
extern int test_ft_test3(void);
extern int test_ft_test4(void);
extern int test_ft_test5(void);
extern int test_ft_test(void);
extern int test_block_allocator(void);
extern int test_cachetable_prefetch_checkpoint(void);
extern int test_cachetable_cleaner_thread_same_fullhash(void);
extern int test_ft_clock(void);
extern int test_cachetable_unpin_and_remove(void);
extern int test_cachetable_unpin(void);
extern int test_cachetable_cleaner_thread_nothing_needs_flushing(void);
extern int test_bjm(void);
extern int test_ft_serialize_sub_block(void);
extern int test_ft_serialize_benchmark(void);
extern int test_ft_test_cursor(void);
extern int test_ft_test_cursor_2(void);
extern int test_ft_test_header(void);
extern int test_ybt(void);
extern int test_subblock_test_compression(void);
extern int test_dump_ft(void);
extern int test_4244(void);
extern int test_ft_4115(void);
extern int test_3884(void);
extern int test_1308a(void);
extern int test_subblock_test_checksum(void);
extern int test_subblock_test_size(void);
extern int test_subblock_test_index(void);
extern int test_ft_bfe_query(void);
extern int test_dirty_flushes_on_cleaner(void);
extern int test_flushes_on_cleaner(void);
extern int test_checkpoint_during_merge(void);
extern int test_checkpoint_during_flush(void);
extern int test_checkpoint_during_rebalance(void);
extern int test_merges_on_cleaner(void);
extern int test_toku_malloc_plain_free(void);
extern int test_block_allocator_merge(void);
extern int test_del_inorder(void);
extern int test_recovery_no_logdir(void);
extern int test_hot_with_bounds(void);
extern int test_inc_split(void);
extern int test_oexcl(void);
extern int test_cachetable_cleaner_checkpoint(void);
extern int test_cachetable_cleaner_thread_everything_pinned(void);
extern int test_cachetable_kibbutz_and_flush_cachefile(void);
extern int test_cachetable_prefetch_close_leak(void);
extern int test_cachetable_clock_all_pinned(void);
extern int test_cachetable_fetch_inducing_evictor(void);
extern int test_cachetable_prefetch2(void);
extern int test_recovery_test5123(void);
extern int test_le_cursor_walk(void);
extern int test_leafentry_nested(void);
extern int test_leafentry_child_txn(void);
extern int recovery_fopen_missing_file(void);
extern int recovery_bad_last_entry(void);
extern int test_le_cursor_right(void);
extern int test_cachetable_clone_partial_fetch_pinned_node(void);
extern int test_quicklz(void);
extern int test_pick_child_to_flush(void);
extern int test_ft_overflow(void);
extern int test_cachetable_simple_unpin_remove_checkpoint(void);
extern int test_ft_serialize(void);
extern int test_orthopush_flush(void);
//extern int test_range_del(void);
//extern int test_range_del2(void);
extern int test_recovery_range_delete(void);

/* src tests */
extern int test_test_log7(void);
extern int test_inflate(void);
extern int test_test_cursor_DB_NEXT_no_dup(void);
extern int test_test_db_dbt_mem_behavior(void);
extern int test_test_db_close_no_open(void);
extern int test_test_simple_read_txn(void);
extern int test_test_4657(void);
extern int test_test_db_env_set_lg_dir(void);
extern int test_test_update_broadcast_stress(void);
extern int test_test_log6(void);
extern int test_test_db_env_set_tmp_dir(void);
extern int test_test_kv_limits(void);
extern int test_test_log4(void);
extern int test_test_log8(void);
extern int test_test_log9(void);
extern int test_test_log10(void);
extern int test_update(void);
extern int test_test_prepare(void);
extern int test_test_prepare2(void);
extern int test_test_prepare3(void);
extern int test_test_db_change_xxx(void);
extern int test_test_restrict(void);
extern int test_test_db_txn_locks_nonheaviside(void);
extern int test_test_log4_abort(void);
extern int test_test_log5(void);
extern int test_test_log6_abort(void);
extern int test_test_log5_abort(void);
extern int test_test_log6a_abort(void);
extern int test_test_update_txn_snapshot_works_correctly_with_deletes(void);
extern int test_test_reverse_compare_fun(void);
extern int test_test_read_txn_invalid_ops(void);
extern int test_test_update_txn_snapshot_works_concurrently(void);
extern int test_test_iterate_pending_lock_requests(void);
extern int test_test_cursor_nonleaf_expand(void);
extern int test_test_db_named_delete_last(void);
extern int test_test_mostly_seq(void);
extern int test_test_db_descriptor(void);
extern int test_test_db_already_exists(void);
extern int test_test_cursor_db_current(void);
extern int test_test_db_set_flags(void);
extern int test_test_db_current_clobbers_db(void);
extern int test_test_insert_cursor_delete_insert(void);
extern int test_test_db_version(void);
extern int test_test_db_change_pagesize(void);
extern int test_test_4368(void);
extern int test_test_4015(void);
extern int test_test_789(void);
extern int test_test_rollinclude(void);
extern int test_test4573_logtrim(void);
extern int test_test_3755(void);
extern int test_test_5469(void);
extern int test_test_5138(void);
extern int test_test_weakxaction(void);
extern int test_transactional_fileops(void);
extern int test_test5092(void);
extern int test_inflate2(void);
extern int test_test_db_txn_locks_read_uncommitted(void);
extern int test_test_txn_cursor_last(void);
extern int test_test_5015(void);
extern int test_test_large_update_broadcast_small_cachetable(void);
extern int test_test_cursor_with_read_txn(void);
extern int test_test_935(void);
extern int test_test938b(void);
extern int test_zombie_db(void);
extern int test_test_cursor_null(void);
extern int test_test_db_subdb_different_flags(void);
extern int test_test_db_remove_subdb(void);
extern int test_test_groupcommit_perf(void);
extern int test_test938(void);
extern int test_test_xa_prepare(void);
extern int test_test_hsoc(void);
extern int test_test_txn_begin_commit(void);
extern int test_test_get_max_row_size(void);
extern int test_rowsize(void);
extern int test_test_insert_memleak(void);
extern int test_test_cursor_2(void);
extern int test_test_db_get_put_flags(void);
extern int test_test_txn_commit8(void);
extern int test_test_db_subdb(void);
extern int test_filesize(void);
extern int test_test_log0(void);
extern int test_test_log1(void);
extern int test_test_log2(void);
extern int test_test_log3(void);
extern int test_test_log2_abort(void);
extern int test_test_log3_abort(void);
extern int test_seqinsert(void);
extern int test_update_broadcast_changes_values(void);
extern int test_update_broadcast_indexer(void);
extern int test_update_multiple_data_diagonal(void);
extern int test_update_multiple_key0(void);
extern int test_update_multiple_nochange(void);
extern int test_update_multiple_with_indexer(void);
extern int test_update_multiple_with_indexer_array(void);
extern int test_00(void);
extern int test_txn_nested1(void);
extern int test_txn_nested2(void);
extern int test_txn_nested3(void);
extern int test_txn_nested4(void);
extern int test_txn_nested5(void);
extern int test_last_verify_time(void);
extern int test_manyfiles(void);
extern int test_test_abort1(void);
extern int test_put_del_multiple_array_indexing(void);
extern int test_test_abort2(void);
extern int test_test_abort3(void);
extern int test_test_abort4(void);
extern int test_test_abort5(void);
extern int test_test_abort_delete_first(void);
extern int test_stat64(void);
extern int test_stat64_null_txn(void);
extern int test_stat64_root_changes(void);
extern int test_stat64_create_modify_times(void);
extern int test_del_multiple(void);
extern int test_del_multiple_srcdb(void);
extern int test_del_multiple_huge_primary_row(void);
extern int test_del_simple(void);
extern int test_db_put_simple_deadlock(void);
extern int test_db_put_update_deadlock(void);
extern int test_test_db_delete(void);
extern int test_test_db_delete(void);
extern int test_blocking_first(void);
extern int test_big_nested_abort_abort(void);
extern int test_big_nested_abort_commit(void);
extern int test_big_nested_commit_abort(void);
extern int test_big_nested_commit_commit(void);
extern int test_medium_nested_commit_commit(void);
extern int test_bigtxn27(void);
extern int test_db_put_simple_lockwait(void);
extern int test_keyrange_merge(void);
extern int test_test_db_env_open_close(void);
extern int test_db_put_simple_deadlock_threads(void);
extern int test_blocking_first_empty(void);
extern int test_blocking_last(void);
extern int test_blocking_next_prev(void);
extern int test_blocking_next_prev_deadlock(void);
extern int test_blocking_put(void);
extern int test_blocking_put_wakeup(void);
extern int test_blocking_put_timeout(void);
extern int test_blocking_prelock_range(void);
extern int test_blocking_set(void);
extern int test_blocking_set_range_0(void);
extern int test_blocking_set_range_n(void);
extern int test_blocking_set_range_reverse_0(void);
extern int test_blocking_table_lock(void);
extern int test_cachetable_race(void);
extern int test_checkpoint_fairness(void);
extern int test_checkpoint_stress(void);
extern int test_cursor_set_range_rmw(void);
extern int test_directory_lock(void);
extern int test_env_loader_memory(void);
extern int test_cursor_set_del_rmw(void);
extern int test_cursor_more_than_a_leaf_provdel(void);
extern int test_preload_db(void);
extern int test_simple(void);
extern int test_multiprocess(void);
extern int test_test_txn_nested_abort(void);
extern int test_bug1381(void);
extern int test_preload_db_nested(void);
extern int test_test_txn_nested_abort(void);
extern int test_test_txn_nested_abort2(void);
extern int test_test_txn_nested_abort3(void);
extern int test_test_txn_nested_abort4(void);
extern int test_shutdown_3344(void);
extern int test_recover_test1(void);
extern int test_recover_test2(void);
extern int test_recover_test3(void);
extern int test_test_txn_recover3(void);
extern int test_test_unused_memory_crash(void);
extern int test_test_update_abort_works(void);
extern int test_test_update_broadcast_abort_works(void);
extern int test_test_update_broadcast_calls_back(void);
extern int test_test_update_broadcast_can_delete_elements(void);
extern int test_test_update_broadcast_nested_updates(void);
extern int test_test_update_broadcast_previously_deleted(void);
extern int test_test_update_broadcast_update_fun_has_choices(void);
extern int test_test_update_broadcast_with_empty_table(void);
extern int test_prelock_read_read(void);
extern int test_get_key_after_bytes_unit(void);
extern int test_prelock_read_write(void);
extern int test_prelock_write_read(void);
extern int test_prelock_write_write(void);
extern int test_insert_dup_prelock(void);
extern int test_mvcc_create_table(void);
extern int test_mvcc_many_committed(void);
extern int test_mvcc_read_committed(void);
extern int test_recover_2483(void);
extern int test_openlimit17(void);
extern int test_openlimit17_metafiles(void);
extern int test_test_updates_single_key(void);
extern int test_openlimit17_locktree(void);
extern int test_dump_env(void);
extern int test_env_put_multiple(void);
extern int test_test_transactional_descriptor(void);
extern int test_test_stress0(void);
extern int test_test_stress1(void);
extern int test_test_stress2(void);
extern int test_test_stress3(void);
extern int test_test_stress4(void);
extern int test_test_stress6(void);
extern int test_test_stress7(void);
extern int test_test_stress_openclose(void);
extern int test_test_stress_with_verify(void);
extern int test_perf_child_txn(void);
extern int test_perf_checkpoint_var(void);
extern int test_perf_rangequery(void);
extern int test_perf_read_txn(void);
extern int test_perf_read_txn_single_thread(void);
extern int test_perf_read_write(void);
extern int test_perf_nop(void);
extern int test_perf_ptquery(void);
extern int test_perf_ptquery2(void);
extern int test_perf_txn_single_thread(void);
extern int test_perf_insert(void);
extern int test_perf_iibench(void);
extern int test_perf_malloc_free(void);
extern int test_perf_cursor_nop(void);
extern int test_stress_test(void);
extern int test_stress_gc(void);
extern int test_stress_gc2(void);
extern int test_test_update_calls_back(void);
extern int test_test1753(void);
extern int test_test1572(void);
extern int test_test1842(void);
extern int test_test3039(void);
extern int test_test3219(void);
extern int test_test3522(void);
extern int test_test3522b(void);
extern int test_test_3645(void);
extern int test_test_3529_insert_2(void);
extern int test_test_3529_table_lock(void);
extern int test_test_thread_insert(void);
extern int test_test_thread_flags(void);
extern int test_test_trans_desc_during_chkpt(void);
extern int test_test_trans_desc_during_chkpt2(void);
extern int test_test_trans_desc_during_chkpt3(void);
extern int test_test_trans_desc_during_chkpt4(void);
extern int test_test_txn_abort5(void);
extern int test_test_txn_abort5a(void);
extern int test_inflate(void);
extern int test_test_txn_abort6(void);
extern int test_test_txn_abort7(void);
extern int test_test_txn_abort8(void);
extern int test_test_txn_abort9(void);
extern int test_test_txn_close_before_commit(void);
extern int test_test_txn_close_before_prepare_commit(void);
extern int test_test_txn_close_open_commit(void);
extern int test_test_env_close_flags(void);
extern int test_test_env_open_flags(void);
extern int test_test_env_create_db_create(void);
extern int test_test_error(void);
extern int test_test_forkjoin(void);
extern int test_test_get_zeroed_dbt(void);
extern int test_test_groupcommit_count(void);
extern int test_test_locking_with_read_txn(void);
extern int test_test_lock_timeout_callback(void);
extern int test_test_locktree_close(void);
extern int test_test_xopen_eclose(void);
extern int test_test_update_with_empty_table(void);
extern int test_update_multiple_data_diagonal(void);
extern int test_test_update_can_delete_elements(void);
extern int test_test_update_changes_values(void);
extern int test_test_update_nested_updates(void);
extern int test_test_update_nonexistent_keys(void);
extern int test_test_update_previously_deleted(void);
extern int test_test_cursor_flags(void);
extern int test_test_blobs_leaf_split(void);
extern int test_test_bulk_fetch(void);
extern int test_test_cmp_descriptor(void);
extern int test_test_compression_methods(void);
extern int test_test_cursor_3(void);
extern int test_test_log1_abort(void);
extern int test_test_cursor_stickyness(void);
extern int test_test_cursor_delete2(void);
extern int test_test_logflush(void);
extern int test_replace_into_write_lock(void);
extern int test_test_query(void);
extern int test_test_nested(void);
extern int test_test_nodup_set(void);
extern int test_test_rand_insert(void);
extern int test_test_update_stress(void);
extern int test_test_zero_length_keys(void);
extern int test_test_db_env_open_nocreate(void);
extern int test_test_db_env_open_open_close(void);
extern int test_test_db_env_set_errpfx(void);
extern int test_test_db_open_notexist_reopen(void);
extern int test_test_db_remove(void);
extern int test_test_iterate_live_transactions(void);
extern int test_root_fifo_1(void);
extern int test_root_fifo_2(void);
extern int test_root_fifo_31(void);
extern int test_root_fifo_32(void);
extern int test_root_fifo_41(void);
extern int test_test_multiple_checkpoints_block_commit(void);
extern int test_test_nested_xopen_eclose(void);
extern int test_print_engine_status(void);
extern int test_queries_with_deletes(void);
extern int test_test_restrict(void);
extern int test_xid_lsn_independent(void);
extern int test_verify_misrouted_msgs(void);
extern int test_test_db_env_strdup_null(void);
extern int test_seqwrite_no_txn(void);
extern int test_rename_simple(void);
extern int test_circle_log_overflow(void);
/* large IO tests */
//extern int logger_test_tables(void);

int list_tests(void);

int test_fail(void) { return -ENOSYS; }

struct {
	char *name;
	int (*fn)(void);
	int timeout;
} tests[] = {
	//{ "fail", test_fail , 5},
	//{"logger-lists", logger_test_tables, 5},
	{ "sfs-dio", test_sfs_dio_read_write, 5},
	{"ubi-root-chkpt", test_ubi_root_chkpt, 5},
	{"orthopush-flush", test_orthopush_flush, 5},
	{ "inflate", test_inflate , 5},
	{ "test_db_env_strdup_null", test_test_db_env_strdup_null, 5},
	{ "test_simple_read_txn", test_test_simple_read_txn , 5},
	{ "inflate2", test_inflate2 , 5},
	{ "test_db_txn_locks_read_uncommitted", test_test_db_txn_locks_read_uncommitted , 5},
	{ "test_txn_cursor_last", test_test_txn_cursor_last , 5},
	{ "test_5015", test_test_5015 , 5},
	{ "test_large_update_broadcast_small_cachetable", test_test_large_update_broadcast_small_cachetable , 5},
	{ "test_cursor_with_read_txn", test_test_cursor_with_read_txn , 5},
	{ "test_db_close_no_open", test_test_db_close_no_open , 5},
	{ "test_4657", test_test_4657 , 5},
	{ "test_db_env_set_lg_dir", test_test_db_env_set_lg_dir , 5},
	{ "test_cursor_DB_NEXT_no_dup", test_test_cursor_DB_NEXT_no_dup , 5},
	{ "test_db_dbt_mem_behavior", test_test_db_dbt_mem_behavior , 5},
	{ "test_update_broadcast_stress", test_test_update_broadcast_stress , 180},
	{ "test_log4_abort", test_test_log4_abort , 5},
	{ "test_log6", test_test_log6 , 5},
	{ "test_db_env_set_tmp_dir", test_test_db_env_set_tmp_dir , 5},
	{ "test_kv_limits", test_test_kv_limits , 5},
	{ "test_log4", test_test_log4 , 5},
	{ "test_log8", test_test_log8 , 5},
	{ "test_log9", test_test_log9 , 5},
	{ "test_log10", test_test_log10 , 5},
	{ "test-update", test_update , 5},
	//{ "test-prepare", test_test_prepare , 5},
	//{ "test-prepare2", test_test_prepare2 , 5},
	//{ "test-prepare3", test_test_prepare3, 5},
	//{ "test_restrict", test_test_restrict , 5},
	{ "test_db_change_xxx", test_test_db_change_xxx , 5},
	{ "test_db_txn_locks_nonheaviside", test_test_db_txn_locks_nonheaviside , 5},
	{ "test_log5", test_test_log5 , 5},
	{ "test_log7", test_test_log7 , 5},
	{ "test_log6_abort", test_test_log6_abort , 5},
	{ "test_log5_abort", test_test_log5_abort , 5},
	{ "test_log6a_abort", test_test_log6a_abort , 5},
	{ "test_update_txn_snapshot_works_correctly_with_deletes", test_test_update_txn_snapshot_works_correctly_with_deletes , 5},
	{ "test_reverse_compare_fun", test_test_reverse_compare_fun , 5},
	{ "test_read_txn_invalid_ops", test_test_read_txn_invalid_ops , 5},
	{ "test_update_txn_snapshot_works_concurrently", test_test_update_txn_snapshot_works_concurrently , 5},
	{ "test_iterate_pending_lock_requests", test_test_iterate_pending_lock_requests , 5},
	{ "test_cursor_nonleaf_expand", test_test_cursor_nonleaf_expand , 5},
	{ "test_db_named_delete_last", test_test_db_named_delete_last , 5},
	{ "test_mostly_seq", test_test_mostly_seq , 5},
	{ "test_db_descriptor", test_test_db_descriptor , 5},
	{ "test_db_already_exists", test_test_db_already_exists , 5},
	{ "test_cursor_db_current", test_test_cursor_db_current , 5},
	{ "test_db_set_flags", test_test_db_set_flags , 5},
	{ "test_db_current_clobbers_db", test_test_db_current_clobbers_db , 5},
	{ "test_insert_cursor_delete_insert", test_test_insert_cursor_delete_insert , 5},
	{ "test_db_version", test_test_db_version , 5},
	{ "test_db_change_pagesize", test_test_db_change_pagesize , 5},
	{ "test_4368", test_test_4368 , 5},
	{ "test_4015", test_test_4015 , 5},
	{ "test_789", test_test_789 , 5},
	{ "test_rollinclude", test_test_rollinclude , 5},
	{ "test4573_logtrim", test_test4573_logtrim , 5},
	{ "test_3755", test_test_3755 , 5},
//	{ "test_5469", test_test_5469 , 5},
	{ "test_5138", test_test_5138 , 5},
	{ "test_weakxaction", test_test_weakxaction , 5},
	{ "transactional_fileops", test_transactional_fileops , 5},
	{ "test5092", test_test5092 , 5},
	{ "test_935", test_test_935 , 5},
	{ "test938b", test_test938b , 5},
	{ "zombie_db", test_zombie_db , 5},
	{ "test_cursor_null", test_test_cursor_null , 5},
	{ "test_db_subdb_different_flags", test_test_db_subdb_different_flags , 5},
	{ "test_db_remove_subdb", test_test_db_remove_subdb , 5},
	{ "test_groupcommit_perf", test_test_groupcommit_perf , 30},
	{ "test938", test_test938 , 5},
//	{ "test-xa-prepare", test_test_xa_prepare , 5},
	{ "test_hsoc", test_test_hsoc , 5},
	{ "test_txn_begin_commit", test_test_txn_begin_commit , 5},
	{ "test_get_max_row_size", test_test_get_max_row_size , 5},
	{ "rowsize", test_rowsize , 5},
	{ "test_insert_memleak", test_test_insert_memleak , 5},
	{ "test_cursor_2", test_test_cursor_2 , 5},
	{ "test_db_get_put_flags", test_test_db_get_put_flags , 5},
	{ "test_txn_commit8", test_test_txn_commit8 , 5},
	{ "test_db_subdb", test_test_db_subdb , 5},
	{ "filesize", test_filesize , 5},
	{ "test-log0",test_test_log0, 5},
	{ "test-log1",test_test_log1, 5},
	{ "test-log2",test_test_log2, 5},
	{ "test-log3",test_test_log3, 5},
	{ "test-log2-abort",test_test_log2_abort, 5},
	{ "test-log3-abort",test_test_log3_abort, 5},
	{ "seqinsert", test_seqinsert, 5},
	{ "test_update_broadcast_changes_values", test_update_broadcast_changes_values, 5},
	{ "test_update_broadcast_indexer", test_update_broadcast_indexer, 5},
	{ "update-multiple-data-diagonal", test_update_multiple_data_diagonal, 5},
	{ "update-multiple-key0", test_update_multiple_key0, 5},
	{ "update-multiple-nochange", test_update_multiple_nochange, 5},
	{ "update-multiple-with-indexer", test_update_multiple_with_indexer, 5},
	{ "update-multiple-with-indexer-array", test_update_multiple_with_indexer_array, 5},
	{ "test_txn_nested1", test_txn_nested1, 5},
	{ "test_txn_nested5", test_txn_nested5, 5},
	{ "test_txn_nested4", test_txn_nested4, 5},
	{ "test_txn_nested3", test_txn_nested3, 5},
	{ "test_txn_nested2", test_txn_nested2, 5},
	{ "last-verify-time", test_last_verify_time, 5},
	{ "manyfiles", test_manyfiles, 5},
	{ "test-db-delete",test_test_db_delete, 5},
	{ "db-put-simple-deadlock", test_db_put_simple_deadlock, 5},
	{ "db-put-update-deadlock", test_db_put_update_deadlock, 5},
	{ "db-put-simple-deadlock-threads", test_db_put_simple_deadlock_threads, 5},
	{ "del-simple", test_del_simple, 5},
	{ "del-multiple", test_del_multiple, 5},
	{ "del-multiple-srcdb", test_del_multiple_srcdb, 5},
	{ "del-multiple-huge-primary-row", test_del_multiple_huge_primary_row, 5},
	{ "stat64", test_stat64, 5},
	{ "stat64-null-txn", test_stat64_null_txn, 5},
	{ "stat64-root-changes", test_stat64_root_changes, 5},
	{ "stat64-create-modify-times", test_stat64_create_modify_times, 5},
	{ "test-abort1", test_test_abort1, 5},
	{ "put-del-multiple-array-indexing", test_put_del_multiple_array_indexing, 5},
	{ "test-abort2", test_test_abort2, 5},
	{ "test-abort3", test_test_abort3, 5},
	{ "test-abort4", test_test_abort4, 5},
	{ "test-abort5", test_test_abort5, 5},
	{ "test-abort-delete-first", test_test_abort_delete_first, 5},
	{ "list-files", list_open_southbound_files , 5},
	{ "list-tests", list_tests , 5},
	{ "mempool",    test_mempool , 5},
	{ "marked-omt", test_marked_omt , 5},
	{ "omt-tmpl",    test_omt_tmpl , 5},
	{ "frwlock-fair-writers", test_frwlock_fair_writers , 5},
	{ "rwlock-unfair-writers", test_rwlock_unfair_writers , 5},
	{ "threadpool-testrunf", test_threadpool_runf , 5},
	{ "rwlock-cheapness", test_rwlock_cheapness , 5},
	{ "rwlock", test_rwlock , 5},
	{ "threadpool", test_threadpool , 5},
	{ "kibbutz", test_kibbutz  , 5},
	{ "sort-tmpl", test_sort_tmpl , 5},
	{ "doubly-linked-list", test_doubly_list  , 5},
	{ "partitioned-counter-5833", test_partitioned_counter_5833 , 5},
	{ "partitioned-counter", test_partitioned_counter, 5},
	{ "gettime", test_gettime , 5},
	{ "gettimeofday", test_gettimeofday , 5},
	{ "get_filesystem_sizes", test_filesystem_sizes , 5},
	{ "stat", test_stat , 5},
	{ "active-cpus", test_active_cpus , 5},
	{ "fair-rwlock", test_fair_rwlock , 5},
	{ "msnfilter", test_msnfilter, 5},
	{ "cpu-frequency", test_cpu_freq , 5},
	{ "cpu-frequency-openlimit", test_cpu_freq_openlimit17, 5},
	{ "hugepage", test_hugepage, 5},
	{ "fsync-directory", test_fsync_directory, 5},
	// Don't need this test anymore.  flock is not required in BetrFS---
	//   superceded by VFS-level locking.
	//{ "flock", test_flock, 5}, /* FIXME 4 shuffle test */
	{ "fsync-files", test_fsync_files, 5},
	{ "fgetc", test_fgetc, 5},

	/*the following two tests are to be dropped*/
	//{ "pthread-rwlock-rwr", test_pthread_rwlock_rwr, 5},
	//{ "pthread-rwlock-rdlock", test_pthread_rwlock_rdlock, 5},
	{ "toku-malloc", test_toku_malloc, 5},
	{ "range-buffer", test_range_buffer, 5},
	{ "wfg", test_wfg, 5},
	{ "manager_cd", test_manager_create_destroy, 5},
	{ "manager-reference-release-lt", test_manager_reference_release_lt, 5},
	{ "txnid-set", test_txnid_set, 5},
	{ "manager-params", test_manager_params, 5},
	{ "verify-dup-in-leaf", test_verify_dup_in_leaf, 5},
	{ "lock-request-create-set", test_lock_request_create_set, 5},
	{ "lock-request-get-set-keys", test_lock_request_get_set_keys, 5},
	{ "lock-request-wait-time-callback", test_lock_request_wait_time_callback, 5},
	{ "lock-request-start-deadlock", test_lock_request_start_deadlock, 5},
	{ "locktree-create-destroy", test_locktree_create_destroy, 5},
	{ "locktree-misc", test_locktree_misc, 5},
	{ "locktree-infinity", test_locktree_infinity, 5},
	{ "locktree-simple-lock", test_locktree_simple_lock, 5},
	{ "locktree-conflicts", test_locktree_conflicts, 5},
	{ "test-update-empty-table", test_test_update_with_empty_table, 5},
	{ "locktree-single-snxid-optimization",
	  test_locktree_single_txnid_optimization, 5},
	{ "concurrent_tree_create_destroy",
	  test_concurrent_tree_create_destroy, 5},
	{"concurrent-tree-lkr-remove-all",
	 test_concurrent_tree_lkr_remove_all, 5},
	{ "concurrent-tree-lkr-insert-remove",
	  test_concurrent_tree_lkr_insert_remove, 5},
	{ "concurrent-tree-lkr-acquire-release",
	  test_concurrent_tree_lkr_acquire_release, 5},
	{ "concurrent-tree-lkr-insert-serial-large",
	  test_concurrent_tree_lkr_insert_serial_large, 5},
	{ "pwrite4g", test_pwrite4g, 5},
	{ "toku-snprintf", test_snprintf, 5},
	{ "manager_lm", test_manager_lm, 5},
	{ "manager-status", test_manager_status, 5},
	{ "omt-test", test_omt, 5},
	{ "slab", test_slab, 5},
	{ "assert", test_assert, 5},
	{ "realloc", test_ftfs_realloc, 5},
	{ "verify-unsorted-leaf", test_verify_unsorted_leaf, 5},
	{ "openclose", test_openclose, 5},
	{ "stat_ftfs", test_stat_ftfs, 5},
	{ "statfs", test_statfs, 5},
	{ "preadwrite", test_preadwrite, 5},
	{ "readwrite", test_readwrite, 5},
	{ "pwrite", test_pwrite, 5},
	{ "write", test_write, 5},
	{ "f-all", test_f_all, 5},
	{ "shortcut", test_shortcut, 5},
	{ "bug1381", test_bug1381, 5},
	{ "cursor-step-over-delete", test_cursor_step_over_delete, 5},
	{ "x1764-test", test_x1764, 5},
	{ "isolation-test", test_isolation, 5},
	{ "cursor-isolation", test_cursor_isolation, 5},
	{ "redirect", test_redirect, 5},
	//{ "create-datadir", test_create_datadir, 5},
	{ "isolation-read-committed", test_isolation_read_committed, 5},
	{ "fsync", test_fsync, 5},
	{ "fcopy", test_fcopy, 5},
	{ "fcopy_dio", test_fcopy_dio, 5},
	{ "test_locktree_overlapping", test_locktree_overlapping, 5},
	{ "test_lockrequest_pending", test_lockrequest_pending, 5},
	{ "test_locktree_escalation", test_locktree_escalation_stalls, 5},
	{ "mem-status", test_memory_status, 5},
	{"test_key",test_key, 5},
	{"test-cachesize", test_test_cachesize, 5},
	{"keyrange", test_keyrange, 5},
	{"progress-test", test_progress, 5},
	{"checkpoint1-test", test_checkpoint1, 5},
	{"test_log",test_log, 5},
	{"is_empty",test_is_empty, 5},
	{"make-tree",test_make_tree, 5},
	{"blackhole",test_blackhole, 5},
	{"test_queue",test_queue, 5},
	{"test_fifo",test_fifo, 5},
	{"logcursor-bw",test_logcursor_bw, 5},
	{"logcursor-fw", test_logcursor_fw, 5},
	{"logcursor-print", test_logcursor_print, 5},
	{"list-test", test_list_test, 5},
	{"logfilemgr_c_d", test_logfilemgr_create_destroy, 5},
	{"logcursor-timestamp", test_logcursor_timestamp, 5},
	{"logcursor-bad-checksum", test_logcursor_bad_checksum, 5},
	{"logcursor-empty-logdir", test_logcursor_empty_logdir, 5},
	{"logcursor-empty-logfile", test_logcursor_empty_logfile, 5},
	{"logcursor-empty-logfile-3", test_logcursor_empty_logfile_3, 5},
	{"logcursor-empty-logfile-2", test_logcursor_empty_logfile_2, 5},
	{"logfilemgr-print", test_logfilemgr_print, 5},
	{"test_log_3", test_log_3, 5},
	{"test_log_2", test_log2, 5},
	{"test_log_5", test_log5, 5},
	{"test_log_6", test_log6, 5},
	{"checkpoint-during-split", test_checkpoint_during_split, 5},
	{"test_log_7", test_log7, 5},
	{"test_log_4", log_test4, 5},
	{"list-test", test_list_test, 5},
	{"comparator-test", test_comparator, 5},

	{"minicron", test_minicron, 5},
	{"benchmark-test", test_benchmark_test, 5},
	{"verify-dup-pivots", test_verify_dup_pivots, 5},
	{"cachetable-test", test_cachetable_test, 5},
	{"cachetable-fd", test_cachetable_fd, 5},
	{"cachetable-4365", test_cachetable_4365, 5},
	{"cachetable-4357", test_cachetable_4357, 5},
	{"cachetable-5978", cachetable_5978, 5},
	{"cachetable-5978-2", cachetable_5978_2, 5},
	{"cachetable-put", test_cachetable_put, 5},
	{"cachetable-put-checkpoint", test_cachetable_put_checkpoint, 5},
	{"cachetable-checkpoint-prefetched-nodes", test_cachetable_checkpoint_prefetched_nodes, 5},
	{"cachetable-pin-nonblocking-checkpoint-clean", test_cachetable_pin_nonblocking_checkpoint_clean, 5},
	{"cachetable-checkpoint-pinned-nodes", test_cachetable_checkpoint_pinned_nodes, 5},
	{"cachetable-debug", test_cachetable_debug, 5},
	{"cachetable-simple-clone", test_cachetable_simple_clone, 5},
	{"cachetable-simple-pin-nonblocking", test_cachetable_simple_pin_nonblocking, 5},
	{"cachetable-simple-pin-nonblocking-cheap", test_cachetable_simple_pin_nonblocking_cheap, 5},
	{"cachetable-simple-pin", test_cachetable_simple_pin, 5},
	{"cachetable-simple-close", test_cachetable_simple_close, 5},
	{"cachetable-simple-pin-cheap", test_cachetable_simple_pin_cheap, 5},
	{"cachetable-simple-unpin-remove-checkpoint", test_cachetable_simple_unpin_remove_checkpoint, 5},
	{"cachetable-getandpin", test_cachetable_getandpin, 5},
	{"cachetable-cleaner-thread-simple", test_cachetable_cleaner_thread_simple, 5},
	{"cachetable-flush-during-cleaner", test_cachetable_flush_during_cleaner, 5},
	{"cachetable-clone-unpin-remove", test_cachetable_clone_unpin_remove, 5},
	{"cachetable-simple-clone2", test_cachetable_simple_clone2, 5},
	{"cachetable-writer-thread-limit", test_cachetable_writer_thread_limit, 5},
	{"cachetable-evictor-class", test_cachetable_evictor_class, 5},
	{"cachetable-checkpoint", test_cachetable_checkpoint, 5},
	{"cachetable-checkpointer-class", test_cachetable_checkpointer_class, 5},
	{"cachetable-prefetch-flowcontrol-test", test_cachetable_prefetch_flowcontrol, 5},
	{"cachetable-rwlock",test_cachetable_rwlock, 5},
	{"cachetable-all-write", test_cachetable_all_write, 5},
	{"cachetable-clone-checkpoint", test_cachetable_clone_checkpoint, 5},
	{"compress-test", test_toku_compress, 5},
	{"logcursor_test" , test_logcursor, 5},
	{"ft-test0", test_ft_test0, 5},
	{"bnc-insert-benchmark", test_bnc_insert_benchmark, 5},
	{"maybe-trim", test_maybe_trim, 5},
	{"test-update-single-key", test_test_updates_single_key, 5},
	{"pqueue-test", test_pqueue, 5},
	{"cachetable-5097", test_cachetable_5097, 5},
	{"cachetable", test_cachetable, 5},
	{"cachetable-checkpoint-pending", test_cachetable_checkpoint_pending, 5},
	{"cachetable-count-pinned", test_cachetable_count_pinned, 5},
	{"cachetable-unpin-rm-and-checkpoint", test_cachetable_unpin_remove_and_checkpoint, 5},
	{"cachetable-clone-pin-nonblocking", test_cachetable_clone_pin_nonblocking, 5},
	{"cachetable-create", test_cachetable_create, 5},
	{"cachetable-clone-partial-fetch", test_cachetable_clone_partial_fetch, 5},
	{"cachetable-thread-empty", test_cachetable_threadempty, 5},
	{"cachetable-simplereadpin", test_cachetable_simplereadpin, 5},
	{"cachetable-prefetch-close", test_cachetable_prefetch_close, 5},
	{"cachetable-clock-eviction", test_cachetable_clockeviction, 5},
	{"cachetable-clock-eviction2", test_cachetable_clock_eviction2, 5},
	{"cachetable-clock-eviction3", test_cachetable_clock_eviction3, 5},
	{"cachetable-clock-eviction4", test_cachetable_clock_eviction4, 5},
	{"cachetable-eviction-close", test_cachetable_eviction_close, 5},
	{"cachetable-eviction-close2", test_cachetable_eviction_close2, 5},
	{"cachetable-eviction-getandpin", test_cachetable_eviction_getandpin, 5},
	{"cachetable-eviction-getandpin2", test_cachetable_eviction_getandpin2, 5},
	{"cachetable-cleanerthread_attrs_accumulate", test_cachetable_cleanerthread_attrs_accumulate, 5},
	{"cachetable-simple-pin", test_cachetable_simplepin_depnodes, 5},
	{"cachetable-simple-put", test_cachetablesimple_put_depnodes, 5},
	{"cachetable-prefetch-maybegetandpin", test_cachetable_prefetch_maybegetandpin, 5},
	{"cachetable-prefetch-getandpin", test_cachetable_prefetch_getandpin, 5},
	{"cachetable-simple-maybegetandpin",test_cachetable_simple_maybegetpin, 5},
	{"cachetable-simple-readpin", test_cachetable_simple_readpin_nonblocking, 5},
	{"cachetable-cleaner-checkpoint2", test_cachetable_cleaner_checkpoint2, 5},

	{"cachetable-partial-fetch", test_cachetable_partial_fetch, 5},
	{"cachetable-flush", test_cachetable_flush, 5},
	{"cachetable-partial-fetch", test_cachetable_partial_fetch, 5},
	{"ft-test1", test_ft_test1, 5},
	{"ft-test2", test_ft_test2, 5},
	{"test-cursor-flags", test_test_cursor_flags, 5},
	{"ft-test3", test_ft_test3, 5},
	{"ft-test4", test_ft_test4, 5},
	{"ft-test5", test_ft_test5, 5},
	{"ft-test", test_ft_test, 5},
	{"block-allocator", test_block_allocator , 5},
	{"cachetable-prefetch-checkpoint", test_cachetable_prefetch_checkpoint, 5},
	{"cachetable-cleaner-thread-same-fullhash", test_cachetable_cleaner_thread_same_fullhash, 5},
	{"ft-clock",test_ft_clock, 5},
	{"cachetable-unpin-and-remove", test_cachetable_unpin_and_remove, 5},
	{"cachetable-unpin", test_cachetable_unpin, 5},
	{"cachetable-cleaner-thread-nothing-needs-flushing", test_cachetable_cleaner_thread_nothing_needs_flushing, 5},
	{"bjm", test_bjm, 5},
	{"ft-serialize-sub-block",test_ft_serialize_sub_block, 5},
	{"ft-serialize-benchmark", test_ft_serialize_benchmark, 5},
	{"ft-serialize", test_ft_serialize, 5},
	{"ft-test-cursor", test_ft_test_cursor, 5},
	{"ft-test-cursor-2", test_ft_test_cursor_2, 5},
	{"ft-test-header",test_ft_test_header, 5},
	{"ybt-test", test_ybt, 5},
	{"test-3856",test_3856, 5},
	{"test-3681",test_3681, 5},
	{"le-cursor-provdel",test_le_cursor_provdel, 5},
	{"subblock-test-compression", test_subblock_test_compression, 5},
	{"dump-ft", test_dump_ft, 5},
	{"test-4244",test_4244, 5},
	{"test-4115",test_ft_4115, 5},
	{"test-3884", test_3884, 5},
	{"test-1308a",test_1308a, 5},
	{"subblock-test-checksum", test_subblock_test_checksum, 5},
	{"subblock-test-size", test_subblock_test_size, 5},
	{"subblock-test-index", test_subblock_test_index, 5},
	{"ft-bfe-query", test_ft_bfe_query, 5},
	{"dirty-flushes-on-cleaner", test_dirty_flushes_on_cleaner, 5},
	{"flushes-on-cleaner", test_flushes_on_cleaner, 5},
	{"checkpoint-during-merge", test_checkpoint_during_merge, 5},
	{"checkpoint-during-flush", test_checkpoint_during_flush, 5},
	{"checkpoint-during-rebalance", test_checkpoint_during_rebalance, 5},
	{"merges-on-cleaner", test_merges_on_cleaner, 5},
	{"toku-malloc-plain-free", test_toku_malloc_plain_free, 5},
	{"recovery-datadir-is-file", test_recovery_datadir_is_file, 5},
	{"txn-child-manager", test_txn_child_manager, 5},
	{"oldest-referenced-xid-flush", test_oldest_referenced_xid_flush, 5},
	{"block-allocator-merge", test_block_allocator_merge, 5},
	{"del-inorder",test_del_inorder, 5},
	{"recovery-no-logdir",test_recovery_no_logdir, 5},
	{"recovery-no-datadir",test_recovery_no_datadir, 5},
 	{"recovery-lsn-error-during-forward-scan",test_recovery_lsn_error_during_forward_scan, 5},
	{"hot-with-bounds", test_hot_with_bounds, 5},
	{"inc-split", test_inc_split, 5},
  	{"pick-child-to-flush", test_pick_child_to_flush, 5},
	{"oexcl",test_oexcl, 5},
	{"cachetable-cleaner-checkpoint", test_cachetable_cleaner_checkpoint, 5},
	{"cachetable-kibbutz-and-flush-cachefile", test_cachetable_kibbutz_and_flush_cachefile, 5},
	{"cachetable-cleaner-thread-everything-pinned", test_cachetable_cleaner_thread_everything_pinned, 5},
	{"cachetable-prefetch-close-leak-test", test_cachetable_prefetch_close_leak, 5},
	{"recovery-cbegin", test_recovery_cbegin, 5},
	{"recovery-cbegin-cend-hello", test_recovery_cbegin_cend_hello, 5},
	{"verify-bad-pivots", test_verify_bad_pivots, 5},
	{"recovery-cbegin-cend", test_recovery_cbegin_cend, 5},
	{"recovery-no-log", test_recovery_no_log, 5},
	{"recovery-cend-cbegin", test_recovery_cend_cbegin, 5},
	{"recovery-empty", test_recovery_empty, 5},
	{"verify-unsorted-pivots", test_verify_unsorted_pivots, 5},
	{"recovery-hello", test_recovery_hello, 5},
	{"verify-bad-msn", test_verify_bad_msn, 5},
	{"cachetable-clock-all-pinned", test_cachetable_clock_all_pinned, 5},
	{"cachetable-pin-checkpoint", test_cachetable_pin_checkpoint, 5},
	{"cachetable-fetch-inducing-evictor", test_cachetable_fetch_inducing_evictor, 5},
	{"cachetable-prefetch2", test_cachetable_prefetch2, 5},
	{"recovery-test5123", test_recovery_test5123, 5},
	{"le-cursor-walk", test_le_cursor_walk, 5},
	{"le-nested", test_leafentry_nested, 5},
	{"le-child-txn", test_leafentry_child_txn, 5},
	{"test_recovery_fopen_missing_file", recovery_fopen_missing_file, 5},
	{"test_recovery_bad_last_entry", recovery_bad_last_entry, 5},
	{"le-cursor-right", test_le_cursor_right, 5},
	{"cachetable-clone-partial-fetch-pinned-node", test_cachetable_clone_partial_fetch_pinned_node, 5},
	{"test-quicklz", test_quicklz, 5},
	{"ft-overflow",test_ft_overflow, 5},
	{"big-nested-aa",test_big_nested_abort_abort, 5},
	{"big-nested-ac",test_big_nested_abort_commit, 5},
	{"big-nested-cc",test_big_nested_commit_commit, 5},
	{"big-nested-ca",test_big_nested_commit_abort, 5},
	{"keyrange-merge", test_keyrange_merge, 5},
	{"blocking-first", test_blocking_first, 5},
	{"blocking-first-empty", test_blocking_first_empty, 5},
	{"blocking-last", test_blocking_last, 5},
	{"db-put-simple-lockwait", test_db_put_simple_lockwait, 5},
	{"bigtxn27", test_bigtxn27, 5},
	{"test_db_env_open_close", test_test_db_env_open_close , 5},
	{"blocking-next-prev",test_blocking_next_prev, 5},
	{"blocking-next-prev-deadlock", test_blocking_next_prev_deadlock , 5},
	{"blocking-prelock-range", test_blocking_prelock_range, 5},
	{"blocking-put", test_blocking_put, 5},
	{"blocking-put-wakeup",test_blocking_put_wakeup, 5},
	{"blocking-put-timeout",test_blocking_put_timeout, 5},
	{"blocking-set", test_blocking_set, 5},
	{"blocking-set-range-0", test_blocking_set_range_0, 5},
	{"blocking-set-range-n",test_blocking_set_range_n, 5},
	{"blocking-set-range-reverse-0", test_blocking_set_range_reverse_0, 5},
	{"blocking-table-lock", test_blocking_table_lock, 5},
	{"cachetable-race",test_cachetable_race, 5},
	{"test-get-key-after-bytes", test_get_key_after_bytes_unit, 5},
	{"checkpoint-fairness",test_checkpoint_fairness, 5},
	{"directory-lock",test_directory_lock, 5},
	{"cursor-set-range-rmw", test_cursor_set_range_rmw, 5},
	{"cursor-set-del-rmw", test_cursor_set_del_rmw, 5},
	{"checkpoint-stress", test_checkpoint_stress, 45},
	{"env-loader-memory", test_env_loader_memory, 5},
	{"test-txn-nested-abort", test_test_txn_nested_abort, 5},
	{"test_bug1381", test_bug1381, 5},
	{"preload-db", test_preload_db, 5},
	{"simple", test_simple, 5},
	{"multiprocess", test_multiprocess, 5},
	{"test-prelock-read-read", test_prelock_read_read, 5},
	{"test-prelock-read-write", test_prelock_read_write, 5},
	{"test-prelock-write-write", test_prelock_write_write, 5},
	{"test-prelock-write-read", test_prelock_write_read, 5},
	{"insert-dup-prelock", test_insert_dup_prelock, 5},
	{"mvcc-create-table", test_mvcc_create_table, 5},
	{"mvcc-many-committed", test_mvcc_many_committed, 5},
	{"mvcc-read-committed", test_mvcc_read_committed, 5},
	{"preload-db-nested", test_preload_db_nested, 5},
	{"test_txn_nested_abort", test_test_txn_nested_abort, 5},
	{"test_txn_nested_abort2", test_test_txn_nested_abort2 , 5},
	{"test_txn_nested_abort3", test_test_txn_nested_abort3 , 5},
	{"test_txn_nested_abort4", test_test_txn_nested_abort4 , 5},
	{"recover-test1", test_recover_test1, 5},
	{"shutdown-3344", test_shutdown_3344, 5},
	{"recover-test2", test_recover_test2, 5},
	{"recover-test3", test_recover_test3, 5},
	{"recover-2483", test_recover_2483, 5},
	{"test_txn_recover3", test_test_txn_recover3 , 5},
	{"openlimit17", test_openlimit17, 5},
	{"openlimit17-metafiles", test_openlimit17_metafiles, 5},
	{"openlimit17-locktree", test_openlimit17_locktree, 5},
	{"test_unused_memory_crash", test_test_unused_memory_crash , 5},
	{"test_update_abort_works", test_test_update_abort_works , 5},
	{"test_update_broadcast_abort_works", test_test_update_broadcast_abort_works , 5},
	{"test_update_broadcast_calls_back", test_test_update_broadcast_calls_back , 5},
	{"test_update_broadcast_can_delete_elements", test_test_update_broadcast_can_delete_elements, 5},
	{"test_update_broadcast_nested_updates", test_test_update_broadcast_nested_updates, 5},
	{"test_update_broadcast_previously_deleted", test_test_update_broadcast_previously_deleted, 5},
	{"test_update_broadcast_update_fun_has_choices", test_test_update_broadcast_update_fun_has_choices, 5},
	{"cursor-more-than-a-leaf-provdel", test_cursor_more_than_a_leaf_provdel , 5},
	{"dump-env", test_dump_env, 5},
	{"env-put-multiple", test_env_put_multiple, 5},
	{"medium-nested-commit-commit", test_medium_nested_commit_commit, 5},
	{"test-xopen-eclose", test_test_xopen_eclose, 5},
	{"test-stress0",test_test_stress0, 45},
	{"test-stress1",test_test_stress1, 45},
	{"test-stress2",test_test_stress2, 45},
	{"test-stress3",test_test_stress3, 45},
	{"test-stress4",test_test_stress4, 180},
	{"test-stress6",test_test_stress6, 45},
	{"test-stress7",test_test_stress7, 90},
	{"test-stress-openclose",test_test_stress_openclose, 45},
	{"test-stress-with-verify",test_test_stress_with_verify, 60},
	{"perf-child-txn",test_perf_child_txn, 30},
	{"perf-rangequery", test_perf_rangequery, 30},
	{"perf-read-txn", test_perf_read_txn, 30},
	{"perf-read-txn-single-thread", test_perf_read_txn_single_thread, 60},
	{"perf-txn-single-thread", test_perf_txn_single_thread, 30},
	{"perf-nop", test_perf_nop, 30},
	{"perf-checkpoint-var", test_perf_checkpoint_var, 30},
	{"perf-malloc-free", test_perf_malloc_free, 30},
	{"perf-cursor-nop", test_perf_cursor_nop, 30},
	{"perf-insert", test_perf_insert, 30},
	{"perf-iibench", test_perf_iibench, 60},
	{"perf-ptquery", test_perf_ptquery, 30},
	{"perf-ptquery2", test_perf_ptquery2, 30},
	{"perf-read-write", test_perf_read_write, 30},
	{"stress-test", test_stress_test, 45},
	{"stress-gc", test_stress_gc, 45},
	{"stress-gc2", test_stress_gc2, 90},
	{"transactional-descriptor",test_test_transactional_descriptor, 5},
	{"thread-flags", test_test_thread_flags, 5},
	{"thread-insert", test_test_thread_insert, 5},
	{"trans-desc-during-chkpt", test_test_trans_desc_during_chkpt, 5},
	{"trans-desc-during-chkpt2", test_test_trans_desc_during_chkpt2, 5},
	{"trans-desc-during-chkpt3", test_test_trans_desc_during_chkpt3, 5},
	{"trans-desc-during-chkpt4", test_test_trans_desc_during_chkpt4, 5},
	{"txn-abort5", test_test_txn_abort5, 5},
	{"txn-abort5a", test_test_txn_abort5a, 5},
	{"txn-abort6", test_test_txn_abort6, 5},
	{"txn-abort7", test_test_txn_abort7, 5},
	{"txn-abort8", test_test_txn_abort8, 5},
	{"txn-abort9", test_test_txn_abort9, 5},
	{"txn-close-before-commit", test_test_txn_close_before_commit, 5},
	{"txn-close-before-prepare-commit", test_test_txn_close_before_prepare_commit, 5},
	{"txn-close-open-commit", test_test_txn_close_open_commit, 5},
	{"test_update_calls_back", test_test_update_calls_back, 5},
	{"test1753", test_test1753, 5},
	{"test1572", test_test1572, 5},
	{"test1842", test_test1842, 5},
	{"test3039", test_test3039, 5},
	{"test3219", test_test3219, 5},
	{"test3522", test_test3522, 5},
	{"test3522b", test_test3522b, 5},
	{"test_3529_insert_2", test_test_3529_insert_2, 5},
	{"test_3529_table_lock", test_test_3529_table_lock, 5},
	{"env-close-flags", test_test_env_close_flags, 5},
	{"env-open-flags", test_test_env_open_flags, 5},
	{"test-update-data-diagonal", test_update_multiple_data_diagonal, 5},
	{"env-create-db-create", test_test_env_create_db_create, 5},
	{"test-error", test_test_error, 5},
	{"test-forkjoin", test_test_forkjoin, 5},
	{"test-get-zeroed-dbt", test_test_get_zeroed_dbt, 5},
	{"test-groupcommit-count", test_test_groupcommit_count, 5},
	{"test-locking-with-read-txn",test_test_locking_with_read_txn, 5},
	{"test-lock-timeout-callback", test_test_lock_timeout_callback, 5},
	{"test-locktree-close",test_test_locktree_close, 5},
	{"test_update_can_delete_elements", test_test_update_can_delete_elements , 5},
	{"test_update_changes_values", test_test_update_changes_values , 5},
	{"test_update_nested_updates", test_test_update_nested_updates , 5},
	{"test_update_nonexistent_keys", test_test_update_nonexistent_keys , 5},
	{"test_update_previously_deleted", test_test_update_previously_deleted , 5},
	{"test-blobs-leaf-split", test_test_blobs_leaf_split, 5},
	{"test-bulk-fetch",test_test_bulk_fetch, 5},
	{"test-cmp-descriptor",test_test_cmp_descriptor, 5},
	{"test-compression-methods", test_test_compression_methods, 5},
	{"test-cursor-3", test_test_cursor_3, 5},
	{"test-cursor-stickyness", test_test_cursor_stickyness, 5},
	{"test-cursor-delete2", test_test_cursor_delete2, 5},
	{"test-logflush", test_test_logflush, 5},
	{"test_update_stress", test_test_update_stress, 45},
	{"test_zero_length_keys",test_test_zero_length_keys, 5},
	{"test_db_env_open_nocreate", test_test_db_env_open_nocreate, 5},
	{"test_db_env_open_open_close", test_test_db_env_open_open_close, 5},
	{"test_db_env_set_errpfx",test_test_db_env_set_errpfx, 5},
	{"test_db_open_notexist_reopen",test_test_db_open_notexist_reopen, 5},
	{"test_db_remove",test_test_db_remove, 5},
	{"test_iterate_live_transactions", test_test_iterate_live_transactions, 5},
	{"test_xopen_eclose", test_test_xopen_eclose, 5},
	{"test-log1-abort", test_test_log1_abort, 5},
	{"replace-into-write-lock", test_replace_into_write_lock, 5},
	{"test-query",test_test_query, 5},
	{"test-nested",test_test_nested, 5},
	{"test-nodup-set",test_test_nodup_set, 5},
	{"test_3645",test_test_3645, 5},
	{"root-fifo-32", test_root_fifo_32, 5},
	{"root-fifo-31", test_root_fifo_31, 5},
	{"root-fifo-41", test_root_fifo_41, 5},
	{"root-fifo-1", test_root_fifo_1, 5},
	{"root-fifo-2", test_root_fifo_2, 5},
	{"test-multiple-checkpoints-block-commit", test_test_multiple_checkpoints_block_commit, 5},
	{"test-rand-insert",test_test_rand_insert, 5},
	{"test-nested-xopen-eclose",test_test_nested_xopen_eclose, 5},
	{"print_engine_status", test_print_engine_status, 5},
	{"xid-lsn-independent", test_xid_lsn_independent, 5},
	{"verify-misrouted-msgs",test_verify_misrouted_msgs, 5},
	{"queries_with_deletes", test_queries_with_deletes, 5},
	{"seqwrite_no_txn", test_seqwrite_no_txn, 5},
	//{"range-del",test_range_del, 5},
	//{"range-del2",test_range_del2, 5},
	{"rename-simple",test_rename_simple, 5},
	{"recovery-range-delete",test_recovery_range_delete, 5},
	{"circle-log-overflow",test_circle_log_overflow, 5}
};


int list_tests(void)
{
	int i;
	for (i = 0; i < sizeof(tests)/sizeof(tests[0]); i++)
		printk("%s\n", tests[i].name);
	return 0;
}

int run_test(char * _test_name){
	int i;
	int times = 1;
	int ran_a_test = 0;
	int result = 0;
	char * p = NULL;
	char * test_name;
	int nsucc = 0;
	int test_all = 0;
	char *_test_name_copy = kmalloc(1 + strlen (_test_name), GFP_USER);

	for (i = 0; i < sizeof(tests)/sizeof(tests[0]); i++) {

		/*
		 * adding test repetition:
		 * "wfg:50" means test wft is
		 * going to run 50 times
		 */
		times = 1;
       		strcpy (_test_name_copy, _test_name);
		if((p = strrchr(_test_name_copy, ':'))) {
			times = atoi(p+1);
			*p = '\0';
		}
		test_name = _test_name_copy;
		if (strcmp(test_name, tests[i].name) == 0 ||
		    strcmp(test_name, "all") == 0) {

			ran_a_test = 1;

			if(strcmp(test_name, "all") == 0) {
				test_all = 1;
			}

			printk(KERN_ALERT "\n<test_descriptor>"
			       "<name>%s</name>"
			       "<timeout>%d</timeout>"
			       "</test_descriptor>\n",
			       test_name, tests[i].timeout);
			while(times) {
				if (tests[i].fn() != 0) {
					result = -1;

					if(test_all == 1) {
						printk(KERN_DEBUG "Testing \"all\" failed at \"%s\" \n", tests[i].name);
					}
					break;
				} else if(test_all == 1){
					printk(KERN_DEBUG "Testing \"all\" passed \"%s\":[%d/%lu] \n", tests[i].name, i+1, sizeof(tests)/sizeof(tests[0]));
				}
				nsucc++;
				times--;
			}
		}
	}

	if (ran_a_test) {
		printk(KERN_ALERT "\n<test_completion>"
		       "<name>%s</name>"
		       "<times_run>%d</times_run>"
		       "<status>%d</status>"
		       "</test_completion>\n",
		       test_name, nsucc, result);
	} else {
		/* test did not exist, return -EINVAL */
		printk(KERN_ALERT "<test_completion>"
		       "<name>invalid</name>"
		       "<times_run>0</times_run>"
		       "<status>-22</status>"
		       "</test_completion>\n");
		result = -22;
	}
	kfree(_test_name_copy);

	return result;
}

struct test_struct {
	char *test_name;
	int retval;
};

static void * tester_func(void *arg)
{
	struct test_struct *test = (struct test_struct *)arg;
	test->retval = run_test(test->test_name);
	return NULL;
}


int thread_run_test(char * test_name)
{
	struct test_struct test;
	pthread_t tester;

	test.test_name = test_name;
	printk(KERN_ALERT "Attempting test %s\n", test_name);
	test.retval = 0;
	pthread_create(&tester, NULL, tester_func, &test);
	pthread_join(tester, NULL);

	printk(KERN_ALERT "\tTest completed (%s): %d\n", test_name,
	       test.retval);

	return test.retval;
}
