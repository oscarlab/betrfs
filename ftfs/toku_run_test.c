/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:
//
#include <linux/string.h>
#include <linux/kernel.h>
#include <linux/slab.h>
#include "ftfs_pthread.h"

extern int atoi(char *);

/* performance tests */
extern int bench_insert(void);

/* southbound test starts */

extern int test_test_cachesize(void);
extern int test_slab(void);
extern int test_assert(void);
extern int test_directio(void);
extern int test_posix_memalign(void);
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
extern int test_getdents64(void);
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
extern int test_test_archive0(void);
extern int test_test_archive1(void);
extern int test_test_archive2(void);
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
extern int test_env_startup(void);
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
extern int test_test3529(void);
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
extern int test_test_logmax(void);
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
extern int test_test_set_func_malloc(void);
extern int test_test_nested_xopen_eclose(void);
extern int test_print_engine_status(void);
extern int test_queries_with_deletes(void);
extern int test_test_restrict(void);
extern int test_xid_lsn_independent(void);
extern int test_verify_misrouted_msgs(void);
extern int test_test_db_env_strdup_null(void);
extern int test_seqwrite_no_txn(void);

int list_tests(void);

int test_fail(void) { return -ENOSYS; }

struct {
	char *name;
	int (*fn)(void);
} tests[] = {
	//{ "fail", test_fail },
	{"orthopush-flush", test_orthopush_flush},
	{ "inflate", test_inflate },
	{ "test_db_env_strdup_null", test_test_db_env_strdup_null},
	{ "test_simple_read_txn", test_test_simple_read_txn },
	{ "inflate2", test_inflate2 },
	{ "test_db_txn_locks_read_uncommitted", test_test_db_txn_locks_read_uncommitted },
	{ "test_txn_cursor_last", test_test_txn_cursor_last },
	{ "test_5015", test_test_5015 },
	{ "test_large_update_broadcast_small_cachetable", test_test_large_update_broadcast_small_cachetable },
	{ "test_cursor_with_read_txn", test_test_cursor_with_read_txn },
	{ "test_db_close_no_open", test_test_db_close_no_open },
	{ "test_4657", test_test_4657 },
	{ "test_db_env_set_lg_dir", test_test_db_env_set_lg_dir },
	{ "test_cursor_DB_NEXT_no_dup", test_test_cursor_DB_NEXT_no_dup },
	{ "test_db_dbt_mem_behavior", test_test_db_dbt_mem_behavior },
	{ "test_update_broadcast_stress", test_test_update_broadcast_stress },
	{ "test_log4_abort", test_test_log4_abort },
	{ "test_log6", test_test_log6 },
	{ "test_db_env_set_tmp_dir", test_test_db_env_set_tmp_dir },
	{ "test_kv_limits", test_test_kv_limits },
	{ "test_log4", test_test_log4 },
	{ "test_log8", test_test_log8 },
	{ "test_log9", test_test_log9 },
	{ "test_log10", test_test_log10 },
	{ "test-update", test_update },
	//{ "test-prepare", test_test_prepare },
	//{ "test-prepare2", test_test_prepare2 },
	//{ "test-prepare3", test_test_prepare3},
	//{ "test_restrict", test_test_restrict },
	{ "test_db_change_xxx", test_test_db_change_xxx },
	{ "test_db_txn_locks_nonheaviside", test_test_db_txn_locks_nonheaviside },
	{ "test_log5", test_test_log5 },
	{ "test_log7", test_test_log7 },
	{ "test_log6_abort", test_test_log6_abort },
	{ "test_log5_abort", test_test_log5_abort },
	{ "test_log6a_abort", test_test_log6a_abort },
	{ "test_update_txn_snapshot_works_correctly_with_deletes", test_test_update_txn_snapshot_works_correctly_with_deletes },
	{ "test_reverse_compare_fun", test_test_reverse_compare_fun },
	{ "test_read_txn_invalid_ops", test_test_read_txn_invalid_ops },
	{ "test_update_txn_snapshot_works_concurrently", test_test_update_txn_snapshot_works_concurrently },
	{ "test_iterate_pending_lock_requests", test_test_iterate_pending_lock_requests },
	{ "test_cursor_nonleaf_expand", test_test_cursor_nonleaf_expand },
	{ "test_db_named_delete_last", test_test_db_named_delete_last },
	{ "test_mostly_seq", test_test_mostly_seq },
	{ "test_db_descriptor", test_test_db_descriptor },
	{ "test_db_already_exists", test_test_db_already_exists },
	{ "test_cursor_db_current", test_test_cursor_db_current },
	{ "test_db_set_flags", test_test_db_set_flags },
	{ "test_db_current_clobbers_db", test_test_db_current_clobbers_db },
	{ "test_insert_cursor_delete_insert", test_test_insert_cursor_delete_insert },
	{ "test_db_version", test_test_db_version },
	{ "test_db_change_pagesize", test_test_db_change_pagesize },
	{ "test_4368", test_test_4368 },
	{ "test_4015", test_test_4015 },
	{ "test_789", test_test_789 },
	{ "test_rollinclude", test_test_rollinclude },
	{ "test4573_logtrim", test_test4573_logtrim },
	{ "test_3755", test_test_3755 },
//	{ "test_5469", test_test_5469 },
	{ "test_5138", test_test_5138 },
	{ "test_weakxaction", test_test_weakxaction },
	{ "transactional_fileops", test_transactional_fileops },
	{ "test5092", test_test5092 },
	{ "test_935", test_test_935 },
	{ "test938b", test_test938b },
	{ "zombie_db", test_zombie_db },
	{ "test_cursor_null", test_test_cursor_null },
	{ "test_db_subdb_different_flags", test_test_db_subdb_different_flags },
	{ "test_db_remove_subdb", test_test_db_remove_subdb },
	{ "test_groupcommit_perf", test_test_groupcommit_perf },
	{ "test938", test_test938 },
//	{ "test-xa-prepare", test_test_xa_prepare },
	{ "test_hsoc", test_test_hsoc },
	{ "test_txn_begin_commit", test_test_txn_begin_commit },
	{ "test_get_max_row_size", test_test_get_max_row_size },
	{ "rowsize", test_rowsize },
	{ "test_insert_memleak", test_test_insert_memleak },
	{ "test_cursor_2", test_test_cursor_2 },
	{ "test_db_get_put_flags", test_test_db_get_put_flags },
	{ "test_txn_commit8", test_test_txn_commit8 },
	{ "test_db_subdb", test_test_db_subdb },
	{ "filesize", test_filesize },
	{ "test-log0",test_test_log0},
	{ "test-log1",test_test_log1},
	{ "test-log2",test_test_log2},
	{ "test-log3",test_test_log3},
	{ "test-log2-abort",test_test_log2_abort},
	{ "test-log3-abort",test_test_log3_abort},
	{ "seqinsert", test_seqinsert},
	{ "test_update_broadcast_changes_values", test_update_broadcast_changes_values},
	{ "test_update_broadcast_indexer", test_update_broadcast_indexer},
	{ "update-multiple-data-diagonal", test_update_multiple_data_diagonal},
	{ "update-multiple-key0", test_update_multiple_key0},
	{ "update-multiple-nochange", test_update_multiple_nochange},
	{ "update-multiple-with-indexer", test_update_multiple_with_indexer},
	{ "update-multiple-with-indexer-array", test_update_multiple_with_indexer_array},
	{ "test_txn_nested1", test_txn_nested1},
	{ "test_txn_nested5", test_txn_nested5},
	{ "test_txn_nested4", test_txn_nested4},
	{ "test_txn_nested3", test_txn_nested3},
	{ "test_txn_nested2", test_txn_nested2},
	{ "last-verify-time", test_last_verify_time},
	{ "manyfiles", test_manyfiles},
	{ "test-db-delete",test_test_db_delete},
	{ "db-put-simple-deadlock", test_db_put_simple_deadlock},
	{ "db-put-update-deadlock", test_db_put_update_deadlock},
	{ "db-put-simple-deadlock-threads", test_db_put_simple_deadlock_threads},
	{ "del-simple", test_del_simple},
	{ "del-multiple", test_del_multiple},
	{ "del-multiple-srcdb", test_del_multiple_srcdb},
	{ "del-multiple-huge-primary-row", test_del_multiple_huge_primary_row},
	{ "stat64", test_stat64},
	{ "stat64-null-txn", test_stat64_null_txn},
	{ "stat64-root-changes", test_stat64_root_changes},
	{ "stat64-create-modify-times", test_stat64_create_modify_times},
	{ "test-abort1", test_test_abort1},
	{ "put-del-multiple-array-indexing", test_put_del_multiple_array_indexing},
	{ "test-abort2", test_test_abort2},
	{ "test-abort3", test_test_abort3},
	{ "test-abort4", test_test_abort4},
	{ "test-abort5", test_test_abort5},
	{ "test-abort-delete-first", test_test_abort_delete_first},
	{ "list-tests", list_tests },
	{ "mempool",    test_mempool },
	{ "marked-omt", test_marked_omt },
	{ "omt-tmpl",    test_omt_tmpl },
	{ "frwlock-fair-writers", test_frwlock_fair_writers },
	{ "rwlock-unfair-writers", test_rwlock_unfair_writers },
	{ "threadpool-testrunf", test_threadpool_runf },
	{ "rwlock-cheapness", test_rwlock_cheapness },
	{ "rwlock", test_rwlock },
	{ "threadpool", test_threadpool },
	{ "kibbutz", test_kibbutz  },
	{ "sort-tmpl", test_sort_tmpl },
	{ "doubly-linked-list", test_doubly_list  },
	{ "partitioned-counter-5833", test_partitioned_counter_5833 },
	{ "partitioned-counter", test_partitioned_counter},
	{ "gettime", test_gettime },
	{ "gettimeofday", test_gettimeofday },
	{ "get_filesystem_sizes", test_filesystem_sizes },
	{ "stat", test_stat },
	{ "active-cpus", test_active_cpus },
	{ "fair-rwlock", test_fair_rwlock },
	{ "msnfilter", test_msnfilter},
	{ "cpu-frequency", test_cpu_freq },
	{ "cpu-frequency-openlimit", test_cpu_freq_openlimit17},
	{ "hugepage", test_hugepage},
	{ "fsync-directory", test_fsync_directory},
	{ "flock", test_flock}, /* FIXME 4 shuffle test */
	{ "fsync-files", test_fsync_files},
	{ "fgetc", test_fgetc},

	/*the following two tests are to be dropped*/
	//{ "pthread-rwlock-rwr", test_pthread_rwlock_rwr},
	//{ "pthread-rwlock-rdlock", test_pthread_rwlock_rdlock},
	{ "toku-malloc", test_toku_malloc},
	{ "range-buffer", test_range_buffer},
	{ "wfg", test_wfg},
	{ "manager_cd", test_manager_create_destroy},
	{ "manager-reference-release-lt", test_manager_reference_release_lt},
	{ "txnid-set", test_txnid_set},
	{ "manager-params", test_manager_params},
	{ "verify-dup-in-leaf", test_verify_dup_in_leaf},
	{ "lock-request-create-set", test_lock_request_create_set},
	{ "lock-request-get-set-keys", test_lock_request_get_set_keys},
	{ "lock-request-wait-time-callback", test_lock_request_wait_time_callback},
	{ "lock-request-start-deadlock", test_lock_request_start_deadlock},
	{ "locktree-create-destroy", test_locktree_create_destroy},
	{ "locktree-misc", test_locktree_misc},
	{ "locktree-infinity", test_locktree_infinity},
	{ "locktree-simple-lock", test_locktree_simple_lock},
	{ "locktree-conflicts", test_locktree_conflicts},
	{ "test-update-empty-table", test_test_update_with_empty_table},
	{ "locktree-single-snxid-optimization",
	  test_locktree_single_txnid_optimization},
	{ "concurrent_tree_create_destroy",
	  test_concurrent_tree_create_destroy},
	{"concurrent-tree-lkr-remove-all",
	 test_concurrent_tree_lkr_remove_all},
	{ "concurrent-tree-lkr-insert-remove",
	  test_concurrent_tree_lkr_insert_remove},
	{ "concurrent-tree-lkr-acquire-release",
	  test_concurrent_tree_lkr_acquire_release},
	{ "concurrent-tree-lkr-insert-serial-large",
	  test_concurrent_tree_lkr_insert_serial_large},
	{ "pwrite4g", test_pwrite4g},
	{ "toku-snprintf", test_snprintf},
	{ "manager_lm", test_manager_lm},
	{ "manager-status", test_manager_status},
	{ "omt-test", test_omt},
	{ "mkdir", test_mkdir},
	{ "remove", test_remove},
	{ "slab", test_slab},
	{ "assert", test_assert},
	{ "posix-memalign", test_posix_memalign},
	{ "dio", test_directio},
	{ "realloc", test_ftfs_realloc},
	{ "mkrmdir", test_mkrmdir},
	{ "verify-unsorted-leaf", test_verify_unsorted_leaf},
	{ "unlink", test_unlink},
	{ "getdents64", test_getdents64},
	{ "openclose", test_openclose},
	{ "stat_ftfs", test_stat_ftfs},
	{ "statfs", test_statfs},
	{ "preadwrite", test_preadwrite},
	{ "readwrite", test_readwrite},
	{ "pwrite", test_pwrite},
	{ "write", test_write},
	{ "readlink", test_readlink},
	{ "f-all", test_f_all},
	{ "f-all", test_shortcut},
	{ "bug1381", test_bug1381},
	{ "cursor-step-over-delete", test_cursor_step_over_delete},
	{ "x1764-test", test_x1764},
	{ "isolation-test", test_isolation},
	{ "cursor-isolation", test_cursor_isolation},
	{ "redirect", test_redirect},
	//{ "create-datadir", test_create_datadir},
	{ "isolation-read-committed", test_isolation_read_committed},
	{ "openclose-dir", test_openclose_dir},
	{ "recursive-deletion", test_recursive_deletion},
	{ "trunc", test_trunc},
	{ "ftrunc", test_ftrunc},
	{ "fsync", test_fsync},
	{ "fcopy", test_fcopy},
	{ "test_locktree_overlapping", test_locktree_overlapping},
	{ "test_lockrequest_pending", test_lockrequest_pending},
	{ "test_locktree_escalation", test_locktree_escalation_stalls},
	{ "mem-status", test_memory_status},
	{"test_key",test_key},
	{"test-cachesize", test_test_cachesize},
	{"keyrange", test_keyrange},
	{"progress-test", test_progress},
	{"checkpoint1-test", test_checkpoint1},
	{"test_log",test_log},
	{"is_empty",test_is_empty},
	{"make-tree",test_make_tree},
	{"blackhole",test_blackhole},
	{"test_queue",test_queue},
	{"test_fifo",test_fifo},
	{"logcursor-bw",test_logcursor_bw},
	{"logcursor-fw", test_logcursor_fw},
	{"logcursor-print", test_logcursor_print},
	{"list-test", test_list_test},
	{"logfilemgr_c_d", test_logfilemgr_create_destroy},
	{"logcursor-timestamp", test_logcursor_timestamp},
	{"logcursor-bad-checksum", test_logcursor_bad_checksum},
	{"logcursor-empty-logdir", test_logcursor_empty_logdir},
	{"logcursor-empty-logfile", test_logcursor_empty_logfile},
	{"logcursor-empty-logfile-3", test_logcursor_empty_logfile_3},
	{"logcursor-empty-logfile-2", test_logcursor_empty_logfile_2},
	{"logfilemgr-print", test_logfilemgr_print},
	{"test_log_3", test_log_3},
	{"test_log_2", test_log2},
	{"test_log_5", test_log5},
	{"test_log_6", test_log6},
	{"checkpoint-during-split", test_checkpoint_during_split},
	{"test_log_7", test_log7},
	{"test_log_4", log_test4},
	{"list-test", test_list_test},
	{"comparator-test", test_comparator},
	
	{"minicron", test_minicron},
	{"benchmark-test", test_benchmark_test},
	{"verify-dup-pivots", test_verify_dup_pivots},
	{"cachetable-test", test_cachetable_test},
	{"cachetable-fd", test_cachetable_fd},
	{"cachetable-4365", test_cachetable_4365},
	{"cachetable-4357", test_cachetable_4357},
	{"cachetable-5978", cachetable_5978},
	{"cachetable-5978-2", cachetable_5978_2},
	{"cachetable-put", test_cachetable_put},
	{"cachetable-put-checkpoint", test_cachetable_put_checkpoint},
	{"cachetable-checkpoint-prefetched-nodes", test_cachetable_checkpoint_prefetched_nodes},
	{"cachetable-pin-nonblocking-checkpoint-clean", test_cachetable_pin_nonblocking_checkpoint_clean},
	{"cachetable-checkpoint-pinned-nodes", test_cachetable_checkpoint_pinned_nodes},
	{"cachetable-debug", test_cachetable_debug},
	{"cachetable-simple-clone", test_cachetable_simple_clone},
	{"cachetable-simple-pin-nonblocking", test_cachetable_simple_pin_nonblocking},
	{"cachetable-simple-pin-nonblocking-cheap", test_cachetable_simple_pin_nonblocking_cheap},
	{"cachetable-simple-pin", test_cachetable_simple_pin},
	{"cachetable-simple-close", test_cachetable_simple_close},
	{"cachetable-simple-pin-cheap", test_cachetable_simple_pin_cheap},
	{"cachetable-simple-unpin-remove-checkpoint", test_cachetable_simple_unpin_remove_checkpoint},
	{"cachetable-getandpin", test_cachetable_getandpin},
	{"cachetable-cleaner-thread-simple", test_cachetable_cleaner_thread_simple},
	{"cachetable-flush-during-cleaner", test_cachetable_flush_during_cleaner},
	{"cachetable-clone-unpin-remove", test_cachetable_clone_unpin_remove},
	{"cachetable-simple-clone2", test_cachetable_simple_clone2},
	{"cachetable-writer-thread-limit", test_cachetable_writer_thread_limit},
	{"cachetable-evictor-class", test_cachetable_evictor_class},
	{"cachetable-checkpoint", test_cachetable_checkpoint},
	{"cachetable-checkpointer-class", test_cachetable_checkpointer_class},
	{"cachetable-prefetch-flowcontrol-test", test_cachetable_prefetch_flowcontrol},
	{"cachetable-rwlock",test_cachetable_rwlock},
	{"cachetable-all-write", test_cachetable_all_write},
	{"cachetable-clone-checkpoint", test_cachetable_clone_checkpoint},
	{"compress-test", test_toku_compress},
	{"logcursor_test" , test_logcursor},
	{"ft-test0", test_ft_test0},
	{"bnc-insert-benchmark", test_bnc_insert_benchmark},
	{"maybe-trim", test_maybe_trim},
	{"test-update-single-key", test_test_updates_single_key},
	{"pqueue-test", test_pqueue},
	{"cachetable-5097", test_cachetable_5097},
	{"cachetable", test_cachetable},
	{"cachetable-checkpoint-pending", test_cachetable_checkpoint_pending},
	{"cachetable-count-pinned", test_cachetable_count_pinned},
	{"cachetable-unpin-rm-and-checkpoint", test_cachetable_unpin_remove_and_checkpoint},
	{"cachetable-clone-pin-nonblocking", test_cachetable_clone_pin_nonblocking},
	{"cachetable-create", test_cachetable_create},
	{"cachetable-clone-partial-fetch", test_cachetable_clone_partial_fetch},
	{"cachetable-thread-empty", test_cachetable_threadempty},
	{"cachetable-simplereadpin", test_cachetable_simplereadpin},
	{"cachetable-prefetch-close", test_cachetable_prefetch_close},
	{"cachetable-clock-eviction", test_cachetable_clockeviction},
	{"cachetable-clock-eviction2", test_cachetable_clock_eviction2},
	{"cachetable-clock-eviction3", test_cachetable_clock_eviction3},
	{"cachetable-clock-eviction4", test_cachetable_clock_eviction4},
	{"cachetable-eviction-close", test_cachetable_eviction_close},
	{"cachetable-eviction-close2", test_cachetable_eviction_close2},
	{"cachetable-eviction-getandpin", test_cachetable_eviction_getandpin},
	{"cachetable-eviction-getandpin2", test_cachetable_eviction_getandpin2},
	{"cachetable-cleanerthread_attrs_accumulate", test_cachetable_cleanerthread_attrs_accumulate},
	{"cachetable-simple-pin", test_cachetable_simplepin_depnodes},
	{"cachetable-simple-put", test_cachetablesimple_put_depnodes},
	{"cachetable-prefetch-maybegetandpin", test_cachetable_prefetch_maybegetandpin},
	{"cachetable-prefetch-getandpin", test_cachetable_prefetch_getandpin},
	{"cachetable-simple-maybegetandpin",test_cachetable_simple_maybegetpin},
	{"cachetable-simple-readpin", test_cachetable_simple_readpin_nonblocking},
	{"cachetable-cleaner-checkpoint2", test_cachetable_cleaner_checkpoint2},

	{"cachetable-partial-fetch", test_cachetable_partial_fetch},
	{"cachetable-flush", test_cachetable_flush},
	{"cachetable-partial-fetch", test_cachetable_partial_fetch},
	{"ft-test1", test_ft_test1},
	{"ft-test2", test_ft_test2},
	{"test-cursor-flags", test_test_cursor_flags},
	{"ft-test3", test_ft_test3},
	{"ft-test4", test_ft_test4},
	{"ft-test5", test_ft_test5},
	{"ft-test", test_ft_test},
	{"block-allocator", test_block_allocator },
	{"cachetable-prefetch-checkpoint", test_cachetable_prefetch_checkpoint},
	{"cachetable-cleaner-thread-same-fullhash", test_cachetable_cleaner_thread_same_fullhash},
	{"ft-clock",test_ft_clock},
	{"cachetable-unpin-and-remove", test_cachetable_unpin_and_remove},
	{"cachetable-unpin", test_cachetable_unpin},
	{"cachetable-cleaner-thread-nothing-needs-flushing", test_cachetable_cleaner_thread_nothing_needs_flushing},
	{"bjm", test_bjm},
	{"ft-serialize-sub-block",test_ft_serialize_sub_block},
	{"ft-serialize-benchmark", test_ft_serialize_benchmark},
	{"ft-serialize", test_ft_serialize},
	{"ft-test-cursor", test_ft_test_cursor},
	{"ft-test-cursor-2", test_ft_test_cursor_2},
	{"ft-test-header",test_ft_test_header},
	{"ybt-test", test_ybt},
	{"test-3856",test_3856},
	{"test-3681",test_3681},
	{"le-cursor-provdel",test_le_cursor_provdel},
	{"subblock-test-compression", test_subblock_test_compression},
	{"dump-ft", test_dump_ft},
	{"test-4244",test_4244},
	{"test-4115",test_ft_4115},
	{"test-3884", test_3884},
	{"test-1308a",test_1308a},
	{"subblock-test-checksum", test_subblock_test_checksum},
	{"subblock-test-size", test_subblock_test_size},
	{"subblock-test-index", test_subblock_test_index},
	{"ft-bfe-query", test_ft_bfe_query},
	{"dirty-flushes-on-cleaner", test_dirty_flushes_on_cleaner},
	{"flushes-on-cleaner", test_flushes_on_cleaner},
	{"checkpoint-during-merge", test_checkpoint_during_merge},
	{"checkpoint-during-flush", test_checkpoint_during_flush},
	{"checkpoint-during-rebalance", test_checkpoint_during_rebalance},
	{"merges-on-cleaner", test_merges_on_cleaner},
	{"toku-malloc-plain-free", test_toku_malloc_plain_free},
	{"recovery-datadir-is-file", test_recovery_datadir_is_file},
	{"txn-child-manager", test_txn_child_manager},
	{"oldest-referenced-xid-flush", test_oldest_referenced_xid_flush},
	{"block-allocator-merge", test_block_allocator_merge},
	{"del-inorder",test_del_inorder},
	{"recovery-no-logdir",test_recovery_no_logdir},
	{"recovery-no-datadir",test_recovery_no_datadir},
 	{"recovery-lsn-error-during-forward-scan",test_recovery_lsn_error_during_forward_scan},
	{"hot-with-bounds", test_hot_with_bounds},
	{"inc-split", test_inc_split},
  	{"pick-child-to-flush", test_pick_child_to_flush},
	{"oexcl",test_oexcl},
	{"cachetable-cleaner-checkpoint", test_cachetable_cleaner_checkpoint},
	{"cachetable-kibbutz-and-flush-cachefile", test_cachetable_kibbutz_and_flush_cachefile},
	{"cachetable-cleaner-thread-everything-pinned", test_cachetable_cleaner_thread_everything_pinned},
	{"cachetable-prefetch-close-leak-test", test_cachetable_prefetch_close_leak},
	{"recovery-cbegin", test_recovery_cbegin},
	{"recovery-cbegin-cend-hello", test_recovery_cbegin_cend_hello},
	{"verify-bad-pivots", test_verify_bad_pivots},
	{"recovery-cbegin-cend", test_recovery_cbegin_cend},
	{"recovery-no-log", test_recovery_no_log},
	{"recovery-cend-cbegin", test_recovery_cend_cbegin},
	{"recovery-empty", test_recovery_empty},
	{"verify-unsorted-pivots", test_verify_unsorted_pivots},
	{"recovery-hello", test_recovery_hello},
	{"verify-bad-msn", test_verify_bad_msn},
	{"cachetable-clock-all-pinned", test_cachetable_clock_all_pinned},
	{"cachetable-pin-checkpoint", test_cachetable_pin_checkpoint},
	{"cachetable-fetch-inducing-evictor", test_cachetable_fetch_inducing_evictor},
	{"cachetable-prefetch2", test_cachetable_prefetch2},
	{"recovery-test5123", test_recovery_test5123},
	{"le-cursor-walk", test_le_cursor_walk},
	{"le-nested", test_leafentry_nested},
	{"le-child-txn", test_leafentry_child_txn},
	{"test_recovery_fopen_missing_file", recovery_fopen_missing_file},
	{"test_recovery_bad_last_entry", recovery_bad_last_entry},
	{"le-cursor-right", test_le_cursor_right},
	{"cachetable-clone-partial-fetch-pinned-node", test_cachetable_clone_partial_fetch_pinned_node}, 
	{"test-quicklz", test_quicklz},
	{"ft-overflow",test_ft_overflow},
	{"big-nested-aa",test_big_nested_abort_abort},
	{"big-nested-ac",test_big_nested_abort_commit},
	{"big-nested-cc",test_big_nested_commit_commit},
	{"big-nested-ca",test_big_nested_commit_abort},
	{"keyrange-merge", test_keyrange_merge},
	{"blocking-first", test_blocking_first},
	{"blocking-first-empty", test_blocking_first_empty},
	{"blocking-last", test_blocking_last},
	{"db-put-simple-lockwait", test_db_put_simple_lockwait},
	{"bigtxn27", test_bigtxn27},
	{"test_db_env_open_close", test_test_db_env_open_close },
	{"blocking-next-prev",test_blocking_next_prev},
	{"blocking-next-prev-deadlock", test_blocking_next_prev_deadlock }, 
	{"blocking-prelock-range", test_blocking_prelock_range},
	{"blocking-put", test_blocking_put},
	{"blocking-put-wakeup",test_blocking_put_wakeup},
	{"blocking-put-timeout",test_blocking_put_timeout},
	{"blocking-set", test_blocking_set},
	{"blocking-set-range-0", test_blocking_set_range_0},
	{"blocking-set-range-n",test_blocking_set_range_n},
	{"blocking-set-range-reverse-0", test_blocking_set_range_reverse_0},
	{"blocking-table-lock", test_blocking_table_lock},
	{"cachetable-race",test_cachetable_race},
	{"test-get-key-after-bytes", test_get_key_after_bytes_unit},
	{"checkpoint-fairness",test_checkpoint_fairness},
	{"directory-lock",test_directory_lock},
	{"cursor-set-range-rmw", test_cursor_set_range_rmw},
	{"cursor-set-del-rmw", test_cursor_set_del_rmw},
	{"checkpoint-stress", test_checkpoint_stress},
	{"env-loader-memory", test_env_loader_memory},
	{"test-txn-nested-abort", test_test_txn_nested_abort},
	{"test_bug1381", test_bug1381},
	{"preload-db", test_preload_db},
	{"simple", test_simple},
	{"multiprocess", test_multiprocess},
	{"test_archive0", test_test_archive0},
	{"test_archive1", test_test_archive1},
	{"test_archive2", test_test_archive2},
	{"test-prelock-read-read", test_prelock_read_read},
	{"test-prelock-read-write", test_prelock_read_write},
	{"test-prelock-write-write", test_prelock_write_write},
	{"test-prelock-write-read", test_prelock_write_read},
	{"insert-dup-prelock", test_insert_dup_prelock},
	{"env-startup", test_env_startup},
	{"mvcc-create-table", test_mvcc_create_table},
	{"mvcc-many-committed", test_mvcc_many_committed},
	{"mvcc-read-committed", test_mvcc_read_committed},
	{"preload-db-nested", test_preload_db_nested},
	{"test_txn_nested_abort", test_test_txn_nested_abort},
	{"test_txn_nested_abort2", test_test_txn_nested_abort2 },
	{"test_txn_nested_abort3", test_test_txn_nested_abort3 },
	{"test_txn_nested_abort4", test_test_txn_nested_abort4 },
	{"recover-test1", test_recover_test1},
	{"shutdown-3344", test_shutdown_3344},
	{"recover-test2", test_recover_test2},
	{"recover-test3", test_recover_test3},
	{"recover-2483", test_recover_2483},
	{"test_txn_recover3", test_test_txn_recover3 },
	{"openlimit17", test_openlimit17},
	{"openlimit17-metafiles", test_openlimit17_metafiles},
	{"openlimit17-locktree", test_openlimit17_locktree},
	{"test_unused_memory_crash", test_test_unused_memory_crash },
	{"test_update_abort_works", test_test_update_abort_works },
	{"test_update_broadcast_abort_works", test_test_update_broadcast_abort_works },
	{"test_update_broadcast_calls_back", test_test_update_broadcast_calls_back },
	{"test_update_broadcast_can_delete_elements", test_test_update_broadcast_can_delete_elements},
	{"test_update_broadcast_nested_updates", test_test_update_broadcast_nested_updates},
	{"test_update_broadcast_previously_deleted", test_test_update_broadcast_previously_deleted},
	{"test_update_broadcast_update_fun_has_choices", test_test_update_broadcast_update_fun_has_choices},
	{"cursor-more-than-a-leaf-provdel", test_cursor_more_than_a_leaf_provdel },
	{"dump-env", test_dump_env},
	{"env-put-multiple", test_env_put_multiple},
	{"medium-nested-commit-commit", test_medium_nested_commit_commit},
	{"test-xopen-eclose", test_test_xopen_eclose},
	{"test-stress0",test_test_stress0},
	{"test-stress1",test_test_stress1},
	{"test-stress2",test_test_stress2},
	{"test-stress3",test_test_stress3},
	{"test-stress4",test_test_stress4},
	{"test-stress6",test_test_stress6},
	{"test-stress7",test_test_stress7},
	{"test-stress-openclose",test_test_stress_openclose},
	{"test-stress-with-verify",test_test_stress_with_verify},
	{"perf-child-txn",test_perf_child_txn},
	{"perf-rangequery", test_perf_rangequery},
	{"perf-read-txn", test_perf_read_txn},
	{"perf-read-txn-single-thread", test_perf_read_txn_single_thread},
	{"perf-txn-single-thread", test_perf_txn_single_thread},
	{"perf-nop", test_perf_nop},
	{"perf-checkpoint-var", test_perf_checkpoint_var},
	{"perf-malloc-free", test_perf_malloc_free},
	{"perf-cursor-nop", test_perf_cursor_nop},
	{"bench-insert", bench_insert},
	{"perf-insert", test_perf_insert},
	{"perf-iibench", test_perf_iibench},
	{"perf-ptquery", test_perf_ptquery},
	{"perf-ptquery2", test_perf_ptquery2},
	{"perf-read-write", test_perf_read_write},
	{"stress-test", test_stress_test},
	{"stress-gc", test_stress_gc},
	{"stress-gc2", test_stress_gc2},
	{"transactional-descriptor",test_test_transactional_descriptor},
	{"thread-flags", test_test_thread_flags},
	{"thread-insert", test_test_thread_insert},
	{"trans-desc-during-chkpt", test_test_trans_desc_during_chkpt},
	{"trans-desc-during-chkpt2", test_test_trans_desc_during_chkpt2},
	{"trans-desc-during-chkpt3", test_test_trans_desc_during_chkpt3},
	{"trans-desc-during-chkpt4", test_test_trans_desc_during_chkpt4},
	{"txn-abort5", test_test_txn_abort5},
	{"txn-abort5a", test_test_txn_abort5a},
	{"txn-abort6", test_test_txn_abort6},
	{"txn-abort7", test_test_txn_abort7},
	{"txn-abort8", test_test_txn_abort8},
	{"txn-abort9", test_test_txn_abort9},
	{"txn-close-before-commit", test_test_txn_close_before_commit},
	{"txn-close-before-prepare-commit", test_test_txn_close_before_prepare_commit},
	{"txn-close-open-commit", test_test_txn_close_open_commit},
	{"test_update_calls_back", test_test_update_calls_back},
	{"test1753", test_test1753},
	{"test1572", test_test1572},
	{"test1842", test_test1842},
	{"test3039", test_test3039},
	{"test3219", test_test3219},
	{"test3522", test_test3522},
	{"test3522b", test_test3522b},
	{"test3529", test_test3529},
	{"test_3529_insert_2", test_test_3529_insert_2},
	{"test_3529_table_lock", test_test_3529_table_lock},
	{"env-close-flags", test_test_env_close_flags},
	{"env-open-flags", test_test_env_open_flags},
	{"test-update-data-diagonal", test_update_multiple_data_diagonal},
	{"env-create-db-create", test_test_env_create_db_create},
	{"test-error", test_test_error},
	{"test-forkjoin", test_test_forkjoin},
	{"test-get-zeroed-dbt", test_test_get_zeroed_dbt},		
	{"test-groupcommit-count", test_test_groupcommit_count},
	{"test-locking-with-read-txn",test_test_locking_with_read_txn},
	{"test-lock-timeout-callback", test_test_lock_timeout_callback},
	{"test-locktree-close",test_test_locktree_close},
	{"test_update_can_delete_elements", test_test_update_can_delete_elements },
	{"test_update_changes_values", test_test_update_changes_values },
	{"test_update_nested_updates", test_test_update_nested_updates },
	{"test_update_nonexistent_keys", test_test_update_nonexistent_keys },
	{"test_update_previously_deleted", test_test_update_previously_deleted },
	{"test-blobs-leaf-split", test_test_blobs_leaf_split},
	{"test-bulk-fetch",test_test_bulk_fetch},
	{"test-cmp-descriptor",test_test_cmp_descriptor},
	{"test-compression-methods", test_test_compression_methods},
	{"test-cursor-3", test_test_cursor_3},
	{"test-cursor-stickyness", test_test_cursor_stickyness},
	{"test-cursor-delete2", test_test_cursor_delete2},
	{"test-logflush", test_test_logflush},
	{"test_update_stress", test_test_update_stress},
	{"test_zero_length_keys",test_test_zero_length_keys},
	{"test_db_env_open_nocreate", test_test_db_env_open_nocreate},
	{"test_db_env_open_open_close", test_test_db_env_open_open_close},
	{"test_db_env_set_errpfx",test_test_db_env_set_errpfx},
	{"test_db_open_notexist_reopen",test_test_db_open_notexist_reopen},
	{"test_db_remove",test_test_db_remove},
	{"test_iterate_live_transactions", test_test_iterate_live_transactions},
	{"test_xopen_eclose", test_test_xopen_eclose},
	{"test-logmax", test_test_logmax},
	{"test-log1-abort", test_test_log1_abort},
	{"replace-into-write-lock", test_replace_into_write_lock},
	{"test-query",test_test_query},
	{"test-nested",test_test_nested},
	{"test-nodup-set",test_test_nodup_set},
	{"test_3645",test_test_3645},
	{"root-fifo-32", test_root_fifo_32},
	{"root-fifo-31", test_root_fifo_31},
	{"root-fifo-41", test_root_fifo_41},
	{"root-fifo-1", test_root_fifo_1},
	{"root-fifo-2", test_root_fifo_2},
	{"test-multiple-checkpoints-block-commit", test_test_multiple_checkpoints_block_commit},
	{"test-rand-insert",test_test_rand_insert},
	{"test-set-func-malloc",test_test_set_func_malloc},
	{"test-nested-xopen-eclose",test_test_nested_xopen_eclose},
	{"print_engine_status", test_print_engine_status},
	{"xid-lsn-independent", test_xid_lsn_independent},
	{"verify-misrouted-msgs",test_verify_misrouted_msgs},
	{"queries_with_deletes", test_queries_with_deletes},
	{"seqwrite_no_txn", test_seqwrite_no_txn},
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
				times-- ;
			}
		}
	}

	if (ran_a_test) {
		if(result == 0) {
			printk (KERN_DEBUG "Test \"%s\" successfully ran %d "
				"times\n", test_name, nsucc);
		} else {
			printk (KERN_DEBUG "Test \"%s\" failed\n", test_name);
		}
	} else {
		printk(KERN_ALERT "Test \"%s\" does not exist\n", test_name);
		result = -1;
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
	test.retval = 0;
	pthread_create(&tester, NULL, tester_func, &test);
	pthread_join(tester, NULL);

	printk(KERN_ALERT "Test completed (%s): %d\n", test_name, test.retval);

	return test.retval;
}
