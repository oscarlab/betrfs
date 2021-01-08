set(tokusrc_kernel_srcs
seq-io
big-nested-abort-abort
big-nested-abort-commit
big-nested-commit-abort
big-nested-commit-commit
bigtxn27
blackhole
blocking-first
blocking-first-empty
blocking-last
blocking-next-prev
blocking-next-prev-deadlock
blocking-prelock-range
blocking-put
blocking-put-timeout
blocking-put-wakeup
blocking-set
blocking-set-range-0
blocking-set-range-n
blocking-set-range-reverse-0
blocking-table-lock
bug1381
cachetable-race
checkpoint1
checkpoint_fairness
checkpoint_stress #(this should be done, but uses a shell script currently)  #phenominally loooong test
circle-log-overflow
create-datadir
cursor-isolation
cursor-more-than-a-leaf-provdel
cursor-set-del-rmw
cursor-set-range-rmw
cursor-step-over-delete
db-put-simple-deadlock
db-put-simple-deadlock-threads
db-put-simple-lockwait
db-put-update-deadlock
del-multiple
del-multiple-huge-primary-row
del-multiple-srcdb
del-simple
directory_lock
#diskfull (should be tested, check tests/run_diskfull_test.sh)
dump-env # example copies 5902 directory? we might not need this?
env-put-multiple #loooong test
env_loader_memory
filesize #loooong test
get_key_after_bytes_unit
#helgrind1 (not going to helgrind in kernel...)
#helgrind2 (not going to helgrind in kernel...)
#helgrind3 (not going to helgrind in kernel...)
#hotindexer-bw (not doing hotindexer)
#hotindexer-error-callback (not doing hotindexer)
#hotindexer-insert-committed (not doing hotindexer)
#hotindexer-insert-committed-optimized (not doing hotindexer)
#hotindexer-insert-provisional (not doing hotindexer)
#hotindexer-lock-test (not doing hotindexer)
#hotindexer-multiclient (not doing hotindexer)
#hotindexer-nested-insert-committed (not doing hotindexer)
#hotindexer-put-abort (not doing hotindexer)
#hotindexer-put-commit (not doing hotindexer)
#hotindexer-put-multiple (not doing hotindexer)
#hotindexer-simple-abort (not doing hotindexer)
#hotindexer-simple-abort-put (not doing hotindexer)
#hotindexer-undo-do-test (not doing hotindexer)
#hotindexer-with-queries (not doing hotindexer)
#hot-optimize-table-tests (not doing hotindexer)
inflate2
inflate
insert-dup-prelock
isolation
isolation-read-committed
keyrange #--get 0, --get 1, --get 0 --randome_keys 1
keyrange-merge
last-verify-time
manyfiles #loooong test
#maxsize-for-loader
medium-nested-commit-commit
multiprocess
mvcc-create-table
mvcc-many-committed
mvcc-read-committed
openlimit17
openlimit17-locktree
openlimit17-metafiles
perf_checkpoint_var #(we weant this test, but we must fix threaded_stress_test_helpers.h first)
perf_child_txn #(we weant this test, but we must fix threaded_stress_test_helpers.h first)
perf_cursor_nop #(we weant this test, but we must fix threaded_stress_test_helpers.h first)
perf_iibench #(we weant this test, but we must fix threaded_stress_test_helpers.h first)
perf_insert #(we weant this test, but we must fix threaded_stress_test_helpers.h first)
perf_malloc_free #(we weant this test, but we must fix threaded_stress_test_helpers.h first)
perf_nop #(we weant this test, but we must fix threaded_stress_test_helpers.h first)
perf_ptquery2 #(we weant this test, but we must fix threaded_stress_test_helpers.h first)
perf_ptquery #(we weant this test, but we must fix threaded_stress_test_helpers.h first)
perf_rangequery #(we weant this test, but we must fix threaded_stress_test_helpers.h first)
perf_read_txn #(we weant this test, but we must fix threaded_stress_test_helpers.h first)
perf_read_txn_single_thread #(we weant this test, but we must fix threaded_stress_test_helpers.h first)
perf_read_write #(we weant this test, but we must fix threaded_stress_test_helpers.h first)
perf_txn_single_thread #(we weant this test, but we must fix threaded_stress_test_helpers.h first)
#powerfail (we want to pass this test, but we must run it by hand, and we must define an appropriate N in the check phase.)
preload-db
preload-db-nested
prelock-read-read
prelock-read-write
prelock-write-read
prelock-write-write
print_engine_status
progress
put-del-multiple-array-indexing
queries_with_deletes
#{recover-*} are abortrecover tests. we need to figure out the abort tests first
recover-2483
#recover-3113
#recover-5146
#recover-checkpoint-fcreate-fdelete-fcreate
#recover-checkpoint-fopen-abort
#recover-checkpoint-fopen-commit
#recover-child-rollback
#recover-compare-db
#recover-compare-db-descriptor
#recover-delboth-after-checkpoint
#recover-delboth-checkpoint
#recover-del-multiple-abort
#recover-del-multiple
#recover-del-multiple-srcdb-fdelete-all
#recover-descriptor10
#recover-descriptor11
#recover-descriptor12
#recover-descriptor2
#recover-descriptor3
#recover-descriptor4
#recover-descriptor5
#recover-descriptor6
#recover-descriptor7
#recover-descriptor8
#recover-descriptor9
#recover-descriptor
#recover-fassociate
#recover-fclose-in-checkpoint
#recover-fcreate-basementnodesize
#recover-fcreate-fclose
#recover-fcreate-fdelete
#recover-fcreate-nodesize
#recover-fcreate-xabort
#recover-flt10
#recover-flt1
#recover-flt2
#recover-flt3
#recover-flt4
#recover-flt5
#recover-flt6
#recover-flt7
#recover-flt8
#recover-flt9
#recover-fopen-checkpoint-fclose
#recover-fopen-fclose-checkpoint
#recover-fopen-fdelete-checkpoint-fcreate
#recover-hotindexer-simple-abort-put
#recover-loader-test
#recover-lsn-filter
#recover-lsn-filter-multiple
#recover-missing-dbfile-2 (should fail. we need to handle failure first)
#recover-missing-dbfile (should fail. we need to handle failure first)
#recover-missing-logfile
#recover-put-multiple-abort
#recover-put-multiple
#recover-put-multiple-fdelete-all
#recover-put-multiple-fdelete-some
#recover-put-multiple-srcdb-fdelete-all
#recover-split-checkpoint
#recover-straddle-txn
#recover-straddle-txn-nested
#recover-tablelock
recover-test1
recover-test2
recover-test3
#recover-test-logsuppress
#recover-test-logsuppress-put
#recover-test_stress1
#recover-test_stress2
#recover-test_stress3
#recover-test_stress_openclose
#recover-update_aborts_before_checkpoint
#recover-update_aborts_before_close
#recover-update_aborts
#recover-update_broadcast_aborts2
#recover-update_broadcast_aborts3
#recover-update_broadcast_aborts_before_checkpoint
#recover-update_broadcast_aborts_before_close
#recover-update_broadcast_aborts
#recover-update_broadcast_changes_values2
#recover-update_broadcast_changes_values3
#recover-update_broadcast_changes_values_before_checkpoint
#recover-update_broadcast_changes_values_before_close
#recover-update_broadcast_changes_values
#recover-update_changes_values_before_checkpoint
#recover-update_changes_values_before_close
#recover-update_changes_values
#recover-update-multiple-abort
#recover-update-multiple
#recover-upgrade-db-descriptor
#recover-upgrade-db-descriptor-multihandle
#recover-x1-abort
#recover-x1-commit
#recover-x1-nested-abort
#recover-x1-nested-commit
#recover-x2-abort
#recover-x2-commit
#recovery_fileops_stress #loooong test
#recovery_fileops_unit (check the run_recovery_fileops_unit.sh shell script)
#recovery_stress
redirect
replace-into-write-lock
root_fifo_1 #(save for later) #loooong test
root_fifo_2 #(save for later) #loooong test
root_fifo_31 #(save for later) #loooong test
root_fifo_32 #(save for later) #loooong test
root_fifo_41 #(save for later)
rowsize  #loooong test in BDB, but I don't know about TDB
seqinsert
shutdown-3344 #loooong test
simple
stat64
stat64-create-modify-times #loooong test
stat64-null-txn
stat64-root-changes
stress-gc2
#(we want this test, but we must fix threaded_stress_test_helpers.h first)
stress-gc
#(we want this test, but we must fix threaded_stress_test_helpers.h first)
stress-test #(we want this test, but we must fix threaded_stress_test_helpers.h first)
#test1426 (custom test)
test1572 #loooong test
test1753
test1842
test3039 # -n 1000
test3219
test3522b
test3522
#test3529  #phenominally loooong test
test_3529_insert_2
test_3529_table_lock
test_3645
test_3755
test_4015 #(drd test)
test_4368
test4573-logtrim
test_4657
test_5015
test5092
test-5138 #(custom test)
test_5469
test_789
test_935
test938b
test938
test938
test_abort1 # we need to figure out how to safely abort
test_abort2 # we need to figure out how to safely abort
test_abort3 # we need to figure out how to safely abort
test_abort4 #for num in {-1 to 19} "-c 0 -1 $num" and "-c 1 -1 $num" #loooong test for 19 0 and 19_1
test_abort5 # we need to figure out how to safely abort. #loooong test
test_abort_delete_first # we need to figure out how to safely abort
test_blobs_leaf_split
test_bulk_fetch
test_cachesize
test_cmp_descriptor
test_compression_methods
test_cursor_2
test_cursor_3
test_cursor_db_current
test_cursor_DB_NEXT_no_dup
test_cursor_delete2
test_cursor_flags
test_cursor_nonleaf_expand
test_cursor_null
test_cursor_stickyness
test_cursor_with_read_txn
test_db_already_exists
test_db_change_pagesize
test_db_change_xxx
test_db_close_no_open
test_db_current_clobbers_db
test_db_dbt_mem_behavior
test_db_delete
test_db_descriptor
test_db_env_open_close
test_db_env_open_nocreate
test_db_env_open_open_close
test_db_env_set_errpfx
test_db_env_set_lg_dir
test_db_env_set_tmp_dir
test_db_env_strdup_null
test_db_get_put_flags
test_db_named_delete_last
#test_db_no_env (should fail. we need to handle failure first)
test_db_open_notexist_reopen
test_db_remove
test_db_remove_subdb
test_db_set_flags
test_db_subdb
test_db_subdb_different_flags
test_db_txn_locks_nonheaviside
test_db_txn_locks_read_uncommitted
test_db_version
test_env_close_flags
test_env_create_db_create
test_env_open_flags
test_error
test_forkjoin
test_get_max_row_size
test_get_zeroed_dbt
test_groupcommit_count #(-n 1) #loooong test
test_groupcommit_perf
test_hsoc
test_insert_cursor_delete_insert
test_insert_memleak
test_iterate_live_transactions
test_iterate_pending_lock_requests
test_kv_limits
test_large_update_broadcast_small_cachetable
test_locking_with_read_txn
test_lock_timeout_callback
test_locktree_close
test_log0
test_log1_abort
test_log1
test_log2_abort
test_log2 #(recover_srcs)
test_log3_abort
test_log3 #(recover_srcs)
test_log4_abort
test_log4
test_log5_abort
test_log5
test_log6a_abort
test_log6_abort
test_log6
test_log7 #(recover_srcs)
test_log8
test_log9
test_log10
test_logflush
test_logmax #loooong test
test_mostly_seq
test_multiple_checkpoints_block_commit
test_nested
test-nested-xopen-eclose
test_nodup_set
test-prepare2
test-prepare3
test-prepare
test_query  #loooong test
test_rand_insert
test_read_txn_invalid_ops
test_redirect_func
test_restrict
test_reverse_compare_fun
test-rollinclude
test_simple_read_txn
test_stress0
# needs to link with lock_tree. (--num_elements 512, --num_seconds 1000 --join_timeout 600 (we weant this test, but we must fix threaded_stress_test_helpers.h first)
test_stress1
#(--num_elements 150000 --num_seconds 1000 --join_timeout 600) (we weant this test, but we must fix threaded_stress_test_helpers.h first)
test_stress2
#(--num_elements 150000 --num_seconds 1000 --join_timeout 600) (we weant this test, but we must fix threaded_stress_test_helpers.h first)
test_stress3 #(--num_elements 150000 --num_seconds 1000 --join_timeout 600) (we weant this test, but we must fix threaded_stress_test_helpers.h first)
test_stress4 #(--num_elements 150000 --num_seconds 1000 --join_timeout 600) (we weant this test, but we must fix threaded_stress_test_helpers.h first)
#test_stress5 #(--num_elements 150000 --num_seconds 1000 --join_timeout 600) (we weant this test, but we must fix threaded_stress_test_helpers.h first)
test_stress6 #(--num_elements 150000 --num_seconds 1000 --join_timeout 600) (we weant this test, but we must fix threaded_stress_test_helpers.h first)
test_stress7 #(--num_elements 150000 --num_seconds 1000 --join_timeout 600) (we weant this test, but we must fix threaded_stress_test_helpers.h first)
test_stress_openclose #(we want this test, but we must fix threaded_stress_test_helpers.h first)
test_stress_with_verify #(we want this test, but we must fix threaded_stress_test_helpers.h first)
test_thread_flags
test_thread_insert
test_transactional_descriptor
test_trans_desc_during_chkpt2
test_trans_desc_during_chkpt3
test_trans_desc_during_chkpt4
test_trans_desc_during_chkpt
test_txn_abort5a  #loooong test
test_txn_abort5  #loooong test
test_txn_abort6
test_txn_abort7
test_txn_abort8
test_txn_abort9
test_txn_begin_commit
test_txn_close_before_commit
test_txn_close_before_prepare_commit
test_txn_close_open_commit
test_txn_commit8
test_txn_cursor_last
test_txn_nested1
test_txn_nested2  #loooong test
test_txn_nested3
test_txn_nested4  #loooong test
test_txn_nested5  #loooong test
test_txn_nested_abort2
test_txn_nested_abort3
test_txn_nested_abort4
test_txn_nested_abort
test_txn_recover3
test_unused_memory_crash
test_update_abort_works
test_update_broadcast_abort_works
test_update_broadcast_calls_back
test_update_broadcast_can_delete_elements
test_update_broadcast_changes_values
test_update_broadcast_indexer
test_update_broadcast_loader
test_update_broadcast_nested_updates
test_update_broadcast_previously_deleted
test_update_broadcast_stress  #loooong test
test_update_broadcast_update_fun_has_choices
test_update_broadcast_with_empty_table
test_update_calls_back
test_update_can_delete_elements
test_update_changes_values
test_update_nested_updates
test_update_nonexistent_keys
test_update_previously_deleted
test_updates_single_key
test_update_stress
test_update_txn_snapshot_works_concurrently
test_update_txn_snapshot_works_correctly_with_deletes
test_update_with_empty_table
test_weakxaction
test-xa-prepare
test-xopen-eclose
test_zero_length_keys
transactional_fileops
update
update-multiple-data-diagonal
update-multiple-key0
update-multiple-nochange
update-multiple-with-indexer-array
update-multiple-with-indexer
#upgrade_simple (transparent_upgrade_sources)
#upgrade-test-1 (transparent_upgrade_sources)
#upgrade-test-2 (transparent_upgrade_sources)
#upgrade-test-3 (transparent_upgrade_sources)
#upgrade-test-4 (transparent_upgrade_sources)
#upgrade-test-5 (transparent_upgrade_sources)
#upgrade-test-6 (transparent_upgrade_sources)
#upgrade-test-7 (transparent_upgrade_sources)
zombie_db
seqwrites-no-txn
)
add_library(tokusrc_kernel_static STATIC ${tokusrc_kernel_srcs})
maybe_add_gcov_to_libraries(tokusrc_static_conv)
set_target_properties(tokusrc_kernel_static PROPERTIES POSITION_INDEPENDENT_CODE ${STATIC_PIC})
add_dependencies(tokusrc_kernel_static install_tdb_h generate_log_code)
