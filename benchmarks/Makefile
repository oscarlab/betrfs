# Verbosity options
ifndef V
	QT=@printf "%-70s"
	QR=@echo ""
	QC=@
	QO=> /dev/null
	QE=2> /dev/null
else
	QT=@printf "%-70s"
	QR=@echo "" 
	QC=
	QO=
	QE=
endif

# Enable forced runs
ifdef F
	FORCED=.PHONY
endif

# Usage: $(PCMD) some_cmd
# will print the command and its output according to the 
# user's verbosity preferences.
PCMD=$(QC) $(QO) $(QE)

.PHONY:

STDDEPS: Makefile $(FORCED)

.SECONDEXPANSION:

# Some tips:
# - Benchmarks should begin and end with
#     $(QT) BMTYPE thing_being_benchmarked
#     ... the benchmark ...
#     $(QR) DONE
#   This will print a friendly benchmark summary in non-verbose mode.
# - Use $(PCMD) to support quiet/verbose mode.
# - Try to make failing benchmarks not fulfill their target, so
#   that they get re-run on subsequent makes.  You can do this
#   by building the output file to some temporary file, e.g. "failed",
#   and moving it to the target after the benchmark has completed 
#   successfully.



# Other information we want to record with the benchmark
COMMIT="uncommitted"
ifeq "$(shell git diff --exit-code > /dev/null 2>&1; echo $$?)" "0"
	COMMIT=$(shell git rev-parse HEAD)
endif

HOST=$(shell hostname)

# directory layout is raw-data/host/commit/benchmark/fs/run

find_fs=$(shell  echo $(1) | sed "s,\(.*\)/.*,\1,g")
find_run=$(shell echo $(1) | sed "s,.*/\(.*\),\1,g")

# find/prepfs/fs
find/prepfs/%: $(STDDEPS)
	$(QT) "PREPARING $* file system for find benchmark"
	$(PCMD) ./setup-$*.sh
	$(PCMD) ./micro/prepare-support-file.sh
	$(QR) DONE

raw-data/find/%: $(STDDEPS) find/prepfs/$$(call find_fs,$$*)
	$(QT) "BENCHMARK fs=$(call find_fs,$*) run=$(call find_run,$*)"
	$(PCMD) mkdir -p $@
	$(PCMD) rmdir $@
	$(PCMD) ./micro/recursive-scan-find/recur-find.sh -o $@
	$(QR) DONE


# bm_filter=$(shell echo $(1) | sed "s,\(.*\)/.*/.*/.*/.*,\1,g")
# bm_fprate=$(shell echo $(1) | sed "s,.*/\(.*\)/.*/.*/.*,\1,g")
# bm_nitems=$(shell echo $(1) | sed "s,.*/.*/\(.*\)/.*/.*,\1,g")
# bm_data=$(shell   echo $(1) | sed "s,.*/.*/.*/\(.*\)/.*,\1,g")
# bm_round=$(shell  echo $(1) | sed "s,.*/.*/.*/.*/\(.*\),\1,g")

# # directory layout is raw-data/host/commit/filter/fprate/nitems/data/run
# # In each directory, there are 3 files: insert.txt, lookup-existing.txt, and lookup-random.txt
# raw-data/$(HOST)/$(COMMIT)/%: bm $(STDDEPS)
# 	$(QT)   "BENCHMARK host=$(HOST) commit=$(COMMIT) prog=$(call  bm_filter,$*) fprate=$(call bm_fprate,$*) nitems=$(call bm_nitems,$*) data=$(call bm_data,$*) round=$(call bm_round,$*)"
# 	$(PCMD) rm -f "$@.tmp"
# 	$(PCMD) mkdir -p "$@.tmp"
# 	$(PCMD) ../$(call bm_filter,$*)/bm -f $(call bm_fprate,$*) -n $(call bm_nitems,$*) -r $(call bm_data,$*) -d "$@.tmp"
# 	$(PCMD) mv "$@.tmp" "$@"
# 	$(QR)   "DONE"



