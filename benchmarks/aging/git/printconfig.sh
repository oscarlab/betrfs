#/bin/bash

# prints configuration to stdout

profile=aged
. "config.sh"

echo "test_name $test_name"
echo "total_pulls $total_pulls"
echo "pulls_per_grep $pulls_per_grep"
echo "gc_on $gc_on"
echo "keep_traces $keep_traces"
echo "clear_cache $clear_cache"
echo "grep_random $grep_random"
echo "aged $mntpnt $partition"

profile=clean
. "config.sh"
if [ ! -z "$mntpnt" ]; then
	echo "clean $mntpnt $partition"
fi

profile=cleaner
. "config.sh"
if [ ! -z "$mntpnt" ]; then
	echo "cleaner $mntpnt $partition"
fi
