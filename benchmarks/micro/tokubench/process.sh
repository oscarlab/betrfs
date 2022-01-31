T="ext4-threaded-1"
echo $T

tail -n3 $T-*.results | grep time | awk '{print $3}'

tail -n3 $T-*.results | grep files | awk '{print $3}'

tail -n3 $T-*.results | grep thro | awk '{print $2}'

T="ext4-threaded-2"
echo $T

tail -n3 $T-*.results | grep time | awk '{print $3}'

tail -n3 $T-*.results | grep files | awk '{print $3}'

tail -n3 $T-*.results | grep thro | awk '{print $2}'

T="ext4-threaded-4"
echo $T

tail -n3 $T-*.results | grep time | awk '{print $3}'

tail -n3 $T-*.results | grep files | awk '{print $3}'

tail -n3 $T-*.results | grep thro | awk '{print $2}'


T="xfs-threaded-1"
echo $T

tail -n3 $T-*.results | grep time | awk '{print $3}'

tail -n3 $T-*.results | grep files | awk '{print $3}'

tail -n3 $T-*.results | grep thro | awk '{print $2}'

T="xfs-threaded-2"
echo $T

tail -n3 $T-*.results | grep time | awk '{print $3}'

tail -n3 $T-*.results | grep files | awk '{print $3}'

tail -n3 $T-*.results | grep thro | awk '{print $2}'

T="xfs-threaded-4"
echo $T

tail -n3 $T-*.results | grep time | awk '{print $3}'

tail -n3 $T-*.results | grep files | awk '{print $3}'

tail -n3 $T-*.results | grep thro | awk '{print $2}'

T="btrfs-threaded-1"
echo $T

tail -n3 $T-*.results | grep time | awk '{print $3}'

tail -n3 $T-*.results | grep files | awk '{print $3}'

tail -n3 $T-*.results | grep thro | awk '{print $2}'

T="btrfs-threaded-2"
echo $T

tail -n3 $T-*.results | grep time | awk '{print $3}'

tail -n3 $T-*.results | grep files | awk '{print $3}'

tail -n3 $T-*.results | grep thro | awk '{print $2}'

T="btrfs-threaded-4"
echo $T

tail -n3 $T-*.results | grep time | awk '{print $3}'

tail -n3 $T-*.results | grep files | awk '{print $3}'

tail -n3 $T-*.results | grep thro | awk '{print $2}'

T="zfs-threaded-1"
echo $T

tail -n3 $T-*.results | grep time | awk '{print $3}'

tail -n3 $T-*.results | grep files | awk '{print $3}'

tail -n3 $T-*.results | grep thro | awk '{print $2}'

T="zfs-threaded-2"
echo $T

tail -n3 $T-*.results | grep time | awk '{print $3}'

tail -n3 $T-*.results | grep files | awk '{print $3}'

tail -n3 $T-*.results | grep thro | awk '{print $2}'

T="zfs-threaded-4"
echo $T

tail -n3 $T-*.results | grep time | awk '{print $3}'

tail -n3 $T-*.results | grep files | awk '{print $3}'

tail -n3 $T-*.results | grep thro | awk '{print $2}'


T="betrfs-threaded-1"
echo $T

tail -n3 $T-*.results | grep time | awk '{print $3}'

tail -n3 $T-*.results | grep files | awk '{print $3}'

tail -n3 $T-*.results | grep thro | awk '{print $2}'

T="betrfs-threaded-2"
echo $T

tail -n3 $T-*.results | grep time | awk '{print $3}'

tail -n3 $T-*.results | grep files | awk '{print $3}'

tail -n3 $T-*.results | grep thro | awk '{print $2}'

T="betrfs-threaded-4"
echo $T

tail -n3 $T-*.results | grep time | awk '{print $3}'

tail -n3 $T-*.results | grep files | awk '{print $3}'

tail -n3 $T-*.results | grep thro | awk '{print $2}'


T="nilfs2-threaded-1"
echo $T

tail -n3 $T-*.results | grep time | awk '{print $3}'

tail -n3 $T-*.results | grep files | awk '{print $3}'

tail -n3 $T-*.results | grep thro | awk '{print $2}'

T="nilfs2-threaded-2"
echo $T

tail -n3 $T-*.results | grep time | awk '{print $3}'

tail -n3 $T-*.results | grep files | awk '{print $3}'

tail -n3 $T-*.results | grep thro | awk '{print $2}'

T="nilfs2-threaded-4"
echo $T

tail -n3 $T-*.results | grep time | awk '{print $3}'

tail -n3 $T-*.results | grep files | awk '{print $3}'

tail -n3 $T-*.results | grep thro | awk '{print $2}'



