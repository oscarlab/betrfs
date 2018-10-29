# Git-Benchmark
### Git benchmark for file system aging
---

### How it works:
The benchmarks ages a destination file system by performing git pulls from a source git repository. After a fixed number of pulls, the benchmark runs a provided script.

### How to measure aging in ext4:
1. Clone a repository to path_to_src_repo
2. Initialize a filesystem at path_to_dest
3. Run `python git_benchmark.py path_to_src_repo path_to_dest output.txt total_pulls pulls_per_test ./grep_ext4.sh path_to_dest dest_blkdev path_to_unaged unaged_blkdev` (must be root)
4. Read results from output.txt

If the file system is mounted at `/mnt/aged` on block device `/dev/sda1`, the unaged version will be created at `/mnt/unaged` on `/dev/sda2`, and we want to perform a grep test every 100 pulls for 10,000 total pulls from a local clone of the linux kernel repository, we run:

`python git_benchmark.py linux /mnt/aged output.txt 10000 100 ./grep.ext4.sh /mnt/aged /dev/sda1 /mnt/unaged /dev/sda2`

### How to run a different test:
The benchmark takes a program to execute together with arbitrary parameters, so any bash script or other executable can be provided and that will be run instead of the grep test.
