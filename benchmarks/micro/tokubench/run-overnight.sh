cd /home/ftfs/ft-index/benchmarks/
sudo ./cleanup-fs.sh
sudo ./setup-ftfs.sh
cd -
./run-benchmark-fs-threaded.sh

cd /home/ftfs/ft-index/benchmarks/
sudo ./cleanup-fs.sh
sudo ./setup-xfs.sh
cd -
./run-benchmark-fs-threaded.sh

cd /home/ftfs/ft-index/benchmarks/
sudo ./cleanup-fs.sh
sudo ./setup-ext4.sh
cd -
./run-benchmark-fs-threaded.sh

cd /home/ftfs/ft-index/benchmarks/
sudo ./cleanup-fs.sh
sudo ./setup-btrfs.sh
cd -
./run-benchmark-fs-threaded.sh

cd /home/ftfs/ft-index/benchmarks/
sudo ./cleanup-fs.sh
sudo ./setup-zfs.sh
cd -
./run-benchmark-fs-threaded.sh
