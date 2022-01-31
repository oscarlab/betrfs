cd ~/range-rename/ft-index/benchmarks/
sudo ./cleanup-fs.sh
sudo ./setup-ftfs.sh
cd -
#sudo blktrace -d /dev/sda6 -b 2048 &
sudo ./delivery_throughput.sh
#sudo pkill -15 blktrace

