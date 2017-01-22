#/bin/bash
#run as root

for i in $(seq 2)
do
	dir=/mnt/benchmark/ftfstest${i}/Maildir
	sudo mkdir -p ${dir}
	sudo chown -R ftfstest${i}:ftfstest${i} /mnt/benchmark/ftfstest${i}
done

sudo mkdir -p traces
sudo mkdir -p clean_temp
