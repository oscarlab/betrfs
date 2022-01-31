#/bin/bash

cp ./benchmark/* /mnt/benchmark/ -rp
echo aging > /mnt/benchmark/grepx
for i in $(seq 10)
do
	dovecot stop || true
	../../../clear-fs-caches.sh
	dovecot
	sleep 1
	python mailserver-aging.py
	../../../clear-fs-caches.sh
	time grep -r aging /mnt/benchmark
done
