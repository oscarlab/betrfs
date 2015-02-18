#dd if=/dev/zero of=~/1gb_image.img bs=1024 count=0 seek=$[1000*1000*1]
sudo losetup /dev/loop1 ~/1gb_image.img
#sudo mkfs.ext4 /dev/loop1

