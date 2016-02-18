#! /bin/bash
# Shutdown the VM and then turn it on again

STATE=$(VBoxManage showvminfo "FTFSTest" --machinereadable | grep 'VMState=' | cut -d '"' -f2)

if [ "$STATE" != "poweroff" ] 
then
	VBoxManage controlvm FTFSTest poweroff 
	sleep 10
else
	echo "The virtual machine is powered off already... "
fi	

# to set up COM1
VBoxManage modifyvm FTFSTest --uart1 0x3F8 4   

NOW=`date +"%Y-%m-%d-%H-%M-%S"`

# link it to physical port ttyS0
VBoxManage modifyvm FTFSTest --uartmode1 file ftfs-${NOW}.out

# start the VM
VBoxManage startvm FTFSTest || exit #?
sleep 2m


GUEST_IP="130.245.153.150"
# Host name is slave
# See /etc/hosts

EXE=/home/ftfs/ft-index/ftfs/userspace-testing/run-tests.py
ARG=/home/ftfs/ft-index/ftfs/userspace-testing/simple.tests

ssh slave  "/home/ftfs/yzj_test.sh < /dev/null > /home/ftfs/logfile 2>&1 &"

