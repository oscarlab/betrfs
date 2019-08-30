#!/bin/bash

source testing-parameters.input

rotate_file() {
    TO_ROTATE=$1
    [ -f ${TO_ROTATE} ] && mv ${TO_ROTATE} ${TO_ROTATE}.last
}

## virtual machine
start_vm_silently() {

        echo "rotating log files"
	rotate_file ${CONSOLE_LOG}
	rotate_file ${STDOUT_LOG}
	rotate_file ${RESULTS_LOG}
	[ -f ${KVM_PID_FILE} ] && rm ${KVM_PID_FILE}

        echo "starting virtual machine"
	kvm -smp ${GUEST_CPUS} -cpu host -m ${GUEST_MEM} \
	    -kernel ../arch/x86/boot/bzImage \
	    -drive file=${ROOT_VIRTUAL_DISK},if=virtio \
	    -drive file=${DEVICE_VIRTUAL_DISK},if=virtio \
	    -append "serial=tty1 console=ttyS0 root=/dev/vda rw --no-log" \
	    -display none -s -serial file:${CONSOLE_LOG} \
	    -enable-kvm \
	    -net nic,model=virtio,macaddr=52:54:00:12:34:56 \
	    -net user,hostfwd=tcp:${IP_ADDRESS}:${SSH_PORT}-:22 \
            -pidfile ${KVM_PID_FILE} &


        # check if the pid file created successfully
        if [ ! -f ${KVM_PID_FILE} ]
        then
                sleep 5
        fi
        if [ ! -f ${KVM_PID_FILE} ]
        then
                return 1
        fi
        # check if the process started successfully
        if [ ! -d /proc/`cat ${KVM_PID_FILE}` ]
        then
                return 1
        fi
}

start_vm() {
        start_vm_silently

        # if start_vm_silently return -1
        if test $? -eq -1
        then
                echo "startup failed. check ${CONSOLE_LOG}"
                exit 1
        else
                echo "startup successfully"
        fi
}

send_cmd() {
        COMMAND=$1
	ssh -p ${SSH_PORT} ${SSH_IDENTITY} ${GUEST_USER}@${IP_ADDRESS} ${COMMAND}
}
get_vm_pid_to() {
        ACTION_TO_DO=$1
        # check if pid file there
        if [ ! -f ${KVM_PID_FILE} ]
        then
                echo "${KVM_PID_FILE} not found, can not ${ACTION_TO_DO}"
                exit 1
        fi
        VM_PID=`cat ${KVM_PID_FILE}`
}
check_vm_status() {
        get_vm_pid_to "check vm status"
        if [ -d /proc/${VM_PID} ]
        then
                echo "vm is running at process id ${VM_PID}"
        else
                echo "vm is not running"
        fi
}
stop_vm() {
        echo "stop virtual machine"

        get_vm_pid_to "stop vm"

        # if the process is still running
        # send command quit to its monitor, and wait
        if [ -d /proc/${VM_PID} ]
        then
                send_cmd "shutdown -h now"
        fi
        # check if the process is still running
        if [ -d /proc/${VM_PID} ]
        then
                sleep 5
        fi
        if [ ! -d /proc/${VM_PID} ]
        then
                # yes, done
                rm ${KVM_PID_FILE}
                echo "vm stopped successfully"
        else
                # no, something wrong there...
                echo "failed to stop vm"
                exit 1
        fi
}
kill_vm() {
        echo "kill virtual machine"

        get_vm_pid_to "kill vm"
        # if the process is still running, kill it
        if [ -d /proc/${VM_PID} ]
        then
                kill ${VM_PID}
        fi
        rm ${KVM_PID_FILE}
        echo "vm killed"
}

### Main switch
case "$1" in
start-vm)
        start_vm
        ;;
status)
        check_vm_status
        ;;
ssh)
        ssh -p ${SSH_PORT} ${SSH_IDENTITY} ${GUEST_USER}@${IP_ADDRESS}
        ;;
stop-vm)
        stop_vm
        ;;
kill)
        kill_vm
        ;;
*)
        echo "You need to specify a action, available actions are:"
        echo "[start-vm] start virtual machine itself"
        echo "[status] check the status of the virtual machine"
        echo "[ssh] ssh to the guest"
        echo "[stop-vm] power off vritual machine"
        echo "[kill] kill the viritual machine"
        exit 1
        ;;
esac
exit
