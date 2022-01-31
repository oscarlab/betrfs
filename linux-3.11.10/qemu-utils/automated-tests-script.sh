#!/bin/bash

source testing-parameters.input

log_size () { echo $(stat -c '%s' ${CONSOLE_LOG}); }

## Shutdown the VM, rebuild the kernel, and then start the VM again
./control-kvm.sh kill

cp kvm-net-config ../.config; cd ..; make -j4; make modules -j4; cd -;

./control-kvm.sh start-vm

# Run the script on the VM
ssh -p ${SSH_PORT} ${SSH_IDENTITY} ${GUEST_USER}@${IP_ADDRESS} ./run-unit-tests.sh < /dev/null > ${STDOUT_LOG} 2>&1 &

# wait for the framework to make some progress
sleep 5m


# poll the console log, checking for test timeouts
CONTINUE_TESTS=1
COMPLETED="none"
while [ $CONTINUE_TESTS -eq 1 -a $(log_size) -lt $MAX_LOGSIZE ]; do

    CURRENT_DESC=$(grep "test_descriptor" ${CONSOLE_LOG}| tail -n 1)
    CURRENT_TEST=$(echo $CURRENT_DESC | sed -n 's:.*<name>\(.*\)</name>.*:\1:p')
    CURRENT_TIMEOUT=$(echo $CURRENT_DESC | sed -n 's:.*<timeout>\(.*\)</timeout>.*:\1:p')

    sleep "${CURRENT_TIMEOUT}m"

    CURRENT_BUG=$(sed -n "/<name>${CURRENT_TEST}<\/name>/,/<name>.*<\/name>/p" ${CONSOLE_LOG} | \
    grep "kernel BUG at")
    CURRENT_COMPLETED=$(grep ${CURRENT_TEST} ${CONSOLE_LOG} | \
	grep "test_completion")

    if [[ ! -z $CURRENT_BUG ]]; then
    # test encountered a kernel bug
        CONTINUE_TESTS=0
        echo "Test ${CURRENT_TEST} encountered a kernel bug." > ${SUMMARY_LOG}
    elif [[ -z $CURRENT_COMPLETED ]]; then
	# test did not complete before timing out...
	CONTINUE_TESTS=0
	echo "Test ${CURRENT_TEST} timed out." > ${SUMMARY_LOG}
    else
	# check if all tests have finished 
	# true if no new test has started since current test completed
        sleep 1m

	LAST_DESC=$(grep "test_descriptor" ${CONSOLE_LOG}| tail -n 1)
	LAST_TEST=$(echo $LAST_DESC | sed -n 's:.*<name>\(.*\)</name>.*:\1:p')

	COMPLETED_TEST=$(echo $CURRENT_COMPLETED | sed -n 's:.*<name>\(.*\)</name>.*:\1:p')
	if [ ${LAST_TEST} == ${COMPLETED_TEST} ]; then
	    CONTINUE_TESTS=0
	    echo "All tests ran to completion." > ${SUMMARY_LOG}
	fi
    fi

done


## the vm is no longer running, so parse the output
python parse-console.log.py ${CONSOLE_LOG} ${RESULTS_LOG}

BRANCH_NAME=$(git branch | sed -n -e 's/^\* \(.*\)/\1/p')
SUBJECT="ftfs error log, branch tested: $BRANCH_NAME"
python email-results.py --subject="$SUBJECT" --addresses=$(cat ../../unit_tests_email) --attachments="$SUMMARY_LOG,$RESULTS_LOG,$CONSOLE_LOG,$STDOUT_LOG"

./control-kvm.sh stop-vm
./control-kvm.sh kill-vm
