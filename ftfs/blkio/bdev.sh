#!/bin/bash

make DEBUG=1
sudo rmmod block_dev_test 
sudo rmmod block_dev
sudo insmod block_dev.ko
sudo insmod block_dev_test.ko

#num_tests=8
#for ((i=1; i<=$num_tests; ++i ))
#do
#    testcase="/proc/quotient_filter/"$i
#    cat $testcase
#done
