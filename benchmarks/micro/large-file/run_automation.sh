#!/bin/bash

. ../../fs-info.sh

gcc random_write.c -o random_write -lrt
gcc -g sequential_write.c -o sequential_write -lrt

# /bin/bash ./clean-mount.sh
/bin/bash ./run-large-file-write.sh -f ftfs $1
/bin/bash ./run-large-file-read.sh -f ftfs $1
/bin/bash ./run-large-file-random_write.sh ftfs $1




