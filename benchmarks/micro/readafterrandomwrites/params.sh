##############################
#### benchmark parameters ####
##############################
f_size=$((10*1024*1024*1024))
mnt="/mnt/benchmark"
input="large.file"


####################################################
##### parameters for dd and sequential_write.c #####
####################################################

# generate randomized buffers of write_size, and each write system
#    call will write 1 whole buffer
io_size=40960  # read/write in io_size units
random_buffers=2000  # alternate among randomized buffers
