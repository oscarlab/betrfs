# my_num.log  my_random.py  rand_num.log  read_ssd.c  seq_num.log  write_ssd.c



cp rand_num.log my_num.log
gcc read_ssd.c -o read_ssd_rand
gcc write_ssd.c -o write_ssd_rand

cp seq_num.log  my_num.log
gcc read_ssd.c -o read_ssd_seq
gcc write_ssd.c -o write_ssd_seq




