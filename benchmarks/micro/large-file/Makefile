all: random_write.c sequential_write.c random_read.c
	gcc random_read.c -o random_read -lrt
	gcc random_write.c -o random_write -lrt
	gcc random_write_4k.c -o random_write_4k -lrt
	gcc -g sequential_write.c -o sequential_write -lrt

clean:
	rm -f random_write sequential_write random_read random_write_4k
