all: random_write.c sequential_write.c
	gcc random_write.c -o random_write -lrt
	gcc -g sequential_write.c -o sequential_write -lrt

clean:
	rm -f random_write sequential_write

