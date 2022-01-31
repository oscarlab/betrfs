all: file-rename.c
	gcc -O3 -o file-rename file-rename.c -lcrypto -lm -lrt
	gcc -O3 -o multifile-rename multifile-rename.c -lcrypto -lm -lrt

clean:
	rm -f file-rename
	rm -f multifile-rename

