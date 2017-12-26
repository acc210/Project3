
all: first mergesort.o server client

first: ; make clean

mergesort.o: mergesort.c
	gcc mergesort.c -c

server: sorter_server.c
	gcc sorter_server.c -pthread mergesort.o -o serv

client: sorter_client.c
	gcc sorter_client.c -pthread -o cli
	
clean:
	rm -rf serv
	rm -rf mergesort.o
	rm -rf cli
	rm -rf t.csv
	rm -rf 2-sorted-.csv
	rm -rf 0-sorted-.csv
	rm -rf 1-sorted-.csv
	rm -rf AllFiles-sorted-.csv
	rm -rf f-sorted-.csv