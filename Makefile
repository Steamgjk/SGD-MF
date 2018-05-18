all: ps
CC=g++
LIBS=-libverbs -lrdmacm -pthread -libverbs -lrdmacm -std=c++11
CFLAGS=-O2 -Wall -g 

server_rdma_op.o: server_rdma_op.cpp
	$(CC) $(CFLAGS) -c server_rdma_op.cpp
client_rdma_op.o: client_rdma_op.cpp
	$(CC) $(CFLAGS) -c client_rdma_op.cpp 
rdma_common.o: rdma_common.c
	$(CC) $(CFLAGS) -c rdma_common.c
ps.o: ps.cpp
	$(CC) $(CFLAGS) ps.o server_rdma_op.o client_rdma_op.o rdma_common.o  -o ps $(LIBS)

clean:
	rm -rf *.o  *~
