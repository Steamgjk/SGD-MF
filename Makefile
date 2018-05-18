all: ps worker
CC=g++
TARGET = ps
TARGET1 = worker
LIBS=-libverbs -lrdmacm -pthread -libverbs -lrdmacm
CFLAGS=-O2 -Wall -g -fpermissive -std=c++11
OBJS=ps.o server_rdma_op.o client_rdma_op.o rdma_common.o

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS) $(LIBS)
$(TARGET1): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET1) $(OBJS) $(LIBS)
worker.o: worker.cpp
	$(CC) $(CFLAGS) -c worker.cpp
ps.o: ps.cpp
	$(CC) $(CFLAGS) -c ps.cpp
server_rdma_op.o: server_rdma_op.cpp
	$(CC) $(CFLAGS) -c server_rdma_op.cpp
client_rdma_op.o: client_rdma_op.cpp
	$(CC) $(CFLAGS) -c client_rdma_op.cpp 
rdma_common.o: rdma_common.cpp
	$(CC) $(CFLAGS) -c rdma_common.cpp

clean:
	rm -rf *.o  *~
