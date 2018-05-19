all: ps worker
CC=g++
TARGET = ps
TARGET1 = worker
TARGET2 = rworker_en
LIBS=-libverbs -lrdmacm -pthread -libverbs -lrdmacm
CFLAGS=-Wall -g -fpermissive -std=c++11
OBJS=ps.o server_rdma_op.o client_rdma_op.o rdma_common.o
OBJS1=worker.o server_rdma_op.o client_rdma_op.o rdma_common.o
OBJS2=rworker_en.o server_rdma_op.o client_rdma_op.o rdma_common.o

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS) $(LIBS)
$(TARGET1): $(OBJS1)
	$(CC) $(CFLAGS) -o $(TARGET1) $(OBJS1) $(LIBS)
$(TARGET2): $(OBJS2)
	$(CC) $(CFLAGS) -o $(TARGET2) $(OBJS2) $(LIBS)
worker.o: worker.cpp
	$(CC) $(CFLAGS) -c worker.cpp
ps.o: ps.cpp
	$(CC) $(CFLAGS) -c ps.cpp
rworker_en.o: rworker_en.cpp
	$(CC) $(CFLAGS) -c rworker_en.cpp
server_rdma_op.o: server_rdma_op.cpp
	$(CC) $(CFLAGS) -c server_rdma_op.cpp
client_rdma_op.o: client_rdma_op.cpp
	$(CC) $(CFLAGS) -c client_rdma_op.cpp 
rdma_common.o: rdma_common.cpp
	$(CC) $(CFLAGS) -c rdma_common.cpp

clean:
	rm -rf *.o  *~
