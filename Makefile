all: ps worker rworker_en testMultiHop
CC=g++
TARGET = ps
TARGET1 = worker
TARGET2 = rworker_en
TARGET3 = testMultiHop
LIBS=-libverbs -lrdmacm -pthread -libverbs -lrdmacm
CFLAGS=-Wall -g -fpermissive -std=c++11
OBJS=ps.o server_rdma_op.o client_rdma_op.o rdma_common.o rdma_two_sided_client_op.o rdma_two_sided_server_op.o common.o
OBJS1=worker.o server_rdma_op.o client_rdma_op.o rdma_common.o rdma_two_sided_client_op.o rdma_two_sided_server_op.o common.o
OBJS2=rworker_en.o server_rdma_op.o client_rdma_op.o rdma_common.o rdma_two_sided_client_op.o rdma_two_sided_server_op.o common.o
OBJS3=testMultiHop.o server_rdma_op.o client_rdma_op.o rdma_common.o rdma_two_sided_client_op.o rdma_two_sided_server_op.o common.o

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS) $(LIBS)
$(TARGET1): $(OBJS1)
	$(CC) $(CFLAGS) -o $(TARGET1) $(OBJS1) $(LIBS)
$(TARGET2): $(OBJS2)
	$(CC) $(CFLAGS) -o $(TARGET2) $(OBJS2) $(LIBS)
$(TARGET3): $(OBJS3)
	$(CC) $(CFLAGS) -o $(TARGET3) $(OBJS3) $(LIBS)

testMultiHop.o: testMultiHop.cpp
	$(CC) $(CFLAGS) -c testMultiHop.cpp
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
rdma_two_sided_client_op.o: rdma_two_sided_client_op.cpp
	$(CC) $(CFLAGS) -c rdma_two_sided_client_op.cpp
rdma_two_sided_server_op.o: rdma_two_sided_client_op.cpp
	$(CC) $(CFLAGS) -c rdma_two_sided_server_op.cpp
common.o: common.cpp
	$(CC) $(CFLAGS) -c common.cpp
clean:
	rm -rf *.o  *~
