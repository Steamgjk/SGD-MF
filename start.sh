#!/bin/sh
rm ps worker rworker rworker_en
g++ -c ps.cpp
g++ -c client_rdma_op.cpp
g++ -c server_rdma_op.cpp
g++ -g ps.o client_rdma_op.o server_rdma_op.o -o pc -pthread -libverbs -lrdmacm -std=c++11 
#g++  -g   ps.cpp -o ps -pthread -libverbs -lrdmacm -std=c++11 
g++  -g   worker.cpp -o worker -pthread -libverbs -lrdmacm -std=c++11
g++  -g   rworker.cpp -o rworker -pthread -libverbs -lrdmacm -std=c++11
g++  -g   rworker_en.cpp -o rworker_en -pthread -libverbs -lrdmacm -std=c++11
g++  -g   genSeq.cpp -o genSeq -pthread -libverbs -lrdmacm -std=c++11
g++  -g   PartitionMap.cpp -o PartitionMap -pthread -libverbs -lrdmacm -std=c++11
g++  -g   genRingSeq.cpp -o genRingSeq -pthread -libverbs -lrdmacm -std=c++11
g++  -g   genMatrix.cpp -o genMatrix -pthread -libverbs -lrdmacm -std=c++11
g++  -g   genNewMatrix.cpp -o genNewMatrix -pthread -libverbs -lrdmacm -std=c++11
g++  -g   CalcRMSE.cpp -o CalcRMSE -pthread -libverbs -lrdmacm -std=c++11
g++  -g   check.cpp -o check -pthread -libverbs -lrdmacm -std=c++11