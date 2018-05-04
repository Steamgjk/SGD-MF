#!/bin/sh
rm ps worker rworker rworker_en
g++  -g  -std=c++11  -pthread ps.cpp -o ps
g++  -g  -std=c++11  -pthread worker.cpp -o worker
g++  -g  -std=c++11  -pthread rworker.cpp -o rworker
g++  -g  -std=c++11  -pthread rworker_en.cpp -o rworker_en
g++  -g  -std=c++11  -pthread genSeq.cpp -o genSeq
g++  -g  -std=c++11  -pthread PartitionMap.cpp -o PartitionMap
g++  -g  -std=c++11  -pthread genRingSeq.cpp -o genRingSeq
g++  -g  -std=c++11  -pthread genMatrix.cpp -o genMatrix
g++  -g  -std=c++11  -pthread genNewMatrix.cpp -o genNewMatrix