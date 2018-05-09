#!/bin/sh
rm ps worker rworker rworker_en
g++  -g   ps.cpp -o ps -pthread -std=c++11
g++  -g   worker.cpp -o worker -pthread -std=c++11
g++  -g   rworker.cpp -o rworker -pthread -std=c++11
g++  -g   rworker_en.cpp -o rworker_en -pthread -std=c++11
g++  -g   genSeq.cpp -o genSeq -pthread -std=c++11
g++  -g   PartitionMap.cpp -o PartitionMap -pthread -std=c++11
g++  -g   genRingSeq.cpp -o genRingSeq -pthread -std=c++11
g++  -g   genMatrix.cpp -o genMatrix -pthread -std=c++11
g++  -g   genNewMatrix.cpp -o genNewMatrix -pthread -std=c++11