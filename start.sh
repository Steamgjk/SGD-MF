#!/bin/sh
g++  -g  -std=c++11  -pthread ps.cpp -o ps
g++  -g  -std=c++11  -pthread worker.cpp -o worker
g++  -g  -std=c++11  -pthread rworker.cpp -o rworker