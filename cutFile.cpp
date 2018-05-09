
#define FILE_NAME "./traina.txt"
#define TEST_NAME "./testa.txt"
#define OUT_TRIAIN "./mdata/traina-"
#define OUT_TEST "./mdata/testa-"
#define N 71567
#define M 65133
#define K  40 //主题个数
#define PORTION_NUM 8
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <cmath>
#include <time.h>
#include <vector>
#include <list>
#include <queue>
#include <map>
#include <thread>
#include <chrono>
#include <algorithm>
#include <mutex>
#include <atomic>
#include <iostream>
#include <fstream>
#include <sys/time.h>
#include <queue>
#include <random>
using namespace std;
//g++  -g   cutFile.cpp -o cutFile -pthread -std=c++11
int main()
{
	printf("ok\n");
	ifstream ifs_train(FILE_NAME);
	ifstream ifs_test(TEST_NAME);
	ofstream ofs_train[PORTION_NUM * PORTION_NUM];
	ofstream ofs_test[PORTION_NUM * PORTION_NUM];
	long hash_id;
	double rate;
	char filena[100];
	for (int i = 0; i < PORTION_NUM; i++)
	{
		for (int j = 0; j < PORTION_NUM; j++)
		{
			int file_no =  i * PORTION_NUM + j;
			sprintf(filena, "%s%d", OUT_TRIAIN, file_no);
			ofs_train[file_no].open(filena, ios::trunc);
			sprintf(filena, "%s%d", OUT_TEST, file_no);
			ofs_test[file_no].open(filena, ios::trunc);
		}
	}
	printf("ok\n");
	long row_idx, col_idx, srow, scol, file_idx;
	long row_unit = N / PORTION_NUM;
	long col_unit = M / PORTION_NUM;
	while (!ifs_train.eof())
	{

		ifs_train >> hash_id >> rate;
		row_idx = hash_id / M;
		col_idx = hash_id % M;

		srow = row_idx / row_unit;
		scol = col_idx / col_unit;
		file_idx = srow * PORTION_NUM + scol;
		//printf("file_idx=%ld\n", file_idx );
		ofs_train[file_idx] << hash_id << " " << rate;

	}
	printf("ok\n");
	while (!ifs_test.eof())
	{
		ifs_test >> hash_id >> rate;
		row_idx = hash_id / M;
		col_idx = hash_id % M;

		srow = row_idx / row_unit;
		scol = col_idx / col_unit;
		file_idx = srow * PORTION_NUM + scol;
		printf("fff %d %d %d %d%ld\n", file_idx, srow, scol, row_idx, col_idx );
		ofs_test[file_idx] << hash_id << " " << rate;

	}
	printf("kkk\n");

}