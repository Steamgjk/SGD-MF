
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
using namespace std;


#define WORKER_TD 32
#define ACTION_NAME "./action"
#define STATE_NAME "./state"


/*
#define FILE_NAME "./data/TrainingMap-"
#define TEST_NAME "./data/TestMap-"
#define N 1000000
#define M 1000000
#define K  100 //主题个数
**/

#define FILE_NAME "./mdata/traina-"
#define TEST_NAME "./mdata/testa-"
#define N 71567
#define M 65133
#define K  40 //主题个数
#define CAP 30
#define SEQ_LEN 2000
#define QU_LEN 10000
#define WORKER_THREAD_NUM 30

int GROUP_NUM = 2;
int DIM_NUM = 8;
int WORKER_NUM = 4;
int CACHE_NUM = 20;


struct Block
{
	int block_id;
	int data_age;
	int sta_idx;
	int height; //height
	int ele_num;
	bool isP;
	vector<double> eles;
	Block()
	{

	}
	Block operator=(Block& bitem)
	{
		block_id = bitem.block_id;
		data_age = bitem.data_age;
		height = bitem.height;
		eles = bitem.eles;
		ele_num = bitem.ele_num;
		sta_idx = bitem.sta_idx;
		return *this;
	}
	void printBlock()
	{

		printf("block_id  %d\n", block_id);
		printf("data_age  %d\n", data_age);
		printf("ele_num  %d\n", ele_num);
		for (int i = 0; i < eles.size(); i++)
		{
			printf("%lf\t", eles[i]);
		}
		printf("\n");

	}
};

struct Block Pblocks[CAP];
struct Block Qblocks[CAP];

void partitionP(int portion_num,  Block * Pblocks)
{
	int i = 0;
	int height = N / portion_num;
	int last_height = N - (portion_num - 1) * height;

	for (i = 0; i < portion_num; i++)
	{
		Pblocks[i].block_id = i;
		Pblocks[i].data_age = 0;
		Pblocks[i].eles.clear();
		Pblocks[i].height = height;
		int sta_idx = i * height;
		if ( i == portion_num - 1)
		{
			Pblocks[i].height = last_height;
		}
		Pblocks[i].sta_idx = sta_idx;
		printf("i-%d sta_idx-%d\n", i, sta_idx );
		Pblocks[i].ele_num = Pblocks[i].height * K;
		Pblocks[i].eles.resize(Pblocks[i].ele_num);
	}

}

void partitionQ(int portion_num,  Block * Qblocks)
{
	int i = 0;
	int height = M / portion_num;
	int last_height = M - (portion_num - 1) * height;

	for (i = 0; i < portion_num; i++)
	{
		Qblocks[i].block_id = i;
		Qblocks[i].data_age = 0;
		Qblocks[i].eles.clear();
		Qblocks[i].height = height;
		int sta_idx = i * height;
		if ( i == portion_num - 1)
		{
			Qblocks[i].height = last_height;
		}
		Qblocks[i].sta_idx = sta_idx;
		Qblocks[i].ele_num = Qblocks[i].height * K;
		Qblocks[i].eles.resize(Qblocks[i].ele_num);

	}

}

int main()
{
	char fn[100];
	partitionQ(8, Pblocks);
	partitionQ(8, Qblocks);
	for (int data_idx = 0; data_idx < 64; data_idx++)
	{
		int row = data_idx / DIM_NUM;
		int col = data_idx % DIM_NUM;

		sprintf(fn, "%s%d", FILE_NAME, data_idx);
		printf("fn=%s  :[%d][%d]\n", fn, row, col );
		ifstream ifs(fn);
		if (!ifs.is_open())
		{
			printf("fail to open %s\n", fn );
			exit(-1);
		}
		long i, j;
		long hash_id;
		double rate;
		while (!ifs.eof())
		{
			ifs >> hash_id >> rate;
			i = ((hash_id) / M) % WORKER_THREAD_NUM;
			j = ((hash_id) % M) % WORKER_THREAD_NUM;
			if (i < 0 || j < 0 || i >= Pblocks[row].height || j >= Qblocks[col].height)
			{
				printf("[%d] continue l [%d][%d] pq [%ld][%ld]  %ld\n", data_idx, row, col, i, j, hash_id);

				//continue;
			}
		}
	}
}