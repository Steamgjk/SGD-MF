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
#define N 1000000
#define M 1000000
#define K 10
//double P[N][K];
//double Q[K][N];
long TrainHash[21000000];
long TestHash[2100000];
double TrainVal[21000000];
double TestVal[2100000];
long train_head = 0;
long train_tail = 0;
long test_head = 0;
long test_tail = 0;

#define TD_NUM 64
#define DIM_SIZE 8
double pi = 3.141592653;
//double val[100] = {0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000001, 0.000002, 0.000005, 0.000015, 0.000040, 0.000101, 0.000249, 0.000589, 0.001338, 0.002919, 0.006119, 0.012322, 0.023841, 0.044318, 0.079155, 0.135830, 0.223945, 0.354746, 0.539910, 0.789502, 1.109208, 1.497275, 1.941861, 2.419707, 2.896916, 3.332246, 3.682701, 3.910427, 3.989423, 3.910427, 3.682701, 3.332246, 2.896916, 2.419707, 1.941861, 1.497275, 1.109208, 0.789502, 0.539910, 0.354746, 0.223945, 0.135830, 0.079155, 0.044318, 0.023841, 0.012322, 0.006119, 0.002919, 0.001338, 0.000589, 0.000249, 0.000101, 0.000040, 0.000015, 0.000005, 0.000002, 0.000001, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000};
double val[100] =
{
	0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000,  0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000001, 0.000003, 0.000008, 0.000020, 0.000050, 0.000118, 0.000268, 0.000584, 0.001224, 0.002464, 0.004768, 0.008864, 0.015831, 0.027166, 0.044789, 0.070949, 0.107982, 0.157900, 0.221842, 0.299455, 0.388372, 0.483941, 0.579383, 0.666449, 0.736540, 0.782085, 0.797885, 0.782085, 0.736540, 0.666449, 0.579383, 0.483941, 0.388372, 0.299455, 0.221842, 0.157900, 0.107982, 0.070949, 0.044789, 0.027166, 0.015831, 0.008864, 0.004768, 0.002464, 0.001224, 0.000584, 0.000268, 0.000118, 0.000050, 0.000020, 0.000008, 0.000003, 0.000001, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000
};
double subM[100][100];
double subP[100][100];
double subQ[100][100];
void WriteTrain()
{
	int td;
	char fn[100];
	ofstream Hofs[64];
	for (int i = 0; i < DIM_SIZE; i++)
	{
		for (int j = 0; j < DIM_SIZE; j++)
		{
			td = i * DIM_SIZE + j;
			sprintf(fn, "./data/TrainingMap-%d", td);
			Hofs[td].open(fn, ios::trunc);
		}
	}
	long sz = (M / 8);
	sz = sz * N / 8;
	int file_idx = 0;


	while (1 == 1)
	{
		//printf("hhhh\n");
		if (train_head < train_tail)
		{
			file_idx = TrainHash[train_head] / sz;
			Hofs[file_idx] << TrainHash[train_head] << " " << TrainVal[train_head] << endl;
			train_head++;
			//printf("train_head=%ld file_idx=%d\n", train_head, file_idx );
			if (train_head % 10000 == 0)
			{
				printf("train_head = %ld\n", train_head );
			}
		}
		if (train_head >= 20000000)
		{
			break;
		}

	}
}
void WriteTest()
{
	int td;
	char fn[100];
	ofstream Tofs[64];
	for (int i = 0; i < DIM_SIZE; i++)
	{
		for (int j = 0; j < DIM_SIZE; j++)
		{
			td = i * DIM_SIZE + j;
			sprintf(fn, "./data/TestMap-%d", td);
			Tofs[td].open(fn, ios::trunc);
		}
	}
	long sz = (M / 8);
	sz = sz * N / 8;
	int file_idx = 0;
	while (1 == 1)
	{
		if (test_head < test_tail)
		{
			file_idx = TestHash[test_head] / sz;
			Tofs[file_idx] << TestHash[test_head] << " " << TestVal[test_head] << endl;
			test_head++;
			if (test_head % 1000 == 0)
			{
				//std::this_thread::sleep_for(std::chrono::milliseconds(1000));
				printf("test_head = %ld\n", test_head );
			}
		}
		if (test_head >= 2000000)
		{
			break;
		}

	}
}
//double val[10] = {0.000755, 0.025004, 0.304611, 1.365174, 2.250791, 1.365174, 0.304611, 0.025004, 0.000755, 0.000008};


void genMatrix()
{

	double sum = 0;
	long hash_id = 0;
	long cnt = 0;
	double r = 0;
	for (long i = 0; i < N; i++)
	{
		for (long j = 0; j < M; j++)
		{
			//20M
			r = rand() % 10000000;
			if (r < 200)
			{
				hash_id = i * M + j;
				sum = subM[i % 100][j % 100];


				//Hofs << hash_id << " " << sum << endl;
				TrainHash[train_tail] =  hash_id;
				TrainVal[train_tail] = sum;
				train_tail++;
				//train_cnt++;
				//2M
				//r = rand() % 1000;
				if (r < 20)
				{

					TestHash[test_tail] = hash_id;
					TestVal[test_tail] = sum;
					test_tail++;
				}
			}
			cnt++;
			if (cnt % 10000000 == 0)
			{
				printf("[%d]:cnt = %ld train_tail=%ld test_tail=%ld\n", 0, cnt, train_tail, test_tail);
				//getchar();
			}
		}
	}
}
int main()
{
	/*
		double theta = 5;
		double miu = 0;
		for (int x = -49; x <= 50; x++)
		{
			double r = 1.0 / (sqrt(2.0 * pi) * theta) * exp(-( (x - miu) * (x - miu) / (2 * theta * theta) ) ) * 10;
			//printf("x=%d  r=%lf\n", x, r );
			printf("%lf,", r);
		}
		**/

	for (int i = 0; i < 100; i++)
	{
		for (int j = 0; j < 100; j++)
		{
			subP[i][j] = val[(i + j) % 100];
			subQ[i][j] = val[(j + i) % 100];
		}
	}
	for (int i = 0; i < 100; i++)
	{
		for (int j = 0; j < 100; j++)
		{
			subM[i][j] = 0;
			for (int k = 0; k < 100; k++)
			{
				subM[i][j] += subP[i][k] * subQ[k][j];
			}
		}
	}
	train_head = train_tail = test_head = test_tail = 0;
	std::thread train_thread(WriteTrain);
	train_thread.detach();

	std::thread test_thread(WriteTest);
	test_thread.detach();

	//std::thread gen_thread(genMatrix);
	//gen_thread.detach();
	genMatrix();


	while (true)
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(1000));
		//printf("sleep\n");
	}



}