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
long TestVal[2100000];
long train_head, train_tail, test_head, test_tail;

#define TD_NUM 64
#define DIM_SIZE 8
double pi = 3.141592653;
double val[100] = {0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000001, 0.000002, 0.000005, 0.000015, 0.000040, 0.000101, 0.000249, 0.000589, 0.001338, 0.002919, 0.006119, 0.012322, 0.023841, 0.044318, 0.079155, 0.135830, 0.223945, 0.354746, 0.539910, 0.789502, 1.109208, 1.497275, 1.941861, 2.419707, 2.896916, 3.332246, 3.682701, 3.910427, 3.989423, 3.910427, 3.682701, 3.332246, 2.896916, 2.419707, 1.941861, 1.497275, 1.109208, 0.789502, 0.539910, 0.354746, 0.223945, 0.135830, 0.079155, 0.044318, 0.023841, 0.012322, 0.006119, 0.002919, 0.001338, 0.000589, 0.000249, 0.000101, 0.000040, 0.000015, 0.000005, 0.000002, 0.000001, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000};
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
			Hofs[i].open(fn, ios::trunc);
		}
	}
	long sz = (M / 8);
	sz = sz * N / 8;
	int file_idx = 0;
	while (train_head < train_tail)
	{
		file_idx = TrainHash[train_head] / sz;
		Hofs[file_idx] << TrainHash[train_head] << " " << TrainVal[train_head] << endl;
		train_head++;
		if (train_head % 10000 == 0)
		{
			printf("train_head = %ld\n", train_head );
		}
	}
}
void WriteTest()
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
			Hofs[i].open(fn, ios::trunc);
		}
	}
	long sz = (M / 8);
	sz = sz * N / 8;
	int file_idx = 0;
	while (test_head < test_tail)
	{
		file_idx = TestHash[test_head] / sz;
		Hofs[file_idx] << TestHash[test_head] << " " << TestVal[test_head] << endl;
		test_head++;
		if (test_head % 1000 == 0)
		{
			printf("test_head = %ld\n", test_head );
		}
	}
}
void work_func(int td)
{
	printf("Thread td %d\n", td );
	double sum = 0;
	long hash_id = 0;
	srand(time(0));
	int r = 0;
	char fn[100];
	sprintf(fn, "./data/TrainingMap-%d", td);
	ofstream Hofs(fn, ios::trunc);
	sprintf(fn, "./data/TestingMap-%d", td);
	ofstream Tofs(fn, ios::trunc);
	int row_idx = td / DIM_SIZE;
	int col_idx = td % DIM_SIZE;
	long row_len = N / DIM_SIZE;
	long col_len = M / DIM_SIZE;
	long row_sta_idx = row_idx * row_len;
	long col_sta_idx = col_idx * col_len;

	long cnt = 0;
	long train_cnt = 0;
	long test_cnt = 0;
	for (long i = row_sta_idx; i < row_sta_idx + row_len; i++)
	{
		for (long j = col_sta_idx; j < col_sta_idx + col_len; j++)
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
					//Tofs << hash_id << " " << sum << endl;
					//test_cnt++;
					TestHash[test_tail] = hash_id;
					TestVal[test_tail] = sum;
					test_tail++;
				}
			}
			cnt++;
			if (cnt % 1000000 == 0)
			{
				printf("[%d]:cnt = %ld train_cnt=%ld test_cnt=%ld\n", td, cnt, train_tail, test_tail);
			}
		}
	}
	printf("[%d]  finished\n", td );

}
//double val[10] = {0.000755, 0.025004, 0.304611, 1.365174, 2.250791, 1.365174, 0.304611, 0.025004, 0.000755, 0.000008};



int main()
{
	/*
		double theta = 5;
		double miu = 0;
		for (int x = -49; x <= 50; x++)
		{
			double r = 1.0 / (sqrt(2.0 * pi) * theta) * exp(-( (x - miu) * (x - miu) / (2 * theta * theta) ) ) * 50;
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

	double sum = 0;
	long hash_id = 0;
	long cnt = 0;
	long train_cnt = 0;
	long test_cnt = 0;
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
				train_cnt++;
				//2M
				//r = rand() % 1000;
				if (r < 20)
				{
					//Tofs << hash_id << " " << sum << endl;
					test_cnt++;
				}
			}
			cnt++;
			if (cnt % 1000000 == 0)
			{
				printf("[%d]:cnt = %ld train_cnt=%ld test_cnt=%ld\n", 0, cnt, train_cnt, test_cnt);
			}
		}
	}

	/*
		srand(time(0));
		char fn[100];
		sprintf(fn, "./data/Pmtx");
		ofstream Pofs(fn, ios::trunc);
		sprintf(fn, "./data/Qmtx");
		ofstream Qofs(fn, ios::trunc);

		long unit_len = N / K;
		for (int i = 0; i < N; i++)
		{
			long offset = i / unit_len;
			for (int j = 0; j < K; j++)
			{
				P[i][j] = val[(j + offset) % K];
				//Pofs << P[i][j] << " ";
			}
			Pofs << endl;
		}
		for (int i = 0; i < K; i++)
		{
			for (int j = 0; j < M; j++)
			{
				long offset = j / unit_len;
				Q[i][j] = val[(i + offset) % K];
				//Qofs << Q[i][j] << " ";
			}
			Qofs << endl;
		}
		cout << "finish P and Q" << endl;
	**/
	/*
		for (int td = 0; td < TD_NUM; td++)
		{
			std::thread worker_thread(work_func, td);
			worker_thread.detach();
		}
		**/
	while (true)
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(1000));
		//printf("sleep\n");
	}


}