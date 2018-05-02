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
#define N 10000000
#define M 10000000
#define K 10
double P[N][K];
double Q[K][N];
#define TD_NUM 64
#define DIM_SIZE 8
double pi = 3.141592653;
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
	long row_len = N / TD_NUM;
	long col_len = M / TD_NUM;
	long row_sta_idx = row_idx * row_len;
	long col_sta_idx = col_idx * col_len;

	long cnt = 0;
	long train_cnt = 0;
	long test_cnt = 0;
	for (long i = row_sta_idx; i < row_sta_idx + row_len; i++)
	{
		for (long j = col_sta_idx; j < col_sta_idx + col_len; j++)
		{
			r = rand() % 10000;
			if (r < 20)
			{
				hash_id = i * M + j;
				sum = 0;
				for (int k = 0; k < K; k++)
				{
					sum += P[i][k] * Q[k][j];
				}
				Hofs << hash_id << " " << sum << endl;
				train_cnt++;
				r = rand() % 10000;
				if (r < 20)
				{
					Tofs << hash_id << " " << sum << endl;
					test_cnt++;
				}
			}
			cnt++;
			if (cnt % 1000000 == 0)
			{
				printf("[%d]:cnt = %ld train_cnt=%ld test_cnt=%ld\n", td, cnt, train_cnt, test_cnt);
			}
		}
	}
	printf("[%d]  finished\n", td );

}
int main()
{
	double theta = 0.1;
	double miu = 0;
	for (int x = -4; x <= 5; x++)
	{
		double r = exp(0.0 - (x * x / theta / theta / 2) ) / theta / sqrt(2.0) / pi;
		printf("x=%d  r=%lf\n", x, r );
	}
	/*
	srand(time(0));
	char fn[100];
	sprintf(fn, "./data/Pmtx");
	ofstream Pofs(fn, ios::trunc);
	sprintf(fn, "./data/Qmtx");
	ofstream Qofs(fn, ios::trunc);
	std::default_random_engine e; //引擎
	std::normal_distribution<double> n(1, 0.1); //均值, 方差
	for (int i = 0; i < N; i++)
	{
		for (int j = 0; j < K; j++)
		{
			P[i][j] = n(e);
			Pofs << P[i][j] << " ";
		}
		Pofs << endl;
	}
	for (int i = 0; i < K; i++)
	{
		for (int j = 0; j < M; j++)
		{
			Q[i][j] = n(e);
			Qofs << Q[i][j] << " ";
		}
		Qofs << endl;
	}
	cout << "finish P and Q" << endl;


	for (int td = 0; td < TD_NUM; td++)
	{
		std::thread worker_thread(work_func, td);
		worker_thread.detach();
	}
	while (true)
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(1000));
		printf("sleep\n");
	}
	**/

}