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
			r = rand() % 1000000;
			if (r < 20)
			{
				hash_id = i * M + j;
				sum = 0;
				for (int k = 0; k < K; k++)
				{
					sum += P[i][k] * Q[k][j];
				}
				//Hofs << hash_id << " " << sum << endl;
				train_cnt++;
				//2M
				r = rand() % 1000;
				if (r < 10)
				{
					//Tofs << hash_id << " " << sum << endl;
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
double val[10] = {0.000755, 0.025004, 0.304611, 1.365174, 2.250791, 1.365174, 0.304611, 0.025004, 0.000755, 0.000008};
int main()
{

	double theta = 1;
	double miu = 0;
	for (int x = -49; x <= 50; x++)
	{
		double r = exp(0.0 - (x * x * 1.0 / 2) ) / (sqrt(2.0) * pi) * 10;
		//printf("x=%d  r=%lf\n", x, r );
		printf("%lf,", r);
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


		for (int td = 0; td < TD_NUM; td++)
		{
			std::thread worker_thread(work_func, td);
			worker_thread.detach();
		}
		while (true)
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(1000));
			//printf("sleep\n");
		}

	**/
}