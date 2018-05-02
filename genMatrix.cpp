#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <random>
using namespace std;
#define N 10000000
#define M 10000000
#define K 10
double P[N][K];
double Q[K][N];
#define TD_NUM 16
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

	long cnt = 0;
	long train_cnt = 0;
	long test_cnt = 0;

	long row_len = N / TD_NUM;
	long row_idx = td * row_len;

	for (long i = row_idx; i < row_idx + row_len; i++)
	{
		for (long j = 0; j < M; j++)
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

}