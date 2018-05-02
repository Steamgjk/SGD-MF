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
int main()
{
	srand(time(0));
	ofstream Pofs("./Pmtx.txt", ios::trunc);
	ofstream Qofs("./Qmtx.txt", ios::trunc);
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
	double sum = 0;
	long hash_id = 0;
	srand(time(0));
	int r = 0;
	ofstream Hofs("./TrainingMap.txt", ios::trunc);
	ofstream Tofs("./TestingMap.txt", ios::trunc);
	long cnt = 0;
	long train_cnt = 0;
	long test_cnt = 0;
	for (long i = 0; i < N; i++)
	{
		for (long j = 0; j < M; j++)
		{
			hash_id = i * M + j;
			sum = 0;
			for (int k = 0; k < K; k++)
			{
				sum += P[i][k] * Q[k][j];
			}
			r = rand() % 1000;
			if (r < 20)
			{
				Hofs << hash_id << " " << sum << endl;
				train_cnt++;
			}
			r = rand() % 10000000;
			if (r < 20)
			{
				Tofs << hash_id << " " << sum << endl;
				test_cnt++;
			}
			cnt++;
			if (cnt % 1000000 == 0)
			{
				printf("cnt = %ld train_cnt=%ld test_cnt=%ld\n", cnt, train_cnt, test_cnt);
			}
		}
	}



}